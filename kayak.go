package kayak

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

type Kayak struct {
	sync.Mutex

	key   KAddress
	keys  []KAddress
	rkeys map[KAddress]int

	n, f, q uint
	round   KRound
	epoch   KEpoch

	time                 KTime
	timeout              KTime
	whatsupT             KTime
	callT                KTime
	bonjourT             KTime
	nextWhatsup          KTime
	earliestJobTimestamp KTime

	storage KStorage

	logData     []KData
	logDataHash []KHash
	logBuzz     []KHash
	logBuzzHash []KHash
	setBuzz     map[KHash]struct{}

	proposes map[KRound]map[KEpoch]map[KAddress]KPropose
	writes   map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{}
	accepts  map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{}
	suspects map[KEpoch]map[KAddress]KSuspect
	heads    map[KRound]map[KEpoch]map[KAddress]struct{}
	syncSent map[KRound]bool
	syncData map[KRound]map[KHash][]KData
	syncBuzz map[KRound]map[KHash][]KHash
	confirms map[KRound]map[KHash]map[KHash]map[KAddress]struct{}

	jobs        map[KHash]*KJob
	currentJob  KJob
	currentBuzz KHash

	extSendF   func(to KAddress, payload interface{})
	extReturnF func(payload interface{})
	extTraceF  func(payload interface{})
	extErrorF  func(error)

	localClient *Client

	consensusState KConsensusState
	lcState        KLCState

	mostRecentRoundKnown  KRound
	mostRecentEpochKnown  KEpoch
	mostRecentRoundToSync KRound
	mostRecentHashToSync  KHash
	mostRecentBuzzToSync  KHash

	indexTolerance KRound

	byzantineFlags int
}

// NewKayak creates and returns an instance of Kayak process
func NewKayak(c *KServerConfig) *Kayak {

	keys := make([]KAddress, len(c.Keys))
	copy(keys[:], c.Keys[:])

	rkeys := make(map[KAddress]int)
	for i, key := range keys {
		rkeys[key] = i
	}

	jobs := make(map[KHash]*KJob)
	proposes := make(map[KRound]map[KEpoch]map[KAddress]KPropose)
	writes := make(map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{})
	accepts := make(map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{})
	suspects := make(map[KEpoch]map[KAddress]KSuspect)
	heads := make(map[KRound]map[KEpoch]map[KAddress]struct{})
	syncSent := make(map[KRound]bool)
	syncData := make(map[KRound]map[KHash][]KData)
	syncBuzz := make(map[KRound]map[KHash][]KHash)
	confirms := make(map[KRound]map[KHash]map[KHash]map[KAddress]struct{})

	setBuzz := make(map[KHash]struct{})

	localClient := NewClient(&KClientConfig{
		Key:        c.Key,
		ServerKeys: keys,
		CallT:      c.CallT,
		BonjourT:   c.BonjourT,
		SendF:      c.SendF,
		ReturnF:    c.ReturnF,
		TraceF:     c.TraceF,
		ErrorF:     c.ErrorF,
	})

	k := Kayak{
		key:            c.Key,
		keys:           keys,
		rkeys:          rkeys,
		timeout:        KTime(c.RequestT),
		whatsupT:       KTime(c.WhatsupT),
		callT:          KTime(c.CallT),
		bonjourT:       KTime(c.BonjourT),
		storage:        c.Storage,
		localClient:    localClient,
		jobs:           jobs,
		proposes:       proposes,
		writes:         writes,
		accepts:        accepts,
		suspects:       suspects,
		heads:          heads,
		syncSent:       syncSent,
		syncData:       syncData,
		syncBuzz:       syncBuzz,
		confirms:       confirms,
		setBuzz:        setBuzz,
		logDataHash:    []KHash{KHash{}},
		logBuzzHash:    []KHash{KHash{}},
		indexTolerance: KRound(c.IndexTolerance),
		extSendF:       c.SendF,
		extReturnF:     c.ReturnF,
		extTraceF:      c.TraceF,
		extErrorF:      c.ErrorF,

		byzantineFlags: c.ByzantineFlags,
	}

	k.updateFactors()

	return &k
}

func (k *Kayak) Start() {
	k.Lock()
	defer k.Unlock()

	k.traceF("<--------------: START")
	t := NewTracer("               ")
	k.proceed(t)
}

// ReceiveCall implements Process.ReceiveCall
func (k *Kayak) ReceiveCall(call interface{}) {
	k.Lock()
	defer k.Unlock()

	k.traceF(fmt.Sprintf("<--------------: CALL %#v", call))
	k.localClient.ReceiveCall(call)
}

// ReceiveNet implements Process.ReceiveNet
func (k *Kayak) ReceiveNet(from KAddress, payload interface{}) {
	k.Lock()
	defer k.Unlock()

	k.traceF(fmt.Sprintf("<------< %#v: RECEIVE %#v", from, payload))

	t := NewTracer("               ")

	switch msg := payload.(type) {
	case KRequest:
		k.receiveRequest(t, from, msg)
	case KPropose:
		k.receivePropose(t, from, msg)
	case KWrite:
		k.receiveWrite(t, from, msg)
	case KAccept:
		k.receiveAccept(t, from, msg)
	case KSuspect:
		k.receiveSuspect(t, from, msg)
	case KWhatsup:
		k.receiveWhatsup(t, from)
	case KBonjour:
		k.receiveBonjour(t, from)
	case KHead:
		k.receiveHead(t, from, msg)
	case KNeed:
		k.receiveNeed(t, from, msg)
	case KEnsure:
		k.receiveEnsure(t, from, msg)
	case KChunk:
		k.receiveChunk(t, from, msg)
	case KConfirm:
		k.receiveConfirm(t, from, msg)
	case KResponse, KTip:
		k.localClient.ReceiveNet(from, payload)
	default:
		k.errorF(t.Errorf("unknown message %#v", payload))
	}

	k.proceed(t)
}

func (k *Kayak) Tick(_tick uint) {
	k.Lock()
	defer k.Unlock()

	tick := KTime(_tick)

	k.traceF(fmt.Sprintf("<--------------: TICK %#v", tick))
	t := NewTracer("               ")

	k.time += tick
	k.traceF(t.Logf("increased time from %#v to %#v", k.time-tick, k.time))

	k.proceed(t)

	k.localClient.Tick(_tick)
}

func (k *Kayak) Status() *KStatus {
	k.Lock()
	defer k.Unlock()

	return &KStatus{
		Round:  k.round,
		Epoch:  k.epoch,
		Leader: k.leader(),
		Keys:   k.keys,
	}
}

func (k *Kayak) proceed(t Tracer) {
	const maxIterations = 1000
	var i int
	for {
		t2 := t.Fork("(%4d:%-4d)", k.round, k.epoch)
		progressMade := k.tryProceed(t2)
		if !progressMade || i == maxIterations {
			break
		}
		i++
	}
	if i == maxIterations {
		k.errorF(t.Errorf("proceed: infinite loop detected"))
	}
	k.traceF(t.Logf("--------------------------------------------------------------------"))
}

func (k *Kayak) tryProceed(t Tracer) bool {

	k.traceF(t.Logf("consensus state : %#v", k.consensusState))
	k.traceF(t.Logf("leader change state : %#v", k.lcState))

	var progressMade bool

	progressMade = progressMade || k.maybeWhatsup(t)
	progressMade = progressMade || k.maybePropose(t)
	progressMade = progressMade || k.maybeWrite(t)
	progressMade = progressMade || k.maybeAccept(t)
	progressMade = progressMade || k.maybeDecide(t)
	progressMade = progressMade || k.maybeSuspect(t)
	progressMade = progressMade || k.maybeLeaderChange(t)
	progressMade = progressMade || k.maybeSync(t)
	progressMade = progressMade || k.maybeUpdate(t)

	return progressMade
}

func (k *Kayak) getSomeoneElseKey() KAddress {
	return k.keys[(k.rkeys[k.key]+1)%len(k.keys)]
}

func (k *Kayak) leader() KAddress {
	return k.keys[uint(k.epoch)%k.n]
}

func (k *Kayak) updateEarliestJobTimestamp() {
	firstIteration := true
	for buzz := range k.jobs {
		if firstIteration {
			k.earliestJobTimestamp = k.jobs[buzz].Timestamp
			firstIteration = false
			continue
		}
		if k.jobs[buzz].Timestamp < k.earliestJobTimestamp {
			k.earliestJobTimestamp = k.jobs[buzz].Timestamp
		}
	}
}

func (k *Kayak) updateFactors() {
	k.f = uint(len(k.keys)-1) / 3
	k.n = uint(len(k.keys))
	k.q = (k.n+k.f)/2 + 1

	// Special case for 2-process system
	// TODO: test 2-process configuration for consensus stability
	if k.n == 2 {
		k.q = 1
	}

}

func (k *Kayak) sendF(to KAddress, payload interface{}) {
	k.traceF(fmt.Sprintf(">------> %#v: SEND %#v", to, payload))
	if k.extSendF != nil {
		k.extSendF(to, payload)
	} else {
		k.errorF(errors.New("send function not defined"))
	}
}

func (k *Kayak) returnF(payload interface{}) {
	k.traceF(fmt.Sprintf("+++++++++++++++: RETURN %#v", payload))
	if k.extReturnF != nil {
		k.extReturnF(payload)
	} else {
		k.errorF(errors.New("return function not defined"))
	}
}

func (k *Kayak) traceF(payload interface{}) {
	if k.extTraceF != nil {
		k.extTraceF(payload)
	} else {
		log.Print(payload)
	}
}

func (k *Kayak) errorF(err error) {
	k.traceF(fmt.Sprintf("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%: ERROR %s", err.Error()))
	if k.extErrorF != nil {
		k.extErrorF(err)
	} else {
		k.traceF(err)
	}
}
