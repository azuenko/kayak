package kayak

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
)

type Client struct {
	sync.Mutex

	key        KAddress
	serverKeys []KAddress
	n, f, q    uint

	time        KTime
	timeout     KTime
	bonjourT    KTime
	nextBonjour KTime

	lastKnownIndex           KIndex
	hasFreshIndex            bool
	isBonjourSentAtLeastOnce bool

	extSendF   func(to KAddress, payload interface{})
	extReturnF func(payload interface{})
	extTraceF  func(payload interface{})
	extErrorF  func(error)

	ticketsToSend map[KNonce]KTicket
	sentTickets   map[KNonce]KTicket

	responseCounters map[KHash]map[KAddress]struct{}
	responses        map[KHash]KResponse
	tipCounters      map[KIndex]map[KAddress]struct{}

	responsesToReturn []KResponse
	ticketsTimeout    []KTicket

	byzantineFlags int
}

func NewClient(c *KClientConfig) *Client {

	serverKeys := make([]KAddress, len(c.ServerKeys))
	copy(serverKeys[:], c.ServerKeys[:])

	ticketsToSend := make(map[KNonce]KTicket)
	sentTickets := make(map[KNonce]KTicket)

	responseCounters := make(map[KHash]map[KAddress]struct{})
	responses := make(map[KHash]KResponse)
	tipCounters := make(map[KIndex]map[KAddress]struct{})

	client := Client{
		key:              c.Key,
		serverKeys:       serverKeys,
		timeout:          KTime(c.CallT),
		bonjourT:         KTime(c.BonjourT),
		ticketsToSend:    ticketsToSend,
		sentTickets:      sentTickets,
		responseCounters: responseCounters,
		responses:        responses,
		tipCounters:      tipCounters,
		extSendF:         c.SendF,
		extReturnF:       c.ReturnF,
		extTraceF:        c.TraceF,
		extErrorF:        c.ErrorF,

		byzantineFlags: c.ByzantineFlags,
	}

	client.updateFactors()

	return &client
}

func (c *Client) Start() {
	c.Lock()
	defer c.Unlock()

	c.traceF("[C] <----------: START")
	t := NewTracer("[C]            ")

	c.proceed(t)
}

func (c *Client) ReceiveCall(payload interface{}) {
	c.Lock()
	defer c.Unlock()

	c.traceF(fmt.Sprintf("[C] <----------: CALL %#v", payload))
	t := NewTracer("[C]            ")

	switch msg := payload.(type) {
	case KCall:
		c.receiveCall(t, msg)
	default:
		c.errorF(t.Errorf("unknown message: %#v", payload))
	}

	c.proceed(t)

}

func (c *Client) ReceiveNet(from KAddress, payload interface{}) {
	c.Lock()
	defer c.Unlock()

	c.traceF(fmt.Sprintf("[C] <--< %#v: RECEIVE %#v", from, payload))

	t := NewTracer("[C]            ")

	switch msg := payload.(type) {
	case KResponse:
		c.receiveResponse(t, from, msg)
	case KTip:
		c.receiveTip(t, from, msg)
	default:
		c.errorF(t.Errorf("unknown message: %#v", payload))
	}

	c.proceed(t)

}
func (c *Client) Tick(_tick uint) {
	c.Lock()
	defer c.Unlock()

	tick := KTime(_tick)

	c.traceF(fmt.Sprintf("[C] <----------: TICK %#v", tick))

	t := NewTracer("[C]            ")

	c.time += tick
	c.traceF(t.Logf("increased time from %#v to %#v", c.time-tick, c.time))

	c.traceF(t.Logf("before timeout: %d sent tickets wait for responses", len(c.sentTickets)))
	for nonce := range c.sentTickets {
		if c.sentTickets[nonce].Timestamp+c.timeout <= c.time {
			c.traceF(t.Logf("ticket timeout %#v", c.sentTickets[nonce]))
			c.ticketsTimeout = append(c.ticketsTimeout, c.sentTickets[nonce])
		}
	}

	for i := range c.ticketsTimeout {
		delete(c.sentTickets, c.ticketsTimeout[i].Nonce)
	}

	c.traceF(t.Logf("after timeout: %d sent tickets wait for responses", len(c.sentTickets)))

	c.traceF(t.Logf("before timeout: %d tickets to send wait for responses", len(c.ticketsToSend)))
	for nonce := range c.ticketsToSend {
		if c.ticketsToSend[nonce].Timestamp+c.timeout <= c.time {
			c.traceF(t.Logf("ticket timeout %#v", c.ticketsToSend[nonce]))
			c.ticketsTimeout = append(c.ticketsTimeout, c.ticketsToSend[nonce])
		}
	}

	for i := range c.ticketsTimeout {
		delete(c.ticketsToSend, c.ticketsTimeout[i].Nonce)
	}

	c.traceF(t.Logf("after timeout: %d tickets to send wait for responses", len(c.ticketsToSend)))

	c.proceed(t)
}

func (c *Client) ReconfigureTo(keys []KAddress) {
	c.Lock()
	defer c.Unlock()

	c.traceF("[C] <----------: RECONFIGURE")
	t := NewTracer("[C]            ")
	keys = append([]KAddress(nil), keys...)

	c.traceF(t.Logf("old keys %#v", c.serverKeys))
	c.traceF(t.Logf("new keys %#v", keys))

	keysMap := make(map[KAddress]struct{})
	removedKeysMap := make(map[KAddress]struct{})

	for _, key := range keys {
		keysMap[key] = struct{}{}
	}

	for _, key := range c.serverKeys {
		if _, found := keysMap[key]; !found {
			removedKeysMap[key] = struct{}{}
		}
	}
	c.traceF(t.Logf("%d removed keys", len(removedKeysMap)))

	responseCounters := make(map[KHash]map[KAddress]struct{})
	for hash := range c.responseCounters {
		if _, found := responseCounters[hash]; !found {
			responseCounters[hash] = make(map[KAddress]struct{})
		}
		for address := range c.responseCounters[hash] {
			if _, found := removedKeysMap[address]; !found {
				responseCounters[hash][address] = c.responseCounters[hash][address]
			}
		}
	}
	c.responseCounters = responseCounters

	tipCounters := make(map[KIndex]map[KAddress]struct{})
	for index := range c.tipCounters {
		if _, found := tipCounters[index]; !found {
			tipCounters[index] = make(map[KAddress]struct{})
		}
		for address := range c.tipCounters[index] {
			if _, found := removedKeysMap[address]; !found {
				tipCounters[index][address] = c.tipCounters[index][address]
			}
		}
	}
	c.tipCounters = tipCounters

	c.serverKeys = keys
	c.updateFactors()

	c.traceF(t.Logf("--------------------------------------------------------------------"))
}

func (c *Client) proceed(t Tracer) {
	const maxIterations = 1000
	var i int
	for {
		progressMade := c.tryProceed(t)
		if !progressMade || i == maxIterations {
			break
		}
		i++
	}
	if i == maxIterations {
		c.errorF(t.Errorf("proceed: infinite loop detected"))
	}
	c.traceF(t.Logf("--------------------------------------------------------------------"))

}

func (c *Client) tryProceed(t Tracer) bool {

	var progressMade bool

	progressMade = progressMade || c.maybeBonjour(t)
	progressMade = progressMade || c.maybeSendTickets(t)
	progressMade = progressMade || c.maybeReturnResponses(t)
	progressMade = progressMade || c.maybeReturnTimeouts(t)

	return progressMade
}

func (c *Client) receiveCall(t Tracer, call KCall) {
	t = t.Fork("receiveCall")

	nonce := c.getNonce()

	// ====== Byzantine behavior if enabled ======
	if c.byzantineFlags&ByzantineFlagClientFixNonce != 0 {
		c.traceF(t.Logf("ByzantineFlagClientFixNonce"))
		nonce = KNonce{}
		nonce[0] = 0xFA
		nonce[1] = 0xCE
	}
	// ======== End of Byzantine behavior ========

	ticket := KTicket{
		Tag:       call.Tag,
		Timestamp: c.time,
		Nonce:     nonce,
		Payload:   call.Payload,
	}

	c.traceF(t.Logf("made %#v", ticket))

	c.ticketsToSend[nonce] = ticket

}

func (c *Client) receiveResponse(t Tracer, from KAddress, response KResponse) {
	t = t.Fork("receiveResponse")

	responseHash := hash(response)

	if _, ok := c.responseCounters[responseHash]; !ok {
		c.responseCounters[responseHash] = make(map[KAddress]struct{})
	}

	c.responseCounters[responseHash][from] = struct{}{}

	if _, ok := c.responses[responseHash]; !ok {
		c.traceF(t.Logf("first response with its hash received"))
		c.responses[responseHash] = response
	} else {
		c.traceF(t.Logf("response with its hash has been already received"))
	}

	if uint(len(c.responseCounters[responseHash])) >= c.q {
		c.traceF(t.Logf("response quorum (%d/%d) reached", uint(len(c.responseCounters[responseHash])), c.q))
		c.responsesToReturn = append(c.responsesToReturn, response)
		delete(c.responseCounters, responseHash)
		delete(c.responses, responseHash)
	} else {
		c.traceF(t.Logf("response quorum (%d/%d) not reached", uint(len(c.responseCounters[responseHash])), c.q))
	}

}

func (c *Client) receiveTip(t Tracer, from KAddress, tip KTip) {
	t = t.Fork("receiveTip")

	if tip.Round < c.lastKnownIndex {
		c.traceF(t.Logf("tip index is less than known %#v, return", c.lastKnownIndex))
		return
	}

	if _, ok := c.tipCounters[tip.Round]; !ok {
		c.tipCounters[tip.Round] = make(map[KAddress]struct{})
	}

	c.tipCounters[tip.Round][from] = struct{}{}

	if uint(len(c.tipCounters[tip.Round])) >= c.q {
		c.traceF(t.Logf("tip quorum (%d/%d) reached", uint(len(c.tipCounters[tip.Round])), c.q))
		c.traceF(t.Logf("update last known index from %#v to %#v", c.lastKnownIndex, tip.Round))
		c.lastKnownIndex = tip.Round
		c.hasFreshIndex = true

		// TODO: cleanup old indexes
	} else {
		c.traceF(t.Logf("tip quorum (%d/%d) not reached", uint(len(c.tipCounters[tip.Round])), c.q))
	}

}

func (c *Client) maybeBonjour(t Tracer) bool {
	t = t.Fork("maybeBonjour")

	if c.time < c.nextBonjour {
		c.traceF(t.Logf("not yet: now %#v, next at %#v", c.time, c.nextBonjour))
		return false
	}

	c.traceF(t.Logf("gogo"))

	bonjour := KBonjour{}
	for _, key := range c.serverKeys {
		c.sendF(key, bonjour)
	}

	c.nextBonjour = c.time + c.bonjourT
	if c.isBonjourSentAtLeastOnce {
		c.traceF(t.Logf("making index dirty"))
		c.hasFreshIndex = false
	} else {
		c.traceF(t.Logf("first time Bonjour - leaving index as fresh"))
		c.hasFreshIndex = true
		c.isBonjourSentAtLeastOnce = true
	}

	return true

}

func (c *Client) maybeSendTickets(t Tracer) bool {
	t = t.Fork("maybeSendTickets")

	if len(c.ticketsToSend) == 0 {
		c.traceF(t.Logf("no tickets to send"))
		return false
	}

	if !c.hasFreshIndex {
		c.traceF(t.Logf("index is not fresh -- sync needed"))
		return false
	}

	c.traceF(t.Logf("gogo"))

	for nonce := range c.ticketsToSend {
		request := KRequest{
			Payload: c.ticketsToSend[nonce].Payload,
			Nonce:   c.ticketsToSend[nonce].Nonce,
			Index:   c.lastKnownIndex,
		}

		for _, key := range c.serverKeys {
			c.sendF(key, request)
		}

		c.sentTickets[nonce] = c.ticketsToSend[nonce]

	}

	c.ticketsToSend = make(map[KNonce]KTicket)

	return true
}

func (c *Client) maybeReturnResponses(t Tracer) bool {
	t = t.Fork("maybeReturnResponses")

	if len(c.responsesToReturn) == 0 {
		c.traceF(t.Logf("no responses to return"))
		return false
	}

	c.traceF(t.Logf("gogo, returning %d responses", len(c.responsesToReturn)))

	for i := range c.responsesToReturn {
		// TODO: check errors
		ticket, ticketFound := c.sentTickets[c.responsesToReturn[i].Nonce]
		if !ticketFound {
			c.errorF(t.Errorf("received response for non-existing request %#v", c.responsesToReturn[i].Nonce))
			continue
		}
		r := KReturn{
			Tag:   ticket.Tag,
			Index: c.responsesToReturn[i].Index,
		}
		c.returnF(r)

		c.traceF(t.Logf("remove ticket as completed %#v", c.sentTickets[c.responsesToReturn[i].Nonce]))
		delete(c.sentTickets, c.responsesToReturn[i].Nonce)
	}

	c.responsesToReturn = nil

	return true

}

func (c *Client) maybeReturnTimeouts(t Tracer) bool {
	t = t.Fork("maybeReturnTimeouts")

	if len(c.ticketsTimeout) == 0 {
		c.traceF(t.Logf("no tickets - no timeouts to return"))
		return false
	}

	c.traceF(t.Logf("gogo, returning %d timeouts", len(c.ticketsTimeout)))

	for i := range c.ticketsTimeout {
		r := KReturn{
			Tag:     c.ticketsTimeout[i].Tag,
			Timeout: true,
		}
		c.returnF(r)
	}

	c.ticketsTimeout = nil

	return true

}

func (c *Client) getNonce() KNonce {
	buf := make([]byte, NonceSize)
	var nonce KNonce

	_, err := rand.Read(buf)
	if err != nil {
		c.errorF(err)
		return KNonce{}
	}

	copy(nonce[:], buf)

	return nonce
}

func (c *Client) updateFactors() {
	c.f = uint(len(c.serverKeys)-1) / 3
	c.n = uint(len(c.serverKeys))
	c.q = (c.n+c.f)/2 + 1

	// Special case for 2-process system
	// TODO: test 2-process configuration for consensus stability
	if c.n == 2 {
		c.q = 1
	}
}

func (c *Client) sendF(to KAddress, payload interface{}) {
	c.traceF(fmt.Sprintf("[C] >--> %#v: SEND %#v", to, payload))
	if c.extSendF != nil {
		c.extSendF(to, payload)
	} else {
		c.errorF(errors.New("send function not defined"))
	}
}

func (c *Client) returnF(payload interface{}) {
	c.traceF(fmt.Sprintf("[C] +++++++++++: RETURN %#v", payload))
	if c.extReturnF != nil {
		c.extReturnF(payload)
	} else {
		c.errorF(errors.New("return function not defined"))
	}
}

func (c *Client) traceF(payload interface{}) {
	if c.extTraceF != nil {
		c.extTraceF(payload)
	} else {
		log.Print(payload)
	}
}

func (c *Client) errorF(err error) {
	c.traceF(fmt.Sprintf("[C] %%%%%%%%%%%%%%%%%%%%%%: ERROR %s", err.Error()))
	if c.extErrorF != nil {
		c.extErrorF(err)
	} else {
		c.traceF(err)
	}
}
