package test

import (
	"github.com/stratumn/kayak"

	"github.com/stratumn/zmey"
)

// PongReplier implements Process interface and runs server algorithm.
type PongReplier struct {
	pid        int
	serverPids []int

	sendF   func(to int, payload interface{})
	returnF func(payload interface{})
	traceF  func(payload interface{})
	errorF  func(error)
}

// NewPongReplier creates and initializes an instance of a Server
func NewPongReplier(pid int, serverPids []int) *PongReplier {
	return &PongReplier{
		pid:        pid,
		serverPids: serverPids,
	}
}

func (p *PongReplier) Init(
	sendF func(to int, payload interface{}),
	returnF func(payload interface{}),
	traceF func(payload interface{}),
	errorF func(error),
) {
	p.sendF = sendF
	p.returnF = returnF
	p.traceF = traceF
	p.errorF = errorF

	t := zmey.NewTracer("[P]        init")
	p.traceF(t.Logf("called"))
}

// ReceiveNet implements Process.ReceiveNet
func (p *PongReplier) ReceiveNet(from int, payload interface{}) {
	t := zmey.NewTracer("[P] from [%4d]", from)

	switch msg := payload.(type) {
	case kayak.KRequest:
		t = t.Fork("%#v", msg)
		p.traceF(t.Logf("received"))
		response := kayak.KResponse{Nonce: msg.Nonce}
		p.sendF(from, response)

	case kayak.KBonjour:
		t = t.Fork("%#v", msg)
		p.traceF(t.Logf("received"))
		response := kayak.KTip{}
		p.sendF(from, response)

	default:
		p.errorF(t.Errorf("cannot coerse to the KRequest type: %+v", payload))
		return
	}
}

// ReceiveCall implements Process.ReceiveCall
func (p *PongReplier) ReceiveCall(call interface{}) {
	t := zmey.NewTracer("[P]        call")
	p.errorF(t.Errorf("PongReplier is not supposed to receive client calls"))
}

// Tick implements Process.Tick
func (p *PongReplier) Tick(tick uint) {
	t := zmey.NewTracer("[P] tick <%4d>", tick)
	p.traceF(t.Logf("received"))
}
