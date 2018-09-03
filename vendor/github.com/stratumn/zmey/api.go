package zmey

import (
	"log"
)

// API lets the process to communicate the data to the framework.
type API interface {
	// Send should be called whenever a process needs to send a message to
	// another process.
	Send(to int, payload interface{})
	// Return should be called whenever a process needs to return a call to its client.
	Return(payload interface{})
	// Trace used for logging
	Trace(payload interface{})
	// ReportError should be used for any errors to be escalated to the upper layer
	// There is no specific error handling implemented, currently the errors are
	// just printed via log package
	ReportError(error)
}

// api implements exported API interface
type api struct {
	// scale   int
	pid     int
	net     *Net
	returnC chan interface{}
	traceC  chan interface{}
	debug   bool
}

func (a *api) BindNet(net *Net) {
	a.net = net
}

func (a *api) Send(to int, payload interface{}) {
	if a.debug {
		log.Printf("[%4d] Send: sending message %+v", a.pid, payload)
	}
	if a.net != nil {
		err := a.net.Send(a.pid, to, payload)
		if err != nil {
			log.Printf("[%4d] Send: Error: %s", a.pid, err)
		}
	} else {
		log.Printf("[%4d] Send: Error: network is nil", a.pid)
	}
	if a.debug {
		log.Printf("[%4d] Send: done", a.pid)
	}
}

func (a *api) Return(c interface{}) {
	if a.debug {
		log.Printf("[%4d] Return: returning call %+v", a.pid, c)
	}
	a.returnC <- c
	if a.debug {
		log.Printf("[%4d] Return: done", a.pid)
	}
}

func (a *api) Trace(t interface{}) {
	if a.debug {
		log.Printf("[%4d] T: %+v", a.pid, t)
	}
	a.traceC <- t
}

func (a *api) ReportError(err error) {
	log.Printf("[%4d] ReportError: %s", a.pid, err)
}
