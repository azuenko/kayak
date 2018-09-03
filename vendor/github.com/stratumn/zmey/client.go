package zmey

import (
	"log"
)

// Client lets the injector to communicate with the process.
type Client interface {
	// Call sends a payload to the process.
	Call(payload interface{})
}

type client struct {
	pid   int
	callC chan interface{}
	debug bool
}

func (c client) Call(payload interface{}) {
	if c.debug {
		log.Printf("[%4d] Call: received %+v", c.pid, payload)
	}
	c.callC <- payload
	if c.debug {
		log.Printf("[%4d] Call: done", c.pid)
	}
}
