package test

import (
	"github.com/stratumn/kayak"
)

type KayakWrapper struct {
	k      *kayak.Kayak
	config *kayak.KServerConfig

	sendZmeyF func(to int, payload interface{})
}

func NewKayakWrapper(config *kayak.KServerConfig) *KayakWrapper {
	return &KayakWrapper{
		config: config,
	}
}

func (w *KayakWrapper) Init(
	sendF func(to int, payload interface{}),
	returnF func(payload interface{}),
	traceF func(payload interface{}),
	errorF func(error),
) {
	w.sendZmeyF = sendF
	w.config.SendF = w.sendF
	w.config.ReturnF = returnF
	w.config.TraceF = traceF
	w.config.ErrorF = errorF

	w.k = kayak.NewKayak(w.config)
	w.k.Start()
}
func (w *KayakWrapper) ReceiveCall(payload interface{}) {
	w.k.ReceiveCall(payload)
}
func (w *KayakWrapper) ReceiveNet(fromZmey int, payload interface{}) {
	fromKayak, found := zmeyToKayak[fromZmey]
	if !found {
		return
	}
	w.k.ReceiveNet(fromKayak, payload)
}
func (w *KayakWrapper) Tick(t uint) {
	w.k.Tick(t)
}
func (w *KayakWrapper) sendF(toKayak kayak.KAddress, payload interface{}) {
	toZmey, found := kayakToZmey[toKayak]
	if !found {
		return
	}
	w.sendZmeyF(toZmey, payload)
}

// ----------------------------------------------------------------------------

type ClientWrapper struct {
	c      *kayak.Client
	config *kayak.KClientConfig

	sendZmeyF func(to int, payload interface{})
}

func NewClientWrapper(config *kayak.KClientConfig) *ClientWrapper {
	return &ClientWrapper{
		config: config,
	}
}

func (w *ClientWrapper) Init(
	sendF func(to int, payload interface{}),
	returnF func(payload interface{}),
	traceF func(payload interface{}),
	errorF func(error),
) {
	w.sendZmeyF = sendF
	w.config.SendF = w.sendF
	w.config.ReturnF = returnF
	w.config.TraceF = traceF
	w.config.ErrorF = errorF

	w.c = kayak.NewClient(w.config)
	w.c.Start()
}
func (w *ClientWrapper) ReceiveCall(payload interface{}) {
	w.c.ReceiveCall(payload)
}
func (w *ClientWrapper) ReceiveNet(fromZmey int, payload interface{}) {
	fromKayak, found := zmeyToKayak[fromZmey]
	if !found {
		return
	}
	w.c.ReceiveNet(fromKayak, payload)
}
func (w *ClientWrapper) Tick(t uint) {
	w.c.Tick(t)
}
func (w *ClientWrapper) sendF(toKayak kayak.KAddress, payload interface{}) {
	toZmey, found := kayakToZmey[toKayak]
	if !found {
		return
	}
	w.sendZmeyF(toZmey, payload)
}
