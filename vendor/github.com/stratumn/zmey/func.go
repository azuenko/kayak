package zmey

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

func (z *Zmey) processLoop(ctx context.Context, wg *sync.WaitGroup, pack *pack, session *Session, net *Net) {
	wg.Add(1)
	defer wg.Done()

	session.ProfProcessStart(pack.pid)

	scale := len(z.packs)

	if !pack.isStarted {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[%4d] processLoop: panic: %v", pack.pid, r)
					debug.PrintStack()
					return
				}
			}()
			pack.process.Init(
				pack.api.Send,
				pack.api.Return,
				pack.api.Trace,
				pack.api.ReportError,
			)
		}()

		pack.isStarted = true
	}

	cases := make([]reflect.SelectCase, scale+4)
	for i, pid := range z.pids {
		recvC, err := net.Recv(pack.pid, pid)
		if err != nil {
			log.Printf("[%4d] processLoop: error: %s", pack.pid, err)
			continue
		}
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(recvC),
		}
	}
	cases[scale] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(pack.callC),
	}

	cases[scale+1] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(pack.tickC),
	}

	cases[scale+3] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	for {

		cases[scale+2] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(timeoutProcess)),
		}

		session.ProfProcessSelectStart(pack.pid)
		chosen, value, ok := reflect.Select(cases)
		session.ProfProcessSelectEnd(pack.pid)

		if chosen != scale+3 && !ok {
			log.Printf("[%4d] processLoop: channel %d is closed", pack.pid, chosen)
			continue
		}

		switch {
		case 0 <= chosen && chosen < scale: // network recv
			payload := value.Interface()

			if z.c.Debug {
				log.Printf("[%4d] processLoop: received message from %d : %+v", pack.pid, chosen, payload)
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[%4d] processLoop: panic: %v", pack.pid, r)
						debug.PrintStack()
						return
					}
				}()
				pack.process.ReceiveNet(z.pids[chosen], payload)
			}()
			if z.c.Debug {
				log.Printf("[%4d] processLoop: message processed", pack.pid)
			}
		case chosen == scale: // client recv
			call := value.Interface()

			if z.c.Debug {
				log.Printf("[%4d] processLoop: received call: %+v", pack.pid, call)
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[%4d] processLoop: panic: %v", pack.pid, r)
						debug.PrintStack()
						return
					}
				}()
				pack.process.ReceiveCall(call)
			}()

			if z.c.Debug {
				log.Printf("[%4d] processLoop: call processed", pack.pid)
			}
		case chosen == scale+1: // tick
			t, ok := value.Interface().(uint)
			if !ok {
				log.Printf("[%4d] processLoop: value cannot be converted to tick: %+v", pack.pid, value)
				continue
			}

			if z.c.Debug {
				log.Printf("[%4d] processLoop: received tick: %d", pack.pid, t)
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[%4d] processLoop: panic: %v", pack.pid, r)
						debug.PrintStack()
						return
					}
				}()
				pack.process.Tick(t)
			}()

		case chosen == scale+2: // timeout
			if z.c.Debug {
				log.Printf("[%4d] processLoop: idle", pack.pid)
			}
			session.ReportProcessIdle(pack.pid)
			time.Sleep(sleepProcess)
			session.ReportProcessBusy(pack.pid)
		case chosen == scale+3: // context cancel
			if z.c.Debug {
				log.Printf("[%4d] processLoop: cancelled", pack.pid)
			}
			session.ReportProcessIdle(pack.pid)
			return
		default:
			log.Printf("[%4d] processLoop: chosen incorrect channel %d", pack.pid, chosen)
		}

	}

}

func (z *Zmey) collectLoop(ctx context.Context, wg *sync.WaitGroup, session *Session) {
	wg.Add(1)
	defer wg.Done()

	session.ProfCollectStart()

	scale := len(z.packs)

	cases := make([]reflect.SelectCase, 2*scale+2)

	for i, pid := range z.pids {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(z.packs[pid].returnC),
		}
	}

	for i, pid := range z.pids {
		cases[scale+i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(z.packs[pid].traceC),
		}
	}

	cases[2*scale+1] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}

	for {
		cases[2*scale] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(timeoutCollect)),
		}

		session.ProfCollectSelectStart()
		chosen, value, ok := reflect.Select(cases)
		session.ProfCollectSelectEnd()

		if chosen != 2*scale+1 && !ok {
			log.Printf("[   C] channel %d is closed", chosen)
			continue
		}

		switch {
		case 0 <= chosen && chosen < scale: // return call
			call := value.Interface()
			if z.c.Debug {
				log.Printf("[   C] appending response for pid %d", chosen)
			}
			pack := z.packs[z.pids[chosen]]
			pack.responses = append(pack.responses, call)
		case scale <= chosen && chosen < 2*scale: // trace call
			trace := value.Interface()
			if z.c.Debug {
				log.Printf("[   C] appending trace for pid %d", chosen)
			}
			pack := z.packs[z.pids[chosen-scale]]
			pack.traces = append(pack.traces, trace)
		case chosen == 2*scale: // timeout
			if z.c.Debug {
				log.Printf("[   C] idle")
			}
			session.ReportCollectIdle()
			time.Sleep(sleepCollect)
			session.ReportCollectBusy()
		case chosen == 2*scale+1: // cancel
			if z.c.Debug {
				log.Printf("[   C] cancelled")
			}
			session.ReportCollectIdle()
			return
		default:
			log.Printf("[   C] chosen incorrect channel %d", chosen)
		}

	}
}

func (z *Zmey) tickF(pack *pack, wg *sync.WaitGroup, t uint) {
	wg.Add(1)
	defer wg.Done()

	if z.c.Debug {
		log.Printf("[%4d] tickF: received %d", pack.pid, t)
	}
	pack.tickC <- t
	if z.c.Debug {
		log.Printf("[%4d] tickF: done", pack.pid)
	}
}

func (z *Zmey) statusLoop(ctx context.Context, wg *sync.WaitGroup, net *Net, session *Session) {
	wg.Add(1)
	defer wg.Done()

	for {
		receivedN, bufferedN, sentN := net.Stats()
		statusStr := fmt.Sprintf("net [%5d/%5d/%5d] session %s profs %s",
			receivedN, bufferedN, sentN,
			session.Status(),
			session.Profs(),
		)

		select {
		case z.statusC <- statusStr:
		case z.bufferStatsC <- net.BufferStats():
		case <-ctx.Done():
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
