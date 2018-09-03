package zmey

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

// TODO: add context

// Net abstracts the inter-process connections
type Net struct {
	scale      int
	pids       []int
	rpids      map[int]int
	session    *Session
	filterF    FilterFunc
	inputCs    []chan interface{}
	outputCs   []chan interface{}
	buffer     [][]interface{}
	bufferLock sync.RWMutex
	bufferedN  int
	sentN      int
	receivedN  int
}

// NewNet creates and returns a new instance of Net. Scale indicates the size
// of the network, session may be optionally provided to report status and stats.
func NewNet(ctx context.Context, wg *sync.WaitGroup, pids []int, session *Session) *Net {

	scale := len(pids)

	rpids := make(map[int]int)

	for i, pid := range pids {
		rpids[pid] = i
	}

	n := Net{
		pids:     pids,
		rpids:    rpids,
		scale:    scale,
		inputCs:  make([]chan interface{}, scale*scale),
		outputCs: make([]chan interface{}, scale*scale),
		buffer:   make([][]interface{}, scale*scale),
		session:  session,
	}

	for i := range pids {
		for j := range pids {
			n.inputCs[i*scale+j] = make(chan interface{})
			n.outputCs[i*scale+j] = make(chan interface{})
		}
	}

	go n.loop(ctx, wg)

	return &n
}

func (n *Net) loop(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	if n.session != nil {
		n.session.ProfNetworkStart()
	}

	cases := make([]reflect.SelectCase, 2*n.scale*n.scale+2)
	for i := range n.inputCs {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(n.inputCs[i]),
		}
	}

	cases[2*n.scale*n.scale+1] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}

	for {
		cases[2*n.scale*n.scale] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(timeoutNetwork)),
		}
		for i := range n.outputCs {
			if len(n.buffer[i]) > 0 {
				cases[n.scale*n.scale+i] = reflect.SelectCase{
					Dir:  reflect.SelectSend,
					Chan: reflect.ValueOf(n.outputCs[i]),
					Send: reflect.ValueOf(n.buffer[i][0]),
				}
			} else {
				// It's easier to add nil channel and keep the array length fixed
				cases[n.scale*n.scale+i] = reflect.SelectCase{
					Dir:  reflect.SelectSend,
					Chan: reflect.ValueOf(nil),
					Send: reflect.ValueOf(struct{}{}),
				}
			}
		}
		if n.session != nil {
			n.session.ProfNetworkSelectStart()
		}
		chosen, value, ok := reflect.Select(cases)
		if n.session != nil {
			n.session.ProfNetworkSelectEnd()
		}

		switch {
		case 0 <= chosen && chosen < n.scale*n.scale: // send
			if chosen != 2*n.scale*n.scale+1 && !ok {
				log.Printf("[   N] channel %d is closed", chosen)
				continue
			}

			from := n.pids[chosen%n.scale]
			to := n.pids[chosen/n.scale]

			if n.filterF == nil || n.filterF(from, to) {
				n.push(chosen, value.Interface())
			}
		case n.scale*n.scale <= chosen && chosen < 2*n.scale*n.scale: // receive
			// No need to use the returned value, it's been already sent
			// over the channel in Select() statement
			_ = n.pop(chosen - n.scale*n.scale)
		case chosen == 2*n.scale*n.scale: // timeout
			if n.bufferedN == 0 {
				if n.session != nil {
					n.session.ReportNetworkIdle()
				}
				time.Sleep(sleepNetwork)
				if n.session != nil {
					n.session.ReportNetworkBusy()
				}
			}
		case chosen == 2*n.scale*n.scale+1: // cancel
			if n.session != nil {
				n.session.ReportNetworkIdle()
			}
			return
		default:
			log.Printf("[   N] chosen incorrect channel %d", chosen)
		}
	}

}

// Filter sets FilterFunc to selectively cut communicational channels
func (n *Net) Filter(filterF FilterFunc) {
	n.filterF = filterF
}

// Send sens the message `m` to the recepeint with process id `to`. `as` should
// represent the id of sender process. If either `as` or `to` is out of range,
// ErrIncorrectPid is returned
func (n *Net) Send(as, to int, m interface{}) error {
	asIndex, ok1 := n.rpids[as]
	toIndex, ok2 := n.rpids[to]
	if !ok1 || !ok2 {
		return ErrIncorrectPid
	}

	n.inputCs[toIndex*n.scale+asIndex] <- m

	return nil
}

// Recv returns the channel of messages. Reading from the channel would
// yield the messages sent by `from` to `as`. If either `as` or `from`
// is out of range, ErrIncorrectPid is returned.
func (n *Net) Recv(as, from int) (chan interface{}, error) {
	asIndex, ok1 := n.rpids[as]
	fromIndex, ok2 := n.rpids[from]
	if !ok1 || !ok2 {
		return nil, ErrIncorrectPid
	}

	return n.outputCs[asIndex*n.scale+fromIndex], nil
}

// BufferStats returns an ASCII-formatted matrix of the sizes of buffers
// (not yet delivered messages)
func (n *Net) BufferStats() string {
	s := "    |  to|\n"
	s += "----+----+" + strings.Repeat("----+", n.scale)
	s += "\n"
	s += "from|    |"
	for i := range n.pids {
		s += fmt.Sprintf("%4d|", n.pids[i])
	}
	s += "\n"
	s += "----+----+" + strings.Repeat("----+", n.scale)
	s += "\n"

	func() {
		n.bufferLock.RLock()
		defer n.bufferLock.RUnlock()

		for i := range n.pids {
			s += fmt.Sprintf("    |%4d|", n.pids[i])
			for j := range n.pids {
				nm := fmt.Sprintf("%4d|", len(n.buffer[i*n.scale+j]))
				if nm == "   0|" {
					nm = "    |"
				}
				s += nm
			}
			s += "\n"
		}
	}()

	s += "----+----+" + strings.Repeat("----+", n.scale)
	s += "\n"

	return s
}

// Stats returns statistics of the network since its creation: number of
// received, buffered and sent messages.
func (n *Net) Stats() (int, int, int) {
	n.bufferLock.RLock()
	defer n.bufferLock.RUnlock()

	return n.receivedN, n.bufferedN, n.sentN
}

func (n *Net) push(index int, item interface{}) {
	n.bufferLock.Lock()
	defer n.bufferLock.Unlock()

	n.bufferedN++
	n.receivedN++

	n.buffer[index] = append(n.buffer[index], item)
}

func (n *Net) pop(index int) interface{} {
	n.bufferLock.Lock()
	defer n.bufferLock.Unlock()

	item := n.buffer[index][0]
	n.buffer[index] = n.buffer[index][1:]
	n.bufferedN--
	n.sentN++

	return item
}
