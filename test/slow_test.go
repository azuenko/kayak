// +build !fast

package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stratumn/kayak"
	"github.com/stratumn/zmey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKayakAtScale(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	serverPids := []int{
		server1Pid,
		server2Pid,
		server3Pid,
		server4Pid,
		server5Pid,
		server6Pid,
		server7Pid,
		server8Pid,
		server9Pid,
		server10Pid,
		server11Pid,
		server12Pid,
		server13Pid,
		server14Pid,
		server15Pid,
		server16Pid,
	}

	serverKeys := []kayak.KAddress{
		server1Key,
		server2Key,
		server3Key,
		server4Key,
		server5Key,
		server6Key,
		server7Key,
		server8Key,
		server9Key,
		server10Key,
		server11Key,
		server12Key,
		server13Key,
		server14Key,
		server15Key,
		server16Key,
	}

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		serverConfig := makeDefaultServerConfig(pid, logs[pid])
		serverConfig.Keys = serverKeys
		z.SetProcess(pid, NewKayakWrapper(serverConfig))
	}

	messages := make(map[int][]kayak.KCall)

	for _, pid := range serverPids {
		messages[pid] = makeCalls(t, 4)
	}

	z.Inject(makeInjectF(messages))

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 120*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	_ = traces
	// printOut(t, responses, traces, logs, "R1")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[serverPids[0]].Entries)
	for pid := range logs {
		assert.Equal(t, logs[serverPids[0]].Entries, logs[pid].Entries)
	}

}

// The test iterates multiple times the following pattern:
// * run some consensus rounds
// * isolate one process
// * run some more consensus rounds
// * restart stopped process
func TestKayakCrashCarousel(t *testing.T) {

	// TODO: test kayak.Status() after each round

	const roundsN = 25
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var filterF zmey.FilterFunc
	var err error

	_ = responses
	_ = traces
	_ = fmt.Sprintf

	var stopPids []int
	for i := 0; i < roundsN; i++ {
		idx := rand.Intn(len(serverPids))
		pid := serverPids[idx]
		stopPids = append(stopPids, pid)
	}

	// stopPids = []int{
	// 	server2Pid,
	// 	server4Pid,
	// }
	for i, pid := range stopPids {
		t.Logf("At round %2d will fail server with pid %d", i+1, pid)
	}

	for i, stopPid := range stopPids {

		// ========== ROUND X.1 ==========
		messages = map[int][]kayak.KCall{
			client1Pid: makeCalls(t, 2),
		}
		messagesAll = append(messagesAll, messages)
		z.Inject(makeInjectF(messages))

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		// ========== ROUND X.2 ==========
		z.Tick(serverTimeout)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		// ========== ROUND X.3 ==========
		filterF = func(from, to int) bool {
			if from == stopPid && to == stopPid {
				return true
			}
			if from == stopPid || to == stopPid {
				return false
			}
			return true
		}

		z.Filter(filterF)

		messages = map[int][]kayak.KCall{
			client1Pid: makeCalls(t, 2),
		}
		messagesAll = append(messagesAll, messages)
		z.Inject(makeInjectF(messages))

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.3", i+1))

		// ========== ROUND X.4 ==========
		z.Tick(serverTimeout)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.4", i+1))

		// ========== ROUND X.5 ==========
		z.Filter(nil)
		messages = map[int][]kayak.KCall{
			client1Pid: makeCalls(t, 2),
		}
		messagesAll = append(messagesAll, messages)
		z.Inject(makeInjectF(messages))

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.5", i+1))

		// ========== ROUND X.6 ==========
		z.Tick(serverTimeout)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.6", i+1))

		entriesExpected := makeEntries(t, messagesAll...)

		require.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
		require.Equal(t, logs[server1Pid].Entries, logs[server2Pid].Entries)
		require.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
		require.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)
	}

}
