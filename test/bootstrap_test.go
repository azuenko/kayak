package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stratumn/kayak"
	"github.com/stratumn/zmey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKayakBootstrap(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	keys := []kayak.KAddress{
		server1Key,
	}

	// ========== ROUND 0 ==========
	logs[server1Pid] = &Storage{}
	server1Config := makeDefaultServerConfig(server1Pid, logs[server1Pid])
	server1Config.Keys = keys
	wrappers[server1Pid] = NewKayakWrapper(server1Config)
	z.SetProcess(server1Pid, wrappers[server1Pid])

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R0")

	pidsToAdd := []int{
		server2Pid,
		server3Pid,
		server4Pid,
		server5Pid,
	}

	for i, pid := range pidsToAdd {
		keyToAdd := zmeyToKayak[pid]
		keys = append(keys, keyToAdd)

		// ========== ROUND X.1 ==========

		messages = map[int][]kayak.KCall{
			server1Pid: {
				kayak.KCall{
					Tag:     getNextTag(),
					Payload: append(kayak.MagicAddProcess[:], keyToAdd[:]...),
				},
			},
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		// ========== ROUND X.2 ==========
		logs[pid] = &Storage{}
		serverConfig := makeDefaultServerConfig(pid, logs[pid])
		serverConfig.Keys = keys
		wrappers[pid] = NewKayakWrapper(serverConfig)
		z.SetProcess(pid, wrappers[pid])

		z.Tick(serverTimeout)
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		// ========== ROUND X.3 ==========
		messages = map[int][]kayak.KCall{
			server1Pid: makeCalls(t, 1),
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.3", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		expectedStatus := kayak.KStatus{
			Round:  kayak.KRound((i + 1) * 2),
			Epoch:  0,
			Leader: server1Key,
			Keys:   keys,
		}
		for _, wrapper := range wrappers {
			status := wrapper.k.Status()
			require.Equal(t, &expectedStatus, status)
		}
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}

func TestKayakTeardown(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	keysMap := make(map[kayak.KAddress]struct{})

	keys := append([]kayak.KAddress(nil), serverKeys...)

	for _, key := range keys {
		keysMap[key] = struct{}{}
	}

	// ========== ROUND 0 ==========
	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R0")

	i := 0
	for {
		keyToRemove := keys[len(keys)-1]
		keys = keys[:len(keys)-1]
		delete(keysMap, keyToRemove)

		// ========== ROUND X.1 ==========

		messages = map[int][]kayak.KCall{
			server1Pid: {
				kayak.KCall{
					Tag:     getNextTag(),
					Payload: append(kayak.MagicRemoveProcess[:], keyToRemove[:]...),
				},
			},
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		// ========== ROUND X.2 ==========

		z.Tick(serverTimeout)
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		// ========== ROUND X.3 ==========
		messages = map[int][]kayak.KCall{
			server1Pid: makeCalls(t, 1),
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.3", i+1))

		for pid := range responses {
			if pid == kayakToZmey[keyToRemove] {
				continue
			}
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		expectedStatus := kayak.KStatus{
			Round:  kayak.KRound((i + 1) * 2),
			Epoch:  0,
			Leader: server1Key,
			Keys:   keys,
		}
		for pid, wrapper := range wrappers {
			if _, found := keysMap[zmeyToKayak[pid]]; !found {
				continue
			}
			status := wrapper.k.Status()
			require.Equal(t, &expectedStatus, status, "process %d status", pid)
		}

		if len(keys) == 1 {
			break
		}
		i++
	}

}
