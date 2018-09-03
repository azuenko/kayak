package test

import (
	"context"
	"testing"
	"time"

	"github.com/stratumn/kayak"
	"github.com/stratumn/zmey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKayakNormal(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 4),
		server2Pid: makeCalls(t, 4),
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R1")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}

func TestKayakCrashFollower(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 4),
		server2Pid: makeCalls(t, 4),
		server3Pid: makeCalls(t, 4),
		server4Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	filterF := func(from, to int) bool {
		if from == server2Pid || to == server2Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R1")

	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server1Pid]),
		extractTagsFromResponses(t, responses[server1Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
		extractTagsFromResponses(t, responses[server3Pid]))
	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server4Pid]),
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server3Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 3 ==========

	z.Tick(clientTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server2Pid]),
		extractTagsFromResponses(t, responses[server2Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server3Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	entriesExpected := [][]byte{}

	for cid := range messages[server1Pid] {
		entriesExpected = append(entriesExpected, messages[server1Pid][cid].Payload)
	}
	for cid := range messages[server3Pid] {
		entriesExpected = append(entriesExpected, messages[server3Pid][cid].Payload)
	}
	for cid := range messages[server4Pid] {
		entriesExpected = append(entriesExpected, messages[server4Pid][cid].Payload)
	}

	require.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)

	require.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
	require.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)

}

func TestKayakCrashLeaderIsolate(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server3Pid: makeCalls(t, 2),
		server4Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messages))

	filterF := func(from, to int) bool {
		if from == server1Pid && to == server1Pid {
			return true
		}
		if from == server1Pid || to == server1Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R1")

	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server3Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
		extractTagsFromResponses(t, responses[server3Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server4Pid]),
		extractTagsFromResponses(t, responses[server4Pid]))

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}

// The test checks partial leader failure due to bad networking conditions.
// The leader cannot send messages to nodes 2 and 3, but can receive messages
// from anybody
func TestKayakBadNetwork(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server3Pid: makeCalls(t, 2),
		server4Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messages))

	var filterF zmey.FilterFunc

	filterF = func(from, to int) bool {
		if from == server1Pid && to == server2Pid {
			return false
		}
		if from == server1Pid && to == server3Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R1")

	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server3Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server1Pid]))
	require.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server2Pid]))
	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
		extractTagsFromResponses(t, responses[server3Pid]))
	require.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server4Pid]),
		extractTagsFromResponses(t, responses[server4Pid]))

	entriesExpected := makeEntries(t, messages)

	require.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	require.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	require.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}
