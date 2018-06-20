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

// The test ensures that the client will still have its requests ordered
// despite the leader (server1) crash and bad link between the client and one
// of the servers.
// The bad link cuts client requests, but not server responses.
// In V1 of the test the bad link is between the client and server4, so
// the next elected leader (server2) has the request to propose
// In V2 of the test the bad link is between the client and server2, and next
// elected leader (server2) will have to learn client request
// from Suspect messages.
func TestKayakPartialBroadcastV1(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 2),
		client2Pid: makeCalls(t, 2),
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var filterF zmey.FilterFunc
	var responsesRound, traces map[int][]interface{}
	var err error

	// ========== ROUND 0 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	// ========== ROUND 1 ==========
	z.Inject(makeInjectF(messages))

	filterF = func(from, to int) bool {
		if from == client1Pid && to == server4Pid {
			return false
		}
		if from == server1Pid || to == server1Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	responsesAll := make(map[int][]interface{})

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R1")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	// ========== ROUND 2 ==========
	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R2")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client2Pid]),
		extractTagsFromResponses(t, responsesAll[client2Pid]))

	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responsesAll[client1Pid]))

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}

func TestKayakPartialBroadcastV2(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 2),
		client2Pid: makeCalls(t, 2),
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var filterF zmey.FilterFunc
	var responsesRound, traces map[int][]interface{}
	var err error

	// ========== ROUND 0 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	// ========== ROUND 1 ==========
	z.Inject(makeInjectF(messages))

	filterF = func(from, to int) bool {
		if from == client1Pid && to == server2Pid {
			return false
		}
		if from == server1Pid || to == server1Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	responsesAll := make(map[int][]interface{})

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R1")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	// ========== ROUND 2 ==========
	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R2")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client2Pid]),
		extractTagsFromResponses(t, responsesAll[client2Pid]))

	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responsesAll[client1Pid]))

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}

// In the following test the client sends its requests to only two processes
// out of 4 (server3 and server4). The leader is server1. Naturally, servers
// 3 and 4 start leader change procedure, broadcasting missing request.client1Pid
// Current (server1) and next (server2) leader receiver 2 suspects each and also
// rebroadcast the missing request. Once leader change is completed, server2
// should become the leader and propose the missing request
func TestKayakLoadRebroadcast(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 1),
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var filterF zmey.FilterFunc
	var responsesRound, traces map[int][]interface{}
	var err error

	// ========== ROUND 0 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	// ========== ROUND 1 ==========
	z.Inject(makeInjectF(messages))

	filterF = func(from, to int) bool {
		if from == client1Pid && to == server1Pid {
			return false
		}
		if from == client1Pid && to == server2Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	responsesAll := make(map[int][]interface{})

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R1")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	// ========== ROUND 2 ==========
	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responsesRound, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responsesRound, traces, logs, "R2")

	for pid := range responsesRound {
		responsesAll[pid] = append(responsesAll[pid], responsesRound[pid]...)
	}

	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responsesAll[client1Pid]))

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}
