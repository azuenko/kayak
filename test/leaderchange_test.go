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

// In the test we simulate the following scenario. The leader (server1) is down,
// and the server4 has increased timeout for requests. Client broadcasts the request
// to server2, server3 and server4. After Tick(), server2 and server3 realise that
// there are unprocessed requests at timeout and simultaneously go into the alerted
// state and broacast suspects. Server4, on its turn is fine, since it has
// the incleased timeout. Server4 should enter the alerted state because of the
// suspects received from server1 and server2. It should also rebroadcast the loads,
// received in suspects from server1 and server2, even if the corresponding requests
// are not locally timeouted. All servers should complete the leader change process
// and process the unprocessed request.
func TestKayakUnballancedSuspects(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	z.SetProcess(server1Pid, NewKayakWrapper(makeDefaultServerConfig(server1Pid, logs[server1Pid])))
	z.SetProcess(server2Pid, NewKayakWrapper(makeDefaultServerConfig(server2Pid, logs[server2Pid])))
	z.SetProcess(server3Pid, NewKayakWrapper(makeDefaultServerConfig(server3Pid, logs[server3Pid])))
	server4Config := makeDefaultServerConfig(server4Pid, logs[server4Pid])
	server4Config.RequestT = 2 * serverTimeout
	z.SetProcess(server4Pid, NewKayakWrapper(server4Config))

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
		if from == server1Pid && to == server1Pid {
			return true
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
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responsesAll[client1Pid]))

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server2Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server2Pid].Entries, logs[server4Pid].Entries)

}
