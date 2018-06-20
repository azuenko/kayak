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

// In this test the initial network consists of 4 processes. The process 4 starts in
// isolation. Processes 1, 2 and 3 then run consensus multiple time, and proceed
// in ordering few log entries. Isolated process 4 doesn't get any messages.
// Then the process 4 is restarted (internal state reset), and the connectivity
// with other processes is reestablished. The process 4 then should figure out it's late,
// ask others to provide missing data to sync up. In the end we test that
// the log entries of all 4 processes are identical.
func TestKayakFollowerFailRestartJoinAndSync(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messages))

	filterF := func(from, to int) bool {
		if from == server4Pid && to == server4Pid {
			return true
		}
		if from == server4Pid || to == server4Pid {
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
		extractTagsFromMessages(t, messages[server2Pid]),
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
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

	// ========== ROUND 3 ==========
	z.Filter(nil)
	z.SetProcess(server4Pid, NewKayakWrapper(makeDefaultServerConfig(server4Pid, logs[server4Pid])))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server2Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)

}

// The test is the continuation of TestKayakFollowerFailRestartJoinAndSync.
// In addition, it ensures, that afrer syncing up the process 4 can participate
// in subsequent consensus rounds as it's always been part of the system.
func TestKayakFollowerFailRestartJoinSyncAndDoConsensus(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messages))

	filterF := func(from, to int) bool {
		if from == server4Pid && to == server4Pid {
			return true
		}
		if from == server4Pid || to == server4Pid {
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
		extractTagsFromMessages(t, messages[server2Pid]),
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
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

	// ========== ROUND 3 ==========
	z.SetProcess(server4Pid, NewKayakWrapper(makeDefaultServerConfig(server4Pid, logs[server4Pid])))

	z.Filter(nil)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	// ========== ROUND 4 ==========
	messagesRound4 := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messagesRound4))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R4")

	entriesExpected := makeEntries(t, messages, messagesRound4)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server2Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)

}

// The test is variation of TestKayakFollowerFailRestartJoinAndSync.
// Instead process 4, we let process 1 (leader) to fail, restart, and join
// to sync up.
func TestKayakLeaderFailRestartJoinAndSync(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
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
		extractTagsFromMessages(t, messages[server2Pid]),
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
		extractTagsFromResponses(t, responses[server3Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 3 ==========
	z.Tick(serverTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	// ========== ROUND 4 ==========
	z.Filter(nil)
	z.SetProcess(server1Pid, NewKayakWrapper(makeDefaultServerConfig(server1Pid, logs[server1Pid])))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R4")

	entriesExpected := makeEntries(t, messages)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server2Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)

}

// The test is variation of TestKayakFollowerFailRestartJoinSyncAndDoConsensus.
// Instead process 4, we let process 1 (leader) to fail, restart, join, sync up
// and continue participate in consensus
func TestKayakLeaderFailRestartJoinSyncAndDoConsensus(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	messages := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
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
		extractTagsFromMessages(t, messages[server2Pid]),
		extractTagsFromResponses(t, responses[server2Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[server3Pid]),
		extractTagsFromResponses(t, responses[server3Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[server4Pid]))

	// ========== ROUND 3 ==========

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	// ========== ROUND 4 ==========
	z.SetProcess(server1Pid, NewKayakWrapper(makeDefaultServerConfig(server1Pid, logs[server1Pid])))

	z.Filter(nil)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R4")

	// ========== ROUND 5 ==========
	messagesRound4 := map[int][]kayak.KCall{
		server2Pid: makeCalls(t, 2),
		server3Pid: makeCalls(t, 2),
	}

	z.Inject(makeInjectF(messagesRound4))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R5")

	entriesExpected := makeEntries(t, messages, messagesRound4)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server2Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server3Pid].Entries)
	assert.Equal(t, logs[server1Pid].Entries, logs[server4Pid].Entries)

}

// TODO: Tick and inject test case
// TODO: autosync with received consensus messages
