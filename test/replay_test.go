package test

import (
	"context"
	"testing"
	"time"

	"github.com/stratumn/zmey"

	"github.com/stratumn/kayak"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayResistanceSameRound(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	client1config := makeDefaultClientConfig(client1Pid)
	client1config.ByzantineFlags = kayak.ByzantineFlagClientFixNonce
	z.SetProcess(client1Pid, NewClientWrapper(client1config))

	client2config := makeDefaultClientConfig(client2Pid)
	client2config.ByzantineFlags = kayak.ByzantineFlagClientFixNonce
	z.SetProcess(client2Pid, NewClientWrapper(client2config))

	calls := makeCalls(t, 1)
	messages := map[int][]kayak.KCall{
		client1Pid: calls,
		client2Pid: calls,
	}

	z.Inject(makeInjectF(messages))

	ctx, cancelF := context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err := z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "")

	expectedEntries := [][]byte{
		calls[0].Payload,
	}

	assert.ElementsMatch(t, expectedEntries, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}

func TestReplayResistanceNextRound(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		z.SetProcess(pid, NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid])))
	}

	client1config := makeDefaultClientConfig(client1Pid)
	client1config.ByzantineFlags = kayak.ByzantineFlagClientFixNonce
	z.SetProcess(client1Pid, NewClientWrapper(client1config))

	calls := makeCalls(t, 1)
	messages := map[int][]kayak.KCall{
		client1Pid: calls,
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

	// ========== ROUND 2 ==========
	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	expectedEntries := [][]byte{
		calls[0].Payload,
	}

	assert.ElementsMatch(t, expectedEntries, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}

func TestReplayResistanceAttackFromThePast(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		serverConfig := makeDefaultServerConfig(pid, logs[pid])
		serverConfig.IndexTolerance = 0
		z.SetProcess(pid, NewKayakWrapper(serverConfig))
	}

	client1config := makeDefaultClientConfig(client1Pid)
	client1config.ByzantineFlags = kayak.ByzantineFlagClientFixNonce
	z.SetProcess(client1Pid, NewClientWrapper(client1config))

	calls := makeCalls(t, 1)
	messages := map[int][]kayak.KCall{
		client1Pid: calls,
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

	// ========== ROUND 2 ==========
	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	expectedEntries := [][]byte{
		calls[0].Payload,
	}

	assert.ElementsMatch(t, expectedEntries, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}
