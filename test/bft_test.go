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

func TestBFTIgnoreRequestsFromClient1024(t *testing.T) {

	require.Equal(t, kayak.KAddress{0xCC, 0x01}, client1Key)

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
	}

	server1Config := makeDefaultServerConfig(server1Pid, logs[server1Pid])
	server1Config.ByzantineFlags = kayak.ByzantineFlagIgnoreRequestsFromClientCC01
	z.SetProcess(server1Pid, NewKayakWrapper(server1Config))
	z.SetProcess(server2Pid, NewKayakWrapper(makeDefaultServerConfig(server2Pid, logs[server2Pid])))
	z.SetProcess(server3Pid, NewKayakWrapper(makeDefaultServerConfig(server3Pid, logs[server3Pid])))
	z.SetProcess(server4Pid, NewKayakWrapper(makeDefaultServerConfig(server4Pid, logs[server4Pid])))

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 4),
		client2Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	var ctx context.Context
	var cancelF context.CancelFunc
	var responsesRound, traces map[int][]interface{}
	var err error

	responsesAll := make(map[int][]interface{})

	// ========== ROUND 1 ==========
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

func TestBFTSendDifferentProposes(t *testing.T) {

	require.Equal(t, kayak.KAddress{0xCC, 0x01}, client1Key)

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
	}

	server1Config := makeDefaultServerConfig(server1Pid, logs[server1Pid])
	server1Config.ByzantineFlags = kayak.ByzantineFlagSendDifferentProposes
	z.SetProcess(server1Pid, NewKayakWrapper(server1Config))
	z.SetProcess(server2Pid, NewKayakWrapper(makeDefaultServerConfig(server2Pid, logs[server2Pid])))
	z.SetProcess(server3Pid, NewKayakWrapper(makeDefaultServerConfig(server3Pid, logs[server3Pid])))
	z.SetProcess(server4Pid, NewKayakWrapper(makeDefaultServerConfig(server4Pid, logs[server4Pid])))

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 4),
		client2Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	var ctx context.Context
	var cancelF context.CancelFunc
	var responsesRound, traces map[int][]interface{}
	var err error

	responsesAll := make(map[int][]interface{})

	// ========== ROUND 1 ==========
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
