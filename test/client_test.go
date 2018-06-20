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

func TestClientWithPong(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	for _, pid := range serverPids {
		z.SetProcess(pid, NewPongReplier(pid, serverPids))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 4),
		client2Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	ctx, cancelF := context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err := z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, nil, "")

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
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responses[client1Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client2Pid]),
		extractTagsFromResponses(t, responses[client2Pid]))
}

func TestClientWithPongTimeoutClient2(t *testing.T) {

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	for _, pid := range serverPids {
		z.SetProcess(pid, NewPongReplier(pid, serverPids))
	}

	z.SetProcess(client1Pid, NewClientWrapper(makeDefaultClientConfig(client1Pid)))
	z.SetProcess(client2Pid, NewClientWrapper(makeDefaultClientConfig(client2Pid)))

	filterF := func(from, to int) bool {
		if from == client2Pid {
			return false
		}
		return true
	}

	z.Filter(filterF)

	messages := map[int][]kayak.KCall{
		client1Pid: makeCalls(t, 4),
		client2Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var err error

	// ========== ROUND 1 ==========
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, nil, "R1")

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
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responses[client1Pid]))
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[client2Pid]))

	// ========== ROUND 2 ==========

	z.Tick(clientTimeout)

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, nil, "R2")

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
	assert.ElementsMatch(t,
		nil,
		extractTagsFromResponses(t, responses[client1Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client2Pid]),
		extractTagsFromResponses(t, responses[client2Pid]))

}

func TestClientWithKayak(t *testing.T) {

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
		client1Pid: makeCalls(t, 4),
		client2Pid: makeCalls(t, 4),
	}

	z.Inject(makeInjectF(messages))

	ctx, cancelF := context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err := z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "")

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
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client1Pid]),
		extractTagsFromResponses(t, responses[client1Pid]))
	assert.ElementsMatch(t,
		extractTagsFromMessages(t, messages[client2Pid]),
		extractTagsFromResponses(t, responses[client2Pid]))

	expectedEntries := makeEntries(t, messages)

	assert.ElementsMatch(t, expectedEntries, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

}
