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

// In the first round we add 5th process
// In the second round we wait for 5th process to sync
// In the third round we run normal consensus with 5 nodes
func TestKayakAddProcessSimple(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	// ========== ROUND 1 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicAddProcess[:], server5Key[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

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

	// ========== ROUND 2 ==========
	logs[server5Pid] = &Storage{}
	z.SetProcess(server5Pid, NewKayakWrapper(makeDefaultServerConfig(server5Pid, logs[server5Pid])))

	z.Tick(serverTimeout)
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	// ========== ROUND 3 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 2),
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	expectedStatus := kayak.KStatus{
		Round:  3,
		Epoch:  0,
		Leader: server1Key,
		Keys: []kayak.KAddress{
			server1Key,
			server2Key,
			server3Key,
			server4Key,
			server5Key,
		},
	}
	for _, wrapper := range wrappers {
		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

func TestKayakAddProcessExisting(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	// ========== ROUND 1 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicAddProcess[:], server1Key[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

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

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	// ========== ROUND 3 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 2),
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	expectedStatus := kayak.KStatus{
		Round:  3,
		Epoch:  0,
		Leader: server1Key,
		Keys: []kayak.KAddress{
			server1Key,
			server2Key,
			server3Key,
			server4Key,
		},
	}
	for _, wrapper := range wrappers {
		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

// The test ensures that epoch jump is performed and the leader remains
// the leader after adding a new process.
// We begin by running few leader failures to let the epoch be greater than
// the number of processes to make the epoch jump visible.
func TestKayakAddProcessEpochJump(t *testing.T) {

	const rounds = 9

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	i := 0
	for {

		// ========== ROUND X.1 ==========
		messages = map[int][]kayak.KCall{
			serverPids[(i+1)%4]: makeCalls(t, 1),
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))

		filterF := func(from, to int) bool {
			if from == serverPids[i%4] && to == serverPids[i%4] {
				return true
			}
			if from == serverPids[i%4] || to == serverPids[i%4] {
				return false
			}
			return true
		}

		z.Filter(filterF)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		// ========== ROUND X.2 ==========
		z.Filter(nil)
		z.Tick(serverTimeout)
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		i++

		if i >= rounds {
			break
		}

	}

	// ========== ROUND N ==========
	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicAddProcess[:], server5Key[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "RN")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	epoch := rounds
	leaderPos := epoch % 4
	for {
		if epoch%5 == leaderPos {
			break
		}
		epoch++
	}

	expectedStatus := kayak.KStatus{
		Round:  rounds + 1,
		Epoch:  kayak.KEpoch(epoch),
		Leader: serverKeys[leaderPos],
		Keys: []kayak.KAddress{
			server1Key,
			server2Key,
			server3Key,
			server4Key,
			server5Key,
		},
	}
	for _, wrapper := range wrappers {
		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

func TestKayakRemoveProcessSimple(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	// ========== ROUND 1 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicRemoveProcess[:], server2Key[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

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

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	// ========== ROUND 3 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 2),
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		if pid == server2Pid {
			continue
		}
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	expectedStatus := kayak.KStatus{
		Round:  3,
		Epoch:  0,
		Leader: server1Key,
		Keys: []kayak.KAddress{
			server1Key,
			server3Key,
			server4Key,
		},
	}
	for pid, wrapper := range wrappers {
		if pid == server2Pid {
			continue
		}

		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

func TestKayakRemoveProcessNonExisting(t *testing.T) {
	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	// ========== ROUND 1 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicRemoveProcess[:], server5Key[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

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

	// ========== ROUND 2 ==========

	z.Tick(serverTimeout)
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R2")

	// ========== ROUND 3 ==========
	messages = map[int][]kayak.KCall{
		server1Pid: makeCalls(t, 2),
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))
	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "R3")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		if pid == server2Pid {
			continue
		}
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	expectedStatus := kayak.KStatus{
		Round:  3,
		Epoch:  0,
		Leader: server1Key,
		Keys: []kayak.KAddress{
			server1Key,
			server2Key,
			server3Key,
			server4Key,
		},
	}
	for pid, wrapper := range wrappers {
		if pid == server2Pid {
			continue
		}

		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

func TestKayakRemoveProcessEpochJumpNotLeader(t *testing.T) {

	const rounds = 9

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	i := 0
	for {

		// ========== ROUND X.1 ==========
		messages = map[int][]kayak.KCall{
			serverPids[(i+1)%4]: makeCalls(t, 1),
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))

		filterF := func(from, to int) bool {
			if from == serverPids[i%4] && to == serverPids[i%4] {
				return true
			}
			if from == serverPids[i%4] || to == serverPids[i%4] {
				return false
			}
			return true
		}

		z.Filter(filterF)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		// ========== ROUND X.2 ==========
		z.Filter(nil)
		z.Tick(serverTimeout)
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		i++

		if i >= rounds {
			break
		}

	}

	// ========== ROUND N ==========

	leaderPos := rounds % 4
	follower := serverKeys[(leaderPos+1)%4]

	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicRemoveProcess[:], follower[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "RN")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	epoch := rounds
	leaderPos = epoch % 4
	for {
		if epoch%3 == leaderPos {
			break
		}
		epoch++
	}

	remainingKeys := []kayak.KAddress{}

	for _, key := range serverKeys {
		if key != follower {
			remainingKeys = append(remainingKeys, key)
		}
	}

	expectedStatus := kayak.KStatus{
		Round:  rounds + 1,
		Epoch:  kayak.KEpoch(epoch),
		Leader: serverKeys[leaderPos],
		Keys:   remainingKeys,
	}
	for _, wrapper := range wrappers {
		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}

func TestKayakRemoveProcessEpochJumpLeader(t *testing.T) {

	const rounds = 9

	z := zmey.NewZmey(&zmey.Config{
	// Debug: true,
	})

	logs := make(map[int]*Storage)
	wrappers := make(map[int]*KayakWrapper)

	for _, pid := range serverPids {
		logs[pid] = &Storage{}
		wrappers[pid] = NewKayakWrapper(makeDefaultServerConfig(pid, logs[pid]))
		z.SetProcess(pid, wrappers[pid])
	}

	var ctx context.Context
	var cancelF context.CancelFunc
	var responses, traces map[int][]interface{}
	var messages map[int][]kayak.KCall
	var messagesAll []map[int][]kayak.KCall
	var err error

	i := 0
	for {

		// ========== ROUND X.1 ==========
		messages = map[int][]kayak.KCall{
			serverPids[(i+1)%4]: makeCalls(t, 1),
		}
		messagesAll = append(messagesAll, messages)

		z.Inject(makeInjectF(messages))

		filterF := func(from, to int) bool {
			if from == serverPids[i%4] && to == serverPids[i%4] {
				return true
			}
			if from == serverPids[i%4] || to == serverPids[i%4] {
				return false
			}
			return true
		}

		z.Filter(filterF)

		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.1", i+1))

		// ========== ROUND X.2 ==========
		z.Filter(nil)
		z.Tick(serverTimeout)
		ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
		responses, traces, err = z.Round(ctx)
		cancelF()

		require.NoError(t, err)

		printOut(t, responses, traces, logs, fmt.Sprintf("R%d.2", i+1))

		for pid := range responses {
			tagsExpected := extractTagsFromMessages(t, messages[pid])
			tagsActual := extractTagsFromResponses(t, responses[pid])
			assert.ElementsMatch(t, tagsExpected, tagsActual)
		}

		i++

		if i >= rounds {
			break
		}

	}

	// ========== ROUND N ==========

	leaderPos := rounds % 4
	leader := serverKeys[leaderPos]

	messages = map[int][]kayak.KCall{
		server1Pid: {
			kayak.KCall{
				Tag:     getNextTag(),
				Payload: append(kayak.MagicRemoveProcess[:], leader[:]...),
			},
		},
	}
	messagesAll = append(messagesAll, messages)

	z.Inject(makeInjectF(messages))

	ctx, cancelF = context.WithTimeout(context.Background(), 3*time.Second)
	responses, traces, err = z.Round(ctx)
	cancelF()

	require.NoError(t, err)

	printOut(t, responses, traces, logs, "RN")

	for pid := range responses {
		tagsExpected := extractTagsFromMessages(t, messages[pid])
		tagsActual := extractTagsFromResponses(t, responses[pid])
		assert.ElementsMatch(t, tagsExpected, tagsActual)
	}

	entriesExpected := makeEntries(t, messagesAll...)

	assert.ElementsMatch(t, entriesExpected, logs[server1Pid].Entries)
	for pid := range logs {
		assert.Equal(t, logs[server1Pid].Entries, logs[pid].Entries)
	}

	epoch := rounds

	remainingKeys := []kayak.KAddress{}

	for _, key := range serverKeys {
		if key != leader {
			remainingKeys = append(remainingKeys, key)
		}
	}

	expectedStatus := kayak.KStatus{
		Round:  rounds + 1,
		Epoch:  kayak.KEpoch(epoch),
		Leader: serverKeys[epoch%3],
		Keys:   remainingKeys,
	}
	for _, wrapper := range wrappers {
		status := wrapper.k.Status()
		require.Equal(t, &expectedStatus, status)
	}

}
