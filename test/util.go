package test

import (
	"math/rand"
	"sync/atomic"
	"testing"

	"fmt"
	"sort"
	"strings"

	"github.com/stratumn/kayak"
	"github.com/stratumn/zmey"
)

const (
	server1Pid  = 1
	server2Pid  = 2
	server3Pid  = 3
	server4Pid  = 4
	server5Pid  = 5
	server6Pid  = 6
	server7Pid  = 7
	server8Pid  = 8
	server9Pid  = 9
	server10Pid = 10
	server11Pid = 11
	server12Pid = 12
	server13Pid = 13
	server14Pid = 14
	server15Pid = 15
	server16Pid = 16

	client1Pid = 1001
	client2Pid = 1002

	serverTimeout  = uint(1000)
	clientTimeout  = uint(5000)
	indexTolerance = uint(100)
)

// TODO: why not const?
var (
	server1Key  = kayak.KAddress{0x00, 0x01}
	server2Key  = kayak.KAddress{0x00, 0x02}
	server3Key  = kayak.KAddress{0x00, 0x03}
	server4Key  = kayak.KAddress{0x00, 0x04}
	server5Key  = kayak.KAddress{0x00, 0x05}
	server6Key  = kayak.KAddress{0x00, 0x06}
	server7Key  = kayak.KAddress{0x00, 0x07}
	server8Key  = kayak.KAddress{0x00, 0x08}
	server9Key  = kayak.KAddress{0x00, 0x09}
	server10Key = kayak.KAddress{0x00, 0x10}
	server11Key = kayak.KAddress{0x00, 0x11}
	server12Key = kayak.KAddress{0x00, 0x12}
	server13Key = kayak.KAddress{0x00, 0x13}
	server14Key = kayak.KAddress{0x00, 0x14}
	server15Key = kayak.KAddress{0x00, 0x15}
	server16Key = kayak.KAddress{0x00, 0x16}

	client1Key = kayak.KAddress{0xCC, 0x01}
	client2Key = kayak.KAddress{0xCC, 0x02}
)

var zmeyToKayak = map[int]kayak.KAddress{
	server1Pid:  server1Key,
	server2Pid:  server2Key,
	server3Pid:  server3Key,
	server4Pid:  server4Key,
	server5Pid:  server5Key,
	server6Pid:  server6Key,
	server7Pid:  server7Key,
	server8Pid:  server8Key,
	server9Pid:  server9Key,
	server10Pid: server10Key,
	server11Pid: server11Key,
	server12Pid: server12Key,
	server13Pid: server13Key,
	server14Pid: server14Key,
	server15Pid: server15Key,
	server16Pid: server16Key,
	client1Pid:  client1Key,
	client2Pid:  client2Key,
}

var kayakToZmey = map[kayak.KAddress]int{
	server1Key:  server1Pid,
	server2Key:  server2Pid,
	server3Key:  server3Pid,
	server4Key:  server4Pid,
	server5Key:  server5Pid,
	server6Key:  server6Pid,
	server7Key:  server7Pid,
	server8Key:  server8Pid,
	server9Key:  server9Pid,
	server10Key: server10Pid,
	server11Key: server11Pid,
	server12Key: server12Pid,
	server13Key: server13Pid,
	server14Key: server14Pid,
	server15Key: server15Pid,
	server16Key: server16Pid,
	client1Key:  client1Pid,
	client2Key:  client2Pid,
}

var serverKeys = []kayak.KAddress{
	server1Key,
	server2Key,
	server3Key,
	server4Key,
}

var clientKeys = []kayak.KAddress{
	client1Key,
	client2Key,
}

var serverPids = []int{
	server1Pid,
	server2Pid,
	server3Pid,
	server4Pid,
}

var clientPids = []int{
	client1Pid,
	client2Pid,
}

func extractTagsFromResponses(t *testing.T, responses []interface{}) []int {
	tags := []int{}

	for i := range responses {
		kreturn, ok := responses[i].(kayak.KReturn)
		if !ok {
			t.Fatalf("cannot convert %+v to KReturn", responses[i])
		}
		tags = append(tags, kreturn.Tag)
	}

	return tags
}

func extractTagsFromMessages(t *testing.T, calls []kayak.KCall) []int {
	tags := []int{}

	for i := range calls {
		tags = append(tags, calls[i].Tag)
	}

	return tags
}

var lastTag uint64

func getNextTag() int {
	tag := atomic.AddUint64(&lastTag, 1)
	return int(tag - 1)
}

func makeCalls(t *testing.T, n int) []kayak.KCall {
	calls := make([]kayak.KCall, n)
	for i := range calls {
		payload := make([]byte, 2)
		_, err := rand.Read(payload)
		if err != nil {
			t.Fatal(err)
		}

		calls[i] = kayak.KCall{Tag: getNextTag(), Payload: payload}
	}
	return calls
}

func makeEntries(t *testing.T, messagesMaps ...map[int][]kayak.KCall) [][]byte {
	entries := [][]byte{}

	for mapID := range messagesMaps {
		for pid := range messagesMaps[mapID] {
			for cid := range messagesMaps[mapID][pid] {
				entries = append(entries, messagesMaps[mapID][pid][cid].Payload)
			}
		}
	}

	return entries
}

func makeInjectF(messages map[int][]kayak.KCall) zmey.InjectFunc {
	injectF := func(pid int, c zmey.Client) {
		for i := range messages[pid] {
			c.Call(messages[pid][i])
		}
	}

	return injectF
}

func makeDefaultServerConfig(pid int, storage *Storage) *kayak.KServerConfig {
	return &kayak.KServerConfig{
		Key:            zmeyToKayak[pid],
		Keys:           serverKeys,
		Storage:        storage,
		RequestT:       serverTimeout,
		CallT:          clientTimeout,
		WhatsupT:       serverTimeout,
		BonjourT:       clientTimeout,
		IndexTolerance: indexTolerance,
	}
}

func makeDefaultClientConfig(pid int) *kayak.KClientConfig {
	return &kayak.KClientConfig{
		Key:        zmeyToKayak[pid],
		ServerKeys: serverKeys,
		CallT:      clientTimeout,
		BonjourT:   clientTimeout,
	}
}

func printOut(t *testing.T, returns, traces map[int][]interface{}, logs map[int]*Storage, prefix string) {
	var pidsR, pidsT, pidsL []int

	for pid := range returns {
		pidsR = append(pidsR, pid)
	}

	for pid := range traces {
		pidsT = append(pidsT, pid)
	}

	for pid := range logs {
		pidsL = append(pidsL, pid)
	}

	sort.Ints(pidsR)
	sort.Ints(pidsT)
	sort.Ints(pidsL)

	fmt.Printf("============================================ %s ============================================\n", prefix)

	if prefix != "" {
		prefix = prefix + ": "
	}

	for _, pid := range pidsT {
		for tid := range traces[pid] {
			fmt.Printf("%s%#v: %s\n", prefix, zmeyToKayak[pid], traces[pid][tid])
		}
	}

	for _, pid := range pidsR {
		for rid := range returns[pid] {
			kreturn, ok := returns[pid][rid].(kayak.KReturn)
			if ok {
				fmt.Printf("%sRETURN OF %#v: %#v\n", prefix, zmeyToKayak[pid], kreturn)
			}
		}
	}

	s := prefix + "LOG DATA\n    | idx|\n"
	s += "----+----+" + strings.Repeat("------+", len(logs))
	s += "\n"
	s += " pid|     "
	for _, pid := range pidsL {
		s += fmt.Sprintf("%#v ", zmeyToKayak[pid])
	}
	s += "\n"
	s += "----+----+" + strings.Repeat("------+", len(logs))
	s += "\n"

	var index int

	for {
		line := ""
		line += fmt.Sprintf("    |%4d|", index)
		atLeastOne := false
		for _, pid := range pidsL {
			if index < len(logs[pid].Entries) {
				switch len(logs[pid].Entries[index]) {
				case 0:
					line += " xnil |"
				case 1:
					line += fmt.Sprintf(" %X  |", logs[pid].Entries[index])
				default:
					line += fmt.Sprintf(" %X |", logs[pid].Entries[index][:2])
				}
				atLeastOne = true
			} else {
				line += "      |"
			}
		}
		if !atLeastOne {
			break
		}
		s += line + "\n"
		index++
	}

	fmt.Println(s)

}
