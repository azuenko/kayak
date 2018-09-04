package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/stratumn/kayak"
)

type Packet struct {
	From    kayak.KAddress
	To      kayak.KAddress
	Payload interface{}
}

type Storage struct {
	Entries [][]byte
}

func (s *Storage) Append(entry []byte) {
	s.Entries = append(s.Entries, entry)
}

var (
	fMe     int
	fOthers string
)

func init() {
	gob.Register(kayak.KData{})
	gob.Register(kayak.KHash{})
	gob.Register(kayak.KNonce{})
	gob.Register(kayak.KAddress{})
	gob.Register(kayak.KCall{})
	gob.Register(kayak.KReturn{})
	gob.Register(kayak.KRequest{})
	gob.Register(kayak.KJob{})
	gob.Register(kayak.KTicket{})
	gob.Register(kayak.KResponse{})
	gob.Register(kayak.KBonjour{})
	gob.Register(kayak.KWhatsup{})
	gob.Register(kayak.KPropose{})
	gob.Register(kayak.KWrite{})
	gob.Register(kayak.KAccept{})
	gob.Register(kayak.KSuspect{})
	gob.Register(kayak.KHead{})
	gob.Register(kayak.KTip{})
	gob.Register(kayak.KNeed{})
	gob.Register(kayak.KEnsure{})
	gob.Register(kayak.KChunk{})
	gob.Register(kayak.KConfirm{})
	gob.Register(kayak.KStatus{})

	flag.IntVar(&fMe, "me", 0, "tcp port to use by the process")
	flag.StringVar(&fOthers, "others", "", "comma-separated list of other processes, identified by their tcp ports")
}

func main() {
	flag.Parse()

	if fMe <= 0 || fMe >= 10000 {
		fmt.Println("`me' should be defined as a correct TCP port in the range [1;9999]")
		printUsageAndExit()
	}

	me := portToAddress(fMe)

	peers := []kayak.KAddress{me}
	if fOthers != "" {
		othersStrings := strings.Split(fOthers, ",")
	OUTER_LOOP:
		for _, otherPeerStr := range othersStrings {
			var err error
			peerInt, err := strconv.Atoi(otherPeerStr)
			if err != nil {
				fmt.Printf("cannot convert `%s' to an integer\n", otherPeerStr)
				printUsageAndExit()
			}
			if peerInt <= 0 || peerInt >= 10000 {
				fmt.Printf("value %d is not a valid TCP port in the range [1;9999]\n", peerInt)
				printUsageAndExit()
			}

			peer := portToAddress(peerInt)
			for _, added := range peers {
				if peer == added {
					continue OUTER_LOOP
				}
			}
			peers = append(peers, peer)
		}
	}

	sort.Slice(peers, func(i, j int) bool {
		for p := range peers[i] {
			if peers[i][p] < peers[j][p] {
				return true
			}
		}
		return false
	})

	log.Printf("staring process %d at 127.0.0.1:%d with total of %d process(s)...\n", fMe, fMe, len(peers))

	storage := Storage{}

	network_in := make(chan Packet, 100)  // Buffer to avoid deadlocks
	network_out := make(chan Packet, 100) // Buffer to avoid deadlocks

	k := kayak.NewKayak(&kayak.KServerConfig{
		Key:            me,
		Keys:           peers,
		Storage:        &storage,
		RequestT:       10,
		CallT:          20,
		WhatsupT:       100,
		BonjourT:       100,
		IndexTolerance: 100,
		SendF: func(to kayak.KAddress, payload interface{}) {
			network_out <- Packet{
				From:    me,
				To:      to,
				Payload: payload,
			}
		},
		ReturnF: func(payload interface{}) {},
		TraceF: func(payload interface{}) {
			log.Printf("%#v: %s", me, payload)
		},
		ErrorF: func(e error) {
			log.Printf("%#v: %s", me, e)
		},
	})

	go func() {
		for packet := range network_in {
			k.ReceiveNet(packet.From, packet.Payload)
		}
	}()

	go func() {
		for packet := range network_out {
			port := addressToPort(packet.To)

			conn, err := net.Dial("udp", "127.0.0.1:"+strconv.Itoa(port))
			if err != nil {
				log.Println("failed to dial: ", err)
				continue
			}
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err = enc.Encode(packet)
			if err != nil {
				log.Println("failed to encode: ", err)
				continue
			}
			buf.WriteTo(conn)
			conn.Close()
		}
	}()

	ticker := time.NewTicker(time.Second)
	go func() {
		for range ticker.C {
			k.Tick(uint(1))
		}
	}()

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:"+strconv.Itoa(fMe))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("error: ", err)
	}

	go func() {
		data := make([]byte, 1024)
		for {
			n, _, err := conn.ReadFromUDP(data)
			if err != nil {
				fmt.Println("error reading from UDP: ", err)
			}

			buf := bytes.NewBuffer(data[0:n])

			dec := gob.NewDecoder(buf)
			var packet Packet
			err = dec.Decode(&packet)
			if err != nil {
				log.Println("error decoding received data: ", err)
				continue
			}

			network_in <- packet
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/", http.NotFoundHandler())
	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "%#v\n", k.Status())
	})
	mux.HandleFunc("/log", func(w http.ResponseWriter, req *http.Request) {
		for i := range storage.Entries {
			data := storage.Entries[i]
			if len(data) == len(kayak.MagicAddProcess)+kayak.AddressSize && bytes.Equal(data[:len(kayak.MagicAddProcess)], kayak.MagicAddProcess[:]) {
				var key kayak.KAddress
				copy(key[:], data[len(kayak.MagicAddProcess):])
				port := addressToPort(key)
				fmt.Fprintf(w, "%d: [add %d]\n", i, port)
			} else if len(data) == len(kayak.MagicRemoveProcess)+kayak.AddressSize && bytes.Equal(data[:len(kayak.MagicRemoveProcess)], kayak.MagicRemoveProcess[:]) {
				var key kayak.KAddress
				copy(key[:], data[len(kayak.MagicRemoveProcess):])
				port := addressToPort(key)
				fmt.Fprintf(w, "%d: [expel %d]\n", i, port)
			} else {
				fmt.Fprintf(w, "%d: %s\n", i, storage.Entries[i])
			}
		}
	})
	mux.HandleFunc("/append", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			fmt.Fprintf(w, "only POST allowed")
			return
		}
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println("error reading request: ", err)
		}
		k.ReceiveCall(kayak.KCall{
			Tag:     0,
			Payload: data,
		})
	})
	mux.HandleFunc("/add", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			fmt.Fprintf(w, "only POST allowed")
			return
		}
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println("error reading request: ", err)
		}
		port, err := strconv.Atoi(string(data))
		if err != nil {
			log.Println("error parsing port number: ", err)
		}
		key := portToAddress(port)
		k.ReceiveCall(kayak.KCall{
			Tag:     0,
			Payload: append(kayak.MagicAddProcess[:], key[:]...),
		})
	})
	mux.HandleFunc("/expel", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			fmt.Fprintf(w, "only POST allowed")
			return
		}
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println("error reading request: ", err)
		}
		port, err := strconv.Atoi(string(data))
		if err != nil {
			log.Println("error parsing port number: ", err)
		}
		key := portToAddress(port)
		k.ReceiveCall(kayak.KCall{
			Tag:     0,
			Payload: append(kayak.MagicRemoveProcess[:], key[:]...),
		})
	})

	s := &http.Server{
		Addr:           "127.0.0.1:" + strconv.Itoa(fMe),
		Handler:        mux,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())

}

func portToAddress(portInt int) kayak.KAddress {
	address := kayak.KAddress{}

	portInt %= 10000
	a := portInt / 1000
	portInt %= 1000
	b := portInt / 100
	portInt %= 100
	c := portInt / 10
	d := portInt % 10

	address[0] = byte(a*16 + b)
	address[1] = byte(c*16 + d)

	return address
}

func addressToPort(address kayak.KAddress) int {
	a := int(address[0] / 16)
	b := int(address[0] % 16)
	c := int(address[1] / 16)
	d := int(address[1] % 16)

	return a*1000 + b*100 + c*10 + d
}

func printUsageAndExit() {
	fmt.Printf(`
Usage:
        kayakexample -me port [-others portlist]

Flags:
`)
	flag.PrintDefaults()
	fmt.Printf(`
Examples:
        kayakexample -me 9001
        kayakexample -me 9003 -others 9001,9002
`)

	os.Exit(1)
}
