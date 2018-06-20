package kayak

import (
	"crypto/sha256"
	"fmt"
)

const (
	ConsensusStateIdle = KConsensusState(iota)
	ConsensusStateIdlePropose
	ConsensusStateProposeWrite
	ConsensusStateWriteAccept
)
const (
	LCStateIdle = KLCState(iota)
	LCStateAlert
)

const NonceSize = 16
const AddressSize = 32

const (
	ByzantineFlagIgnoreRequestsFromClientCC01 = 1 << iota
	ByzantineFlagSendDifferentProposes
	ByzantineFlagClientFixNonce
)

var MagicAddProcess = [32]byte{
	0xFA, 0xCE, 0xF5, 0x0B, 0xFC, 0xDA, 0xD3, 0x29,
	0xF4, 0x0E, 0xE6, 0x79, 0x3B, 0x0D, 0x5D, 0xA8,
	0xB7, 0x6F, 0xCA, 0xF8, 0x20, 0x78, 0x71, 0x71,
	0x0A, 0xFC, 0x39, 0x19, 0xBB, 0x6E, 0x3C, 0x25,
}

var MagicRemoveProcess = [32]byte{
	0xDE, 0xAD, 0x0A, 0x3C, 0x83, 0x9F, 0x43, 0xAB,
	0xC1, 0x11, 0x74, 0x70, 0xC0, 0x19, 0x48, 0x8F,
	0xE2, 0x07, 0x59, 0x74, 0xB0, 0x66, 0x48, 0xFF,
	0x20, 0x63, 0x26, 0x30, 0x28, 0x71, 0xEA, 0x68,
}

type KRound uint
type KIndex = KRound
type KEpoch uint
type KTime uint
type KData []byte

type KHash [sha256.Size]byte
type KNonce [NonceSize]byte
type KAddress [AddressSize]byte

type KConsensusState int
type KLCState int

type KStorage interface {
	Append([]byte)
}

type KServerConfig struct {
	Key            KAddress
	Keys           []KAddress
	Storage        KStorage
	RequestT       uint
	CallT          uint
	WhatsupT       uint
	BonjourT       uint
	IndexTolerance uint
	SendF          func(to KAddress, payload interface{})
	ReturnF        func(payload interface{})
	TraceF         func(payload interface{})
	ErrorF         func(error)
	ByzantineFlags int
}

type KClientConfig struct {
	Key            KAddress
	ServerKeys     []KAddress
	CallT          uint
	BonjourT       uint
	SendF          func(to KAddress, payload interface{})
	ReturnF        func(payload interface{})
	TraceF         func(payload interface{})
	ErrorF         func(error)
	ByzantineFlags int
}

type KCall struct {
	Tag     int
	Payload KData
}

type KReturn struct {
	Tag     int
	Index   KIndex
	Timeout bool
}

type KRequest struct {
	Nonce   KNonce
	Payload KData
	Index   KIndex
}

type KJob struct {
	From      KAddress
	Timestamp KTime
	Request   KRequest
}

type KTicket struct {
	Nonce     KNonce
	Tag       int
	Timestamp KTime
	Payload   KData
}

type KResponse struct {
	Index KRound
	Nonce KNonce
	// TODO
	// ErrorIDReplay bool
	// ErrorIDAhead  bool
	// ErrorInvalid  bool
}

type KPropose struct {
	Round KRound
	Epoch KEpoch
	Job   KJob
}

type KWrite struct {
	Round KRound
	Epoch KEpoch
	Hash  KHash
}

type KAccept struct {
	Round KRound
	Epoch KEpoch
	Hash  KHash
}

type KLoad struct {
	From    KAddress
	Request KRequest
}

type KSuspect struct {
	Epoch KEpoch
	Loads []KLoad
}

type KWhatsup struct{}
type KBonjour struct{}

type KHead struct {
	Round KRound
	Epoch KEpoch
}

type KTip struct {
	Round KRound
}

type KNeed struct {
	Last  KRound
	First KRound
}

type KEnsure struct {
	Last KRound
}

type KChunk struct {
	Last KRound
	Data []KData
	Buzz []KHash
}

type KConfirm struct {
	Last     KRound
	DataHash KHash
	BuzzHash KHash
}

type KStatus struct {
	Round  KRound
	Epoch  KEpoch
	Leader KAddress
	Keys   []KAddress
}

func (k KRound) GoString() string {
	return fmt.Sprintf("(%4d)", k)
}

func (k KEpoch) GoString() string {
	return fmt.Sprintf("(%-4d)", k)
}

func (k KTime) GoString() string {
	return fmt.Sprintf("‹%4d›", k%10000)
}

func (k KData) GoString() string {
	if len(k) == 0 {
		return "xnil"
	}
	if len(k) == 1 {
		return fmt.Sprintf("%X  ", k)
	}
	return fmt.Sprintf("%X", k[:2])
}

func (k KHash) GoString() string {
	return fmt.Sprintf("0x%X", k[:2])
}

func (k KNonce) GoString() string {
	return fmt.Sprintf("0x%X", k[:2])
}

func (k KAddress) GoString() string {
	return fmt.Sprintf("[%X]", k[:2])
}

func (k KConsensusState) GoString() string {
	switch k {
	case ConsensusStateIdle:
		return "Idle"
	case ConsensusStateIdlePropose:
		return "IdlePropose"
	case ConsensusStateProposeWrite:
		return "ProposeWrite"
	case ConsensusStateWriteAccept:
		return "WriteAccept"
	default:
		return "INVALID"
	}
}

func (k KLCState) GoString() string {
	switch k {
	case LCStateIdle:
		return "Idle"
	case LCStateAlert:
		return "Alert"
	default:
		return "INVALID"
	}
}

func (k KCall) GoString() string {
	return fmt.Sprintf("KCall of %d with payload %#v", k.Tag, k.Payload)
}

func (k KReturn) GoString() string {
	if k.Timeout {
		return fmt.Sprintf("KReturn of %d (timeout)", k.Tag)
	}
	return fmt.Sprintf("KReturn of %d to put at index %d", k.Tag, k.Index)
}

func (k KRequest) GoString() string {
	return fmt.Sprintf("KRequest %#v with payload %#v and index %#v", k.Nonce, k.Payload, k.Index)
}

func (k KJob) GoString() string {
	return fmt.Sprintf("KJob from %#v created at %#v with %#v", k.From, k.Timestamp, k.Request)
}

func (k KTicket) GoString() string {
	return fmt.Sprintf("KTicket %#v with tag %d created at %#v with payload %#v", k.Nonce, k.Tag, k.Timestamp, k.Payload)
}

func (k KResponse) GoString() string {
	return fmt.Sprintf("KResponse %#v at index %d", k.Nonce, k.Index)
}

func (k KPropose) GoString() string {
	return fmt.Sprintf("KPropose (%4d:%-4d) with %#v", k.Round, k.Epoch, k.Job)
}

func (k KWrite) GoString() string {
	return fmt.Sprintf("KWrite (%4d:%-4d) with hash %#v", k.Round, k.Epoch, k.Hash)
}

func (k KAccept) GoString() string {
	return fmt.Sprintf("KAccept (%4d:%-4d) with hash %#v", k.Round, k.Epoch, k.Hash)
}

func (k KLoad) GoString() string {
	return fmt.Sprintf("KLoad from %#v with %#v", k.From, k.Request)
}

func (k KSuspect) GoString() string {
	return fmt.Sprintf("KSuspect to transition to %#v with %d loads", k.Epoch, len(k.Loads))
}

func (k KWhatsup) GoString() string {
	return fmt.Sprintf("KWhatsup")
}

func (k KBonjour) GoString() string {
	return fmt.Sprintf("KBonjour")
}

func (k KHead) GoString() string {
	return fmt.Sprintf("KHead reporting (%4d:%-4d)", k.Round, k.Epoch)
}

func (k KTip) GoString() string {
	return fmt.Sprintf("KTip reporting %#v", k.Round)
}

func (k KNeed) GoString() string {
	return fmt.Sprintf("KNeed requesting data at %#v ... %#v", k.First, k.Last)
}

func (k KEnsure) GoString() string {
	return fmt.Sprintf("KEnsure requesting hash at %#v", k.Last)
}

func (k KChunk) GoString() string {
	return fmt.Sprintf("KChunk with %d entries ending at %#v", len(k.Data), k.Last)
}

func (k KConfirm) GoString() string {
	return fmt.Sprintf("KConfirm of %#v with data hash %#v and buzz hash %#v", k.Last, k.DataHash, k.BuzzHash)
}
