package kayak

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	// "log"
)

// encoding/gob is not stable for hashing
// even github.com/dave/stablegob seems not to be stable
// JSON is not the best choice, but at least it provides the desired features
// TODO: replace JSON with better encoding
func hash(obj interface{}) KHash {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	err := encoder.Encode(obj)
	if err != nil {
		panic("cannot encode object")
	}

	data := buffer.Bytes()
	h := sha256.Sum256(data)

	// log.Printf("hashing %#v represented as %s with hash %X", obj, data, h)
	return h
}

func cumDataHash(base KHash, items ...KData) KHash {
	var err error
	cumHash := base
	for _, item := range items {
		hasher := sha256.New()
		_, err = hasher.Write(cumHash[:])
		if err != nil {
			panic("cannot hash object")
		}
		_, err = hasher.Write(item)
		if err != nil {
			panic("cannot hash object")
		}
		copy(cumHash[:], hasher.Sum(nil))
	}

	return cumHash
}

func cumBuzzHash(base KHash, items ...KHash) KHash {
	var err error
	cumHash := base
	for _, item := range items {
		hasher := sha256.New()
		_, err = hasher.Write(cumHash[:])
		if err != nil {
			panic("cannot hash object")
		}
		_, err = hasher.Write(item[:])
		if err != nil {
			panic("cannot hash object")
		}
		copy(cumHash[:], hasher.Sum(nil))
	}

	return cumHash
}
