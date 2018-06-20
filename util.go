package kayak

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
)

// TODO: replace gob with faster custom encoder
func hash(obj interface{}) KHash {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(obj)
	if err != nil {
		panic("cannot encode object")
	}

	return sha256.Sum256(buffer.Bytes())
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
