package combinator

import (
	"github.com/hscells/cqr"
	"github.com/peterbourgon/diskv"
	"encoding/gob"
	"bytes"
	"strconv"
	"errors"
)

var CacheMissError = errors.New("cache miss error")

// BlockTransform determines how diskv should partition folders.
func BlockTransform(blockSize int) func(string) []string {
	return func(s string) []string {
		var (
			sliceSize = len(s) / blockSize
			pathSlice = make([]string, sliceSize)
		)
		for i := 0; i < sliceSize; i++ {
			from, to := i*blockSize, (i*blockSize)+blockSize
			pathSlice[i] = s[from:to]
		}
		return pathSlice
	}
}

// ClauseToBytes encodes a clause to bytes.
func ClauseToBytes(docs Documents) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(docs)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func constructor() {
	gob.Register(cqr.Keyword{})
	gob.Register(cqr.BooleanQuery{})
	gob.Register(Clause{})
}

// QueryCacher models a way to cache (either persistent or not) queries and the documents they retrieve.
type QueryCacher interface {
	Get(query cqr.CommonQueryRepresentation) (Documents, error)
	Set(query cqr.CommonQueryRepresentation, docs Documents) error
}

// QueryCache embeds a privately defined query cacher into a public struct.
type QueryCache struct {
	QueryCacher
}

type mapQueryCache struct {
	m map[uint64]Documents
}

func (m mapQueryCache) Get(query cqr.CommonQueryRepresentation) (Documents, error) {
	if d, ok := m.m[HashCQR(query)]; ok {
		return d, nil
	}
	return Documents{}, nil
}

func (m mapQueryCache) Set(query cqr.CommonQueryRepresentation, docs Documents) error {
	m.m[HashCQR(query)] = docs
	return nil
}

// NewMapQueryCache creates a query cache out of a regular go map.
func NewMapQueryCache() QueryCache {
	constructor()
	return QueryCache{mapQueryCache{make(map[uint64]Documents)}}
}

type diskvQueryCache struct {
	*diskv.Diskv
}

func (d diskvQueryCache) Get(query cqr.CommonQueryRepresentation) (Documents, error) {
	b, err := d.Read(strconv.Itoa(int(HashCQR(query))))
	if err != nil {
		return Documents{}, CacheMissError
	}
	dec := gob.NewDecoder(bytes.NewReader(b))
	var c Documents
	err = dec.Decode(&c)
	if err != nil {
		return Documents{}, err
	}
	return c, nil
}

func (d diskvQueryCache) Set(query cqr.CommonQueryRepresentation, docs Documents) error {
	b, err := ClauseToBytes(docs)
	if err != nil {
		return err
	}
	return d.Write(strconv.Itoa(int(HashCQR(query))), b)
}

// NewDiskvQueryCache creates a new on-disk cache with the specified diskv parameters.
func NewDiskvQueryCache(dv *diskv.Diskv) QueryCache {
	constructor()
	return QueryCache{diskvQueryCache{dv}}
}
