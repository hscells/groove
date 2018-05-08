package combinator

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/hscells/cqr"
	"github.com/peterbourgon/diskv"
	"strconv"
	"fmt"
	"sort"
	"io/ioutil"
	"path"
	"encoding/binary"
	"os"
	"github.com/hashicorp/golang-lru"
)

// ErrCacheMiss indicates that a read did not fail, but the item was not present in the cache.
var ErrCacheMiss = errors.New("cache miss error")

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

// docsToBytes encodes a retrieved set of documents  to bytes.
func docsToBytes(docs Documents) ([]byte, error) {
	if len(docs) == 0 {
		return []byte{}, nil
	}
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

// MapQueryCache caches results to memory.
type MapQueryCache struct {
	m map[uint64]Documents
}

// Get looks up results in a map.
func (m MapQueryCache) Get(query cqr.CommonQueryRepresentation) (Documents, error) {
	if d, ok := m.m[HashCQR(query)]; ok {
		return d, nil
	}
	return Documents{}, ErrCacheMiss
}

// Set caches results to a map.
func (m MapQueryCache) Set(query cqr.CommonQueryRepresentation, docs Documents) error {
	sort.Sort(docs)
	m.m[HashCQR(query)] = docs
	return nil
}

// NewMapQueryCache creates a query cache out of a regular go map.
func NewMapQueryCache() QueryCacher {
	constructor()
	return MapQueryCache{make(map[uint64]Documents)}
}

// DiskvQueryCache caches results using diskv.
type DiskvQueryCache struct {
	*diskv.Diskv
}

// Get looks up results from disk.
func (d DiskvQueryCache) Get(query cqr.CommonQueryRepresentation) (Documents, error) {
	b, err := d.Read(strconv.Itoa(int(HashCQR(query))))
	if err != nil {
		return Documents{}, ErrCacheMiss
	}
	if len(b) == 0 {
		return Documents{}, nil
	}
	dec := gob.NewDecoder(bytes.NewReader(b))
	var c Documents
	err = dec.Decode(&c)
	if err != nil {
		return Documents{}, err
	}
	return c, nil
}

// Set caches results to disk.
func (d DiskvQueryCache) Set(query cqr.CommonQueryRepresentation, docs Documents) error {
	sort.Sort(docs)
	b, err := docsToBytes(docs)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return d.Write(strconv.Itoa(int(HashCQR(query))), b)
}

// NewDiskvQueryCache creates a new on-disk cache with the specified diskv parameters.
func NewDiskvQueryCache(dv *diskv.Diskv) QueryCacher {
	constructor()
	return DiskvQueryCache{dv}
}

// FileQueryCache caches results in a flat-file format in a single directory. This cacher will be faster than diskv as
// it does not use gob encoding.
type FileQueryCache struct {
	path  string
	cache *lru.Cache
}

// NewFileQueryCache creates a new disk-based file query cache.
func NewFileQueryCache(path string) QueryCacher {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		panic(err)
	}
	c, err := lru.New(1000)
	if err != nil {
		panic(err)
	}
	return FileQueryCache{
		path:  path,
		cache: c,
	}
}

// Get looks up results from disk.
func (f FileQueryCache) Get(query cqr.CommonQueryRepresentation) (Documents, error) {
	h := HashCQR(query)
	if v, ok := f.cache.Get(h); ok {
		return v.(Documents), nil
	}

	fn := path.Join(f.path, fmt.Sprintf("%v", h))
	if _, err := os.Stat(fn); err != nil && os.IsNotExist(err) {
		return nil, ErrCacheMiss
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	d := make(Documents, len(b)/4)
	for i, j := 0, 0; i < len(b); i += 4 {
		d[j] = Document(binary.LittleEndian.Uint32(b[i : i+4]))
		j++
	}
	f.cache.Add(h, d)
	return d, nil

}

// Set caches results to disk.
func (f FileQueryCache) Set(query cqr.CommonQueryRepresentation, docs Documents) error {
	sort.Sort(docs)
	h := HashCQR(query)
	f.cache.Add(h, docs)
	b := make([]byte, len(docs)*4)
	i := 0
	for _, id := range docs {
		d := make([]byte, 4)
		binary.LittleEndian.PutUint32(d, uint32(id))
		b[i] = d[0]
		b[i+1] = d[1]
		b[i+2] = d[2]
		b[i+3] = d[3]
		i += 4
	}
	return ioutil.WriteFile(path.Join(f.path, fmt.Sprintf("%v", h)), b, 0644)
}
