// Package analysis provides measurements and analysis tools for queries.
package analysis

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/fields"
	"github.com/peterbourgon/diskv"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
)

var mu sync.Mutex

// Measurement is a representation for how a measurement fits into the pipeline.
type Measurement interface {
	// Name is the name of the measurement in the output. It should not contain any spaces.
	Name() string
	// Execute computes the implemented measurement for a query and optionally using the specified statistics.
	Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error)
}

// MeasurementCacher is a cache that can store the measurements for queries.
type MeasurementCacher interface {
	Read(key string) ([]byte, error)
	Write(key string, val []byte) error
}

// MemoryMeasurementCache caches measurement results in memory.
type MemoryMeasurementCache map[string][]byte

// Read reads a measurement result from memory.
func (m MemoryMeasurementCache) Read(key string) ([]byte, error) {
	if v, ok := m[key]; ok {
		return v, nil
	}
	return nil, combinator.ErrCacheMiss
}

// Write writes a measurement result to memory.
func (m MemoryMeasurementCache) Write(key string, val []byte) error {
	m[key] = val
	return nil
}

// MeasurementExecutor executes measurements while caching the results to improve performance.
type MeasurementExecutor struct {
	cache MeasurementCacher
}

// NewDiskMeasurementExecutor creates a measurement executor that caches to disk.
func NewDiskMeasurementExecutor(d *diskv.Diskv) MeasurementExecutor {
	return MeasurementExecutor{
		cache: d,
	}
}

// NewMemoryMeasurementExecutor creates a measurement executor that caches to memory.
func NewMemoryMeasurementExecutor() MeasurementExecutor {
	return MeasurementExecutor{
		cache: make(MemoryMeasurementCache),
	}
}

// hash hashes a query and measurement pair ready to be cached.
func hash(representation cqr.CommonQueryRepresentation, measurement Measurement) string {
	if representation == nil {
		return "0"
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(representation.String()+measurement.Name())))
	//h := fnv.New32()
	//h.Write([]byte(representation.String() + measurement.Name()))
	//return strconv.Itoa(int(h.Sum32()))
}

// Execute executes the specified measurements on the query using the statistics source.
func (m MeasurementExecutor) Execute(query pipeline.Query, ss stats.StatisticsSource, measurements ...Measurement) ([]float64, error) {
	results := make([]float64, len(measurements))
	for i, measurement := range measurements {
		qHash := hash(query.Query, measurement)
		if v, err := m.cache.Read(qHash); err == nil && len(v) > 0 {
			bits := binary.BigEndian.Uint64(v)
			f := math.Float64frombits(bits)
			results[i] = f
			continue
		} else if reflect.TypeOf(err) != reflect.TypeOf(&os.PathError{}) {
			return nil, err
		}

		var v float64
		v, err := measurement.Execute(query, ss)
		if err != nil {
			return nil, err
		}
		results[i] = v
		buff := make([]byte, 8)
		binary.BigEndian.PutUint64(buff[:], math.Float64bits(v))
		mu.Lock()
		m.cache.Write(qHash, buff)
		mu.Unlock()
	}
	return results, nil
}

// QueryTerms extracts the terms from a query.
func QueryTerms(r cqr.CommonQueryRepresentation) (terms []string) {
	for _, keyword := range QueryKeywords(r) {
		terms = append(terms, strings.Split(keyword.QueryString, " ")...)
	}
	return
}

// QueryFields extracts the fields from a query.
func QueryFields(r cqr.CommonQueryRepresentation) (fields []string) {
	switch q := r.(type) {
	case cqr.Keyword:
		return q.Fields
	case cqr.BooleanQuery:
		for _, c := range q.Children {
			fields = append(fields, QueryFields(c)...)
		}
	}
	return
}

// QueryFields extracts the fields from a query.
func QueryFieldsOfField(r cqr.CommonQueryRepresentation, field string) (fields int) {
	switch q := r.(type) {
	case cqr.Keyword:
		for _, f := range q.Fields {
			if field == f {
				return 1
			}
		}
		return 0
	case cqr.BooleanQuery:
		for _, c := range q.Children {
			fields += QueryFieldsOfField(c, field)
		}
	}
	return
}

// QueryKeywords extracts the keywords from a query.
func QueryKeywords(r cqr.CommonQueryRepresentation) (keywords []cqr.Keyword) {
	switch q := r.(type) {
	case cqr.Keyword:
		keywords = append(keywords, q)
	case cqr.BooleanQuery:
		for _, child := range q.Children {
			keywords = append(keywords, QueryKeywords(child)...)
		}
	}
	return
}

// QueryBooleanQueries extracts all of the sub-queries from a Boolean query, recursively.
func QueryBooleanQueries(r cqr.CommonQueryRepresentation) (children []cqr.BooleanQuery) {
	switch q := r.(type) {
	case cqr.BooleanQuery:
		children = append(children, q)
		for _, child := range q.Children {
			switch c := child.(type) {
			case cqr.BooleanQuery:
				children = append(children, c)
				children = append(children, QueryBooleanQueries(c)...)
			}
		}
	}
	return
}

// QueryBooleanClauses extracts all of the clauses from a Boolean query, recursively.
func QueryBooleanClauses(r cqr.CommonQueryRepresentation) (children []cqr.BooleanQuery) {
	switch q := r.(type) {
	case cqr.BooleanQuery:
		q.Operator = strings.ToLower(strings.TrimSpace(q.Operator))
		if q.Operator == cqr.AND || q.Operator == cqr.OR || q.Operator == cqr.NOT {
			children = append(children, q)
		} else if strings.Contains(q.Operator, "adj") {
			children = append(children, q)
		}
		for _, child := range q.Children {
			switch c := child.(type) {
			case cqr.BooleanQuery:
				children = append(children, QueryBooleanClauses(c)...)
			}
		}
	}
	return
}

// QueryBooleanClauses extracts all of the clauses from a Boolean query, recursively.
func QueryBooleanClauseCount(r cqr.CommonQueryRepresentation, operator string) (n int) {
	switch q := r.(type) {
	case cqr.BooleanQuery:
		q.Operator = strings.TrimSpace(q.Operator)
		if strings.ToLower(operator) == strings.ToLower(cqr.AND) && strings.Contains(strings.ToLower(q.Operator), "adj") {
			n++
		} else if strings.ToLower(q.Operator) == strings.ToLower(operator) {
			n++
		}
		for _, child := range q.Children {
			switch c := child.(type) {
			case cqr.BooleanQuery:
				n += QueryBooleanClauseCount(c, operator)
			}
		}
	}
	return
}

// MeshKeywords extracts all keywords which have a particular field.
func KeywordsWithField(r cqr.CommonQueryRepresentation, f string) (k []cqr.Keyword) {
	keywords := QueryKeywords(r)
	for _, kw := range keywords {
		for _, field := range kw.Fields {
			if field == f {
				k = append(k, kw)
				break
			}
		}
	}
	return
}

// MeshExplodedKeywords extracts all mesh heading keywords that are exploded.
func MeshExplodedKeywords(r cqr.CommonQueryRepresentation) (exploded []cqr.Keyword) {
	keywords := KeywordsWithField(r, fields.MeshHeadings)
	for _, kw := range keywords {
		if exp, ok := kw.Options[cqr.ExplodedString]; ok && exp.(bool) {
			exploded = append(exploded, kw)
		}
	}
	return
}

// MeshExplodedKeywords extracts all mesh heading keywords that are not exploded.
func MeshNonExplodedKeywords(r cqr.CommonQueryRepresentation) (exploded []cqr.Keyword) {
	keywords := KeywordsWithField(r, fields.MeshHeadings)
	for _, kw := range keywords {
		if exp, ok := kw.Options[cqr.ExplodedString]; ok && !exp.(bool) {
			exploded = append(exploded, kw)
		}
	}
	return
}

// ExplodedKeywords gets the keywords in the query that are exploded.
func ExplodedKeywords(r cqr.CommonQueryRepresentation) (exploded []cqr.Keyword) {
	keywords := QueryKeywords(r)
	for _, kw := range keywords {
		if option, ok := kw.Options["exploded"]; ok {
			if exp, ok := option.(bool); ok && exp {
				exploded = append(exploded, kw)
			}
		}
	}
	return
}

// TruncatedKeywords gets the keywords in the query that are exploded.
func TruncatedKeywords(r cqr.CommonQueryRepresentation) (truncated []cqr.Keyword) {
	keywords := QueryKeywords(r)
	for _, kw := range keywords {
		if option, ok := kw.Options["truncated"]; ok {
			if trunc, ok := option.(bool); ok && trunc {
				truncated = append(truncated, kw)
			}
		}
	}
	return
}

// ContainsMeshField returns if a keyword query contains a mesh heading field.
func ContainsMeshField(kw cqr.Keyword) bool {
	for _, field := range kw.Fields {
		if field == fields.MeshHeadings {
			return true
		}
	}
	return false
}
