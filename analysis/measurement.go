// Package analysis provides measurements and analysis tools for queries.
package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"strings"
	"github.com/hashicorp/golang-lru"
	"hash/fnv"
)

// Measurement is a representation for how a measurement fits into the pipeline.
type Measurement interface {
	// Name is the name of the measurement in the output. It should not contain any spaces.
	Name() string
	// Execute computes the implemented measurement for a query and optionally using the specified statistics.
	Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error)
}

type MeasurementExecutor struct {
	*lru.Cache
}

func NewMeasurementExecutor(cacheSize int) MeasurementExecutor {
	l, _ := lru.New(cacheSize)
	return MeasurementExecutor{
		l,
	}
}

func hash(representation cqr.CommonQueryRepresentation, measurement Measurement) uint32 {
	h := fnv.New32()
	h.Write([]byte(representation.String() + measurement.Name()))
	return h.Sum32()
}

func (m MeasurementExecutor) Execute(query groove.PipelineQuery, ss stats.StatisticsSource, measurements ...Measurement) (results []float64, err error) {
	results = make([]float64, len(measurements))
	for i, measurement := range measurements {
		if v, ok := m.Get(hash(query.Query, measurement)); ok {
			results[i] = v.(float64)
			continue
		}

		var v float64
		v, err = measurement.Execute(query, ss)
		if err != nil {
			return
		}
		results[i] = v
		m.Add(hash(query.Query, measurement), v)
	}
	return
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
