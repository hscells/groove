// Package analysis provides measurements and analysis tools for queries.
package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"strings"
	"github.com/hscells/groove"
)

// Measurement is a representation for how a measurement fits into the pipeline.
type Measurement interface {
	// Name is the name of the measurement in the output. It should not contain any spaces.
	Name() string
	// Execute computes the implemented measurement for a query and optionally using the specified statistics.
	Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error)
}

// QueryTerms extracts the terms from a query.
func QueryTerms(r cqr.CommonQueryRepresentation) (terms []string) {
	for _, keyword := range QueryKeywords(r) {
		terms = append(terms, strings.Split(keyword.QueryString, " ")...)
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
