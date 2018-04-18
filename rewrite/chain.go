// Package rewrite uses query chains to rewrite queries.
package rewrite

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/stats"
)

// QueryChain contains implementations for transformations to apply to a query and the selector to pick a candidate.
type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
	stats.StatisticsSource
	analysis.MeasurementExecutor
}

// QueryChainCandidateSelector describes how transformed queries are chosen from the set of transformations.
type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error)
	StoppingCriteria() bool
}

// LearntCandidateQuery is the serialised struct written from the oracle query chain candidate selector.
type LearntCandidateQuery struct {
	Topic     int64              `json:"topic"`
	Depth     int64              `json:"Depth"`
	Candidate CandidateQuery     `json:"candidate"`
	Eval      map[string]float64 `json:"eval"`
}

// NewQueryChain creates a new query chain with implementations for a selector and transformations.
func NewQueryChain(selector QueryChainCandidateSelector, ss stats.StatisticsSource, me analysis.MeasurementExecutor, transformations ...Transformation) QueryChain {
	return QueryChain{
		CandidateSelector:   selector,
		Transformations:     transformations,
		MeasurementExecutor: me,
		StatisticsSource:    ss,
	}
}

// Execute executes a query chain in full. At each "transition point" in the chain, the candidate selector is queried
// in order to see if the chain should continue or not. At the end of the chain, the selector is cleaned using the
// finalise method.
func (qc QueryChain) Execute(query groove.PipelineQuery) (TransformedQuery, error) {
	var (
		stop bool
	)
	stop = qc.CandidateSelector.StoppingCriteria()
	tq := NewTransformedQuery(query)
	for !stop {
		candidates, err := Variations(NewCandidateQuery(tq.PipelineQuery.Query, nil), qc.StatisticsSource, qc.MeasurementExecutor, qc.Transformations...)
		if err != nil {
			return TransformedQuery{}, err
		}

		tq, qc.CandidateSelector, err = qc.CandidateSelector.Select(tq, candidates)
		if err != nil && err != combinator.CacheMissError {
			return TransformedQuery{}, err
		}
		stop = qc.CandidateSelector.StoppingCriteria()
	}
	return tq, nil
}
