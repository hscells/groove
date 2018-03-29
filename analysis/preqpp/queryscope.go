package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"math"
	"github.com/hscells/groove/analysis"
)

type queryScope struct{}

// QueryScope aims to measure the specificity of a query. e.g. "Olympic Games" versus a specific query
// "Olympics Sydney". Query scope is defined as the ratio of the number of documents that contain at least one of the
// query terms (N_Q) to the number of documents in the collection (N).
var QueryScope = queryScope{}

func (qs queryScope) Name() string {
	return "QueryScope"
}

func (qs queryScope) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	Nq, err := s.RetrievalSize(q.Query)
	if err != nil {
		return 0.0, err
	}
	fields := analysis.QueryFields(q.Query)
	var N float64
	for _, field := range fields {
		n, err := s.VocabularySize(field)
		if err != nil {
			return 0.0, err
		}
		N += n
	}
	return -math.Log((1.0 + Nq) / (1.0 + N)), nil
}
