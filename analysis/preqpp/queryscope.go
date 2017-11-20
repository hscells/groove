package preqpp

import (
	"github.com/hscells/groove/stats"
	"math"
	"github.com/hscells/groove"
)

// QueryScope aims to measure the specificity of a query. e.g. "Olympic Games" versus a specific query
// "Olympics Sydney". Query scope is defined as the ratio of the number of documents that contain at least one of the
// query terms (N_Q) to the number of documents in the collection (N).
type QueryScope struct{}

func (qs QueryScope) Name() string {
	return "QueryScope"
}

func (qs QueryScope) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	Nq, err := s.RetrievalSize(q.Original())
	if err != nil {
		return 0.0, err
	}
	N, err := s.VocabularySize()
	if err != nil {
		return 0.0, err
	}
	return -math.Log((Nq + 1) / N), nil
}
