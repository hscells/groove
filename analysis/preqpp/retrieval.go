package preqpp

import (
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
)

var RetrievalSize = retrievalSize{}

type retrievalSize struct{}

func (retrievalSize) Name() string {
	return "RetrievalSize"
}

func (retrievalSize) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return s.RetrievalSize(q.Query)
}
