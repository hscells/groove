package preqpp

import (
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
)

// RetrievalSize is the total number of results retrieved for a query.
var RetrievalSize = retrievalSize{}

type retrievalSize struct{}

func (retrievalSize) Name() string {
	return "RetrievalSize"
}

func (retrievalSize) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return s.RetrievalSize(q.Query)
}
