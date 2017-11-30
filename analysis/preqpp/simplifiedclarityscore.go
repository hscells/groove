package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"math"
)

// SimplifiedClarityScore (SCS) aims to measure the intrinsic clarity or ambiguity of a query. SCS does this by
// computing the maximum likelihood of a query language model of the term w in query Q:
type SimplifiedClarityScore struct{}

func (qs SimplifiedClarityScore) Name() string {
	return "SimplifiedClarityScore"
}

func (qs SimplifiedClarityScore) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	m := float64(len(analysis.QueryTerms(q.Transformed())))
	avgICTFPredictor := AvgICTF{}
	avgICTF, err := avgICTFPredictor.Execute(q, s)
	if err != nil {
		return 0.0, err
	}
	return math.Log2(1.0/m) + avgICTF, nil
}
