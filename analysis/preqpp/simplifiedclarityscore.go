package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"math"
)

type simplifiedClarityScore struct{}

// SimplifiedClarityScore (SCS) aims to measure the intrinsic clarity or ambiguity of a query. SCS does this by
// computing the maximum likelihood of a query language model of the term w in query Q:
var SimplifiedClarityScore = simplifiedClarityScore{}

func (qs simplifiedClarityScore) Name() string {
	return "SimplifiedClarityScore"
}

func (qs simplifiedClarityScore) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	m := float64(len(analysis.QueryTerms(q.Query)))
	avgICTFPredictor := AvgICTF
	avgICTF, err := avgICTFPredictor.Execute(q, s)
	if err != nil {
		return 0.0, err
	}
	return math.Log2(1.0/m) + avgICTF, nil
}
