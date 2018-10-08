package analysis

import (
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
)

type termCount struct{}

var (
	// TermCount is a measurement that counts the number of terms in the query.
	TermCount = termCount{}
)

// Name is TermCount.
func (tc termCount) Name() string {
	return "TermCount"
}

// TermCount counts the total number of terms in a query. If a Keyword has more than one terms, it will split it and
// count each individual term in that query string.
func (tc termCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryTerms(q.Query))), nil
}
