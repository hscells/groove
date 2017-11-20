package analysis

import (
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
)

// TermCount is a measurement that counts the number of terms in the query.
type TermCount struct{}

// KeywordCount is a measurement that counts the number of keywords in the query.
type KeywordCount struct{}

// BooleanQueryCount is a measure that counts the number of Boolean queries in the query.
type BooleanQueryCount struct{}

// Name is TermCount.
func (tc TermCount) Name() string {
	return "TermCount"
}

// TermCount counts the total number of terms in a query. If a Keyword has more than one terms, it will split it and
// count each individual term in that query string.
func (tc TermCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryTerms(q.Processed()))), nil
}

// Name is KeywordCount.
func (kc KeywordCount) Name() string {
	return "KeywordCount"
}

// TermCount counts the total number of keywords in a query.
func (kc KeywordCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryKeywords(q.Processed()))), nil
}

// Name is BooleanQueryCount.
func (bc BooleanQueryCount) Name() string {
	return "BooleanQueryCount"
}

// BooleanQueryCount counts the total number of Boolean queries in a query.
func (bc BooleanQueryCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanQueries(q.Processed()))), nil
}
