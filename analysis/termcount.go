package analysis

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
)

type termCount struct{}
type keywordCount struct{}
type booleanQueryCount struct{}

var (
	// TermCount is a measurement that counts the number of terms in the query.
	TermCount = termCount{}
	// KeywordCount is a measurement that counts the number of keywords in the query.
	KeywordCount = keywordCount{}
	// BooleanQueryCount is a measure that counts the number of Boolean queries in the query.
	BooleanQueryCount = booleanQueryCount{}
)

// Name is TermCount.
func (tc termCount) Name() string {
	return "TermCount"
}

// TermCount counts the total number of terms in a query. If a Keyword has more than one terms, it will split it and
// count each individual term in that query string.
func (tc termCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryTerms(q.Transformed()))), nil
}

// Name is KeywordCount.
func (kc keywordCount) Name() string {
	return "KeywordCount"
}

// TermCount counts the total number of keywords in a query.
func (kc keywordCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryKeywords(q.Transformed()))), nil
}

// Name is BooleanQueryCount.
func (bc booleanQueryCount) Name() string {
	return "BooleanQueryCount"
}

// BooleanQueryCount counts the total number of Boolean queries in a query.
func (bc booleanQueryCount) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanQueries(q.Transformed()))), nil
}
