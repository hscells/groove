package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
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
func (tc TermCount) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryTerms(q))), nil
}

// Name is KeywordCount.
func (kc KeywordCount) Name() string {
	return "KeywordCount"
}

// TermCount counts the total number of keywords in a query.
func (kc KeywordCount) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryKeywords(q))), nil
}

// Name is BooleanQueryCount.
func (bc BooleanQueryCount) Name() string {
	return "BooleanQueryCount"
}

// BooleanQueryCount counts the total number of Boolean queries in a query.
func (bc BooleanQueryCount) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanQueries(q))), nil
}
