package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/pkg/errors"
	"reflect"
)

// QueryComplexity is a measure of how "complex" a Boolean query is. It takes into account the number of keywords and
// Boolean queries in each of the top-level sub-contexts, with relation to the number of sub-contexts, and the size of
// the context (or query as a whole).
type QueryComplexity struct{}

// Name is QueryComplexity
func (qc QueryComplexity) Name() string {
	return "QueryComplexity"
}

// Execute computes query complexity.
func (qc QueryComplexity) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	// Ensure we are looking at a Boolean query.
	if q.Type() != reflect.TypeOf(cqr.BooleanQuery{}) {
		return 0.0, errors.New("query complexity must be used in conjunction with a Boolean query")
	}

	// Grab the context.
	context := q.(cqr.BooleanQuery)

	// Calculate the number of keywords and Boolean queries for each top-level sub-context.
	var cqSum, ckSum float64
	for _, subquery := range context.Children {
		cqSum += float64(len(QueryBooleanQueries(subquery)))
		ckSum += float64(len(QueryKeywords(subquery)))
	}

	querySize := float64(len(QueryBooleanQueries(context))) + float64(len(QueryKeywords(context)))

	// Compute the query complexity.
	return ((cqSum / ckSum) / float64(len(context.Children))) * querySize, nil
}
