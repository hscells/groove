package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
)

var (
	BooleanFields    booleanFields
	BooleanKeywords  booleanKeywords
	BooleanClauses   booleanClauses
	BooleanExploded  booleanExploded
	BooleanTruncated booleanTruncated
)

type booleanFields struct{}

func (booleanFields) Name() string {
	return "BooleanFields"
}

func (booleanFields) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryFields(q.Query))), nil
}

type booleanKeywords struct{}

func (booleanKeywords) Name() string {
	return "BooleanKeywords"
}

func (booleanKeywords) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryKeywords(q.Query))), nil
}

type booleanClauses struct{}

func (booleanClauses) Name() string {
	return "BooleanClauses"
}

func (booleanClauses) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanQueries(q.Query))), nil
}

type booleanExploded struct{}

func (booleanExploded) Name() string {
	return "BooleanExploded"
}

func (booleanExploded) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(ExplodedKeywords(q.Query))), nil
}

type booleanTruncated struct{}

func (booleanTruncated) Name() string {
	return "BooleanTruncated"
}

func (booleanTruncated) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	return float64(len(TruncatedKeywords(q.Query))), nil
}

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
