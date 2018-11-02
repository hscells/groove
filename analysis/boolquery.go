package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/fields"
)

var (
	BooleanFields          booleanFields
	BooleanKeywords        booleanKeywords
	BooleanClauses         booleanClauses
	BooleanAtomicNonAtomic booleanAtomicNonAtomic
	BooleanTruncated       booleanTruncated
	BooleanFieldsTitle     booleanFieldsTitle
	BooleanFieldsAbstract  booleanFieldsAbstract
	BooleanFieldsMeSH      booleanFieldsMeSH
	BooleanFieldsOther     booleanFieldsOther
	BooleanAndCount        booleanAndCount
	BooleanOrCount         booleanOrCount
	BooleanNotCount        booleanNotCount
)

type booleanNotCount struct{}

func (booleanNotCount) Name() string {
	return "BooleanNotCount"
}

func (booleanNotCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryBooleanClauseCount(q.Query, cqr.NOT)), nil
}

type booleanOrCount struct{}

func (booleanOrCount) Name() string {
	return "BooleanOrCount"
}

func (booleanOrCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryBooleanClauseCount(q.Query, cqr.OR)), nil
}

type booleanAndCount struct{}

func (booleanAndCount) Name() string {
	return "BooleanAndCount"
}

func (booleanAndCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryBooleanClauseCount(q.Query, cqr.AND)), nil
}

type booleanFieldsTitle struct{}

func (booleanFieldsTitle) Name() string {
	return "BooleanFieldsTitle"
}

func (booleanFieldsTitle) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryFieldsOfField(q.Query, fields.Title)), nil
}

type booleanFieldsAbstract struct{}

func (booleanFieldsAbstract) Name() string {
	return "BooleanFieldsAbstract"
}

func (booleanFieldsAbstract) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryFieldsOfField(q.Query, fields.Abstract)), nil
}

type booleanFieldsMeSH struct{}

func (booleanFieldsMeSH) Name() string {
	return "BooleanFieldsMeSH"
}

func (booleanFieldsMeSH) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(QueryFieldsOfField(q.Query, fields.MeSHTerms) +
		QueryFieldsOfField(q.Query, fields.MeSHSubheading) +
		QueryFieldsOfField(q.Query, fields.MeshHeadings) +
		QueryFieldsOfField(q.Query, fields.MeSHMajorTopic)), nil
}

type booleanFieldsOther struct{}

func (booleanFieldsOther) Name() string {
	return "BooleanFieldsOther"
}

func (booleanFieldsOther) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(
		len(QueryFields(q.Query)) - (QueryFieldsOfField(q.Query, fields.Abstract) +
			QueryFieldsOfField(q.Query, fields.Title) +
			QueryFieldsOfField(q.Query, fields.MeSHTerms) +
			QueryFieldsOfField(q.Query, fields.MeSHSubheading) +
			QueryFieldsOfField(q.Query, fields.MeshHeadings) +
			QueryFieldsOfField(q.Query, fields.MeSHMajorTopic))), nil
}

type booleanAtomicNonAtomic struct{}

func (booleanAtomicNonAtomic) Name() string {
	return "BooleanAtomicNonAtomic"
}

func (booleanAtomicNonAtomic) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanClauses(q.Query))), nil
}

type booleanFields struct{}

func (booleanFields) Name() string {
	return "BooleanFields"
}

func (booleanFields) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryFields(q.Query))), nil
}

type booleanKeywords struct{}

func (booleanKeywords) Name() string {
	return "BooleanKeywords"
}

func (booleanKeywords) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryKeywords(q.Query))), nil
}

type booleanClauses struct{}

func (booleanClauses) Name() string {
	return "BooleanClauses"
}

func (booleanClauses) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(QueryBooleanQueries(q.Query))), nil
}

type booleanTruncated struct{}

func (booleanTruncated) Name() string {
	return "BooleanTruncated"
}

func (booleanTruncated) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
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
