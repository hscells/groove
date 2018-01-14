package rewrite

import (
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/cqr"
)

// Feature is some value that is applicable to a query transformation.
type Feature struct {
	Name  string
	Score float64
}

// FeatureFamily is the
type FeatureFamily struct {
	Features []Feature
}

// TransformedQuery is the current most query in the query chain.
type TransformedQuery struct {
	QueryChain []cqr.CommonQueryRepresentation
	Query      groove.PipelineQuery
}

// CandidateQuery is a possible transformation a query can take.
type CandidateQuery struct {
	FeatureFamily
	Query cqr.CommonQueryRepresentation
}

// ComputeGlobalFeatures computes some `global` (i.e. applicable to all transformations/queries) for a query.
func ComputeGlobalFeatures(query groove.PipelineQuery, ss stats.StatisticsSource) (f []Feature, err error) {
	predictor := []analysis.Measurement{preqpp.AvgIDF, preqpp.AvgICTF, preqpp.AverageCollectionQuerySimilarity}

	for _, qpp := range predictor {
		score, err := qpp.Execute(query, ss)
		if err != nil {
			return nil, err
		}
		f = append(f, NewFeature(qpp.Name(), score))
	}

	return
}

// AverageScore compute the average feature score for a group of features.
func (ff FeatureFamily) AverageScore() float64 {
	if len(ff.Features) == 0 {
		return 0
	}

	totalScore := 0.0
	for _, f := range ff.Features {
		totalScore += f.Score
	}

	if totalScore == 0 {
		return 0
	}

	return totalScore / float64(len(ff.Features))
}

// Append adds the most recent query transformation to the chain and updates the current query.
func (t TransformedQuery) Append(query groove.PipelineQuery) TransformedQuery {
	t.QueryChain = append(t.QueryChain, t.Query.Transformed())
	t.Query = query
	return t
}

// NewFeature is a constructor for a feature.
func NewFeature(name string, score float64) Feature {
	return Feature{
		Name:  name,
		Score: score,
	}
}

// NewFeatureFamily creates a family of features alongside global features.
func NewFeatureFamily(query groove.PipelineQuery, ss stats.StatisticsSource, features ...Feature) (FeatureFamily, error) {
	ff := FeatureFamily{}
	globalFeatures, err := ComputeGlobalFeatures(query, ss)
	if err != nil {
		return ff, err
	}

	ff.Features = append(ff.Features, features...)
	ff.Features = append(ff.Features, globalFeatures...)
	return ff, nil
}

func NewTransformedQuery(query groove.PipelineQuery, chain ...cqr.CommonQueryRepresentation) TransformedQuery {
	return TransformedQuery{
		QueryChain: chain,
		Query:      query,
	}
}

func NewCandidateQuery(query cqr.CommonQueryRepresentation, ff FeatureFamily) CandidateQuery {
	return CandidateQuery{
		FeatureFamily: ff,
		Query:         query,
	}
}
