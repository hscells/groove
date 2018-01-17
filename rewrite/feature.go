package rewrite

import (
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/cqr"
	"io"
	"fmt"
)

// Feature is some value that is applicable to a query transformation.
type Feature struct {
	Id          byte
	Index       byte
	Score       float64
	MaxFeatures byte
}

// FeatureFamily is the group of features used to learn or predict a score.
type FeatureFamily []Feature

// LearntFeature contains the features that were used to produce a particular score.
type LearntFeature struct {
	FeatureFamily
	Score float64
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

	for i, qpp := range predictor {
		score, err := qpp.Execute(query, ss)
		if err != nil {
			return nil, err
		}
		f = append(f, NewFeature16(0xf0, byte(i), score))
	}

	return
}

func (lf LearntFeature) WriteLibSVM(writer io.Writer) (int, error) {
	features := make(map[int64]float64)
	for _, feature := range lf.FeatureFamily {
		index := int64(feature.Id + feature.Index)
		features[index] = feature.Score
	}

	line := fmt.Sprintf("%v", lf.Score)
	for index, value := range features {
		line += fmt.Sprintf(" %v:%v", index, value)
	}

	return writer.Write([]byte(line + "\n"))
}

// AverageScore compute the average feature score for a group of features.
func (ff FeatureFamily) AverageScore() float64 {
	if len(ff) == 0 {
		return 0
	}

	totalScore := 0.0
	for _, f := range ff {
		totalScore += f.Score
	}

	if totalScore == 0 {
		return 0
	}

	return totalScore / float64(len(ff))
}

// Append adds the most recent query transformation to the chain and updates the current query.
func (t TransformedQuery) Append(query groove.PipelineQuery) TransformedQuery {
	t.QueryChain = append(t.QueryChain, t.Query.Transformed())
	t.Query = query
	return t
}

// NewFeature is a constructor for a feature.
func NewFeature16(id byte, index byte, score float64) Feature {
	return Feature{
		Id:          id,
		Index:       index,
		Score:       score,
		MaxFeatures: 0xf,
	}
}

// NewFeatureFamily creates a family of features alongside global features.
func NewFeatureFamily(query groove.PipelineQuery, ss stats.StatisticsSource, features ...Feature) (FeatureFamily, error) {
	ff := FeatureFamily{}
	globalFeatures, err := ComputeGlobalFeatures(query, ss)
	if err != nil {
		return ff, err
	}

	ff = append(ff, features...)
	ff = append(ff, globalFeatures...)
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
