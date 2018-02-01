package rewrite

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/stats"
	"io"
)

// Feature is some value that is applicable to a query transformation.
type Feature struct {
	ID          byte
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
	QueryChain    []cqr.CommonQueryRepresentation
	PipelineQuery groove.PipelineQuery
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

// CompactFeatureSVM compacts a feature into a dense format.
func CompactFeatureSVM(id, index, max byte) byte {
	c := id + index
	if id+index <= 1 {
		return c
	}
	switch c {
	case 0x10:
		return 0x3
	case 0x11:
		return 0x4
	case 0x20:
		return 0x5
	case 0x21:
		return 0x6
	case 0x30:
		return 0x7
	case 0x31:
		return 0x8
	case 0x40:
		return 0x9
	default:
		return 0x0
	}
}

// String returns the string of a feature family.
func (ff FeatureFamily) String() string {
	var s string
	for _, f := range ff {
		s += fmt.Sprintf("%v:%v ", f.ID+f.Index, f.Score)
	}
	return s
}

// WriteLibSVM writes a LIBSVM compatible line to a writer.
func (lf LearntFeature) WriteLibSVM(writer io.Writer, comment ...interface{}) (int, error) {
	line := fmt.Sprintf("%v", lf.Score)
	for _, f := range lf.FeatureFamily {
		line += fmt.Sprintf(" %v:%v", CompactFeatureSVM(f.ID, f.Index, f.MaxFeatures), f.Score)
	}
	if len(comment) > 0 {
		line += " #"
		for _, c := range comment {
			line += fmt.Sprintf(" %v", c)
		}
	}

	return writer.Write([]byte(line + "\n"))
}

// WriteLibSVMRank writes a LIBSVM^rank compatible line to a writer.
func (lf LearntFeature) WriteLibSVMRank(writer io.Writer, topic int64, comment string) (int, error) {
	line := fmt.Sprintf("%v qid:%v", lf.Score, topic)
	for _, f := range lf.FeatureFamily {
		b := CompactFeatureSVM(f.ID, f.Index, f.MaxFeatures)
		line += fmt.Sprintf(" %v:%v", b+1, f.Score)
	}
	line += " # " + comment

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
	t.QueryChain = append(t.QueryChain, t.PipelineQuery.Query)
	t.PipelineQuery = query
	return t
}

// NewFeature16 is a constructor for a feature containing a maximum of 16 features to a feature family.
func NewFeature16(id byte, index byte, score float64) Feature {
	return Feature{
		ID:          id,
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

// NewTransformedQuery creates a new transformed query.
func NewTransformedQuery(query groove.PipelineQuery, chain ...cqr.CommonQueryRepresentation) TransformedQuery {
	return TransformedQuery{
		QueryChain:    chain,
		PipelineQuery: query,
	}
}

// NewCandidateQuery creates a new candidate query.
func NewCandidateQuery(query cqr.CommonQueryRepresentation, ff FeatureFamily) CandidateQuery {
	return CandidateQuery{
		FeatureFamily: ff,
		Query:         query,
	}
}
