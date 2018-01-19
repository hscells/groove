package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
	"math"
)

type summedCollectionQuerySimilarity struct{}
type maxCollectionQuerySimilarity struct{}
type averageCollectionQuerySimilarity struct{}

var (
	// SummedCollectionQuerySimilarity (CQS) combines the collection term frequencies (cf (w)) and inverse document
	// frequency (idf (w)). The summed collection query similarity (SCQS) is a QPP in a family of predictors, much like how
	// idf can be summarised and used as a predictor
	SummedCollectionQuerySimilarity = summedCollectionQuerySimilarity{}
	// MaxCollectionQuerySimilarity is similar to SummedCollectionQuerySimilarity except, as the name implies, it computes
	// the maximum value rather than the sum.
	MaxCollectionQuerySimilarity     = maxCollectionQuerySimilarity{}
	AverageCollectionQuerySimilarity = averageCollectionQuerySimilarity{}
)

func (sc summedCollectionQuerySimilarity) Name() string {
	return "SummedCollectionQuerySimilarity"
}

func (sc summedCollectionQuerySimilarity) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	sumSCQ := 0.0
	for _, term := range terms {
		s, err := collectionQuerySimilarity(term, s)
		if err != nil {
			return 0.0, err
		}
		sumSCQ += s
	}

	return sumSCQ, nil
}

func (sc maxCollectionQuerySimilarity) Name() string {
	return "MaxCollectionQuerySimilarity"
}

func (sc maxCollectionQuerySimilarity) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	var scq []float64
	for _, term := range terms {
		s, err := collectionQuerySimilarity(term, s)
		if err != nil {
			return 0.0, err
		}
		scq = append(scq, s)
	}

	return floats.Max(scq), nil
}

func (sc averageCollectionQuerySimilarity) Name() string {
	return "AverageCollectionQuerySimilarity"
}

func (sc averageCollectionQuerySimilarity) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	var scq []float64
	for _, term := range terms {
		s, err := collectionQuerySimilarity(term, s)
		if err != nil {
			return 0.0, err
		}
		scq = append(scq, s)
	}

	return stat.Mean(scq, nil), nil
}

func collectionQuerySimilarity(term string, s stats.StatisticsSource) (float64, error) {
	tf, err := s.TotalTermFrequency(term)
	if err != nil {
		return 0.0, err
	}
	idf, err := s.InverseDocumentFrequency(term)
	if err != nil {
		return 0.0, err
	}
	return (1.0 + math.Log(1.0+tf)) * math.Log(1.0+idf), nil
}
