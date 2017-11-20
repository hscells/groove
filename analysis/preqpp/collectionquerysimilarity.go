package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/floats"
	"math"
)

// SummedCollectionQuerySimilarity (CQS) combines the collection term frequencies (cf (w)) and inverse document
// frequency (idf (w)). The summed collection query similarity (SCQS) is a QPP in a family of predictors, much like how
// idf can be summarised and used as a predictor
type SummedCollectionQuerySimilarity struct{}

// MaxCollectionQuerySimilarity is similar to SummedCollectionQuerySimilarity except, as the name implies, it computes
// the maximum value rather than the sum.
type MaxCollectionQuerySimilarity struct{}

func (sc SummedCollectionQuerySimilarity) Name() string {
	return "SummedCollectionQuerySimilarity"
}

func (sc SummedCollectionQuerySimilarity) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

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

func (sc MaxCollectionQuerySimilarity) Name() string {
	return "MaxCollectionQuerySimilarity"
}

func (sc MaxCollectionQuerySimilarity) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	scq := []float64{}
	for _, term := range terms {
		s, err := collectionQuerySimilarity(term, s)
		if err != nil {
			return 0.0, err
		}
		scq = append(scq, s)
	}

	return floats.Max(scq), nil
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
	return (1.0 + math.Log(1+tf)) * math.Log(1+idf), nil
}
