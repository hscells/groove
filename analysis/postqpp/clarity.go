package postqpp

import (
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/stat"
)

type clarityScore struct{}

// ClarityScore aims to measure how much the query language model diverges from the collection language model.
var ClarityScore = clarityScore{}

func (clarityScore) Name() string {
	return "ClarityScore"
}

func (clarityScore) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	lambda, ok := s.Parameters()["lambda"]
	if !ok {
		lambda = 0.6
	}

	results, err := s.Execute(q, s.SearchOptions())
	if err != nil {
		return 0.0, nil
	}

	N := len(results)
	docIds := make([]string, N)
	scores := make([]float64, N)
	weights := make([]float64, N)

	for i, result := range results {
		docIds[i] = result.DocId
		scores[i] = result.Score
	}

	avgScore := stat.Mean(scores, nil)

	for i, score := range scores {
		weights[i] = (score / avgScore) / float64(N)
	}

	lm, err := stats.NewLanguageModel(s, docIds, scores, "tiab", stats.LanguageModelWeights(weights))
	if err != nil {
		return 0.0, err
	}

	return lm.KLDivergence(lambda, stats.JelinekMercerTermProbability(lambda))
}
