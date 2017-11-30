package postqpp

import (
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"math"
)

type WeightedInformationGain struct{}
type WeightedExpansionGain struct{}

func (wig WeightedInformationGain) Name() string {
	return "WIG"
}

func (wig WeightedInformationGain) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	queryLength := float64(len(analysis.QueryTerms(q.Transformed())))
	results, err := s.Execute(q, s.SearchOptions())
	if err != nil {
		return 0.0, err
	}
	if len(results) == 0 {
		return 0.0, nil
	}
	D := results[len(results)-1].Score
	totalScore := 0.0

	k := s.Parameters()["k"]
	if float64(len(results)) < k {
		k = float64(len(results))
	}
	if k < 1 {
		k = 1
	}

	for _, result := range results {
		d := result.Score
		totalScore += (1.0 / math.Sqrt(queryLength)) * (d - D)
	}

	return (1.0 / k) * totalScore, nil
}

func (wig WeightedExpansionGain) Name() string {
	return "WEG"
}

func (wig WeightedExpansionGain) cnprf(k float64, results trecresults.ResultList) float64 {
	n := len(results) - int(k)
	nprf := 0.0
	for _, result := range results[n:] {
		nprf += result.Score
	}
	return nprf / float64(len(results[n:]))
}

func (wig WeightedExpansionGain) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	queryLength := float64(len(analysis.QueryTerms(q.Transformed())))
	results, err := s.Execute(q, s.SearchOptions())
	if err != nil {
		return 0.0, err
	}
	if len(results) == 0 {
		return 0.0, nil
	}

	k := s.Parameters()["k"]
	if float64(len(results)) < k {
		k = float64(len(results))
	}
	if k < 1 {
		k = 1
	}

	D := wig.cnprf(k, results)
	totalScore := 0.0

	for _, result := range results {
		d := result.Score
		totalScore += (1.0 / math.Sqrt(queryLength)) * (d - D)
	}

	return (1.0 / k) * totalScore, nil
}
