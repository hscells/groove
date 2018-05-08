package postqpp

import (
	"github.com/hscells/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"math"
)

type normalisedQueryCommitment struct{}

// NormalisedQueryCommitment NOT IMPLEMENTED.
var NormalisedQueryCommitment = normalisedQueryCommitment{}

func (normalisedQueryCommitment) Name() string {
	return "NormalisedQueryCommitment"
}

func (normalisedQueryCommitment) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	results, err := s.Execute(q, s.SearchOptions())
	if err != nil {
		return 0.0, nil
	}

	// Handle the case that the query retrieves less than k documents.
	k := s.Parameters()["k"]
	if float64(len(results)) < k {
		k = float64(len(results))
	}
	if k < 1 {
		k = 1
	}

	D := results[len(results)-1].Score

	return 1.0 / D * math.Sqrt(sum(k, results)), nil

}

func mu(k float64, results trecresults.ResultList) float64 {
	score := 0.0

	for i, result := range results {
		if float64(i) > k {
			break
		}
		score += result.Score
	}

	return (1.0 / k) * score
}

func sum(k float64, results trecresults.ResultList) (score float64) {
	mu := mu(k, results)

	for i, result := range results {
		if float64(i) > k {
			break
		}

		d := result.Score
		score += (1.0 / k) * math.Pow(d-mu, 2)
	}

	return
}
