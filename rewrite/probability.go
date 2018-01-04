package rewrite

import (
	"github.com/hscells/groove/analysis/probability"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
)

func ProbabilisticRewrite(query groove.PipelineQuery, s stats.StatisticsSource, measurement probability.ProbabilisticMeasurement, transformation preprocess.Transformation, minimumPredictions probability.PredictionPair) (groove.PipelineQuery, error) {
	result, err := measurement.Compute(query, s)
	if err != nil {
		return groove.PipelineQuery{}, err
	}

	if result.Addition.RecallProbability > minimumPredictions.RecallProbability {

	}

	return query, nil
}
