// Package probability provides abstractions for how to compute how precision and recall is affected by measurements.
package probability

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
)

// Probability computes the likelihood of a Measurement affecting precision and recall. For this interface
// to be properly implemented, four probabilities must be implemented: how the Measurement affects retrieval
// performance if the query is rewritten to add or remove aspects that increase or decrease the Measurement.
type Probability interface {
	ComputeAdditionProbability(float64) PredictionPair
	ComputeReductionProbability(float64) PredictionPair
}

// ProbabilisticMeasurement is the union of a Measurement (i.e. some QPP), and a probability (i.e. how this
// Measurement affects retrieval performance).
type ProbabilisticMeasurement struct {
	analysis.Measurement
	Probability
}

// PredictionPair is a pair of probabilities from predicting how a Measurement affects retrieval performance.
type PredictionPair struct {
	PrecisionProbability float64
	RecallProbability    float64
}

// NewPredictionPair creates a new prediction pair.
func NewPredictionPair(precision, recall float64) PredictionPair {
	return PredictionPair{
		PrecisionProbability: precision,
		RecallProbability:    recall,
	}
}

// ProbabilisticMeasurementResult is the result of computing the likelihoods for how a Measurement affects
// precision and recall by adding and removing to a query.
type ProbabilisticMeasurementResult struct {
	Measurement float64
	Addition    PredictionPair
	Reduction   PredictionPair
}

// NewProbabilisticMeasurement creates a ProbabilisticMeasurement from a single Probability implementation.
// This should ensure at compile-time that a ProbabilisticMeasurement has both interfaces implemented.
func NewProbabilisticMeasurement(measurement Probability) ProbabilisticMeasurement {
	return ProbabilisticMeasurement{
		Probability: measurement,
		Measurement: measurement.(analysis.Measurement),
	}
}

// Compute a Measurement and how that Measurement affects precision and recall.
func (pm ProbabilisticMeasurement) Compute(q groove.PipelineQuery, s stats.StatisticsSource) (ProbabilisticMeasurementResult, error) {
	measurement, err := pm.Measurement.Execute(q, s)
	if err != nil {
		return ProbabilisticMeasurementResult{}, err
	}

	addition := pm.Probability.ComputeAdditionProbability(measurement)
	reduction := pm.Probability.ComputeReductionProbability(measurement)

	return ProbabilisticMeasurementResult{
		Measurement: measurement,
		Addition:    addition,
		Reduction:   reduction,
	}, nil
}
