// Package output provides different formats of output for experiments.
package output

import (
	"encoding/json"
)

// EvaluationFormatter is used in the a groove pipeline to output evaluation results.
type EvaluationFormatter func(map[int64]map[string]float64) (string, error)

// JsonMeasurementFormatter outputs results in a JSON format.
func JsonEvaluationFormatter(results map[int64]map[string]float64) (string, error) {
	v, err := json.MarshalIndent(results, "", "    ")
	if err != nil {
		return "", err
	}
	return string(v), nil
}
