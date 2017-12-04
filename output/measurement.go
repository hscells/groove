// Package output provides different formats of output for experiments.
package output

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strconv"
)

// MeasurementFormatter is used in the a groove pipeline to output measurements in various formats. These methods should not be
// used directly since there are some assumptions made about the inputs; for instance, the length of each argument.
type MeasurementFormatter func(topics, headers []string, data [][]float64) (string, error)

// JsonMeasurementFormatter outputs results in a JSON format.
func JsonMeasurementFormatter(topics, headers []string, data [][]float64) (string, error) {
	m := map[string]map[string]float64{}
	for j, topic := range topics {
		m[topic] = map[string]float64{}
		for i, header := range headers {
			m[topic][header] = data[i][j]
		}
	}

	v, err := json.MarshalIndent(m, "", "    ")
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// CsvMeasurementFormatter outputs results in CSV format.
func CsvMeasurementFormatter(topics, headers []string, data [][]float64) (string, error) {
	b := bytes.NewBufferString("")
	w := csv.NewWriter(b)
	h := []string{"Topic"}
	h = append(h, headers...)
	w.Write(h)
	for j := range data[0] {
		record := make([]string, len(data)+1)
		record[0] = topics[j]
		for i := range data {
			record[i+1] = strconv.FormatFloat(data[i][j], 'f', -1, 64)
		}
		w.Write(record)
	}
	w.Flush()
	return b.String(), nil
}
