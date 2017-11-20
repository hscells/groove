// Package output provides different formats of output for experiments.
package output

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"log"
	"strconv"
)

type Formatter func(headers []string, data [][]float64) (s string)

// BasicFormatter outputs the headers and data in a very basic format.
func BasicFormatter(headers []string, data [][]float64) (s string) {
	for _, h := range headers {
		s += h
	}
	for i, cols := range data {
		for j := range cols {
			s += strconv.FormatFloat(data[i][j], 'f', -1, 64)
		}
	}
	return
}

// JsonFormatter outputs results in a JSON format.
func JsonFormatter(headers []string, data [][]float64) string {
	m := map[string][]float64{}
	for i, header := range headers {
		m[header] = make([]float64, len(data[i]))
		for j := range data[i] {
			m[header][j] = data[i][j]
		}
	}

	v, err := json.MarshalIndent(m, "", "    ")
	if err != nil {
		log.Fatalln(err)
	}
	return string(v)
}

// CsvFormatter outputs results in CSV format.
func CsvFormatter(headers []string, data [][]float64) string {
	b := bytes.NewBufferString("")
	w := csv.NewWriter(b)
	w.Write(headers)
	for j := range data[0] {
		record := make([]string, len(data))
		for i := range data {
			record[i] = strconv.FormatFloat(data[i][j], 'f', -1, 64)
		}
		w.Write(record)
	}
	w.Flush()
	return b.String()
}
