package output

import (
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

	v, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	return string(v)
}
