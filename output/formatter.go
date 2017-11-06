package output

type Formatter func(headers []string, data [][]float64) string
