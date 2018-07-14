package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
	"math"
)

type avgIDF struct{}
type sumIDF struct{}
type maxIDF struct{}
type stdDevIDF struct{}

var (
	// AvgIDF is the average IDF.
	AvgIDF = avgIDF{}
	// SumIDF is the sum IDF.
	SumIDF = sumIDF{}
	// MaxIDF is the maximum IDF.
	MaxIDF = maxIDF{}
	// StdDevIDF is the standard deviation of the IDF.
	StdDevIDF = stdDevIDF{}
)

func (avg avgIDF) Name() string {
	return "AvgIDF"
}

func (avg avgIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	keywords := analysis.QueryKeywords(q.Query)
	sumIDF := 0.0

	for _, k := range keywords {
		for _, field := range k.Fields {
			idf, err := s.InverseDocumentFrequency(k.QueryString, field)
			if err != nil {
				return 0.0, err
			}
			sumIDF += idf
		}
	}

	return sumIDF / float64(len(keywords)), nil
}

func (sum sumIDF) Name() string {
	return "SumIDF"
}

func (sum sumIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	keywords := analysis.QueryKeywords(q.Query)
	sumIDF := 0.0

	for _, k := range keywords {
		for _, field := range k.Fields {
			idf, err := s.InverseDocumentFrequency(k.QueryString, field)
			if err != nil {
				return 0.0, err
			}
			sumIDF += idf
		}
	}

	return sumIDF, nil
}

func (sum maxIDF) Name() string {
	return "MaxIDF"
}

func (sum maxIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	var scores []float64
	keywords := analysis.QueryKeywords(q.Query)

	for _, k := range keywords {
		for _, field := range k.Fields {
			idf, err := s.InverseDocumentFrequency(k.QueryString, field)
			if err != nil {
				return 0.0, err
			}
			scores = append(scores, idf)
		}
	}

	if len(scores) == 0 {
		return 0, nil
	}

	return floats.Max(scores), nil
}

func (sum stdDevIDF) Name() string {
	return "StdDevIDF"
}

func (sum stdDevIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	var scores []float64
	keywords := analysis.QueryKeywords(q.Query)

	if len(keywords) == 1 {
		return 0, nil
	}

	for _, k := range keywords {
		for _, field := range k.Fields {
			idf, err := s.InverseDocumentFrequency(k.QueryString, field)
			if err != nil {
				return 0.0, err
			}
			scores = append(scores, idf)
		}
	}

	stdDev := stat.StdDev(scores, nil)

	if stdDev > math.Inf(-1) && stdDev < math.Inf(1) && !math.IsNaN(stdDev) {
		return stdDev, nil
	}
	return 0, nil
}
