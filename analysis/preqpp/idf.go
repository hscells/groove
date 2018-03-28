package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

type avgIDF struct{}
type sumIDF struct{}
type maxIDF struct{}
type stdDevIDF struct{}

var (
	AvgIDF    = avgIDF{}
	SumIDF    = sumIDF{}
	MaxIDF    = maxIDF{}
	StdDevIDF = stdDevIDF{}
)

func (avg avgIDF) Name() string {
	return "AvgIDF"
}

func (avg avgIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)
	sumIDF := 0.0

	fields := analysis.QueryFields(q.Query)

	for _, field := range fields {
		for _, term := range terms {
			idf, err := s.InverseDocumentFrequency(term, field)
			if err != nil {
				return 0.0, err
			}
			sumIDF += idf
		}
	}

	return sumIDF / float64(len(terms)), nil
}

func (sum sumIDF) Name() string {
	return "SumIDF"
}

func (sum sumIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	sumIDF := 0.0
	fields := analysis.QueryFields(q.Query)

	for _, field := range fields {
		for _, term := range terms {
			idf, err := s.InverseDocumentFrequency(term, field)
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
	terms := analysis.QueryTerms(q.Query)

	var scores []float64
	fields := analysis.QueryFields(q.Query)

	for _, field := range fields {
		for _, term := range terms {
			idf, err := s.InverseDocumentFrequency(term, field)
			if err != nil {
				return 0.0, err
			}
			scores = append(scores, idf)
		}
	}

	return floats.Max(scores), nil
}

func (sum stdDevIDF) Name() string {
	return "StdDevIDF"
}

func (sum stdDevIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	var scores []float64
	fields := analysis.QueryFields(q.Query)

	for _, field := range fields {
		for _, term := range terms {
			idf, err := s.InverseDocumentFrequency(term, field)
			if err != nil {
				return 0.0, err
			}
			scores = append(scores, idf)
		}
	}

	return stat.StdDev(scores, nil), nil
}
