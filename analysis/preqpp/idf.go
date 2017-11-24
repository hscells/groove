package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

type AvgIDF struct{}
type SumIDF struct{}
type MaxIDF struct{}
type StdDevIDF struct{}

func (avg AvgIDF) Name() string {
	return "AvgIDF"
}

func (avg AvgIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	sumIdf := 0.0
	for _, term := range terms {
		idf, err := s.InverseDocumentFrequency(term)
		if err != nil {
			return 0.0, err
		}
		sumIdf += idf
	}

	return sumIdf / float64(len(terms)), nil
}

func (sum SumIDF) Name() string {
	return "SumIDF"
}

func (sum SumIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	sumIdf := 0.0
	for _, term := range terms {
		idf, err := s.InverseDocumentFrequency(term)
		if err != nil {
			return 0.0, err
		}
		sumIdf += idf
	}

	return sumIdf, nil
}

func (sum MaxIDF) Name() string {
	return "MaxIDF"
}

func (sum MaxIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	scores := []float64{}
	for _, term := range terms {
		idf, err := s.InverseDocumentFrequency(term)
		if err != nil {
			return 0.0, err
		}
		scores = append(scores, idf)
	}

	return floats.Max(scores), nil
}

func (sum StdDevIDF) Name() string {
	return "StdDevIDF"
}

func (sum StdDevIDF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	scores := []float64{}
	for _, term := range terms {
		idf, err := s.InverseDocumentFrequency(term)
		if err != nil {
			return 0.0, err
		}
		scores = append(scores, idf)
	}

	return stat.StdDev(scores, nil), nil
}
