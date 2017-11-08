package preqpp

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
)

type AvgIDF struct{}
type SumIDF struct{}
type StdDevIDF struct{}

func (avg AvgIDF) Name() string {
	return "AvgIDF"
}

func (avg AvgIDF) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q)

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

func (sum SumIDF) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q)

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
