package preqpp

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/fields"
	"math"
)

type SCQ struct{}

func (scq SCQ) Name() string {
	return "SimilarityCollectionQuery"
}

func (scq SCQ) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)
	sum := 0.0
	N, err := s.CollectionSize()
	if err != nil {
		return 0, err
	}
	for _, term := range terms {
		ttf, err := s.TotalTermFrequency(term, fields.TitleAbstract)
		if err != nil {
			return 0, err
		}
		df, err := s.RetrievalSize(cqr.NewKeyword(term, fields.TitleAbstract))
		if err != nil {
			return 0, err
		}
		sum += (1 + math.Log(ttf)) * math.Log(1+(N/df))
	}
	return sum, nil
}
