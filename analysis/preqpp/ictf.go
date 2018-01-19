package preqpp

import (
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"math"
)

type avgICTF struct{}

// AvgICTF is similar to idf, however it attempts to take into account the term frequencies. Inverse collection term
// frequency is defined as the ratio of unique terms in the collection to the term frequency of a term in a document,
// logarithmically smoothed.
var AvgICTF = avgICTF{}

func (avgi avgICTF) Name() string {
	return "AvgICTF"
}

func (avgi avgICTF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Query)

	W, err := s.VocabularySize()
	if err != nil {
		return 0.0, err
	}

	sumICTF := 0.0
	for _, term := range terms {
		tf, err := s.TotalTermFrequency(term)
		if err != nil {
			return 0.0, err
		}
		sumICTF += math.Log2(W) - math.Log2(1+tf)
	}

	return (1.0 / float64(len(terms))) * sumICTF, nil
}
