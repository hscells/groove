package preqpp

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"math"
	"github.com/hscells/groove"
)

// AvgICTF is similar to idf, however it attempts to take into account the term frequencies. Inverse collection term
// frequency is defined as the ratio of unique terms in the collection to the term frequency of a term in a document,
// logarithmically smoothed.
type AvgICTF struct{}

func (avgi AvgICTF) Name() string {
	return "AvgICTF"
}

func ictf(W, cf float64) float64 {
	return math.Log((W - cf) / cf)
}

func (avgi AvgICTF) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	terms := analysis.QueryTerms(q.Processed())

	W, err := s.VocabularySize()
	if err != nil {
		return 0.0, err
	}

	sumICTF := 0.0
	for _, term := range terms {
		cf, err := s.TotalTermFrequency(term)
		if err != nil {
			return 0.0, err
		}
		sumICTF += ictf(W, cf+1)
	}

	return (1.0 / float64(len(terms))) * sumICTF, nil
}
