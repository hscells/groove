// Package stats provides implementations of statistic sources.
package stats

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
	"math"
	"github.com/hscells/groove"
)

type SearchOptions struct {
	Size    int
	RunName string
}

// StatisticsSource represents the way statistics are calculated for a collection.
type StatisticsSource interface {
	SearchOptions() SearchOptions
	Parameters() map[string]float64

	TermFrequency(term, document string) (float64, error)
	DocumentFrequency(term string) (float64, error)
	TotalTermFrequency(term string) (float64, error)
	InverseDocumentFrequency(term string) (float64, error)
	RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error)
	VocabularySize() (float64, error)
	Execute(query groove.PipelineQuery, options SearchOptions) (trecresults.ResultList, error)
}

// idf calculates inverse document frequency, or the ratio of of documents in the collection to the number of documents
// the term appears in, logarithmically smoothed.
func idf(N, nt float64) float64 {
	return math.Log(1 + (N / nt))
}
