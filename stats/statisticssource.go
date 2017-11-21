// Package stats provides implementations of statistic sources.
package stats

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
)

type SearchOptions struct {
	Size    int
	Topic   int64
	RunName string
}

// StatisticsSource represents the way statistics are calculated for a collection.
type StatisticsSource interface {
	TermFrequency(term, document string) (float64, error)
	DocumentFrequency(term string) (float64, error)
	TotalTermFrequency(term string) (float64, error)
	InverseDocumentFrequency(term string) (float64, error)
	RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error)
	VocabularySize() (float64, error)
	Execute(query cqr.CommonQueryRepresentation, options SearchOptions) (trecresults.ResultList, error)
}
