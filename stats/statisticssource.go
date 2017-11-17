package stats

// StatisticsSource represents the way statistics are calculated for a collection.
type StatisticsSource interface {
	TermFrequency(term, document string) (float64, error)
	DocumentFrequency(term, document string) (float64, error)
	TotalTermFrequency(term string) (float64, error)
	InverseDocumentFrequency(term string) (float64, error)
}
