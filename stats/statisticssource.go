package stats

// StatisticsSource represents the way statistics are calculated for a collection.
type StatisticsSource interface {
	TermFrequency(term string) (float64, error)
	DocumentFrequency(term string) (float64, error)
	TotalTermFrequency(term string) (float64, error)
	InverseDocumentFrequency(term string) (float64, error)
}
