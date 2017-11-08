package stats

// ElasticsearchStatisticsSource is a way of gathering statistics for a collection using Elasticsearch.
type ElasticsearchStatisticsSource struct{}

func (es ElasticsearchStatisticsSource) TermFrequency(term string) (float64, error) {
	return 0.0, nil
}

func (es ElasticsearchStatisticsSource) DocumentFrequency(term string) (float64, error) {
	return 0.0, nil
}

func (es ElasticsearchStatisticsSource) TotalTermFrequency(term string) (float64, error) {
	return 0.0, nil
}

func (es ElasticsearchStatisticsSource) InverseDocumentFrequency(term string) (float64, error) {
	return 0.0, nil
}

func NewElasticsearchStatisticsSource() ElasticsearchStatisticsSource {
	return ElasticsearchStatisticsSource{}
}
