package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"math"
)

// ElasticsearchStatisticsSource is a way of gathering statistics for a collection using Elasticsearch.
type ElasticsearchStatisticsSource struct {
	client       *elastic.Client
	documentType string
	index        string
	field        string
}

// TermFrequency is the term frequency in the field.
func (es ElasticsearchStatisticsSource) TermFrequency(term, document string) (float64, error) {
	resp, err := es.client.TermVectors(es.index, es.documentType).Id(document).Do(context.Background())
	if err != nil {
		return 0, err
	}

	if tv, ok := resp.TermVectors[es.field]; ok {
		return float64(tv.Terms[term].TermFreq), nil
	}

	return 0.0, nil
}

// DocumentFrequency is the document frequency (the number of documents containing the current term).
func (es ElasticsearchStatisticsSource) DocumentFrequency(term string) (float64, error) {
	resp, err := es.client.TermVectors(es.index, es.documentType).Doc(map[string]string{es.field: term}).Do(context.Background())
	if err != nil {
		return 0, err
	}

	if tv, ok := resp.TermVectors[es.field]; ok {
		return float64(tv.Terms[term].DocFreq), nil
	}

	return 0.0, nil
}

//TotalTermFrequency is a sum of total term frequencies (the sum of total term frequencies of each term in this field).
func (es ElasticsearchStatisticsSource) TotalTermFrequency(term string) (float64, error) {
	resp, err := es.client.TermVectors(es.index, es.documentType).
		TermStatistics(true).
		Doc(map[string]string{es.field: term}).
		Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	if tv, ok := resp.TermVectors[es.field]; ok {
		return float64(tv.Terms[term].Ttf), nil
	}

	return 0.0, nil
}

// InverseDocumentFrequency is the ratio of of documents in the collection to the number of documents the term appears
// in, logarithmically smoothed.
func (es ElasticsearchStatisticsSource) InverseDocumentFrequency(term string) (float64, error) {
	resp1, err := es.client.IndexStats(es.index).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	N := resp1.All.Total.Docs.Count

	resp2, err := es.client.TermVectors(es.index, es.documentType).
		FieldStatistics(true).
		TermStatistics(true).
		Doc(map[string]string{es.field: term}).
		Do(context.Background())
	if err != nil {
		return 0, err
	}

	if tv, ok := resp2.TermVectors[es.field]; ok {
		nt := tv.Terms[term].DocFreq
		if nt == 0 {
			return 0.0, nil
		}
		return math.Log(float64(N) / float64(nt)), nil
	}

	return 0.0, nil
}

// VocabularySize is the total number of terms in the vocabulary.
func (es ElasticsearchStatisticsSource) VocabularySize() (float64, error) {
	resp, err := es.client.TermVectors(es.index, es.documentType).
		Doc(map[string]string{es.field: "garbage"}).
		Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	return float64(resp.TermVectors[es.field].FieldStatistics.SumTtf), nil
}

// RetrievalSize is the minimum number of documents that contains at least one of the query terms.
func (es ElasticsearchStatisticsSource) RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error) {
	switch q := query.(type) {
	case cqr.Keyword:
		// When we have a keyword query, we can just use a multi match (in case there are multiple fields).
		result, err := es.client.Search(es.index).
			Query(elastic.NewMultiMatchQuery(q.QueryString, q.Fields...)).
			Do(context.Background())
		if err != nil {
			return 0.0, err
		}
		return float64(result.Hits.TotalHits), nil
	case cqr.BooleanQuery:
		// For a Boolean query, it gets a little tricky.
		// First we need to get the string representation of the cqr.
		repr := backend.NewCQRQuery(q).StringPretty()
		// Then we need to compile it into an Elasticsearch query.
		p := pipeline.NewPipeline(parser.NewCQRParser(), backend.NewElasticsearchCompiler(), pipeline.TransmutePipelineOptions{RequiresLexing: false})
		esQuery, err := p.Execute(repr)
		if err != nil {
			return 0.0, err
		}
		// After that, we need to unmarshal it to get the underlying structure.
		var tmpQuery map[string]interface{}
		err = json.Unmarshal(bytes.NewBufferString(esQuery.String()).Bytes(), &tmpQuery)
		if err != nil {
			return 0.0, err
		}
		// So that we can get rid of the outer "query".
		tmpQuery = tmpQuery["query"].(map[string]interface{})
		byteQuery, err := json.MarshalIndent(tmpQuery, "", " ")
		if err != nil {
			return 0.0, err
		}
		// Only then can we issue it to Elasticsearch using our API.
		result, err := es.client.Search(es.index).
			Query(elastic.NewRawStringQuery(bytes.NewBuffer(byteQuery).String())).
			Do(context.Background())
		if err != nil {
			return 0.0, err
		}
		return float64(result.Hits.TotalHits), nil
	}
	return 0.0, nil
}

// ElasticsearchHosts sets the hosts for the Elasticsearch client.
func ElasticsearchHosts(hosts ...string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		var err error
		if len(hosts) == 0 {
			es.client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"))
			if err != nil {
				log.Fatal(err)
			}
		} else {
			esHosts := make([]string, len(hosts))
			for i, host := range hosts {
				esHosts[i] = host
			}
			es.client, err = elastic.NewClient(elastic.SetURL(esHosts...))
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}
}

// ElasticsearchDocumentType sets the document type for the Elasticsearch client.
func ElasticsearchDocumentType(documentType string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.documentType = documentType
		return
	}
}

// ElasticsearchIndex sets the index for the Elasticsearch client.
func ElasticsearchIndex(index string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.index = index
		return
	}
}

// ElasticsearchField sets the field for the Elasticsearch client.
func ElasticsearchField(field string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.field = field
		return
	}
}

// NewElasticsearchStatisticsSource creates a new ElasticsearchStatisticsSource using functional options.
func NewElasticsearchStatisticsSource(options ...func(*ElasticsearchStatisticsSource)) *ElasticsearchStatisticsSource {
	es := ElasticsearchStatisticsSource{}

	if len(options) == 0 {
		var err error

		es.client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		for _, option := range options {
			option(&es)
		}
	}

	return &es
}
