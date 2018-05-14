package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/trecresults"
	"github.com/satori/go.uuid"
	"gopkg.in/olivere/elastic.v5"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ElasticsearchStatisticsSource is a way of gathering statistics for a collection using Elasticsearch.
type ElasticsearchStatisticsSource struct {
	client       *elastic.Client
	documentType string
	index        string
	field        string

	options    SearchOptions
	parameters map[string]float64

	Scroll       bool
	Analyser     string
	AnalyseField string

	wg sync.WaitGroup
}

// SearchOptions gets the immutable search options for the statistics source.
func (es *ElasticsearchStatisticsSource) SearchOptions() SearchOptions {
	return es.options
}

// Parameters gets the immutable parameters for the statistics source.
func (es *ElasticsearchStatisticsSource) Parameters() map[string]float64 {
	return es.parameters
}

// TermFrequency is the term frequency in the field.
func (es *ElasticsearchStatisticsSource) TermFrequency(term, document string) (float64, error) {
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
func (es *ElasticsearchStatisticsSource) DocumentFrequency(term string) (float64, error) {
	analyseField := es.field
	if len(es.AnalyseField) > 0 {
		analyseField = es.field + "." + es.AnalyseField
	}

	resp, err := es.client.TermVectors(es.index, es.documentType).
		Doc(map[string]string{es.field: term}).
		FieldStatistics(false).
		TermStatistics(true).
		Offsets(false).
		Positions(false).
		Payloads(false).
		Fields(analyseField).
		PerFieldAnalyzer(map[string]string{analyseField: ""}).
		Do(context.Background())
	if err != nil {
		return 0, err
	}

	if tv, ok := resp.TermVectors[es.field]; ok {
		return float64(tv.Terms[term].DocFreq), nil
	}

	return 0.0, nil
}

// TotalTermFrequency is a sum of total term frequencies (the sum of total term frequencies of each term in this field).
func (es *ElasticsearchStatisticsSource) TotalTermFrequency(term, field string) (float64, error) {
	docField := strings.Replace(field, es.AnalyseField, "", -1)

	req := es.client.TermVectors(es.index, es.documentType).
		Doc(map[string]string{field: term}).
		TermStatistics(true).
		Offsets(false).
		Positions(false).
		Payloads(false)

	if strings.ContainsRune(term, '*') {
		req = req.PerFieldAnalyzer(map[string]string{docField: "medline_analyser"})
	}

	resp, err := req.Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	if tv, ok := resp.TermVectors[field]; ok {
		t := strings.ToLower(strings.Replace(strings.Replace(strings.Replace(term, "\"", "", -1), "*", "", -1), "~", "", -1))
		return float64(tv.Terms[t].Ttf), nil
	}

	return 0.0, nil
}

// InverseDocumentFrequency is the ratio of of documents in the collection to the number of documents the term appears
// in, logarithmically smoothed.
func (es *ElasticsearchStatisticsSource) InverseDocumentFrequency(term, field string) (float64, error) {
	resp1, err := es.client.IndexStats(es.index).Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	N := resp1.All.Total.Docs.Count

	docField := strings.Replace(field, es.AnalyseField, "", -1)

	req := es.client.TermVectors(es.index, es.documentType).
		Doc(map[string]string{docField: term}).
		FieldStatistics(false).
		TermStatistics(true).
		Offsets(false).
		Positions(false).
		Pretty(false).
		Payloads(false)

	if strings.ContainsRune(term, '*') {
		req = req.PerFieldAnalyzer(map[string]string{docField: "medline_analyser"})
	}

	resp2, err := req.Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	if tv, ok := resp2.TermVectors[field]; ok {
		nt := tv.Terms[term].DocFreq
		if nt == 0 {
			return 0.0, nil
		}
		return idf(float64(N), float64(nt)), nil
	}

	return 0.0, nil
}

// VocabularySize is the total number of terms in the vocabulary.
func (es *ElasticsearchStatisticsSource) VocabularySize(field string) (float64, error) {
	resp, err := es.client.TermVectors(es.index, es.documentType).
		Doc(map[string]string{field: uuid.NewV4().String()}).
		Offsets(false).
		Positions(false).
		Realtime(false).
		Pretty(false).
		Payloads(false).
		Fields(field).
		PerFieldAnalyzer(map[string]string{field: ""}).
		Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	return float64(resp.TermVectors[field].FieldStatistics.SumTtf), nil
}

// RetrievalSize is the minimum number of documents that contains at least one of the query terms.
func (es *ElasticsearchStatisticsSource) RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error) {
	// Transform the query to an Elasticsearch query.
	q, err := toElasticsearch(query)
	if err != nil {
		return 0.0, err
	}

	// Only then can we issue it to Elasticsearch using our API.
	result, err := es.client.Count(es.index).
		Query(elastic.NewRawStringQuery(q)).
		Do(context.Background())
	if err != nil {
		return 0.0, err
	}

	return float64(result), nil
}

// TermVector retrieves the term vector for a document.
func (es *ElasticsearchStatisticsSource) TermVector(document string) (TermVector, error) {
	tv := TermVector{}

	req := es.client.TermVectors(es.index, es.documentType).
		Id(document).
		FieldStatistics(true).
		TermStatistics(true).
		Offsets(false).
		Pretty(false).
		Positions(false).
		Payloads(false).
		Fields("*")

	if len(es.AnalyseField) > 0 {
		req = req.PerFieldAnalyzer(map[string]string{es.field: "medline_analyser"})
	}

	resp, err := req.Do(context.Background())
	if err != nil {
		return tv, err
	}

	for field, vector := range resp.TermVectors {
		for term, vec := range vector.Terms {
			tv = append(tv, TermVectorTerm{
				Term:               term,
				Field:              field,
				DocumentFrequency:  float64(vec.DocFreq),
				TermFrequency:      float64(vec.TermFreq),
				TotalTermFrequency: float64(vec.Ttf),
			})
		}
	}

	return tv, nil
}

// ExecuteFast executes an Elasticsearch query and retrieves only the document ids in the fastest possible way. Do not
// use this for ranked results as the concurrency of this method does not guarantee order.
func (es *ElasticsearchStatisticsSource) ExecuteFast(query groove.PipelineQuery, options SearchOptions) ([]uint32, error) {
	// Transform the query to an Elasticsearch query.
	q, err := toElasticsearch(query.Query)
	if err != nil {
		return nil, err
	}

	// Set the limit to how many goroutines can be run.
	// http://jmoiron.net/blog/limiting-concurrency-in-go/
	concurrency := runtime.NumCPU()
	sem := make(chan bool, concurrency)
	hits := make([][]uint32, concurrency)

	fmt.Println("executing fast", query.Query)
	for i := 0; i < concurrency; i++ {
		sem <- true

		go func(n int) {
			defer func() { <-sem }()
		search:
			hits[n] = []uint32{}

			// Scroll search.
			svc := es.client.Scroll(es.index).
				FetchSource(false).
				Pretty(false).
				Type(es.documentType).
				KeepAlive("10m").
				Slice(elastic.NewSliceQuery().Id(n).Max(concurrency)).
				SearchSource(
				elastic.NewSearchSource().
					NoStoredFields().
					FetchSource(false).
					Size(options.Size).
					Slice(elastic.NewSliceQuery().Id(n).Max(concurrency)).
					TrackScores(false).
					Query(elastic.NewRawStringQuery(q)))

			for {
				result, err := svc.Do(context.Background())
				if err == io.EOF {
					break
				}
				if elastic.IsConnErr(err) {
					fmt.Println(err)
					fmt.Println("retrying...")
					goto search
				}
				if err != nil {
					fmt.Println(err)
					return
				}

				for _, hit := range result.Hits.Hits {
					id, err := strconv.Atoi(hit.Id)
					if err != nil {
						fmt.Println(err)
						return
					}
					hits[n] = append(hits[n], uint32(id))
				}
				fmt.Printf("%v: %v/%v\n", n, len(hits[n]), result.Hits.TotalHits)
			}

			err = svc.Clear(context.Background())
			if err != nil {
				fmt.Println(err)
				//panic(err)
				return
			}
		}(i)

	}

	// Wait until the last goroutine has read from the semaphore.
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	var results []uint32
	for _, hit := range hits {
		results = append(results, hit...)
	}

	fmt.Println("done")

	//for i, h := range ids {
	//	fmt.Println(i)
	//	hits = append(hits, <-h...)
	//}

	fmt.Println(len(results))

	return results, nil
}

// Execute runs the query on Elasticsearch and returns results in trec format.
func (es *ElasticsearchStatisticsSource) Execute(query groove.PipelineQuery, options SearchOptions) (trecresults.ResultList, error) {
	// Transform the query to an Elasticsearch query.
	q, err := toElasticsearch(query.Query)
	if err != nil {
		return nil, err
	}

	// Only then can we issue it to Elasticsearch using our API.
	if es.Scroll {

		var hits []*elastic.SearchHit

		// Scroll search.
		svc := es.client.Scroll(es.index).
			FetchSource(false).
			Pretty(false).
			Type(es.documentType).
			KeepAlive("30m").
			SearchSource(
			elastic.NewSearchSource().
				NoStoredFields().
				FetchSource(false).
				Size(options.Size).
				TrackScores(false).
				Query(elastic.NewRawStringQuery(q)))

		for {
			result, err := svc.Do(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			hits = append(hits, result.Hits.Hits...)
		}

		err = svc.Clear(context.Background())
		if err != nil {
			panic(err)
		}

		fmt.Println(len(hits))

		// Block until all the channels have completed.
		results := make(trecresults.ResultList, len(hits))
		for i, hit := range hits {
			results[i] = &trecresults.Result{
				Topic:     query.Topic,
				Iteration: "Q0",
				DocId:     hit.Id,
				Rank:      int64(i),
				Score:     *hit.Score,
				RunName:   options.RunName,
			}
		}

		return results, nil
	}

	// Regular search.
	result, err := es.client.Search(es.index).
		Index(es.index).
		Type(es.documentType).
		Query(elastic.NewRawStringQuery(q)).
		Size(options.Size).
		NoStoredFields().
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	// Construct the results from the Elasticsearch hits.
	N := len(result.Hits.Hits)
	results := make(trecresults.ResultList, N)
	for i, hit := range result.Hits.Hits {
		results[i] = &trecresults.Result{
			Topic:     query.Topic,
			Iteration: "Q0",
			DocId:     hit.Id,
			Rank:      int64(i),
			Score:     *hit.Score,
			RunName:   options.RunName,
		}
	}

	return results, nil
}

// Analyse is a specific Elasticsearch method used in the analyse transformation.
func (es *ElasticsearchStatisticsSource) Analyse(text, analyser string) (tokens []string, err error) {
	res, err := es.client.IndexAnalyze().Index(es.index).Analyzer(analyser).Text(text).Do(context.Background())
	if err != nil {
		return
	}
	for _, token := range res.Tokens {
		tokens = append(tokens, token.Token)
	}
	return
}

// toElasticsearch transforms a cqr query into an Elasticsearch query.
func toElasticsearch(query cqr.CommonQueryRepresentation) (string, error) {
	var result map[string]interface{}
	// For a Boolean query, it gets a little tricky.
	// First we need to get the string representation of the cqr.
	repr, err := backend.NewCQRQuery(query).StringPretty()
	if err != nil {
		return "", err
	}

	// Then we need to compile it into an Elasticsearch query.
	p := pipeline.NewPipeline(
		parser.NewCQRParser(),
		backend.NewElasticsearchCompiler(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: true,
			},
			RequiresLexing: false,
		})
	esQuery, err := p.Execute(repr)
	if err != nil {
		return "", err
	}
	// After that, we need to unmarshal it to get the underlying structure.
	var tmpQuery map[string]interface{}
	s, err := esQuery.String()
	if err != nil {
		return "", err
	}
	err = json.Unmarshal(bytes.NewBufferString(s).Bytes(), &tmpQuery)
	if err != nil {
		return "", err
	}
	result = tmpQuery["query"].(map[string]interface{})

	b, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return bytes.NewBuffer(b).String(), nil
}

// ElasticsearchHosts sets the hosts for the Elasticsearch client.
func ElasticsearchHosts(hosts ...string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		var err error
		if len(hosts) == 0 {
			es.client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"))
			if err != nil {
				panic(err)
			}
		} else {
			es.client, err = elastic.NewClient(elastic.SetURL(hosts...))
			if err != nil {
				panic(err)
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

// ElasticsearchSearchOptions sets the search options for the statistic source.
func ElasticsearchSearchOptions(options SearchOptions) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.options = options
		return
	}
}

// ElasticsearchParameters sets the parameters for the statistic source.
func ElasticsearchParameters(params map[string]float64) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.parameters = params
		return
	}
}

// ElasticsearchAnalyser sets the analyser for the statistic source.
func ElasticsearchAnalyser(analyser string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.Analyser = analyser
		return
	}
}

// ElasticsearchAnalysedField sets the analyser for the statistic source.
func ElasticsearchAnalysedField(field string) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.AnalyseField = field
		return
	}
}

// ElasticsearchScroll sets the scroll for the statistic source.
func ElasticsearchScroll(scroll bool) func(*ElasticsearchStatisticsSource) {
	return func(es *ElasticsearchStatisticsSource) {
		es.Scroll = scroll
		return
	}
}

// NewElasticsearchStatisticsSource creates a new ElasticsearchStatisticsSource using functional options.
func NewElasticsearchStatisticsSource(options ...func(*ElasticsearchStatisticsSource)) *ElasticsearchStatisticsSource {
	es := &ElasticsearchStatisticsSource{}

	if len(options) == 0 {
		var err error

		es.client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"),
			elastic.SetSniff(false),
			elastic.SetHealthcheckTimeout(1*time.Hour))
		if err != nil {
			panic(err)
		}
	} else {
		for _, option := range options {
			option(es)
		}
	}

	return es
}
