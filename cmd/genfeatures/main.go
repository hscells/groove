package main

import (
	"github.com/hscells/groove/combinator"
	"github.com/peterbourgon/diskv"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove"
	"github.com/hscells/groove/eval"
	"github.com/TimothyJones/trecresults"
	"bytes"
	"os"
	"io/ioutil"
	"sync"
	"log"
	"github.com/hscells/groove/analysis"
)

func main() {
	// TODO this should come from command line argument.
	// Load the qrels from file.
	b, err := ioutil.ReadFile("sigir2018medline.qrels")
	if err != nil {
		panic(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

	// TODO this should come from command line argument.
	// Load the queries from directory.
	cqrPipeline := pipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})
	qs := query.NewTransmuteQuerySource(cqrPipeline)
	queries, err := qs.Load("/Users/harryscells/gocode/src/github.com/hscells/boogie/testing")
	if err != nil {
		panic(err)
	}

	// Configure the Statistics Source.
	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{Size: 10000, RunName: "test"}))

	// Transformers and evaluators.
	transformations := []rewrite.Transformer{rewrite.NewLogicalOperatorTransformer(), rewrite.NewAdjacencyReplacementTransformer(), rewrite.NewAdjacencyRangeTransformer(), rewrite.NewMeSHExplosionTransformer(), rewrite.NewFieldRestrictionsTransformer()}
	evaluators := []eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator}

	// Cache for queries and the documents they retrieve.
	cache := combinator.NewDiskvQueryCache(diskv.New(diskv.Options{
		BasePath:     "../cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	}))

	// TODO make this come from command line arguments.
	// Load the file that features will be written to.
	f, err := os.OpenFile("precision.features", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Measurement Executor
	me := analysis.NewMeasurementExecutor(1024 * 4080)

	// We would like to generate a large amount of training data for the learning to rank

	/*
	1. Generate candidates.
	2. Measure all candidates.
	3. Generate new candidates from generated candidates.
	4. Repeat process N time.
	 */

	var mu sync.Mutex
	var wg sync.WaitGroup

	// TODO make this come from command line arguments.
	nTimes := 5

	for i := 0; i < nTimes; i++ {
		log.Println("loop #", i)
		for _, candidateQuery := range queries {
			wg.Add(1)
			go func(q groove.PipelineQuery) {
				defer wg.Done()

				candidates, err := rewrite.Variations(q.Query, ss, me, transformations...)
				if err != nil {
					panic(err)
				}
				for _, candidate := range candidates {
					queries = append(queries, groove.NewPipelineQuery(q.Name, q.Topic, candidate.Query))
					go func(c rewrite.CandidateQuery) {
						gq := groove.NewPipelineQuery(q.Name, q.Topic, c.Query)
						tree, _, err := combinator.NewLogicalTree(gq, ss, cache)
						if err != nil {
							panic(err)
						}
						results := tree.Documents().Results(gq, "features")
						evaluation := eval.Evaluate(evaluators, &results, qrels, gq.Topic)
						lf := rewrite.NewLearntFeature(evaluation["Precision"], c.Features)

						var buff bytes.Buffer
						_, err = lf.WriteLibSVMRank(&buff, gq.Topic, gq.Name)
						if err != nil {
							panic(err)
						}

						mu.Lock()
						f.Write(buff.Bytes())
						mu.Unlock()
					}(candidate)
				}
			}(candidateQuery)
		}
		wg.Wait()
	}
}
