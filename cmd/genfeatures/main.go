package main

import (
	"github.com/hscells/groove/combinator"
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
	"math/rand"
	"math"
	"fmt"
	"github.com/peterbourgon/diskv"
	"runtime/pprof"
	"runtime/debug"
	"runtime"
)

// sample n% of candidate queries.
func sample(n int, a []rewrite.CandidateQuery) []rewrite.CandidateQuery {
	// shuffle the items to sample.
	s := rand.Perm(len(a))

	// sample n% items from shuffled slice.
	p := int(math.Ceil((float64(n) / 100.0) * float64(len(a))))
	c := make([]rewrite.CandidateQuery, p)
	for i := 0; i < p; i++ {
		c[i] = a[s[i]]
	}
	return c
}

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
	evaluators := []eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator}

	// Cache for queries and the documents they retrieve.
	queryCache := combinator.NewDiskvQueryCache(diskv.New(diskv.Options{
		BasePath:  "../cache",
		Transform: combinator.BlockTransform(8),
		//CacheSizeMax: 4096 * 1024,
		Compression: diskv.NewGzipCompression(),
	}))
	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	// TODO make this come from command line arguments.
	// Load the file that features will be written to.
	f, err := os.OpenFile("precision.features", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Measurement Executor
	me := analysis.NewMeasurementExecutor(statisticsCache)

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

	profFile, err := os.Create("genfeatures.pprof")
	if err != nil {
		log.Fatal(err)
	}

	for _, cq := range queries {
		var queryCandidates []groove.PipelineQuery
		queryCandidates = append(queryCandidates, cq)
		for i := 0; i < nTimes; i++ {
			log.Printf("loop #%v with %v candidate(s)", i, len(queryCandidates))
			for _, candidateQuery := range queryCandidates {
				//wg.Add(1)
				func(q groove.PipelineQuery) {
					//defer wg.Done()

					//var innerWg sync.WaitGroup

					transformations := []rewrite.Transformation{rewrite.NewLogicalOperatorTransformer(), rewrite.NewAdjacencyReplacementTransformer(), rewrite.NewAdjacencyRangeTransformer(), rewrite.NewMeSHExplosionTransformer(), rewrite.NewFieldRestrictionsTransformer()}

					// Generate variations.
					candidates, err := rewrite.Variations(q.Query, ss, me, transformations...)
					if err != nil {
						panic(err)
					}

					fmt.Println("generated", len(candidates), "candidates")

					// Sample 20% of the candidates that were generated.
					candidates = sample(20, candidates)

					fmt.Println("sampled", len(candidates), "candidates")

					// Set the limit to how many goroutines can be run.
					// http://jmoiron.net/blog/limiting-concurrency-in-go/
					concurrency := runtime.NumCPU()

					sem := make(chan bool, concurrency)
					for i, candidate := range candidates {
						sem <- true
						queryCandidates = append(queryCandidates, groove.NewPipelineQuery(q.Name, q.Topic, candidate.Query))

						//innerWg.Add(1)
						go func(c rewrite.CandidateQuery, n int) {
							defer func() { <-sem }()
							//defer innerWg.Done()
							gq := groove.NewPipelineQuery(q.Name, q.Topic, c.Query)
							tree, _, err := combinator.NewLogicalTree(gq, ss, queryCache)
							if err != nil {
								fmt.Println(err)
								return
							}
							r := tree.Documents(queryCache).Results(gq, "features")

							//ids, err := stats.GetDocumentIDs(gq, ss)
							//if err != nil {
							//	panic(err)
							//}
							//
							//results := make(combinator.Documents, len(ids))
							//for i, id := range ids {
							//	results[i] = combinator.Document(id)
							//}
							//r := results.Results(gq, gq.Name)
							evaluation := eval.Evaluate(evaluators, &r, qrels, gq.Topic)

							lf := rewrite.NewLearntFeature(evaluation["Precision"], c.Features)

							var buff bytes.Buffer
							_, err = lf.WriteLibSVMRank(&buff, gq.Topic, gq.Name)
							if err != nil {
								panic(err)
							}

							fmt.Printf("%v/%v [%v] %v\n", n, len(candidates), lf.Score, lf.Features)

							//tree.Root = nil
							//debug.FreeOSMemory()
							//runtime.GC()

							mu.Lock()
							f.Write(buff.Bytes())
							mu.Unlock()
						}(candidate, i)
					}
					// Wait until the last goroutine has read from the semaphore.
					for i := 0; i < cap(sem); i++ {
						sem <- true
					}
					//innerWg.Wait()
					debug.FreeOSMemory()
					runtime.GC()
				}(candidateQuery)
			}
		}
		wg.Wait()
	}

	pprof.WriteHeapProfile(profFile)
	profFile.Close()
	return
}