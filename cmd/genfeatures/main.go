package main

import (
	"bytes"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/trecresults"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
)

type args struct {
	Queries  string `arg:"help:path to queries,required"`
	Qrels    string `arg:"help:relevance Assessments file,required"`
	Features string `arg:"help:features output file,required"`
	Depth    int    `arg:"help:depth of queries to generate,required"`
}

func (args) Version() string {
	return "6.Apr.2018"
}

func (args) Description() string {
	return `generate features from seed queries`
}

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

func generateCandidates(query cqr.CommonQueryRepresentation, depth int, maxDepth int, cache map[uint64]struct{}, ss stats.StatisticsSource, me analysis.MeasurementExecutor, transformations ...rewrite.Transformation) ([]rewrite.CandidateQuery, error) {
	var variations []rewrite.CandidateQuery

	if depth >= maxDepth {
		return variations, nil
	}

	candidates, err := rewrite.Variations(rewrite.NewCandidateQuery(query, nil), ss, me, transformations...)
	if err != nil {
		return nil, err
	}

	fmt.Printf("%v...", depth)

	for _, candidate := range candidates {
		hash := combinator.HashCQR(candidate.Query)
		if _, ok := cache[hash]; !ok {
			variations = append(variations, candidate)
			cache[hash] = struct{}{}
			c, err := generateCandidates(candidate.Query, depth+1, maxDepth, cache, ss, me, transformations...)
			if err != nil {
				panic(err)
			}
			for _, x := range c {
				h := combinator.HashCQR(x.Query)
				variations = append(variations, x)
				cache[h] = struct{}{}
			}
		}
	}

	return variations, nil
}

func main() {
	// Parse the command line arguments.
	var args args
	arg.MustParse(&args)

	// TODO this should come from command line argument.
	// Load the qrels from file.
	b, err := ioutil.ReadFile(args.Qrels)
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
	queries, err := qs.Load(args.Queries)
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
	evaluators := []eval.Evaluator{eval.F05Measure}

	// TODO make this come from command line arguments.
	// Cache for queries and the documents they retrieve.
	queryCache := combinator.NewFileQueryCache("file_cache")
	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	// Load the file that features will be written to.
	f, err := os.OpenFile(args.Features, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Measurement Executor
	me := analysis.NewDiskMeasurementExecutor(statisticsCache)

	// We would like to generate a large amount of training data for the learning to rank

	/*
		1. Generate candidates.
		2. Measure all candidates.
		3. Generate new candidates from generated candidates.
		4. Repeat process N time.
	*/

	var mu sync.Mutex
	//var wg sync.WaitGroup

	nTimes := args.Depth

	for _, cq := range queries {
		var queryCandidates []rewrite.CandidateQuery
		var nextCandidates []rewrite.CandidateQuery
		queryCandidates = append(queryCandidates, rewrite.NewCandidateQuery(cq.Query, nil))
		for i := 0; i < nTimes; i++ {
			log.Printf("loop #%v with %v candidate(s)", i, len(queryCandidates))
			for j, q := range queryCandidates {
				cache := make(map[uint64]struct{})

				//wg.Add(1)
				//defer wg.Done()

				//var innerWg sync.WaitGroup

				transformations := []rewrite.Transformation{rewrite.NewLogicalOperatorTransformer(), rewrite.NewAdjacencyReplacementTransformer(), rewrite.NewAdjacencyRangeTransformer(), rewrite.NewMeSHExplosionTransformer(), rewrite.NewFieldRestrictionsTransformer()}

				// Generate variations.
				log.Println("generating variations...")
				log.Println(len(queryCandidates)-j, "to go")

				candidates, err := rewrite.Variations(q, ss, me, transformations...)
				if err != nil {
					panic(err)
				}

				log.Println("generated", len(candidates), "candidates")
				for i := 0; i < len(candidates); i++ {
					hash := combinator.HashCQR(candidates[i].Query)
					if _, ok := cache[hash]; !ok {
						candidates = append(candidates[:i], candidates[i+1:]...)
						cache[hash] = struct{}{}
					}
				}

				log.Println("cut to", len(candidates), "candidates")

				// Sample 20% of the candidates that were generated.
				//candidates = sample(20, candidates)
				//
				//log.Println("sampled", len(candidates), "candidates")

				// Set the limit to how many goroutines can be run.
				// http://jmoiron.net/blog/limiting-concurrency-in-go/
				maxConcurrency := 16
				concurrency := runtime.NumCPU()
				if concurrency > maxConcurrency {
					concurrency = maxConcurrency
				}
				log.Println("nthreads:", concurrency)

				sem := make(chan bool, concurrency)
				for i, candidate := range candidates {
					sem <- true
					nextCandidates = append(nextCandidates, candidate)

					//innerWg.Add(1)
					go func(c rewrite.CandidateQuery, n int) {
						defer func() { <-sem }()
						//defer innerWg.Done()
						gq := groove.NewPipelineQuery(cq.Name, cq.Topic, c.Query)

						tree, _, err := combinator.NewLogicalTree(gq, ss, queryCache)
						if err != nil {
							fmt.Println(err)
							return
						}
						r := tree.Documents(queryCache).Results(gq, "features")

						evaluation := eval.Evaluate(evaluators, &r, qrels, gq.Topic)

						lf := rewrite.NewLearntFeature(evaluation[eval.F05Measure.Name()], c.Features)

						var buff bytes.Buffer
						_, err = lf.WriteLibSVMRank(&buff, gq.Topic, gq.Name)
						if err != nil {
							panic(err)
						}

						log.Printf("<%v> %v/%v [%v]\n", gq.Topic, n, len(candidates), lf.Score)

						mu.Lock()
						f.Write(buff.Bytes())
						mu.Unlock()
					}(candidate, i)
				}
				// Wait until the last goroutine has read from the semaphore.
				for i := 0; i < cap(sem); i++ {
					sem <- true
				}
				log.Println("finished processing variations")
			}
			queryCandidates = nextCandidates
		}
		//wg.Wait()
	}

	return
}
