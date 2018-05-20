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
	"github.com/hscells/transmute"
	"github.com/hscells/cui2vec"
	"github.com/hscells/metawrap"
	"path"
	"strconv"
	"sort"
	"gopkg.in/olivere/elastic.v5"
	"net/http"
	"io"
)

type args struct {
	Queries  string `arg:"help:path to queries,required"`
	Qrels    string `arg:"help:relevance Assessments file,required"`
	Features string `arg:"help:features output file,required"`
	Depth    int    `arg:"help:depth of queries to generate,required"`

	CUIs    string `arg:"help:path to cui mapping."`
	CUI2Vec string `arg:"help:path to pretrained cui2vec features."`
	MetaMap string `arg:"help:path to MetaMap binary."`

	LogFile string `arg:"help:path to output logs to."`
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

	if len(args.LogFile) > 0 {
		f, err := os.OpenFile(args.LogFile, os.O_CREATE|os.O_WRONLY, 0644)
		defer f.Close()
		f.Truncate(0)
		if err != nil {
			log.Fatal(err)
		}
		mw := io.MultiWriter(os.Stdout, f)
		log.SetOutput(mw)
	}

	log.Println("loading and compiling queries...")
	// Load the qrels from file.
	b, err := ioutil.ReadFile(args.Qrels)
	if err != nil {
		panic(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

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

	log.Println("configuring cache and evaluation metrics...")
	var N float64 = 26758795
	// Transformers and evaluators.
	evaluators := []eval.Evaluator{eval.F1Measure, eval.F3Measure, eval.NewWSSEvaluator(N), eval.F05Measure, eval.PrecisionEvaluator, eval.RecallEvaluator}

	evaluationFiles := make([]*os.File, len(evaluators))
	for i, e := range evaluators {
		// Load the file that features will be written to.
		f, err := os.OpenFile(fmt.Sprintf("%v_%v", e.Name(), args.Features), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		evaluationFiles[i] = f
	}

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

	// Measurement Executor
	me := analysis.NewDiskMeasurementExecutor(statisticsCache)

	log.Println("loading cui2vec...")
	c2vf, err := os.OpenFile(args.CUI2Vec, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer c2vf.Close()
	vector, err := cui2vec.Load(c2vf, true)
	if err != nil {
		panic(err)
	}

	log.Println("loading cui mapping...")
	mapping, err := cui2vec.LoadCUIMapping(args.CUIs)
	if err != nil {
		panic(err)
	}

	log.Println("loading MetaMap client...")
	mm := metawrap.NewMetaMapClient(args.MetaMap)

	// Configure the Statistics Source.
	log.Println("initiating connection to Elasticsearch...")

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	ss, err := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{Size: 10000, RunName: "test"}))
	if err == elastic.ErrNoClient {
		panic(err)
	}
	// We would like to generate a large amount of training data for the learning to rank
	/*
		1. Generate candidates.
		2. Measure all candidates.
		3. Generate new candidates from generated candidates.
		4. Repeat process N time.
	*/

	log.Println("ready to begin!")
	var mu sync.Mutex
	nTimes := args.Depth
	for _, cq := range queries {
		var queryCandidates []rewrite.CandidateQuery
		var nextCandidates []rewrite.CandidateQuery
		queryCandidates = append(queryCandidates, rewrite.NewCandidateQuery(cq.Query, nil))
		cache := make(map[uint64]struct{})
		log.Println("this is topic", cq.Topic)
		for i := 0; i < nTimes; i++ {
			log.Printf("loop #%v with %v candidate(s)", i, len(queryCandidates))
			for j, q := range queryCandidates {

				transformations := []rewrite.Transformation{
					rewrite.NewLogicalOperatorTransformer(),
					rewrite.NewAdjacencyReplacementTransformer(),
					rewrite.NewAdjacencyRangeTransformer(),
					rewrite.NewMeSHExplosionTransformer(),
					rewrite.NewFieldRestrictionsTransformer(),
					rewrite.NewClauseRemovalTransformer(),
					rewrite.Newcui2vecExpansionTransformer(vector, mapping, mm),
				}

				// Generate variations.
				log.Println(len(queryCandidates)-j, "to go")
				log.Println("generating variations...")

				candidates, err := rewrite.Variations(q, ss, me, transformations...)
				if err != nil {
					panic(err)
				}

				log.Println("generated", len(candidates), "candidates")

				log.Println("computing number of candidates to sample")
				totalCandidates := len(candidates)
				if totalCandidates > 20 {
					totalCandidates = 20
					totalCandidates += len(sample(10, candidates))
					if totalCandidates > len(candidates) {
						totalCandidates = len(candidates)
					}
				}

				log.Println("computed", totalCandidates, "candidates to sample")

				sort.Slice(candidates, func(i, j int) bool {
					return candidates[i].TransformationID > candidates[j].TransformationID
				})

				log.Println("sorted candidates")

				max := make(map[int]int)
				sam := make(map[int]int)
				can := make(map[int][]rewrite.CandidateQuery)
				for _, candidate := range candidates {
					max[candidate.TransformationID]++
					can[candidate.TransformationID] = append(can[candidate.TransformationID], candidate)
					sam[candidate.TransformationID] = 0
				}

				var c []rewrite.CandidateQuery
				for len(c) < totalCandidates {
					for t := range sam {
						if sam[t] < max[t] {
							c = append(c, can[t][sam[t]])
							sam[t]++
							fmt.Printf("%v/%v...", len(c), totalCandidates)
							if len(c) >= totalCandidates {
								break
							}
						}
					}
				}
				fmt.Println()

				//Sample 20% of the candidates that were generated.
				//if len(candidates) > 50 {
				//	candidates = sample(20, candidates)
				//}
				//log.Print(totalCandidates, len(c))
				candidates = c

				log.Println("sampled", len(candidates), "candidates")

				for i := 0; i < len(candidates); i++ {
					hash := combinator.HashCQR(candidates[i].Query)
					if _, ok := cache[hash]; !ok {
						candidates = append(candidates[:i], candidates[i+1:]...)
						cache[hash] = struct{}{}
					}
				}
				log.Println("cut to", len(candidates), "candidates")

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

						// Configure the Statistics Source.

						tree, _, err := combinator.NewLogicalTree(gq, ss, queryCache)
						if err != nil {
							fmt.Println(err)
							return
						}
						r := tree.Documents(queryCache).Results(gq, "features")

						evaluation := eval.Evaluate(evaluators, &r, qrels, gq.Topic)

						log.Printf("<%v> %v/%v [f0.5: %.6f, p: %.6f, r: %.6f, wss: %.6f]\n", gq.Topic, n, len(candidates), evaluation[eval.F05Measure.Name()], evaluation[eval.PrecisionEvaluator.Name()], evaluation[eval.RecallEvaluator.Name()], evaluation[eval.NewWSSEvaluator(N).Name()])

						s, _ := backend.NewCQRQuery(c.Query).String()
						x, _ := transmute.Cqr2Medline.Execute(s)
						s, _ = x.String()

						fn := strconv.Itoa(int(combinator.HashCQR(c.Query)))
						// Write the query outside the lock.
						ioutil.WriteFile(
							path.Join("transformed_queries", fn),
							bytes.NewBufferString(s).Bytes(),
							0644)

						// Lock and write the results for each evaluation metric to file.
						mu.Lock()
						for i, e := range evaluators {
							lf := rewrite.NewLearntFeature(evaluation[e.Name()], c.Features)
							var buff bytes.Buffer
							_, err = lf.WriteLibSVMRank(&buff, gq.Topic, fn)
							if err != nil {
								panic(err)
							}
							evaluationFiles[i].Write(buff.Bytes())
						}
						mu.Unlock()
					}(candidate, i)
				}
				// Wait until the last goroutine has read from the semaphore.
				for i := 0; i < cap(sem); i++ {
					sem <- true
				}
				log.Println("finished processing variations")
			}

			log.Println("sampling the generated candidates...")
			queryCandidates = sample(10, nextCandidates)
		}
	}

	for i := range evaluators {
		evaluationFiles[i].Close()
	}

	return
}
