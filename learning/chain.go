// Package rewrite uses query chains to rewrite queries.
package learning

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/stats"
	"io"
	"reflect"
	"github.com/hscells/groove"
	"github.com/hscells/groove/eval"
	"log"
	"sync"
	"fmt"
	"runtime"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute"
	"strconv"
	"path"
	"bytes"
	"math"
	"math/rand"
	"sort"
	"github.com/hscells/trecresults"
	"io/ioutil"
	"os"
	"github.com/go-errors/errors"
)

// QueryChain contains implementations for transformations to apply to a query and the selector to pick a candidate.
type QueryChain struct {
	Transformations     []Transformation
	Measurements        []analysis.Measurement
	CandidateSelector   QueryChainCandidateSelector
	StatisticsSource    stats.StatisticsSource
	MeasurementExecutor analysis.MeasurementExecutor
	Queries             []groove.PipelineQuery
	LearntFeatures      []LearntFeature
	GenerationDepth     int
	GeneartionFile      string
	Evaluators          []eval.Evaluator
	QueryCacher         combinator.QueryCacher
	QrelsFile           trecresults.QrelsFile
}

// sample n% of candidate queries.
func sample(n int, a []CandidateQuery) []CandidateQuery {
	// shuffle the items to sample.
	s := rand.Perm(len(a))

	// sample n% items from shuffled slice.
	p := int(math.Ceil((float64(n) / 100.0) * float64(len(a))))
	c := make([]CandidateQuery, p)
	for i := 0; i < p; i++ {
		c[i] = a[s[i]]
	}
	return c
}

// Generate will create test data sampling using random stratified sampling.
func (qc *QueryChain) Generate() error {
	w, err := os.OpenFile(qc.GeneartionFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	var mu sync.Mutex
	nTimes := 5
	for _, cq := range qc.Queries {
		var queryCandidates []CandidateQuery
		var nextCandidates []CandidateQuery
		queryCandidates = append(queryCandidates, NewCandidateQuery(cq.Query, nil))
		cache := make(map[uint64]struct{})
		for i := 0; i < nTimes; i++ {
			log.Printf("loop #%v with %v candidate(s)", i, len(queryCandidates))
			for j, q := range queryCandidates {

				log.Println("this is topic", cq.Topic)

				// Generate variations.
				log.Println(len(queryCandidates)-j, "to go")
				log.Println("generating variations...")

				candidates, err := Variations(q, qc.StatisticsSource, qc.MeasurementExecutor, qc.Measurements, qc.Transformations...)
				if err != nil {
					return err
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
				can := make(map[int][]CandidateQuery)
				for _, candidate := range candidates {
					max[candidate.TransformationID]++
					can[candidate.TransformationID] = append(can[candidate.TransformationID], candidate)
					sam[candidate.TransformationID] = 0
				}

				var c []CandidateQuery
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

				sem := make(chan bool, concurrency)
				for i, candidate := range candidates {
					sem <- true
					nextCandidates = append(nextCandidates, candidate)

					//innerWg.Add(1)
					go func(c CandidateQuery, n int) {
						defer func() { <-sem }()
						//defer innerWg.Done()
						gq := groove.NewPipelineQuery(cq.Name, cq.Topic, c.Query)

						// Configure the Statistics Source.

						tree, _, err := combinator.NewLogicalTree(gq, qc.StatisticsSource, qc.QueryCacher)
						if err != nil {
							fmt.Println(err)
							fmt.Println(errors.Wrap(err, 0).ErrorStack())
							return
						}
						r := tree.Documents(qc.QueryCacher).Results(gq, "features")

						evaluation := eval.Evaluate(qc.Evaluators, &r, qc.QrelsFile, gq.Topic)

						s, _ := backend.NewCQRQuery(c.Query).String()
						x, _ := transmute.Cqr2Medline.Execute(s)
						s, _ = x.String()

						fn := strconv.Itoa(int(combinator.HashCQR(c.Query)))
						// Write the query outside the lock.
						err = ioutil.WriteFile(
							path.Join("transformed_queries", fn),
							bytes.NewBufferString(s).Bytes(),
							0644)
						if err != nil {
							fmt.Println(err)
							fmt.Println(errors.Wrap(err, 0).ErrorStack())
							return
						}

						// Lock and write the results for each evaluation metric to file.
						lf := NewLearntFeature(c.Features)
						lf.Topic = gq.Topic
						lf.Comment = fn
						lf.Scores = make([]float64, len(qc.Evaluators))
						for i, e := range qc.Evaluators {
							lf.Scores[i] = evaluation[e.Name()]
						}
						mu.Lock()
						defer mu.Unlock()
						err = qc.CandidateSelector.Output(lf, w)
						if err != nil {
							fmt.Println(err)
							fmt.Println(errors.Wrap(err, 0).ErrorStack())
							return
						}
						return
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
	return nil
}

func (qc *QueryChain) Test() (interface{}, error) {
	tqs := make([]TransformedQuery, len(qc.Queries))
	for i, q := range qc.Queries {
		tq, err := qc.Execute(q)
		if err != nil {
			return nil, err
		}
		tqs[i] = tq
	}
	return tqs, nil
}

func (qc *QueryChain) Output(w io.Writer) error {
	for _, lf := range qc.LearntFeatures {
		err := qc.CandidateSelector.Output(lf, w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qc *QueryChain) Type() reflect.Type {
	return reflect.TypeOf([]TransformedQuery{})
}

func (qc *QueryChain) Train() error {
	_, err := qc.CandidateSelector.Train(qc.LearntFeatures)
	return err
}

func (qc *QueryChain) Validate() error {
	return nil
}

// QueryChainCandidateSelector describes how transformed queries are chosen from the set of transformations.
type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error)
	Train(lfs []LearntFeature) ([]byte, error)
	Output(lf LearntFeature, w io.Writer) error
	StoppingCriteria() bool
}

// LearntCandidateQuery is the serialised struct written from the oracle query chain candidate selector.
type LearntCandidateQuery struct {
	Topic     int64              `json:"topic"`
	Depth     int64              `json:"Depth"`
	Candidate CandidateQuery     `json:"candidate"`
	Eval      map[string]float64 `json:"eval"`
}

// NewQueryChain creates a new query chain with implementations for a selector and transformations.
func NewQueryChain(selector QueryChainCandidateSelector, ss stats.StatisticsSource, me analysis.MeasurementExecutor, measurements []analysis.Measurement, transformations ...Transformation) QueryChain {
	return QueryChain{
		CandidateSelector:   selector,
		Transformations:     transformations,
		Measurements:        measurements,
		MeasurementExecutor: me,
		StatisticsSource:    ss,
	}
}

// Execute executes a query chain in full. At each "transition point" in the chain, the candidate selector is queried
// in order to see if the chain should continue or not. At the end of the chain, the selector is cleaned using the
// finalise method.
func (qc *QueryChain) Execute(q groove.PipelineQuery) (TransformedQuery, error) {
	var (
		stop bool
	)
	stop = qc.CandidateSelector.StoppingCriteria()
	tq := NewTransformedQuery(q)
	for !stop {
		candidates, err := Variations(NewCandidateQuery(tq.PipelineQuery.Query, nil), qc.StatisticsSource, qc.MeasurementExecutor, qc.Measurements, qc.Transformations...)
		if err != nil {
			return TransformedQuery{}, err
		}
		if len(candidates) == 0 {
			stop = true
			break
		}

		tq, qc.CandidateSelector, err = qc.CandidateSelector.Select(tq, candidates)
		if err != nil && err != combinator.ErrCacheMiss {
			return TransformedQuery{}, err
		}
		stop = qc.CandidateSelector.StoppingCriteria()
	}
	return tq, nil
}

func NewLearningToRankQueryChain(modelFile string) *QueryChain {
	return &QueryChain{
		CandidateSelector: NewLTRQueryCandidateSelector(modelFile),
		GenerationDepth:   5,
	}
}

func NewReinforcementQueryChain() *QueryChain {
	return &QueryChain{
		CandidateSelector: ReinforcementQueryCandidateSelector{},
		GenerationDepth:   5,
	}
}
