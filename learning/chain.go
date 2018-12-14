// Package rewrite uses query chains to rewrite queries.
package learning

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/trecresults"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

// QueryChain contains implementations for transformations to apply to a query and the selector to pick a candidate.
type QueryChain struct {
	Transformations     []Transformation
	Measurements        []analysis.Measurement
	CandidateSelector   QueryChainCandidateSelector
	StatisticsSource    stats.StatisticsSource
	MeasurementExecutor analysis.MeasurementExecutor
	Queries             []pipeline.Query
	TransformedOutput   string
	LearntFeatures      []LearntFeature
	GenerationFile      string
	Evaluators          []eval.Evaluator
	QueryCacher         combinator.QueryCacher
	QrelsFile           trecresults.QrelsFile
	Sampler             Sampler
	GenerationExplorer  QueryChainGenerationExplorer
}

// Generate will create test data sampling using random stratified sampling.
func (qc *QueryChain) Generate() error {
	w, err := os.OpenFile(qc.GenerationFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer w.Close()
	var mu sync.Mutex

	// Set the limit to how many goroutines can be run.
	// http://jmoiron.net/blog/limiting-concurrency-in-go/
	maxConcurrency := 16
	concurrency := runtime.NumCPU()
	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
	}

	// Create the output folder if it does not exist.
	if _, err := os.Stat("transformed_queries"); os.IsNotExist(err) {
		err := os.Mkdir("transformed_queries", os.ModePerm)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	for _, cq := range qc.Queries {
		var candidates []CandidateQuery
		candidates = append(candidates, )

		c := make(chan GenerationResult)

		go qc.GenerationExplorer.Traverse(NewCandidateQuery(cq.Query, cq.Topic, nil), c)

		for result := range c {

			if result.error != nil {
				return result.error
			}

			candidate := result.CandidateQuery

			log.Printf("evaluating candidate at depth %d...\n", len(candidate.Chain))

			var (
				errOnce sync.Once
				e       error
			)

			s1, err := transmute.CompileCqr2PubMed(candidate.Query)
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}

			s2, err := transmute.Pubmed2Cqr.Execute(s1)
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}

			s3, err := s2.Representation()
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}

			gq := pipeline.NewQuery(cq.Name, cq.Topic, s3.(cqr.CommonQueryRepresentation))

			tree, _, err := combinator.NewLogicalTree(gq, qc.StatisticsSource, qc.QueryCacher)
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}
			r := tree.Documents(qc.QueryCacher).Results(gq, "Features")

			evaluation := eval.Evaluate(qc.Evaluators, &r, qc.QrelsFile, gq.Topic)

			fn := strconv.Itoa(int(combinator.HashCQR(candidate.Query)))

			s, _ := s2.String()
			// Write the query outside the lock.
			err = ioutil.WriteFile(
				path.Join("transformed_queries", fn),
				bytes.NewBufferString(s).Bytes(),
				0644)
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}

			// Lock and write the results for each evaluation metric to file.
			lf := NewLearntFeature(candidate.Features)
			lf.Topic = gq.Topic
			lf.Comment = fn
			lf.Scores = make([]float64, len(qc.Evaluators))
			for i, e := range qc.Evaluators {
				lf.Scores[i] = evaluation[e.Name()]
			}
			mu.Lock()
			err = qc.CandidateSelector.Output(lf, w)
			if err != nil {
				errOnce.Do(func() {
					fmt.Println(err)
					fmt.Println(errors.Wrap(err, 0).ErrorStack())
					e = err
					return
				})
			}
			mu.Unlock()
		}
		log.Println("finished processing variations")
	}
	return nil
}

func (qc *QueryChain) Test() error {
	// Create directory if not exists.
	err := os.MkdirAll(qc.TransformedOutput, 0777)
	if err != nil {
		return err
	}

	for _, q := range qc.Queries {
		p := path.Join(qc.TransformedOutput, q.Topic)

		// Do not process if the file already exists.
		_, err := os.Stat(p)
		if err == nil {
			log.Println(fmt.Sprintf("skipping topic %s as it already exists", q.Topic))
			continue
		}
		log.Println(fmt.Sprintf("starting topic %s", q.Topic))

		// Perform the query chain process on the query.
		tq, err := qc.Execute(q)
		if err != nil {
			return err
		}

		// Transform query to medline.
		cq, err := backend.NewCQRQuery(tq.Query).String()
		if err != nil {
			return err
		}

		// Write query to file.
		err = ioutil.WriteFile(p, bytes.NewBufferString(cq).Bytes(), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

// Train hands off the training to the candidate selector.
func (qc *QueryChain) Train() error {
	_, err := qc.CandidateSelector.Train(qc.LearntFeatures)
	return err
}

func (qc *QueryChain) Validate() error {
	log.Println("WARN: validation of query chain happens inside candidate selector")
	return nil
}

// QueryChainCandidateSelector describes how transformed queries are chosen from the set of transformations.
type QueryChainCandidateSelector interface {
	Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error)
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
func NewQueryChain(selector QueryChainCandidateSelector, ss stats.StatisticsSource, me analysis.MeasurementExecutor, measurements []analysis.Measurement, transformations ...Transformation) *QueryChain {
	return &QueryChain{
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
func (qc *QueryChain) Execute(q pipeline.Query) (CandidateQuery, error) {
	var (
		stop bool
	)
	cq := NewCandidateQuery(q.Query, q.Topic, nil)
	sel := qc.CandidateSelector
	stop = sel.StoppingCriteria()
	d := 0
	for !stop {
		log.Println("generating candidates...")
		candidates, err := Variations(cq, qc.StatisticsSource, qc.MeasurementExecutor, qc.Measurements, qc.Transformations...)
		if err != nil {
			return CandidateQuery{}, err
		}

		if len(candidates) == 0 {
			stop = true
			break
		}

		candidates = append(candidates, cq)

		d++

		cq, sel, err = sel.Select(cq, candidates)
		if err != nil && err != combinator.ErrCacheMiss {
			return CandidateQuery{}, err
		}
		log.Println(transmute.CompileCqr2PubMed(cq.Query))
		log.Printf("topic: %s, depth: %d, stoping: %t", q.Topic, d, sel.StoppingCriteria())
		log.Println("candidates:", len(candidates))
		log.Println("chain length:", len(cq.Chain))
		log.Println("applied:", cq.TransformationID)
		stop = sel.StoppingCriteria()
	}
	return cq, nil
}

type ranking struct {
	rank  float64
	query CandidateQuery
}

func getRanking(filename string, candidates []CandidateQuery) (CandidateQuery, error) {
	if candidates == nil || len(candidates) == 0 {
		return CandidateQuery{}, nil
	}

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return CandidateQuery{}, err
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(b))
	i := 0
	ranks := make([]ranking, len(candidates))
	for scanner.Scan() {
		r, err := strconv.ParseFloat(scanner.Text(), 64)
		if err != nil {
			return CandidateQuery{}, err
		}
		ranks[i] = ranking{
			r,
			candidates[i],
		}
		i++
	}

	sort.Slice(ranks, func(i, j int) bool {
		return ranks[i].rank > ranks[j].rank
	})

	if len(ranks) == 0 {
		return CandidateQuery{}, nil
	}

	return ranks[0].query, nil
}

func NewNearestNeighbourQueryChain(options ...func(c *NearestNeighbourQueryCandidateSelector)) *QueryChain {
	return &QueryChain{
		CandidateSelector: NewNearestNeighbourCandidateSelector(options...),
	}
}

func NewQuickRankQueryChain(binary string, arguments map[string]interface{}, options ...func(c *QuickRankQueryCandidateSelector)) *QueryChain {
	return &QueryChain{
		CandidateSelector: NewQuickRankQueryCandidateSelector(binary, arguments, options...),
	}
}

func NewReinforcementQueryChain() *QueryChain {
	return &QueryChain{
		CandidateSelector: ReinforcementQueryCandidateSelector{},
	}
}
