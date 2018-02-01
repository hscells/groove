// Package rewrite uses query chains to rewrite queries.
package rewrite

import (
	"encoding/json"
	"fmt"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/stats"
	"io/ioutil"
	"log"
	"runtime"
	"runtime/debug"
	"time"
)

// QueryChain contains implementations for transformations to apply to a query and the selector to pick a candidate.
type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
}

// QueryChainCandidateSelector describes how transformed queries are chosen from the set of transformations.
type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error)
	StoppingCriteria() bool
	Finalise()
}

// LearntCandidateQuery is the serialised struct written from the oracle query chain candidate selector.
type LearntCandidateQuery struct {
	Topic     int64              `json:"topic"`
	Depth     int64              `json:"depth"`
	Candidate CandidateQuery     `json:"candidate"`
	Eval      map[string]float64 `json:"eval"`
}

// NewQueryChain creates a new query chain with implementations for a selector and transformations.
func NewQueryChain(selector QueryChainCandidateSelector, transformations ...Transformation) QueryChain {
	return QueryChain{
		CandidateSelector: selector,
		Transformations:   transformations,
	}
}

// Execute executes a query chain in full. At each "transition point" in the chain, the candidate selector is queried
// in order to see if the chain should continue or not. At the end of the chain, the selector is cleaned using the
// finalise method.
func (qc QueryChain) Execute(query groove.PipelineQuery) (TransformedQuery, error) {
	var (
		stop bool
		err  error
	)
	stop = qc.CandidateSelector.StoppingCriteria()
	tq := NewTransformedQuery(query)
	for !stop {
		tq, qc.CandidateSelector, err = qc.CandidateSelector.Select(tq, qc.Transformations)
		if err != nil {
			return TransformedQuery{}, err
		}
		stop = qc.CandidateSelector.StoppingCriteria()
	}
	qc.CandidateSelector.Finalise()
	return tq, nil
}

// Features creates features using a oracle query chain candidate selector.
func (oc OracleQueryChainCandidateSelector) Features(query groove.PipelineQuery, transformations []Transformation) (lf []LearntFeature, err error) {

	bestQuery := query
	for i := 0; i < 5; i++ {
		// Apply the transformations to all of the queries.
		for _, transformation := range transformations {
			cqs, err := transformation.Apply(bestQuery.Query)
			if err != nil {
				return nil, err
			}

			for _, candidate := range cqs {
				nq := groove.NewPipelineQuery(query.Name, query.Topic, candidate.Query)

				// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
				results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
				if err != nil {
					continue
				}

				evaluation := eval.Evaluate([]eval.Evaluator{eval.PrecisionEvaluator}, &results, oc.qrels, bestQuery.Topic)
				precision := evaluation[eval.PrecisionEvaluator.Name()]

				lf = append(lf, LearntFeature{FeatureFamily: candidate.FeatureFamily, Score: precision})
			}
		}
	}
	return
}

// OracleQueryChainCandidateSelector finds the best possible combination of query rewrites.
type OracleQueryChainCandidateSelector struct {
	depth      int
	minResults float64

	bestRelRet float64
	prevRelRet float64
	bestRet    float64
	prevRet    float64

	qrels trecresults.QrelsFile
	ss    stats.StatisticsSource

	seen map[uint64]combinator.LogicalTreeNode
}

func writeQuery(query groove.PipelineQuery, depth int, candidate CandidateQuery, evaluation map[string]float64) error {
	f := fmt.Sprintf("chain/%v", combinator.HashCQR(query.Query))
	b, err := json.MarshalIndent(map[string]interface{}{
		"topic":     query.Topic,
		"depth":     depth,
		"candidate": candidate,
		"eval":      evaluation,
	}, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(f, b, 0644)
}

// Select is a grid search for the best possible query transformation chain.
func (oc OracleQueryChainCandidateSelector) Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error) {
	oc.depth++
	oc.prevRelRet = oc.bestRelRet
	oc.prevRet = oc.bestRet

	if oc.seen == nil {
		oc.seen = make(map[uint64]combinator.LogicalTreeNode)
	}

	if oc.minResults == 0 {
		var err error
		var tree combinator.LogicalTree
		log.Printf("topic %v - getting the baseline\n", query.PipelineQuery.Topic)
		tree, oc.seen, err = combinator.NewLogicalTree(query.PipelineQuery, oc.ss, oc.seen)
		if err != nil {
			return query, oc, err
		}
		results := tree.Documents().Results(query.PipelineQuery, query.PipelineQuery.Name)
		oc.minResults = float64(len(results))
		evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.PipelineQuery.Topic)
		if err != nil {
			return query, oc, err
		}
		oc.bestRelRet = evaluation[eval.NumRelRet.Name()]
		oc.bestRet = evaluation[eval.NumRet.Name()]
		writeQuery(query.PipelineQuery, oc.depth, NewCandidateQuery(query.PipelineQuery.Query, FeatureFamily{}), evaluation)
	}

	bestRelRet, bestRet := oc.bestRelRet, oc.bestRet
	log.Printf("topic %v - RR %v, RL %v\n", query.PipelineQuery.Topic, oc.bestRelRet, oc.bestRet)

	// Apply the transformations to all of the queries.
	var transformed groove.PipelineQuery
	for _, transformation := range transformations {

		start := time.Now()

		log.Printf("topic %v - generating %v transformation candidates\n", query.PipelineQuery.Topic, transformation.Name())

		queries, err := transformation.Apply(query.PipelineQuery.Query)
		if err != nil {
			return TransformedQuery{}, oc, err
		}

		log.Printf("topic %v - generated %v %v transformation candidates (%v mins)\n", query.PipelineQuery.Topic, len(queries), transformation.Name(), time.Now().Sub(start).Minutes())

		for _, applied := range queries {

			start := time.Now()
			// The new query.
			nq := groove.NewPipelineQuery(query.PipelineQuery.Name, query.PipelineQuery.Topic, applied.Query)

			// Test if the query actually is executable.
			_, err := oc.ss.RetrievalSize(applied.Query)
			if err != nil {
				continue
			}

			// Don't continue if the query is retrieving MORE results and test if the query is capable of being executed.
			var tree combinator.LogicalTree
			tree, oc.seen, err = combinator.NewLogicalTree(nq, oc.ss, oc.seen)
			if err != nil {
				return query, oc, err
			}

			// Now we can transform the results of the logical tree into results to be evaluated.
			results := tree.Documents().Results(nq, nq.Name)

			// Evaluate the results using qrels.
			evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.PipelineQuery.Topic)
			numRelRet := evaluation[eval.NumRelRet.Name()]
			numRet := evaluation[eval.NumRet.Name()]

			// Write the query out to a file.
			writeQuery(nq, oc.depth, applied, evaluation)

			log.Printf("topic %v - wrote query to %v", nq.Topic, combinator.HashCQR(nq.Query))

			if numRelRet > 0 && numRelRet >= bestRelRet && numRet <= bestRet {
				bestRelRet = numRelRet
				bestRet = numRet
				oc.bestRelRet = bestRelRet
				oc.bestRet = bestRet
				log.Printf("topic %v - P %v, R %v, %v %v, %v %v, %v %v\n", query.PipelineQuery.Topic, evaluation[eval.PrecisionEvaluator.Name()], evaluation[eval.RecallEvaluator.Name()], eval.NumRel.Name(), evaluation[eval.NumRel.Name()], eval.NumRet.Name(), evaluation[eval.NumRet.Name()], eval.NumRelRet.Name(), evaluation[eval.NumRelRet.Name()])
				transformed = groove.NewPipelineQuery(query.PipelineQuery.Name, query.PipelineQuery.Topic, applied.Query)
			}

			log.Printf("topic %v - query took %v minutes; features: %v", nq.Topic, time.Now().Sub(start).Minutes(), applied.FeatureFamily.String())

			results = nil
			debug.FreeOSMemory()
			runtime.GC()
			debug.PrintStack()
		}
	}
	query = query.Append(transformed)
	return query, oc, nil
}

// Finalise cleans up the qrels file, statistic service, and clears the seen map.
func (oc OracleQueryChainCandidateSelector) Finalise() {
	oc.qrels = trecresults.QrelsFile{}
	oc.ss = nil
	for k := range oc.seen {
		delete(oc.seen, k)
	}
	oc.seen = nil
}

// StoppingCriteria defines stopping criteria.
func (oc OracleQueryChainCandidateSelector) StoppingCriteria() bool {
	if oc.depth >= 5 || (oc.bestRelRet == oc.bestRelRet && oc.bestRet == oc.prevRet) {
		return true
	}
	return false
}

// NewOracleQueryChainCandidateSelector creates a new oracle query chain candidate selector.
func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file trecresults.QrelsFile) OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:         source,
		qrels:      file,
		bestRelRet: -1.0,
		bestRet:    -1.0,
	}
	return oc
}
