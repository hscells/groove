package rewrite

import (
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"log"
	"github.com/hscells/groove/combinator"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"github.com/hscells/groove/eval"
	"time"
	"runtime"
	"runtime/debug"
)

type LearntCandidateQuery struct {
	Topic     int64              `json:"topic"`
	Depth     int64              `json:"depth"`
	Candidate CandidateQuery     `json:"candidate"`
	Eval      map[string]float64 `json:"eval"`
}

type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
}

type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error)
	StoppingCriteria() bool
	Finalise()
}

func NewQueryChain(selector QueryChainCandidateSelector, transformations ...Transformation) QueryChain {
	return QueryChain{
		CandidateSelector: selector,
		Transformations:   transformations,
	}
}

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
	}

	bestRelRet, bestRet := oc.bestRelRet, oc.bestRet
	log.Printf("topic %v - RR %v, RL %v\n", query.PipelineQuery.Topic, oc.bestRelRet, oc.bestRet)

	// Apply the transformations to all of the queries.
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

			//fmt.Println(json.MarshalIndent(applied, "", "  "))
			results := tree.Documents().Results(nq, nq.Name)
			//
			//log.Printf("topic %v - %v ? %v\n", nq.Topic, resultSize, oc.minResults)
			//if resultSize == 0 || resultSize > 100000 || resultSize >= oc.minResults {
			//	continue
			//}

			// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
			//results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
			//if err != nil {
			//	continue
			//}

			evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.PipelineQuery.Topic)
			numRelRet := evaluation[eval.NumRelRet.Name()]
			numRet := evaluation[eval.NumRet.Name()]

			// Write the query out to a file.
			f := fmt.Sprintf("chain/%v", combinator.HashCQR(nq.Query))
			b, err := json.MarshalIndent(map[string]interface{}{
				"topic":     nq.Topic,
				"depth":     oc.depth,
				"candidate": applied,
				"eval":      evaluation,
			}, "", "  ")
			if err != nil {
				return query, nil, err
			}
			ioutil.WriteFile(f, b, 0644)
			log.Printf("topic %v - wrote query to %v", nq.Topic, f)

			if numRelRet > 0 && numRelRet >= bestRelRet && numRet <= bestRet {
				bestRelRet = numRelRet
				bestRet = numRet
				oc.bestRelRet = bestRelRet
				oc.bestRet = bestRet
				log.Printf("topic %v - P %v, R %v, %v %v, %v %v, %v %v\n", query.PipelineQuery.Topic, evaluation[eval.PrecisionEvaluator.Name()], evaluation[eval.RecallEvaluator.Name()], eval.NumRel.Name(), evaluation[eval.NumRel.Name()], eval.NumRet.Name(), evaluation[eval.NumRet.Name()], eval.NumRelRet.Name(), evaluation[eval.NumRelRet.Name()])
				transformed := groove.NewPipelineQuery(query.PipelineQuery.Name, query.PipelineQuery.Topic, applied.Query)
				query = query.Append(transformed)
			}

			log.Printf("topic %v - query took %v minutes; features: %v", nq.Topic, time.Now().Sub(start).Minutes(), applied.FeatureFamily.String())

			results = nil
			debug.FreeOSMemory()
			runtime.GC()
			debug.PrintStack()
		}
	}
	return query, oc, nil
}

func (oc OracleQueryChainCandidateSelector) Finalise() {
	oc.qrels = trecresults.QrelsFile{}
	oc.ss = nil
	for k := range oc.seen {
		delete(oc.seen, k)
	}
	oc.seen = nil
}

func (oc OracleQueryChainCandidateSelector) StoppingCriteria() (bool) {
	if oc.depth >= 5 || (oc.bestRelRet == oc.bestRelRet && oc.bestRet == oc.prevRet) {
		return true
	}
	return false
}

func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file trecresults.QrelsFile) OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:         source,
		qrels:      file,
		bestRelRet: -1.0,
		bestRet:    -1.0,
	}
	return oc
}
