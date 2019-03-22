package learning

import (
	"encoding/json"
	"fmt"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"io"
	"io/ioutil"
	"log"
	"time"
)

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

	seen combinator.QueryCacher
}

func (oc OracleQueryChainCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	panic("implement me")
}

func (oc OracleQueryChainCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

// Features creates Features using a oracle query chain candidate selector.
//func (oc OracleQueryChainCandidateSelector) Features(query pipeline.Query, transformations []Transformer) (lf []LearntFeature, err error) {
//	bestQuery := query
//
//	candidates, err := Variations(query.Query, transformations...)
//	if err != nil {
//
//	}
//
//	// Apply the transformations to all of the queries.
//	for _, candidate := range candidates {
//		nq := groove.NewPipelineQuery(query.Name, query.Topic, candidate.Query)
//
//		// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
//		results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
//		if err != nil {
//			continue
//		}
//
//		evaluation := eval.Evaluate([]eval.Evaluator{eval.Precision}, &results, oc.qrels, bestQuery.Topic)
//		precision := evaluation[eval.Precision.Name()]
//
//		lf = append(lf, LearntFeature{Features: candidate.Features, Score: precision})
//	}
//	return
//}

func writeQuery(query pipeline.Query, depth int, candidate CandidateQuery, evaluation map[string]float64) error {
	f := fmt.Sprintf("chain/%v", combinator.HashCQR(query.Query))
	b, err := json.MarshalIndent(map[string]interface{}{
		"topic":     query.Topic,
		"Depth":     depth,
		"candidate": candidate,
		"eval":      evaluation,
	}, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(f, b, 0644)
}

// Select is a grid search for the best possible query transformation chain.
func (oc OracleQueryChainCandidateSelector) Select(query CandidateQuery, candidates []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	oc.depth++
	oc.prevRelRet = oc.bestRelRet
	oc.prevRet = oc.bestRet

	if oc.seen == nil {
		oc.seen = combinator.NewMapQueryCache()
	}

	if oc.minResults == 0 {
		var err error
		var tree combinator.LogicalTree
		log.Printf("topic %v - getting the baseline\n", query.Topic)
		pq := pipeline.NewQuery(query.Topic, query.Topic, query.Query)
		tree, oc.seen, err = combinator.NewLogicalTree(pq, oc.ss, oc.seen)
		if err != nil {
			return query, oc, err
		}
		results := tree.Documents(oc.seen).Results(pq, query.Topic)
		oc.minResults = float64(len(results))
		evaluation := eval.Evaluate([]eval.Evaluator{eval.Recall, eval.Precision, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.Topic)
		if err != nil {
			return query, oc, err
		}
		oc.bestRelRet = evaluation[eval.NumRelRet.Name()]
		oc.bestRet = evaluation[eval.NumRet.Name()]
		//err = writeQuery(query.PipelineQuery, oc.depth, NewCandidateQuery(query.PipelineQuery.Query, Features{}), evaluation)
		//if err != nil {
		//	return TransformedQuery{}, nil, err
		//}
	}

	bestRelRet, bestRet := oc.bestRelRet, oc.bestRet
	log.Printf("topic %v - RR %v, RL %v\n", query.Topic, oc.bestRelRet, oc.bestRet)

	// Apply the transformations to all of the queries.
	var transformed pipeline.Query

	for _, applied := range candidates {

		start := time.Now()
		// The new query.
		nq := pipeline.NewQuery(query.Topic, query.Topic, applied.Query)

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
		results := tree.Documents(oc.seen).Results(nq, nq.Name)

		// Evaluate the results using qrels.
		evaluation := eval.Evaluate([]eval.Evaluator{eval.Recall, eval.Precision, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.Topic)
		numRelRet := evaluation[eval.NumRelRet.Name()]
		numRet := evaluation[eval.NumRet.Name()]

		// Write the query out to a file.
		//err = writeQuery(nq, oc.depth, applied, evaluation)
		//if err != nil {
		//	return TransformedQuery{}, nil, err
		//}

		log.Printf("topic %v - wrote query to %v", nq.Topic, combinator.HashCQR(nq.Query))

		if numRelRet > 0 && numRelRet >= bestRelRet && numRet <= bestRet {
			bestRelRet = numRelRet
			bestRet = numRet
			oc.bestRelRet = bestRelRet
			oc.bestRet = bestRet
			log.Printf("topic %v - P %v, R %v, %v %v, %v %v, %v %v\n", query.Topic, evaluation[eval.Precision.Name()], evaluation[eval.Recall.Name()], eval.NumRel.Name(), evaluation[eval.NumRel.Name()], eval.NumRet.Name(), evaluation[eval.NumRet.Name()], eval.NumRelRet.Name(), evaluation[eval.NumRelRet.Name()])
			transformed = pipeline.NewQuery(query.Topic, query.Topic, applied.Query)
		}

		log.Printf("topic %v - query took %v minutes; Features: %v", nq.Topic, time.Now().Sub(start).Minutes(), applied.Features.String())

		//results = nil
		//debug.FreeOSMemory()
		//runtime.GC()
	}
	if transformed.Query != nil {
		query = NewCandidateQuery(transformed.Query, query.Topic, nil).Append(query)
	}
	return query, oc, nil
}

// StoppingCriteria defines stopping criteria.
func (oc OracleQueryChainCandidateSelector) StoppingCriteria() bool {
	if oc.depth >= 5 || (oc.bestRelRet == oc.bestRelRet && oc.bestRet == oc.prevRet) {
		return true
	}
	return false
}

// NewOracleQueryChainCandidateSelector creates a new oracle query chain candidate selector.
func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file trecresults.QrelsFile, cache combinator.QueryCacher) OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:         source,
		qrels:      file,
		bestRelRet: -1.0,
		bestRet:    -1.0,
		seen:       cache,
	}
	return oc
}
