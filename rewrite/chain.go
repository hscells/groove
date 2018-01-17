package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/stats"
	"log"
	"math"
)

type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
}

type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error)
	StoppingCriteria() bool
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
	return tq, nil
}

func (oc OracleQueryChainCandidateSelector) Features(query groove.PipelineQuery, transformations []Transformation) (lf []LearntFeature, err error) {

	bestQuery := query
	for i := 0; i < 5; i++ {
		// Apply the transformations to all of the queries.
		for _, transformation := range transformations {
			cqs, err := transformation.Apply(bestQuery.Transformed())
			if err != nil {
				return nil, err
			}

			for _, candidate := range cqs {
				nq := groove.NewPipelineQuery(query.Name(), query.Topic(), query.Transformed()).SetTransformed(func() cqr.CommonQueryRepresentation { return candidate.Query })

				// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
				results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
				if err != nil {
					continue
				}

				evaluation := eval.Evaluate([]eval.Evaluator{eval.PrecisionEvaluator}, &results, *oc.qrels, bestQuery.Topic())
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
	bestRel    float64
	prevRel    float64

	qrels *trecresults.QrelsFile
	ss    stats.StatisticsSource
}

// Select is a grid search for the best possible query transformation chain.
func (oc OracleQueryChainCandidateSelector) Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error) {
	oc.depth++
	oc.prevRelRet = oc.bestRelRet
	oc.prevRel = oc.bestRel

	if oc.minResults == 0 {
		var err error
		oc.minResults, err = oc.ss.RetrievalSize(query.Query.Transformed())
		if err != nil {
			return query, oc, err
		}
	}

	var queries []CandidateQuery

	// Apply the transformations to all of the queries.
	for _, transformation := range transformations {
		cqs, err := transformation.Apply(query.Query.Transformed())
		if err != nil {
			return TransformedQuery{}, oc, err
		}

		queries = append(queries, cqs...)
	}

	log.Printf("topic %v: generated %v transformation candidates\n", query.Query.Topic(), len(queries))

	bestRelRet, bestRel, precisionQuery := 0.0, math.MaxFloat64, query.Query.Transformed()
	for _, applied := range queries {
		// The new query.
		nq := groove.NewPipelineQuery(query.Query.Name(), query.Query.Topic(), query.Query.Transformed()).SetTransformed(func() cqr.CommonQueryRepresentation { return applied.Query })

		// Don't continue if the query is retrieving MORE results and test if the query is capable of being executed.
		resultSize, err := oc.ss.RetrievalSize(applied.Query)
		if err != nil {
			continue
		}

		log.Printf("topic %v - %v ? %v\n", nq.Topic(), resultSize, oc.minResults)
		if resultSize == 0 || resultSize > 100000 || resultSize >= oc.minResults {
			continue
		}

		// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
		results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
		if err != nil {
			continue
		}

		evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, *oc.qrels, query.Query.Topic())
		numRelRet := evaluation[eval.NumRelRet.Name()]
		numRel := evaluation[eval.NumRel.Name()]

		log.Printf("topic %v - %v: %v, %v: %v", query.Query.Topic(), numRelRet, bestRelRet, numRel, bestRel)
		if numRelRet > 0 && numRelRet >= bestRelRet && numRel <= bestRel {
			bestRelRet = numRelRet
			bestRel = numRel
			oc.bestRelRet = bestRelRet
			oc.bestRel = bestRel
			oc.minResults = resultSize
			precisionQuery = applied.Query
			log.Printf("topic %v - P %v, R %v, %v %v, %v %v, %v %v\n", query.Query.Topic(), evaluation[eval.PrecisionEvaluator.Name()], evaluation[eval.RecallEvaluator.Name()], eval.NumRel.Name(), evaluation[eval.NumRel.Name()], eval.NumRet.Name(), evaluation[eval.NumRet.Name()], eval.NumRelRet.Name(), evaluation[eval.NumRelRet.Name()])
		}

	}

	transformed := groove.NewPipelineQuery(query.Query.Name(), query.Query.Topic(), precisionQuery).SetTransformed(func() cqr.CommonQueryRepresentation { return precisionQuery })
	return query.Append(transformed), oc, nil
}

func (oc OracleQueryChainCandidateSelector) StoppingCriteria() (bool) {
	if oc.depth >= 5 || (oc.bestRelRet == oc.bestRelRet && oc.bestRel == oc.prevRel) {
		return true
	}
	return false
}

func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file *trecresults.QrelsFile) *OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:         source,
		qrels:      file,
		bestRelRet: -1.0,
		bestRel:    -1.0,
	}
	return &oc
}
