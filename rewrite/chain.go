package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/stats"
	"fmt"
)

type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
}

type QueryChainCandidateSelector interface {
	Select(query groove.PipelineQuery, transformations []Transformation) (groove.PipelineQuery, error)
	StoppingCriteria() bool
}

func NewQueryChain(selector QueryChainCandidateSelector, transformations ...Transformation) *QueryChain {
	return &QueryChain{
		CandidateSelector: selector,
		Transformations:   transformations,
	}
}

func (qc QueryChain) Execute(query groove.PipelineQuery) (groove.PipelineQuery, error) {
	nq := query
	if !qc.CandidateSelector.StoppingCriteria() {
		fmt.Println("---------------------------------------")
		var err error
		nq, err = qc.CandidateSelector.Select(query, qc.Transformations)
		if err != nil {
			return groove.PipelineQuery{}, err
		}
		return qc.Execute(nq)
	}
	return nq, nil
}

type OracleQueryChainCandidateSelector struct {
	depth         int
	minResults    float64
	bestPrecision float64
	prevPrecision float64
	qrels         trecresults.QrelsFile
	ss            stats.StatisticsSource
}

func (oc *OracleQueryChainCandidateSelector) Select(query groove.PipelineQuery, transformations []Transformation) (groove.PipelineQuery, error) {
	oc.depth++
	oc.prevPrecision = oc.bestPrecision

	if oc.minResults == 0 {
		var err error
		oc.minResults, err = oc.ss.RetrievalSize(query.Transformed())
		if err != nil {
			return groove.PipelineQuery{}, err
		}
	}

	var queries []cqr.CommonQueryRepresentation

	// Apply the transformations to all of the queries.
	for _, transformation := range transformations {
		q, err := transformation.Apply(query.Transformed())
		if err != nil {
			return groove.PipelineQuery{}, err
		}

		queries = append(queries, q...)
	}

	fmt.Printf("generated %v transformation candidates\n", len(queries))

	bestPrecision, precisionQuery := 0.0, cqr.BooleanQuery{}
	for _, applied := range queries {
		// The new query.
		nq := groove.NewPipelineQuery(query.Name(), query.Topic(), query.Transformed()).SetTransformed(func() cqr.CommonQueryRepresentation { return applied })

		// Don't continue if the query is retrieving MORE results and test if the query is capable of being executed.
		resultSize, err := oc.ss.RetrievalSize(applied)
		if err != nil {
			continue
		}
		fmt.Println(resultSize, oc.minResults)
		if resultSize == 0 || resultSize > oc.minResults || resultSize > 1000000 {
			continue
		}

		// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
		results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
		if err != nil {
			return groove.PipelineQuery{}, err
		}

		evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, oc.qrels, query.Topic())
		precision := evaluation[eval.PrecisionEvaluator.Name()]

		if precision > bestPrecision {
			bestPrecision = precision
			oc.bestPrecision = bestPrecision
			oc.minResults = resultSize
			precisionQuery = applied.(cqr.BooleanQuery)
		}

		fmt.Println(evaluation)

	}
	return groove.NewPipelineQuery(query.Name(), query.Topic(), precisionQuery).SetTransformed(func() cqr.CommonQueryRepresentation { return precisionQuery }), nil
}

func (oc *OracleQueryChainCandidateSelector) StoppingCriteria() bool {
	if oc.depth == 10 || oc.prevPrecision == oc.bestPrecision {
		oc.depth = 0
		oc.prevPrecision = -1
		oc.bestPrecision = 0
		oc.minResults = 0
		return true
	}
	return false
}

func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file trecresults.QrelsFile) *OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:    source,
		qrels: file,
	}
	oc.prevPrecision = -1
	return &oc
}
