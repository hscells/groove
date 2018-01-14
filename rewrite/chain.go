package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/stats"
	"log"
)

type QueryChain struct {
	Transformations   []Transformation
	CandidateSelector QueryChainCandidateSelector
}

type QueryChainCandidateSelector interface {
	Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error)
	StoppingCriteria() (QueryChainCandidateSelector, bool)
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
	qc.CandidateSelector, stop = qc.CandidateSelector.StoppingCriteria()
	tq := NewTransformedQuery(query)
	for !stop {
		tq, qc.CandidateSelector, err = qc.CandidateSelector.Select(tq, qc.Transformations)
		if err != nil {
			return TransformedQuery{}, err
		}
		qc.CandidateSelector, stop = qc.CandidateSelector.StoppingCriteria()
	}
	return tq, nil
}

type OracleQueryChainCandidateSelector struct {
	depth         int
	minResults    float64
	bestPrecision float64
	prevPrecision float64
	qrels         *trecresults.QrelsFile
	ss            stats.StatisticsSource
}

func (oc OracleQueryChainCandidateSelector) Select(query TransformedQuery, transformations []Transformation) (TransformedQuery, QueryChainCandidateSelector, error) {
	oc.depth++
	oc.prevPrecision = oc.bestPrecision

	if oc.minResults == 0 {
		var err error
		oc.minResults, err = oc.ss.RetrievalSize(query.Query.Transformed())
		if err != nil {
			return TransformedQuery{}, oc, nil
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

	bestPrecision, precisionQuery := 0.0, query.Query.Transformed()
	for _, applied := range queries {
		// The new query.
		nq := groove.NewPipelineQuery(query.Query.Name(), query.Query.Topic(), query.Query.Transformed()).SetTransformed(func() cqr.CommonQueryRepresentation { return applied.Query })

		// Don't continue if the query is retrieving MORE results and test if the query is capable of being executed.
		resultSize, err := oc.ss.RetrievalSize(applied.Query)
		if err != nil {
			continue
		}

		//fmt.Println(resultSize, oc.minResults)
		if resultSize == 0 || resultSize >= oc.minResults || resultSize > 100000 {
			continue
		}

		// Now, using an oracle heuristic, get the precision and recall for all of the transformations.
		results, err := oc.ss.Execute(nq, oc.ss.SearchOptions())
		if err != nil {
			continue
		}

		evaluation := eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results, *oc.qrels, query.Query.Topic())
		precision := evaluation[eval.PrecisionEvaluator.Name()]

		if precision > bestPrecision {
			bestPrecision = precision
			oc.bestPrecision = bestPrecision
			oc.minResults = resultSize
			precisionQuery = applied.Query
			log.Printf("topic %v: P %v, R %v, %v %v, %v %v, %v %v\n", query.Query.Topic(), evaluation[eval.PrecisionEvaluator.Name()], evaluation[eval.RecallEvaluator.Name()], eval.NumRel.Name(), evaluation[eval.NumRel.Name()], eval.NumRet.Name(), evaluation[eval.NumRet.Name()], eval.NumRelRet.Name(), evaluation[eval.NumRelRet.Name()])
		}

	}

	transformed := groove.NewPipelineQuery(query.Query.Name(), query.Query.Topic(), precisionQuery).SetTransformed(func() cqr.CommonQueryRepresentation { return precisionQuery })
	return query.Append(transformed), oc, nil
}

func (oc OracleQueryChainCandidateSelector) StoppingCriteria() (QueryChainCandidateSelector, bool) {
	if oc.depth == 10 || oc.prevPrecision == oc.bestPrecision {
		oc.depth = 0
		oc.prevPrecision = -1
		oc.bestPrecision = 0
		oc.minResults = 0
		return oc, true
	}
	return oc, false
}

func NewOracleQueryChainCandidateSelector(source stats.StatisticsSource, file *trecresults.QrelsFile) *OracleQueryChainCandidateSelector {
	oc := OracleQueryChainCandidateSelector{
		ss:    source,
		qrels: file,
	}
	oc.prevPrecision = -1
	return &oc
}
