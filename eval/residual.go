package eval

import (
	"fmt"
	"github.com/hscells/trecresults"
)

// ResidualEvaluator evaluates using an evaluator in the same manner, however
// it considers all unjudged documents as relevant.
type ResidualEvaluator struct {
	Evaluator
}

// Residual is the set of unjudged documents that are retrieved by a query.
// That is, the documents that do not have explicit relevance labels.
func (r ResidualEvaluator) Residual(results *trecresults.ResultList, qrels trecresults.Qrels) trecresults.Qrels {
	// Create a copy of the qrels to return.
	unjudged := make(trecresults.Qrels)
	for k, v := range qrels {
		unjudged[k] = v
	}
	// Add the unjudged documents into the qrels with a positive relevance label.
	for _, result := range *results {
		d := result.DocId
		if _, ok := unjudged[d]; !ok {
			unjudged[d] = &trecresults.Qrel{
				Topic:     result.Topic,
				Iteration: "Q0",
				DocId:     d,
				Score:     1,
			}
		}
	}
	return unjudged
}

func (r ResidualEvaluator) Name() string {
	return fmt.Sprintf("%s%s", "Residual", r.Evaluator.Name())
}

func (r ResidualEvaluator) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	return r.Evaluator.Score(results, r.Residual(results, qrels))
}

// NewResidualEvaluator creates a new evaluator which wraps an existing evaluator.
func NewResidualEvaluator(evaluator Evaluator) ResidualEvaluator {
	return ResidualEvaluator{
		Evaluator: evaluator,
	}
}
