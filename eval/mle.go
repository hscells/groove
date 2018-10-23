package eval

import (
	"fmt"
	"github.com/hscells/trecresults"
	"math"
)

// MaximumLikelihoodEvaluator is similar to ResidualEvaluator, except that the
// proportion of the residual that should be labelled relevant is computed as
// a maximum likelihood probability. That is, the number of unjudged documents
// that should be labelled with explicit positive relevance labels is computed
// using the ratio of relevant documents to non-relevant documents.
type MaximumLikelihoodEvaluator struct {
	Evaluator
}

// Probability computes the maximum likelihood that a given unjudged document
// can be considered relevant.
func (m MaximumLikelihoodEvaluator) Probability(qrels trecresults.Qrels) int64 {
	var r, nr float64 = 0, 0
	// Consider scores above 1 as relevant.
	for _, q := range qrels {
		switch q.Score {
		case 0:
			nr++
		default:
			r++
		}
	}
	// We take the floor of the result because it doesn't
	// make sense to have a fraction of a document.
	return int64(math.Floor(r / nr))
}

func (m MaximumLikelihoodEvaluator) Residual(results *trecresults.ResultList, qrels trecresults.Qrels) trecresults.Qrels {
	// Create a copy of the qrels to return.
	unjudged := make(trecresults.Qrels)
	for k, v := range qrels {
		unjudged[k] = v
	}

	mle := m.Probability(qrels)
	var n int64 = 0

	// Add the unjudged documents into the qrels with a positive relevance label while n < mle.
	for _, result := range *results {
		// For performance, we can simply exit the loop when n >= mle.
		if n >= mle {
			break
		}
		// Add the document to the qrels if it is in the unjudged set.
		d := result.DocId
		if _, ok := unjudged[d]; !ok {
			unjudged[d] = &trecresults.Qrel{
				Topic:     result.Topic,
				Iteration: "Q0",
				DocId:     d,
				Score:     1,
			}
			// Do not forget to increase n.
			n++
		}
	}
	return unjudged
}

func (m MaximumLikelihoodEvaluator) Name() string {
	return fmt.Sprintf("%s%s", "MLE", m.Evaluator.Name())
}

func (m MaximumLikelihoodEvaluator) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	return m.Evaluator.Score(results, m.Residual(results, qrels))
}

// NewMaximumLikelihoodEvaluator creates a new mle residual evaluator
// by wrapping an existing evaluation metric.
func NewMaximumLikelihoodEvaluator(evaluator Evaluator) MaximumLikelihoodEvaluator {
	return MaximumLikelihoodEvaluator{
		Evaluator: evaluator,
	}
}
