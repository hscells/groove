package eval

import (
	"github.com/TimothyJones/trecresults"
)

type distributedness struct{}

var Distributedness = distributedness{}

// generateWeights computes weights for each n retrieved item.
func generateWeights(n int) (weights []float64) {
	weights = make([]float64, n)
	linearWeights := make([]float64, n)

	// First, use a linear function to generate a scaled slice of weights of size n.
	for x := n - 1; x >= 0; x-- {
		// Scale x between -1 and 1.
		xPrime := 2.0*(float64(x)/(float64(n))) - 1.0
		linearWeights[x] = xPrime
	}

	// Now compute actual weights to score documents.
	prevWeight := linearWeights[0]
	for i := 1; i < n; i++ {
		w := linearWeights[i]
		w = w - prevWeight
		if linearWeights[i] < 0 {
			weights[i] = -w
		} else if linearWeights[i] == 0 {
			weights[i] = 0
		} else {
			weights[i] = w
		}
		prevWeight = linearWeights[i]
	}

	return
}

func (distributedness) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	weights := generateWeights(results.Len())
	sumWeights := 0.0
	for i, result := range *results {
		docId := result.DocId
		if score, ok := qrels[docId]; ok {
			if score.Score > 0 {
				sumWeights += weights[i]
			}
		}
	}
	return sumWeights
}

func (distributedness) Name() string {
	return "Distributedness"
}
