package eval

import (
	"fmt"
	"github.com/hscells/trecresults"
	"math"
)

type recallEvaluator struct{}
type precisionEvaluator struct{}
type numRel struct{}
type numRet struct{}
type numRelRet struct{}
type workSavedOverSampling struct{ N float64 }

// FMeasure computes f-measure, with the beta parameter controlling the precision and recall trade-off.
type FMeasure struct {
	beta float64
}

var (
	// RecallEvaluator calculates recall.
	RecallEvaluator = recallEvaluator{}
	// PrecisionEvaluator calculates precision.
	PrecisionEvaluator = precisionEvaluator{}
	// NumRel is the number of relevant documents.
	NumRel = numRel{}
	// NumRet is the number of retrieved documents.
	NumRet = numRet{}
	// NumRelRet is the number of relevant documents retrieved.
	NumRelRet = numRelRet{}

	// F1Measure is f-measure with beta=1.
	F1Measure = FMeasure{beta: 1}
	// F05Measure is f-measure with beta=0.5.
	F05Measure = FMeasure{beta: 0.5}
	// F3Measure is f-measure with beta=3.
	F3Measure = FMeasure{beta: 3}
)

func (rec recallEvaluator) Name() string {
	return "Recall"
}

func (rec recallEvaluator) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	numRel := 0.0
	numRelRet := 0.0
	for _, result := range *results {
		docID := result.DocId
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > 0 {
				numRelRet++
			}
		}
	}

	for _, qrel := range qrels {
		if qrel.Score > 0 {
			numRel++
		}
	}

	if numRel == 0 {
		return 0.0
	}

	return numRelRet / numRel
}

func (rec precisionEvaluator) Name() string {
	return "Precision"
}

func (rec precisionEvaluator) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	numRet := float64(len(*results))
	numRelRet := 0.0
	for _, result := range *results {
		docID := result.DocId
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > 0 {
				numRelRet++
			}
		}
	}

	if numRet == 0 {
		return 0.0
	}

	return numRelRet / numRet
}

func (numRel) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	n := 0.0
	for _, qrel := range qrels {
		if qrel.Score > 0 {
			n++
		}
	}
	return n
}

func (numRel) Name() string {
	return "NumRel"
}

func (numRet) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	return float64(len(*results))
}

func (numRet) Name() string {
	return "NumRet"
}

func (numRelRet) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	n := 0.0
	for _, result := range *results {
		docID := result.DocId
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > 0 {
				n++
			}
		}
	}
	return n
}

func (numRelRet) Name() string {
	return "NumRelRet"
}

// Score uses the beta parameter to compute f-measure.
func (f FMeasure) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	precision := PrecisionEvaluator.Score(results, qrels)
	recall := RecallEvaluator.Score(results, qrels)
	if precision == 0 || recall == 0 {
		return 0
	}
	betaSquared := math.Pow(f.beta, 2)
	return ((1 + betaSquared) * (precision * recall)) / ((betaSquared * precision) + recall)
}

// Name calculates the name of the f-measure with beta parameter.
func (f FMeasure) Name() string {
	return fmt.Sprintf("F%vMeasure", f.beta)
}

func (w workSavedOverSampling) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	// WSS computes work saved over sampling. This is converted from the Python function below:
	//
	// # TN: total_col - num_ret - (num_rel - rel_ret)
	// # FN: (num_rel - rel_ret)
	// # WSS = (total_col - num_ret / total_colN) - (1 - recall)
	// wss = lambda N, num_ret, rel_ret, recall: ((N - num_ret) / N) - (1 - recall)
	ret := NumRet.Score(results, qrels)
	recall := RecallEvaluator.Score(results, qrels)
	return ((w.N - ret) / w.N) - (1.0 - recall)
}

func (workSavedOverSampling) Name() string {
	return "WSS"
}

func NewWSSEvaluator(collectionSize float64) Evaluator {
	return workSavedOverSampling{
		N: collectionSize,
	}
}
