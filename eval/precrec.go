package eval

import (
	"fmt"
	"github.com/hscells/trecresults"
	"math"
)

var RelevanceGrade int64 = 1

type recall struct{}
type precision struct{}
type PrecisionAtK struct{ K int }
type RecallAtK struct{ K int }

type numRel struct{}
type numRet struct{}
type numRelRet struct{}
type WorkSavedOverSampling struct{ N float64 }

// FMeasure computes f-measure, with the beta parameter controlling the precision and recall trade-off.
type FMeasure struct {
	beta float64
}

var (
	// Recall calculates recall.
	Recall = recall{}
	// Precision calculates precision.
	Precision = precision{}
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

func (rec recall) Name() string {
	return "Recall"
}

func (rec recall) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	if float64(len(*results)) == 0 {
		return 0.0
	}
	numRel := 0.0
	numRelRet := 0.0    // tp
	numRelNotRet := 0.0 // fn
	seen := make(map[string]struct{})
	for _, result := range *results {
		docID := result.DocId
		if _, ok := seen[docID]; ok {
			continue
		}
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > RelevanceGrade {
				numRelRet++
			}
		}
		seen[docID] = struct{}{}
	}

	for _, qrel := range qrels {
		if qrel.Score > RelevanceGrade {
			numRel++
		}
	}

	if numRel == 0 {
		return 0.0
	}

	numRelNotRet = numRel - numRelRet

	return numRelRet / (numRelRet + numRelNotRet)
}

func (rec precision) Name() string {
	return "Precision"
}

func (rec precision) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	if float64(len(*results)) == 0 {
		return 0.0
	}
	numRelRet := 0.0    // tp
	numNonRelRet := 0.0 // fp
	seen := make(map[string]struct{})
	for _, result := range *results {
		docID := result.DocId
		if _, ok := seen[docID]; ok {
			continue
		}
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > RelevanceGrade {
				numRelRet++
			} else {
				numNonRelRet++
			}
		} else {
			numNonRelRet++
		}
		seen[docID] = struct{}{}
	}

	if numRelRet == 0 || numNonRelRet == 0 {
		return 0.0
	}

	return numRelRet / (numRelRet + numNonRelRet)
}

func (e PrecisionAtK) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	if results.Len() < e.K {
		for i := results.Len(); i < e.K; i++ {
			*results = append(*results, &trecresults.Result{
				Score: 0,
				Rank:  int64(i),
			})
		}
		return Precision.Score(results, qrels)
	} else {
		rl := (*results)[:e.K]
		return Precision.Score(&rl, qrels)
	}
}

func (e PrecisionAtK) Name() string {
	return fmt.Sprintf("Precision@%d", e.K)
}

func (e RecallAtK) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	if results.Len() < e.K {
		return Recall.Score(results, qrels)
	} else {
		r := make(trecresults.ResultList, e.K)
		for i, res := range *results {
			if i >= e.K {
				break
			}
			r[i] = res
		}
		return Recall.Score(&r, qrels)
	}
}

func (e RecallAtK) Name() string {
	return fmt.Sprintf("Recall@%d", e.K)
}

func (numRel) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	n := 0.0
	seen := make(map[string]struct{})
	for _, qrel := range qrels {
		docID := qrel.DocId
		if _, ok := seen[docID]; ok {
			continue
		}
		if qrel.Score > RelevanceGrade {
			n++
		}
		seen[docID] = struct{}{}
	}
	return n
}

func (numRel) Name() string {
	return "NumRel"
}

func (numRet) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	n := 0.0
	seen := make(map[string]struct{})
	for _, result := range *results {
		docID := result.DocId
		if _, ok := seen[docID]; ok {
			continue
		}
		n++
		seen[docID] = struct{}{}
	}
	return n
}

func (numRet) Name() string {
	return "NumRet"
}

func (numRelRet) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	n := 0.0
	seen := make(map[string]struct{})
	for _, result := range *results {
		docID := result.DocId
		if _, ok := seen[docID]; ok {
			continue
		}
		if qrel, ok := qrels[docID]; ok {
			if qrel.Score > RelevanceGrade {
				n++
			}
		}
		seen[docID] = struct{}{}
	}
	return n
}

func (numRelRet) Name() string {
	return "NumRelRet"
}

// Score uses the beta parameter to compute f-measure.
func (f FMeasure) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	precision := Precision.Score(results, qrels)
	recall := Recall.Score(results, qrels)
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

func (w WorkSavedOverSampling) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	// WSS computes work saved over sampling. This is converted from the Python function below:
	//
	// # TN: total_col - num_ret - (num_rel - rel_ret)
	// # FN: (num_rel - rel_ret)
	// # WSS = (total_col - num_ret / total_colN) - (1 - recall)
	// wss = lambda N, num_ret, rel_ret, recall: ((N - num_ret) / N) - (1 - recall)
	ret := NumRet.Score(results, qrels)
	recall := Recall.Score(results, qrels)
	return ((w.N - ret) / w.N) - (1.0 - recall)
}

func (WorkSavedOverSampling) Name() string {
	return "WSS"
}

func NewWSSEvaluator(collectionSize float64) Evaluator {
	return WorkSavedOverSampling{
		N: collectionSize,
	}
}
