package eval

import (
	"github.com/hscells/trecresults"
)

type recallEvaluator struct{}
type precisionEvaluator struct{}
type numRel struct{}
type numRet struct{}
type numRelRet struct{}

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
)

func (rec recallEvaluator) Name() string {
	return "Recall"
}

func (rec recallEvaluator) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	numRel := 0.0
	numRelRet := 0.0
	for _, result := range *results {
		docId := result.DocId
		if qrel, ok := qrels[docId]; ok {
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
		docId := result.DocId
		if qrel, ok := qrels[docId]; ok {
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
		docId := result.DocId
		if qrel, ok := qrels[docId]; ok {
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
