package eval

import (
	"github.com/hscells/trecresults"
)

var (
	// NNR computes the number of documents needed to read.
	// Or in other words, the gain required per relevant document.
	NNR = numberNeededToRead{}
)

type numberNeededToRead struct{}

func (n numberNeededToRead) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	return (NumRet.Score(results, qrels) + 1) / (NumRelRet.Score(results, qrels) + 1)
}

func (n numberNeededToRead) Name() string {
	return "NNR"
}
