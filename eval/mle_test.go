package eval_test

import (
	"github.com/hscells/groove/eval"
	"github.com/hscells/trecresults"
	"testing"
)

func TestMLE(t *testing.T) {
	qrels := trecresults.Qrels{
		"0": &trecresults.Qrel{
			Topic:     "1",
			Score:     0,
			DocId:     "0",
			Iteration: "0",
		},
		"1": &trecresults.Qrel{
			Topic:     "1",
			Score:     0,
			DocId:     "1",
			Iteration: "0",
		},
		"2": &trecresults.Qrel{
			Topic:     "1",
			Score:     1,
			DocId:     "0",
			Iteration: "0",
		},
		"3": &trecresults.Qrel{
			Topic:     "1",
			Score:     1,
			DocId:     "3",
			Iteration: "0",
		},
		"4": &trecresults.Qrel{
			Topic:     "1",
			Score:     1,
			DocId:     "4",
			Iteration: "0",
		},
		"5": &trecresults.Qrel{
			Topic:     "1",
			Score:     1,
			DocId:     "5",
			Iteration: "0",
		},
		"6": &trecresults.Qrel{
			Topic:     "1",
			Score:     1,
			DocId:     "6",
			Iteration: "0",
		},
	}

	e := eval.NewMaximumLikelihoodEvaluator(eval.PrecisionEvaluator)
	t.Log(e.Probability(qrels))

	results := &trecresults.ResultList{
		&trecresults.Result{Topic: "1", DocId: "2"},
		&trecresults.Result{Topic: "1", DocId: "3"},
		&trecresults.Result{Topic: "1", DocId: "4"},
		&trecresults.Result{Topic: "1", DocId: "5"},
		&trecresults.Result{Topic: "1", DocId: "6"},
		&trecresults.Result{Topic: "1", DocId: "7"},
		&trecresults.Result{Topic: "1", DocId: "8"},
		&trecresults.Result{Topic: "1", DocId: "9"},
	}

	t.Log(eval.PrecisionEvaluator.Score(results, qrels))
	t.Log(e.Score(results, qrels))
}
