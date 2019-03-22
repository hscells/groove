package eval_test

import (
	"github.com/hscells/groove/eval"
	"github.com/hscells/trecresults"
	"os"
	"testing"
)

func TestPrecision(t *testing.T) {
	f, err := os.OpenFile("testdata/positive_precision.results", os.O_RDONLY, 0777)
	if err != nil {
		t.Fatal(err)
	}
	results, err := trecresults.ResultsFromReader(f)
	if err != nil {
		t.Fatal(err)
	}
	f, err = os.OpenFile("testdata/sigir2018medline.qrels", os.O_RDONLY, 0777)
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(f)
	if err != nil {
		t.Fatal(err)
	}

	topic := "107"

	l := results.Results[topic]
	t.Log(eval.Precision.Score(&l, qrels.Qrels[topic]))
	t.Log(eval.Recall.Score(&l, qrels.Qrels[topic]))
}
