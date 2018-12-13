package learning_test

import (
	"bytes"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/learning"
	"github.com/hscells/trecresults"
	"io/ioutil"
	"testing"
)

func TestTransformationSampler(t *testing.T) {
	candidates := []learning.CandidateQuery{
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(2),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
		learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(3),
	}

	tb, err := learning.NewTransformationSampler(20, 0.1, learning.BalancedTransformationStrategy).Sample(candidates)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(len(tb))
	for _, c := range tb {
		t.Log(c.TransformationID)
	}

	t.Log("----")

	ts, err := learning.NewTransformationSampler(20, 0.1, learning.StratifiedTransformationStrategy).Sample(candidates)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(len(ts))
	for _, c := range ts {
		t.Log(c.TransformationID)
	}
}

func TestSamplerEvaluation(t *testing.T) {
	candidates := []learning.ScoredCandidateQuery{
		{Score: 0.0, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.2, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.2, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.1, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.6, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.123, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.14, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.234, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.001, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.023, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.004, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.001, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.043, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.07, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.032, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.12, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.136, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(1)},
		{Score: 0.045, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.064, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.078, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.01, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.001, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.001, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
		{Score: 0.01, CandidateQuery: learning.NewCandidateQuery(nil, "1", nil).SetTransformationID(0)},
	}

	b, err := ioutil.ReadFile("../../boogie/sigir2018medline.qrels")
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	scores := map[string]float64{
		"1": 0.1,
	}

	eb := learning.NewEvaluationSampler(20, 0.1, nil, qrels, nil, nil, scores, learning.BalancedScoredStrategy).ScoredStrategy(candidates, scores, 15)

	t.Log(len(eb))
	for _, c := range eb {
		t.Log(c.TransformationID)
	}

	t.Log("----")

	es := learning.NewEvaluationSampler(20, 0.1, nil, qrels, nil, nil, scores, learning.StratifiedScoredStrategy).ScoredStrategy(candidates, scores, 15)

	t.Log(len(es))
	for _, c := range es {
		t.Log(c.TransformationID)
	}

	t.Log("----")

	ep := learning.NewEvaluationSampler(20, 0.1, nil, qrels, nil, nil, scores, learning.PositiveBiasScoredStrategy).ScoredStrategy(candidates, scores, 15)

	t.Log(len(ep))
	for _, c := range ep {
		t.Log(c.TransformationID)
	}

	t.Log("----")

	en := learning.NewEvaluationSampler(20, 0.1, nil, qrels, nil, nil, scores, learning.NegativeBiasScoredStrategy).ScoredStrategy(candidates, scores, 15)

	t.Log(len(en))
	for _, c := range en {
		t.Log(c.TransformationID)
	}

	t.Log("----")

	em := learning.NewEvaluationSampler(20, 0.1, nil, qrels, nil, nil, scores, learning.MaximalMarginalRelevanceScoredStrategy(0.5, cui2vec.Cosine)).ScoredStrategy(candidates, scores, 15)

	t.Log(len(em))
	for _, c := range em {
		t.Log(c.TransformationID)
	}

}
