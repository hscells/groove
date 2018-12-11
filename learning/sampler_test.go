package learning_test

import (
	"github.com/hscells/groove/learning"
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
