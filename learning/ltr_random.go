package learning

import (
	"io"
	"math/rand"
)

type RandomCandidateSelector struct {
	depth int
}

func (c RandomCandidateSelector) Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	c.depth++
	if len(transformations) == 0 {
		return query, c, nil
	}
	rand.Shuffle(len(transformations), func(i, j int) {
		transformations[i], transformations[j] = transformations[j], transformations[i]
	})
	return transformations[0], c, nil
}

func (c RandomCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	return []byte{}, nil
}

func (c RandomCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	_, err := lf.WriteLibSVMRank(w)
	return err
}

func (c RandomCandidateSelector) StoppingCriteria() bool {
	if c.depth > 5 {
		return true
	}
	return false
}

func NewRandomCandidateSelector() RandomCandidateSelector {
	return RandomCandidateSelector{
		depth: 0,
	}
}
