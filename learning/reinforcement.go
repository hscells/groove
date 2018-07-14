package learning

import "io"

type ReinforcementQueryCandidateSelector struct {
	Depth int
}

func (ReinforcementQueryCandidateSelector) Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error) {
	panic("implement me")
}

func (ReinforcementQueryCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	panic("implement me")
}

func (ReinforcementQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (sel ReinforcementQueryCandidateSelector) StoppingCriteria() bool {
	return sel.Depth > 5
}
