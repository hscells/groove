package learning

import (
	"io"
	"bytes"
	"fmt"
	"sort"
)

type ReinforcementQueryCandidateSelector struct {
	Depth    int
	features []LearntFeature
}

func (ReinforcementQueryCandidateSelector) Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error) {
	panic("implement me")
}

func (ReinforcementQueryCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	panic("implement me")
}

func (ReinforcementQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	var b bytes.Buffer
	b.WriteString(lf.Topic)
	b.WriteString(" *")
	for _, score := range lf.Scores {
		b.WriteString(fmt.Sprintf(" %f", score))
	}
	b.WriteString(" *")
	sort.Sort(lf.Features)
	for _, feature := range lf.Features {
		b.WriteString(fmt.Sprintf(" %d:%f", feature.ID, feature.Score))
	}
	b.WriteString(" # ")
	b.WriteString(lf.Comment)
	b.WriteString("\n")
	_, err := w.Write(b.Bytes())
	return err
}

func (sel ReinforcementQueryCandidateSelector) StoppingCriteria() bool {
	return sel.Depth > 5
}
