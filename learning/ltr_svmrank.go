//+build svmrank

package learning

import (
	"fmt"
	"github.com/hscells/svmrank"
	"math"
	"os"
	"io"
)

// SVMRankQueryCandidateSelector uses learning to rank to select query chain candidates.
type SVMRankQueryCandidateSelector struct {
	depth     int32
	modelFile string
}

func (sel SVMRankQueryCandidateSelector) Train(features []LearntFeature) ([]byte, error) {
	return nil, nil
}

func (sel SVMRankQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	_, err := lf.WriteLibSVMRank(w)
	return err
}

// Select uses a Ranking SVM to select the next most likely candidate.
func (sel SVMRankQueryCandidateSelector) Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	f, err := os.OpenFile("tmp.features", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, applied := range transformations {
		f.WriteString(fmt.Sprintf("%v%v", applied.Features.String(), "\n"))
	}
	svmrank.Predict("tmp.features", sel.modelFile, "tmp.output")
	candidate, err := getRanking("tmp.output", transformations)

	sel.depth++
	f.Truncate(0)
	f.Seek(0, 0)
	err2 := os.Remove("tmp.features")
	if err2 != nil {
		return CandidateQuery{}, nil, err2
	}

	if err != nil {
		return CandidateQuery{}, nil, err
	}
	if query.Query.String() == candidate.String() {
		sel.depth = math.MaxInt32
	}

	return query.Append(candidate), sel, nil
}

// StoppingCriteria stops when the depth approaches 500.
func (sel SVMRankQueryCandidateSelector) StoppingCriteria() bool {
	return sel.depth >= 5
}

// NewSVMRankQueryCandidateSelector creates a new learning to rank candidate selector.
func NewSVMRankQueryCandidateSelector(modelFile string) SVMRankQueryCandidateSelector {
	return SVMRankQueryCandidateSelector{
		modelFile: modelFile,
	}
}

func NewSVMRankQueryChain(modelFile string) *QueryChain {
	return &QueryChain{
		CandidateSelector: NewSVMRankQueryCandidateSelector(modelFile),
	}
}