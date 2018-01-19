package pipeline

import (
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove"
	"io"
	"bytes"
	"github.com/ewalker544/libsvm-go"
)

type QueryChainSVM struct {
	Selector        rewrite.OracleQueryChainCandidateSelector
	Transformations []rewrite.Transformation
	Queries         []groove.PipelineQuery
	Features        string
	Model           string
	ShouldTrain     bool
	ShouldExtract   bool
}

func (q *QueryChainSVM) AppendQuery(query groove.PipelineQuery) {
	q.Queries = append(q.Queries, query)
}

// WriteFeatures extracts features from queries and writes them to a LIBSVM compatible file.
func (q QueryChainSVM) WriteFeatures(writer io.Writer) (int, error) {
	var learntFeatures []rewrite.LearntFeature
	for _, query := range q.Queries {
		t, err := q.Selector.Features(query, q.Transformations)
		if err != nil {
			return 0, err
		}
		learntFeatures = append(learntFeatures, t...)
	}

	b := new(bytes.Buffer)
	for _, learntFeature := range learntFeatures {
		_, err := learntFeature.WriteLibSVM(b)
		if err != nil {
			return 0, err
		}
	}

	return writer.Write(b.Bytes())
}

// TrainModel trains an SVM using features in LIBSVM format.
func (q QueryChainSVM) TrainModel() error {
	param := libSvm.NewParameter()
	param.SvmType = libSvm.NU_SVR
	param.KernelType = libSvm.POLY

	model := libSvm.NewModel(param)

	problem, err := libSvm.NewProblem(q.Features, param)
	if err != nil {
		return err
	}

	model.Train(problem)

	return model.Dump(q.Model)
}
