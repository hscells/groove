package groove

import (
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/cqr"
)

// QueryResult is the result of a transformation.
type QueryResult struct {
	Name           string
	Transformation cqr.CommonQueryRepresentation
}

type ResultType uint8

const (
	Measurement    ResultType = iota
	Evaluation
	Transformation
	TrecResult
	Error
	Done
)

// PipelineResult is the output of a groove pipeline.
type PipelineResult struct {
	Measurements    []string
	Evaluations     []string
	Transformations []QueryResult
	TrecResults     *trecresults.ResultList
	Error           error
	Topic           int64
	Type            ResultType
}
