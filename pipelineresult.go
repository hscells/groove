package groove

import (
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/cqr"
)

// QueryResult is the result of a transformation.
type QueryResult struct {
	Topic          int64
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
	Topic          int64
	Measurements   []string
	Evaluations    []string
	Transformation QueryResult
	TrecResults    *trecresults.ResultList
	Type           ResultType
	Error          error
}

func (qr QueryResult) ToGroovePipelineQuery() PipelineQuery {
	return PipelineQuery{
		Topic: qr.Topic,
		Name:  qr.Name,
		Query: qr.Transformation,
	}
}
