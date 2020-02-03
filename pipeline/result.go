package pipeline

import (
	"github.com/hscells/cqr"
	"github.com/hscells/trecresults"
)

// QueryResult is the result of a transformation.
type QueryResult struct {
	Topic          string
	Name           string
	Transformation cqr.CommonQueryRepresentation
}

// ResultType is the type of result being returned through a pipeline channel.
type ResultType uint8

const (
	// Measurement is is a value about the query (e.g. QPP)
	Measurement ResultType = iota
	// Evaluation is an evaluation result.
	Evaluation
	// Transformation is a transformation made to the query.
	Transformation
	// TrecResult is a complete trec-style result.
	TrecResult
	// Error indicates an error was raised.
	Error
	// Done indicates the pipeline has completed.
	Done
)

// Result is the output of a groove pipeline.
type Result struct {
	Topic          string
	Measurements   map[string]float64
	Evaluations    map[string]float64
	Transformation QueryResult
	TrecResults    *trecresults.ResultList
	Type           ResultType
	Error          error
}

// ToGroovePipelineQuery converts a QueryResult into a pipeline query.
func (qr QueryResult) ToGroovePipelineQuery() Query {
	return Query{
		Topic: qr.Topic,
		Name:  qr.Name,
		Query: qr.Transformation,
	}
}
