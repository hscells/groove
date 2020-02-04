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

// Data contains the actual data saved and how to write it to disk.
type Data struct {
	Name  string
	Value DataMarshaler
}

// DataMarshaler describes how data should be written to disk.
type DataMarshaler interface {
	Marshal() ([]byte, error)
}

// SupplementalData is extra data than may output from the formulation process.
type SupplementalData struct {
	Name string
	Data []Data
}

type FormulationResut struct {
	Queries []cqr.CommonQueryRepresentation
	Sup     []SupplementalData
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
	// Formulation is a completed set of query formulations.
	Formulation
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
	Formulation    FormulationResut
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
