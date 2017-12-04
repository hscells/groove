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

// PipelineResult is the output of a groove pipeline.
type PipelineResult struct {
	Measurements    []string
	Evaluations     []string
	Transformations []QueryResult
	TrecResults     *trecresults.ResultList
}
