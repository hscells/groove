package groove

import (
	"github.com/hscells/cqr"
	"github.com/TimothyJones/trecresults"
)

type QueryResult struct {
	Name           string
	Transformation cqr.CommonQueryRepresentation
}

type PipelineResult struct {
	Measurements    []string
	Transformations []QueryResult
	TrecResults     *trecresults.ResultList
}
