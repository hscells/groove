package groove

import (
	"github.com/hscells/cqr"
)

type QueryResult struct {
	Name           string
	Transformation cqr.CommonQueryRepresentation
}

type PipelineResult struct {
	Measurements    []string
	Transformations []QueryResult
}
