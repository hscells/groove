package groove

import "github.com/hscells/cqr"

type PipelineQuery struct {
	original  cqr.CommonQueryRepresentation
	processed cqr.CommonQueryRepresentation
}

func (gq PipelineQuery) Original() cqr.CommonQueryRepresentation {
	return gq.original
}

func (gq PipelineQuery) Processed() cqr.CommonQueryRepresentation {
	return gq.processed
}

func NewPipelineQuery(original, processed cqr.CommonQueryRepresentation) PipelineQuery {
	return PipelineQuery{original: original, processed: processed}
}
