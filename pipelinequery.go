package groove

import (
	"github.com/hscells/cqr"
)

type PipelineQuery struct {
	name        string
	topic       int64
	original    cqr.CommonQueryRepresentation
	processed   cqr.CommonQueryRepresentation
	transformed cqr.CommonQueryRepresentation
}

func (gq PipelineQuery) Original() cqr.CommonQueryRepresentation {
	return gq.original
}

func (gq *PipelineQuery) SetProcessed(q cqr.CommonQueryRepresentation) PipelineQuery {
	gq.processed = q
	return *gq
}

func (gq PipelineQuery) Processed() cqr.CommonQueryRepresentation {
	return gq.processed
}

func (gq *PipelineQuery) SetTransformed(q cqr.CommonQueryRepresentation) PipelineQuery {
	gq.transformed = q
	return *gq
}

func (gq PipelineQuery) Transformed() cqr.CommonQueryRepresentation {
	return gq.transformed
}

func (gq PipelineQuery) Name() string {
	return gq.name
}

func (gq PipelineQuery) Topic() int64 {
	return gq.topic
}

func NewPipelineQuery(name string, topic int64, original cqr.CommonQueryRepresentation, params ...func(*PipelineQuery)) *PipelineQuery {
	pq := PipelineQuery{name: name, original: original, topic: topic}
	for _, param := range params {
		param(&pq)
	}
	return &pq
}
