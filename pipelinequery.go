package groove

import (
	"github.com/hscells/cqr"
)

// PipelineQuery stores information about a query before it is measured, analysed, or executed.
type PipelineQuery struct {
	name        string
	topic       int64
	original    cqr.CommonQueryRepresentation
	processed   cqr.CommonQueryRepresentation
	transformed cqr.CommonQueryRepresentation
}

// Original is the initial query before preprocessing or transformation.
func (gq *PipelineQuery) Original() cqr.CommonQueryRepresentation {
	return gq.original
}

// SetProcessed initiates the processed variation of the query.
func (gq *PipelineQuery) SetProcessed(q cqr.CommonQueryRepresentation) PipelineQuery {
	gq.processed = q
	return *gq
}

// Processed gets the processed variation of the query.
func (gq *PipelineQuery) Processed() cqr.CommonQueryRepresentation {
	return gq.processed
}

// SetTransformed initiates the transformed variation of the query.
func (gq *PipelineQuery) SetTransformed(q func() cqr.CommonQueryRepresentation) PipelineQuery {
	gq.transformed = q()
	return *gq
}

// Transformed gets the transformed variation of the query.
func (gq *PipelineQuery) Transformed() cqr.CommonQueryRepresentation {
	return gq.transformed
}

// Name is the name of the query.
func (gq *PipelineQuery) Name() string {
	return gq.name
}

// Topic is the topic of the query for use in trec runs.
func (gq *PipelineQuery) Topic() int64 {
	return gq.topic
}

// NewPipelineQuery creates a new groove pipeline query.
func NewPipelineQuery(name string, topic int64, original cqr.CommonQueryRepresentation, params ...func(*PipelineQuery)) *PipelineQuery {
	pq := PipelineQuery{name: name, original: original, topic: topic}
	for _, param := range params {
		param(&pq)
	}
	return &pq
}
