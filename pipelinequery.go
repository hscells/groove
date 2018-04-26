package groove

import (
	"github.com/hscells/cqr"
)

// PipelineQuery stores information about a query before it is measured, analysed, or executed.
// In most circumstances, the `transformed` query should be used, as it is the preprocessed,
// transformed, and rewritten query.
type PipelineQuery struct {
	Topic string
	Name  string
	Query cqr.CommonQueryRepresentation
}

// NewPipelineQuery creates a new groove pipeline query.
func NewPipelineQuery(name string, topic string, query cqr.CommonQueryRepresentation) PipelineQuery {
	return PipelineQuery{Name: name, Topic: topic, Query: query}
}
