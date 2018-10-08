package pipeline

import (
	"github.com/hscells/cqr"
)

// Query stores information about a query before it is measured, analysed, or executed.
// In most circumstances, the `transformed` query should be used, as it is the preprocessed,
// transformed, and rewritten query.
type Query struct {
	Topic string
	Name  string
	Query cqr.CommonQueryRepresentation
}

// NewQuery creates a new groove pipeline query.
func NewQuery(name string, topic string, query cqr.CommonQueryRepresentation) Query {
	return Query{Name: name, Topic: topic, Query: query}
}
