// Package query provides sources for loading queries in different formats.
package query

import "github.com/hscells/cqr"

// QueriesSource represents a source for queries and how to parse them.
type QueriesSource interface {
	// Load determines how a query is loaded and parsed into the common query representation format.
	Load(directory string) ([]cqr.CommonQueryRepresentation, error)
}
