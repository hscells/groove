package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
)

// Measurement is a representation for how a measurement fits into the pipeline.
type Measurement interface {
	// Name is the name of the measurement in the output. It should not contain any spaces.
	Name() string
	// Execute computes the implemented measurement for a query and optionally using the specified statistics.
	Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error)
}
