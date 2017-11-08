package pipeline

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
)

// GroovePipeline contains all the information for executing a pipeline for query analysis.
type GroovePipeline struct {
	QueriesSource    query.QueriesSource
	StatisticsSource stats.StatisticsSource
	Measurements     []analysis.Measurement
	OutputFormats    []output.Formatter
}

// NewGroovePipeline creates a new groove pipeline.
func NewGroovePipeline(qs query.QueriesSource, ss stats.StatisticsSource, m []analysis.Measurement, fmts []output.Formatter) GroovePipeline {
	return GroovePipeline{
		QueriesSource:    qs,
		StatisticsSource: ss,
		Measurements:     m,
		OutputFormats:    fmts,
	}
}

// Execute runs a groove pipeline for a particular directory of queries.
func (pipeline GroovePipeline) Execute(directory string) ([]string, error) {
	outputs := []string{}

	// Load and process the queries.
	queries, err := pipeline.QueriesSource.Load(directory)
	if err != nil {
		return outputs, err
	}

	// Compute measurements for each of the queries.
	headers := []string{}
	data := make([][]float64, len(pipeline.Measurements))
	for mi, measurement := range pipeline.Measurements {
		headers = append(headers, measurement.Name())
		data[mi] = make([]float64, len(queries))
		for qi, queryRep := range queries {
			data[mi][qi], err = measurement.Execute(queryRep, pipeline.StatisticsSource)
			if err != nil {
				return outputs, err
			}
		}
	}

	// Format the measurement results into specified formats.
	for _, formatter := range pipeline.OutputFormats {
		outputs = append(outputs, formatter(headers, data))
	}

	// Return the formatted results.
	return outputs, nil
}
