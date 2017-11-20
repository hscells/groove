// Package pipeline provides a framework for constructing reproducible query experiments.
package pipeline

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
	"log"
)

type empty struct{}

// GroovePipeline contains all the information for executing a pipeline for query analysis.
type GroovePipeline struct {
	QueriesSource    query.QueriesSource
	StatisticsSource stats.StatisticsSource
	Preprocess       []preprocess.QueryProcessor
	Measurements     []analysis.Measurement
	OutputFormats    []output.Formatter
}

// NewGroovePipeline creates a new groove pipeline.
func NewGroovePipeline(qs query.QueriesSource, ss stats.StatisticsSource, pre []preprocess.QueryProcessor, m []analysis.Measurement, fmts []output.Formatter) GroovePipeline {
	return GroovePipeline{
		QueriesSource:    qs,
		StatisticsSource: ss,
		Preprocess:       pre,
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

	measurementQueries := make([]groove.PipelineQuery, len(queries))
	for i, q := range queries {
		for _, p := range pipeline.Preprocess {
			measurementQueries[i] = groove.NewPipelineQuery(queries[i], preprocess.ProcessQuery(q, p))
		}
	}

	// Compute measurements for each of the queries.
	// The measurements are computed in parallel.
	N := len(pipeline.Measurements)
	headers := make([]string, N)
	data := make([][]float64, N)
	sem := make(chan empty, N)
	for mi, measurement := range pipeline.Measurements {
		// The inner loop is run concurrently.
		go func(i int, m analysis.Measurement) {
			headers[i] = m.Name()
			data[i] = make([]float64, len(queries))
			for qi, measurementQuery := range measurementQueries {
				data[i][qi], err = m.Execute(measurementQuery, pipeline.StatisticsSource)
				if err != nil {
					log.Fatal(err)
				}
			}
			sem <- empty{}
		}(mi, measurement)
	}
	for i := 0; i < N; i++ {
		<-sem
	}
	// Format the measurement results into specified formats.
	for _, formatter := range pipeline.OutputFormats {
		outputs = append(outputs, formatter(headers, data))
	}

	// Return the formatted results.
	return outputs, nil
}
