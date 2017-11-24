// Package pipeline provides a framework for constructing reproducible query experiments.
package pipeline

import (
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"log"
)

type empty struct{}

// GroovePipeline contains all the information for executing a pipeline for query analysis.
type GroovePipeline struct {
	QueriesSource    query.QueriesSource
	StatisticsSource stats.StatisticsSource
	Preprocess       []preprocess.QueryProcessor
	Transformations  preprocess.QueryTransformations
	Measurements     []analysis.Measurement
	OutputFormats    []output.Formatter
	OutputTrec       output.TrecResults
}

// Preprocess adds preprocessors to the pipeline.
func Preprocess(processor ...preprocess.QueryProcessor) func() interface{} {
	return func() interface{} {
		return processor
	}
}

// Measurement adds measurements to the pipeline.
func Measurement(measurements ...analysis.Measurement) func() interface{} {
	return func() interface{} {
		return measurements
	}
}

// Output adds outputs to the pipeline.
func Output(formatter ...output.Formatter) func() interface{} {
	return func() interface{} {
		return formatter
	}
}

// Transformations adds transformations to the pipeline.
func Transformations(path string, transformations preprocess.Transformations) func() interface{} {
	return func() interface{} {
		return preprocess.QueryTransformations{
			Transformations: transformations,
			Output:          path,
		}
	}
}

// Trec configures trec output.
func Trec(path string) func() interface{} {
	return func() interface{} {
		return output.TrecResults{
			Path: path,
		}
	}
}

// NewGroovePipeline creates a new groove pipeline. The query source and statistics source are required. Additional
// components are provided via the optional functional arguments.
func NewGroovePipeline(qs query.QueriesSource, ss stats.StatisticsSource, components ...func() interface{}) GroovePipeline {
	gp := GroovePipeline{
		QueriesSource:    qs,
		StatisticsSource: ss,
	}

	for _, component := range components {
		val := component()
		switch v := val.(type) {
		case []preprocess.QueryProcessor:
			gp.Preprocess = v
		case []analysis.Measurement:
			gp.Measurements = v
		case []output.Formatter:
			gp.OutputFormats = v
		case preprocess.QueryTransformations:
			gp.Transformations = v
		}
	}

	return gp
}

// Execute runs a groove pipeline for a particular directory of queries.
func (pipeline GroovePipeline) Execute(directory string) (groove.PipelineResult, error) {
	result := groove.PipelineResult{}

	// Load and process the queries.
	queries, err := pipeline.QueriesSource.Load(directory)
	if err != nil {
		return result, err
	}

	// This means preprocessing the query.
	measurementQueries := make([]groove.PipelineQuery, len(queries))
	for i, q := range queries {
		for _, p := range pipeline.Preprocess {
			measurementQueries[i] = queries[i].SetProcessed(preprocess.ProcessQuery(q.Original(), p))
		}
		// As well as applying any transformations.
		measurementQueries[i] = measurementQueries[i].SetTransformed(pipeline.Transformations.Transformations.Apply(q))
	}

	// Compute measurements for each of the queries.
	// The measurements are computed in parallel.
	N := len(pipeline.Measurements)
	headers := make([]string, N)
	data := make([][]float64, N)
	//sem := make(chan empty, N)
	//for mi, measurement := range pipeline.Measurements {
	//	// The inner loop is run concurrently.
	//	go func(i int, m analysis.Measurement) {
	//		headers[i] = m.Name()
	//		data[i] = make([]float64, len(queries))
	//		for qi, measurementQuery := range measurementQueries {
	//			data[i][qi], err = m.Execute(measurementQuery, pipeline.StatisticsSource)
	//			if err != nil {
	//				log.Fatal(err)
	//			}
	//		}
	//		sem <- empty{}
	//	}(mi, measurement)
	//}
	//for i := 0; i < N; i++ {
	//	<-sem
	//}
	for i, m := range pipeline.Measurements {
		headers[i] = m.Name()
		data[i] = make([]float64, len(queries))
		for qi, measurementQuery := range measurementQueries {
			data[i][qi], err = m.Execute(measurementQuery, pipeline.StatisticsSource)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	// Format the measurement results into specified formats.
	outputs := make([]string, len(pipeline.OutputFormats))
	for i, formatter := range pipeline.OutputFormats {
		outputs[i] = formatter(headers, data)
	}
	result.Measurements = outputs

	// Output the transformed queries
	transformations := make([]groove.QueryResult, len(measurementQueries))
	for i, mq := range measurementQueries {
		transformations[i] = groove.QueryResult{Name: mq.Name(), Transformation: mq.Transformed()}
	}
	result.Transformations = transformations

	// Output the trec results.
	if len(pipeline.OutputTrec.Path) > 0 {
		trecResults := make(trecresults.ResultList, 0)
		for _, q := range queries {
			r, err := pipeline.StatisticsSource.Execute(q, pipeline.StatisticsSource.SearchOptions())
			if err != nil {
				log.Fatal(err)
			}
			trecResults = append(trecResults, r...)
		}
		result.TrecResults = &trecResults
	}
	// Return the formatted results.
	return result, nil
}
