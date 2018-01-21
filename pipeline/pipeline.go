// Package pipeline provides a framework for constructing reproducible query experiments.
package pipeline

import (
	"errors"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"log"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/rewrite"
	"runtime"
)

type empty struct{}

// GroovePipeline contains all the information for executing a pipeline for query analysis.
type GroovePipeline struct {
	QueriesSource         query.QueriesSource
	StatisticsSource      stats.StatisticsSource
	Preprocess            []preprocess.QueryProcessor
	Transformations       preprocess.QueryTransformations
	Measurements          []analysis.Measurement
	MeasurementFormatters []output.MeasurementFormatter
	Evaluations           []eval.Evaluator
	EvaluationFormatters  []output.EvaluationFormatter
	EvaluationQrels       trecresults.QrelsFile
	OutputTrec            output.TrecResults
	QueryChain            rewrite.QueryChain
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
func Output(formatter ...output.MeasurementFormatter) func() interface{} {
	return func() interface{} {
		return formatter
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
		case []output.MeasurementFormatter:
			gp.MeasurementFormatters = v
		case preprocess.QueryTransformations:
			gp.Transformations = v
		}
	}

	return gp
}

// Execute runs a groove pipeline for a particular directory of queries.
func (pipeline GroovePipeline) Execute(directory string, c chan groove.PipelineResult) {
	// Load and process the queries.
	queries, err := pipeline.QueriesSource.Load(directory)
	if err != nil {
		c <- groove.PipelineResult{
			Error: err,
			Type:  groove.Error,
		}
		return
	}

	// This means preprocessing the query.
	measurementQueries := make([]groove.PipelineQuery, len(queries))
	topics := make([]string, len(queries))
	for i, q := range queries {
		topics[i] = q.Name
		// Ensure there is a processed query.

		// And apply the processing if there is any.
		for _, p := range pipeline.Preprocess {
			q = groove.NewPipelineQuery(q.Name, q.Topic, preprocess.ProcessQuery(q.Query, p))
		}

		// Apply any transformations.
		for _, t := range pipeline.Transformations.BooleanTransformations {
			q = groove.NewPipelineQuery(q.Name, q.Topic, t(q.Query)())
		}
		for _, t := range pipeline.Transformations.ElasticsearchTransformations {
			if s, ok := pipeline.StatisticsSource.(*stats.ElasticsearchStatisticsSource); ok {
				q = groove.NewPipelineQuery(q.Name, q.Topic, t(q.Query, s)())
			} else {
				log.Fatal("Elasticsearch transformations only work with an Elasticsearch statistics source.")
			}
		}

		measurementQueries[i] = q
	}

	// Compute measurements for each of the queries.
	// The measurements are computed in parallel.
	N := len(pipeline.Measurements)
	headers := make([]string, N)
	data := make([][]float64, N)

	// data[measurement][queryN]
	for i, m := range pipeline.Measurements {
		headers[i] = m.Name()
		data[i] = make([]float64, len(queries))
		for qi, measurementQuery := range measurementQueries {
			data[i][qi], err = m.Execute(measurementQuery, pipeline.StatisticsSource)
			if err != nil {
				c <- groove.PipelineResult{
					Error: err,
					Type:  groove.Error,
				}
				return
			}
		}
	}
	// Format the measurement results into specified formats.
	outputs := make([]string, len(pipeline.MeasurementFormatters))
	for i, formatter := range pipeline.MeasurementFormatters {
		if len(data) > 0 && len(topics) != len(data[0]) {
			c <- groove.PipelineResult{
				Error: errors.New("the length of topics and data must be the same"),
				Type:  groove.Error,
			}
		}
		outputs[i], err = formatter(topics, headers, data)
		if err != nil {
			c <- groove.PipelineResult{
				Error: err,
				Type:  groove.Error,
			}
			return
		}
	}
	c <- groove.PipelineResult{
		Measurements: outputs,
		Type:         groove.Measurement,
	}

	// Output the transformed queries.
	transformations := make([]groove.QueryResult, len(measurementQueries))

	// This section is run concurrently, since the results can sometimes get quite large and we don't want to eat ram.
	if len(pipeline.Evaluations) > 0 || len(pipeline.OutputTrec.Path) > 0 {

		// Store the measurements to be output later.
		measurements := make(map[int64]map[string]float64)

		// Set the limit to how many goroutines can be run.
		// http://jmoiron.net/blog/limiting-concurrency-in-go/
		concurrency := runtime.NumCPU()
		//if pipeline.QueryChain.CandidateSelector != nil && len(pipeline.QueryChain.Transformations) > 0 {
		//	concurrency = 1
		//}
		sem := make(chan bool, concurrency)
		for i, q := range measurementQueries {
			sem <- true
			go func(idx int, query groove.PipelineQuery) {
				defer func() { <-sem }()

				// PipelineQuery chain.
				if pipeline.QueryChain.CandidateSelector != nil && len(pipeline.QueryChain.Transformations) > 0 {
					nq, err := pipeline.QueryChain.Execute(query)
					if err != nil {
						c <- groove.PipelineResult{
							Topic: query.Topic,
							Error: err,
							Type:  groove.Error,
						}
						return
					}

					query = groove.NewPipelineQuery(query.Name, query.Topic, nq.PipelineQuery.Query)
				}

				// Execute the query.
				trecResults, err := pipeline.StatisticsSource.Execute(query, pipeline.StatisticsSource.SearchOptions())
				if err != nil {
					c <- groove.PipelineResult{
						Topic: query.Topic,
						Error: err,
						Type:  groove.Error,
					}
					return
				}

				// Set the evaluation results.
				if len(pipeline.Evaluations) > 0 {
					measurements[query.Topic] = eval.Evaluate(pipeline.Evaluations, &trecResults, pipeline.EvaluationQrels, query.Topic)
				}

				// Output the trec results.
				if len(pipeline.OutputTrec.Path) > 0 {
					c <- groove.PipelineResult{
						Topic:       query.Topic,
						TrecResults: &trecResults,
						Type:        groove.TrecResult,
					}
				}

				transformations[i] = groove.QueryResult{Name: query.Name, Topic: query.Topic, Transformation: query.Query}

				log.Printf("completed topic %v\n", query.Topic)
			}(i, q)
		}

		// Wait until the last goroutine has read from the semaphore.
		for i := 0; i < cap(sem); i++ {
			sem <- true
		}

		// Output the evaluation results.
		evaluations := make([]string, len(pipeline.EvaluationFormatters))
		// Now we can finally get to formatting the evaluation results.
		for i, f := range pipeline.EvaluationFormatters {
			r, err := f(measurements)
			if err != nil {
				c <- groove.PipelineResult{
					Error: err,
					Type:  groove.Error,
				}
				return
			}
			evaluations[i] = r
		}

		// And send the through the channel.
		c <- groove.PipelineResult{
			Evaluations: evaluations,
			Type:        groove.Evaluation,
		}
	}

	// Send the through the channel.
	c <- groove.PipelineResult{
		Transformations: transformations,
		Type:            groove.Transformation,
	}

	// Return the formatted results.
	c <- groove.PipelineResult{
		Type: groove.Done,
	}
}
