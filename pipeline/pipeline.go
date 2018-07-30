// Package pipeline provides a framework for constructing reproducible query experiments.
package pipeline

import (
	"bytes"
	"errors"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/learning"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"io/ioutil"
	"log"
	"runtime"
	"sort"
	"github.com/peterbourgon/diskv"
)

// GroovePipeline contains all the information for executing a pipeline for query analysis.
type GroovePipeline struct {
	QueryPath             string
	QueriesSource         query.QueriesSource
	StatisticsSource      stats.StatisticsSource
	Preprocess            []preprocess.QueryProcessor
	Transformations       preprocess.QueryTransformations
	Measurements          []analysis.Measurement
	MeasurementFormatters []output.MeasurementFormatter
	MeasurementExecutor   analysis.MeasurementExecutor
	Evaluations           []eval.Evaluator
	EvaluationFormatters  EvaluationOutputFormat
	OutputTrec            output.TrecResults
	QueryCache            combinator.QueryCacher
	Model                 learning.Model
	ModelConfiguration    ModelConfiguration
}

// ModelConfiguration specifies what actions of a model should be taken by the pipeline.
type ModelConfiguration struct {
	Generate bool
	Train    bool
	Test     bool
}

// EvaluationOutputFormat specifies out evaluation output should be formatted.
type EvaluationOutputFormat struct {
	EvaluationFormatters []output.EvaluationFormatter
	EvaluationQrels      trecresults.QrelsFile
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

// Evaluation adds evaluation measures to the pipeline.
func Evaluation(measures ...eval.Evaluator) func() interface{} {
	return func() interface{} {
		return measures
	}
}

// MeasurementOutput adds outputs to the pipeline.
func MeasurementOutput(formatter ...output.MeasurementFormatter) func() interface{} {
	return func() interface{} {
		return formatter
	}
}

// TrecOutput configures trec output.
func TrecOutput(path string) func() interface{} {
	return func() interface{} {
		return output.TrecResults{
			Path: path,
		}
	}
}

// EvaluationOutput configures trec output.
func EvaluationOutput(qrels string, formatters ...output.EvaluationFormatter) func() interface{} {
	b, err := ioutil.ReadFile(qrels)
	if err != nil {
		panic(err)
	}
	f, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	return func() interface{} {
		return EvaluationOutputFormat{
			EvaluationQrels:      f,
			EvaluationFormatters: formatters,
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
func (pipeline GroovePipeline) Execute(c chan groove.PipelineResult) {
	defer close(c)
	// TODO this method needs some serious refactoring done to it.

	// Configure caches.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	if pipeline.QueryCache == nil {
		pipeline.QueryCache = combinator.NewFileQueryCache("file_cache")
	}

	pipeline.MeasurementExecutor = analysis.NewDiskMeasurementExecutor(statisticsCache)

	if len(pipeline.QueryPath) > 0 {
		// Load and process the queries.
		queries, err := pipeline.QueriesSource.Load(pipeline.QueryPath)
		if err != nil {
			c <- groove.PipelineResult{
				Error: err,
				Type:  groove.Error,
			}
			return
		}

		// Here we need to configure how the queries are loaded into each learning model.
		if pipeline.Model != nil {
			switch m := pipeline.Model.(type) {
			case *learning.QueryChain:
				m.Queries = queries
				m.QueryCacher = pipeline.QueryCache
				m.MeasurementExecutor = pipeline.MeasurementExecutor
			}
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

		// Sort the transformed queries by size.
		sort.Slice(measurementQueries, func(i, j int) bool {
			return len(analysis.QueryBooleanQueries(measurementQueries[i].Query)) < len(analysis.QueryBooleanQueries(measurementQueries[j].Query))
		})

		for _, mq := range measurementQueries {
			log.Println(mq.Topic)
		}

		// Compute measurements for each of the queries.
		// The measurements are computed in parallel.
		N := len(pipeline.Measurements)
		headers := make([]string, N)
		data := make([][]float64, N)

		// Only perform the measurements if there are some measurement formatters to output them to.
		if len(pipeline.MeasurementFormatters) > 0 {
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
		}

		// This section is run concurrently, since the results can sometimes get quite large and we don't want to eat ram.
		if len(pipeline.Evaluations) > 0 && (len(pipeline.OutputTrec.Path) > 0 || len(pipeline.EvaluationFormatters.EvaluationFormatters) > 0) {

			// Store the measurements to be output later.
			measurements := make(map[string]map[string]float64)

			// Set the limit to how many goroutines can be run.
			// http://jmoiron.net/blog/limiting-concurrency-in-go/
			concurrency := runtime.NumCPU()

			sem := make(chan bool, concurrency)
			for i, q := range measurementQueries {
				sem <- true
				go func(idx int, query groove.PipelineQuery) {
					defer func() { <-sem }()

					// Execute the query.
					docIds, err := stats.GetDocumentIDs(query, pipeline.StatisticsSource)
					if err != nil {
						c <- groove.PipelineResult{
							Topic: query.Topic,
							Error: err,
							Type:  groove.Error,
						}
						return
					}
					results := make(combinator.Documents, len(docIds))
					for i, id := range docIds {
						results[i] = combinator.Document(id)
					}
					pipeline.QueryCache.Set(query.Query, results)
					trecResults := results.Results(query, query.Name)
					//trecResults, err := pipeline.StatisticsSource.Execute(query, pipeline.StatisticsSource.SearchOptions())

					// Set the evaluation results.
					if len(pipeline.Evaluations) > 0 {
						measurements[query.Topic] = eval.Evaluate(pipeline.Evaluations, &trecResults, pipeline.EvaluationFormatters.EvaluationQrels, query.Topic)
					}

					// MeasurementOutput the trec results.
					if len(pipeline.OutputTrec.Path) > 0 {
						c <- groove.PipelineResult{
							Topic:       query.Topic,
							TrecResults: &trecResults,
							Type:        groove.TrecResult,
						}
					}

					// Send the transformation through the channel.
					c <- groove.PipelineResult{
						Transformation: groove.QueryResult{Name: query.Name, Topic: query.Topic, Transformation: query.Query},
						Type:           groove.Transformation,
					}

					log.Printf("completed topic %v\n", query.Topic)
				}(i, q)
			}

			// Wait until the last goroutine has read from the semaphore.
			for i := 0; i < cap(sem); i++ {
				sem <- true
			}

			// MeasurementOutput the evaluation results.
			evaluations := make([]string, len(pipeline.EvaluationFormatters.EvaluationFormatters))
			// Now we can finally get to formatting the evaluation results.
			for i, f := range pipeline.EvaluationFormatters.EvaluationFormatters {
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
	}

	if pipeline.Model != nil {
		if pipeline.ModelConfiguration.Generate {
			log.Println("generating features for model")
			err := pipeline.Model.Generate()
			if err != nil {
				c <- groove.PipelineResult{
					Error: err,
					Type:  groove.Error,
				}
				return
			}
		}
		if pipeline.ModelConfiguration.Train {
			log.Println("training model")
			err := pipeline.Model.Train()
			if err != nil {
				c <- groove.PipelineResult{
					Error: err,
					Type:  groove.Error,
				}
				return
			}
		}
	}

	// Return the formatted results.
	c <- groove.PipelineResult{
		Type: groove.Done,
	}
	return
}
