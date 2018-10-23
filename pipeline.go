// Package pipeline provides a framework for constructing reproducible query experiments.
package groove

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/learning"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"log"
	"runtime"
	"sort"
)

// Pipeline contains all the information for executing a pipeline for query analysis.
type Pipeline struct {
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

//// Measurement adds measurements to the pipeline.
//func Measurement(measurements ...analysis.Measurement) func() interface{} {
//	return func() interface{} {
//		return measurements
//	}
//}
//
//// Evaluation adds evaluation measures to the pipeline.
//func Evaluation(measures ...eval.Evaluator) func() interface{} {
//	return func() interface{} {
//		return measures
//	}
//}

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
func NewGroovePipeline(qs query.QueriesSource, ss stats.StatisticsSource, components ...func() interface{}) Pipeline {
	gp := Pipeline{
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
func (p Pipeline) Execute(c chan pipeline.Result) {
	defer close(c)
	log.Println("starting groove pipeline...")

	// TODO this method needs some serious refactoring done to it.

	// Configure caches.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	if p.QueryCache == nil {
		p.QueryCache = combinator.NewFileQueryCache("file_cache")
	}

	p.MeasurementExecutor = analysis.NewDiskMeasurementExecutor(statisticsCache)

	// Only perform this section if there are some queries.
	if len(p.QueryPath) > 0 {
		log.Println("loading queries...")
		// Load and process the queries.
		queries, err := p.QueriesSource.Load(p.QueryPath)
		if err != nil {
			c <- pipeline.Result{
				Error: err,
				Type:  pipeline.Error,
			}
			return
		}

		// Here we need to configure how the queries are loaded into each learning model.
		if p.Model != nil {
			switch m := p.Model.(type) {
			case *learning.QueryChain:
				m.Queries = queries
				m.QueryCacher = p.QueryCache
				m.MeasurementExecutor = p.MeasurementExecutor
			}
		}

		// This means preprocessing the query.
		measurementQueries := make([]pipeline.Query, len(queries))
		topics := make([]string, len(queries))
		for i, q := range queries {
			topics[i] = q.Name
			// Ensure there is a processed query.

			// And apply the processing if there is any.
			for _, p := range p.Preprocess {
				q = pipeline.NewQuery(q.Name, q.Topic, preprocess.ProcessQuery(q.Query, p))
			}

			// Apply any transformations.
			for _, t := range p.Transformations.BooleanTransformations {
				q = pipeline.NewQuery(q.Name, q.Topic, t(q.Query)())
			}
			for _, t := range p.Transformations.ElasticsearchTransformations {
				if s, ok := p.StatisticsSource.(*stats.ElasticsearchStatisticsSource); ok {
					q = pipeline.NewQuery(q.Name, q.Topic, t(q.Query, s)())
				} else {
					log.Fatal("Elasticsearch transformations only work with an Elasticsearch statistics source.")
				}
			}
			measurementQueries[i] = q
		}

		log.Println("sorting queries by complexity...")

		// Sort the transformed queries by size.
		sort.Slice(measurementQueries, func(i, j int) bool {
			return len(analysis.QueryBooleanQueries(measurementQueries[i].Query)) < len(analysis.QueryBooleanQueries(measurementQueries[j].Query))
		})

		for _, mq := range measurementQueries {
			fmt.Printf("%s ", mq.Topic)
		}
		fmt.Println()

		// Compute measurements for each of the queries.
		// The measurements are computed in parallel.
		N := len(p.Measurements)
		headers := make([]string, N)
		data := make([][]float64, N)

		// Only perform the measurements if there are some measurement formatters to output them to.
		if len(p.MeasurementFormatters) > 0 {
			// data[measurement][queryN]
			for i, m := range p.Measurements {
				headers[i] = m.Name()
				data[i] = make([]float64, len(queries))
				for qi, measurementQuery := range measurementQueries {
					data[i][qi], err = m.Execute(measurementQuery, p.StatisticsSource)
					if err != nil {
						c <- pipeline.Result{
							Error: err,
							Type:  pipeline.Error,
						}
						return
					}
				}
			}

			// Format the measurement results into specified formats.
			outputs := make([]string, len(p.MeasurementFormatters))
			for i, formatter := range p.MeasurementFormatters {
				if len(data) > 0 && len(topics) != len(data[0]) {
					c <- pipeline.Result{
						Error: errors.New("the length of topics and data must be the same"),
						Type:  pipeline.Error,
					}
				}
				outputs[i], err = formatter(topics, headers, data)
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
			}
			c <- pipeline.Result{
				Measurements: outputs,
				Type:         pipeline.Measurement,
			}
		}

		// This section is run concurrently, since the results can sometimes get quite large and we don't want to eat ram.
		if len(p.Evaluations) > 0 && (len(p.OutputTrec.Path) > 0 || len(p.EvaluationFormatters.EvaluationFormatters) > 0) {
			// Store the measurements to be output later.
			measurements := make(map[string]map[string]float64)

			// Set the limit to how many goroutines can be run.
			// http://jmoiron.net/blog/limiting-concurrency-in-go/
			concurrency := runtime.NumCPU()

			log.Printf("starting to execute queries with %d goroutines\n", concurrency)

			sem := make(chan bool, concurrency)
			for i, q := range measurementQueries {
				sem <- true
				go func(idx int, query pipeline.Query) {
					defer func() { <-sem }()
					log.Printf("starting topic %v\n", query.Topic)

					tree, cache, err := combinator.NewLogicalTree(query, p.StatisticsSource, p.QueryCache)
					if err != nil {
						c <- pipeline.Result{
							Topic: query.Topic,
							Error: err,
							Type:  pipeline.Error,
						}
						return
					}
					docIds := tree.Documents(cache)
					if err != nil {
						c <- pipeline.Result{
							Topic: query.Topic,
							Error: err,
							Type:  pipeline.Error,
						}
						return
					}
					trecResults := docIds.Results(query, query.Name)

					// Set the evaluation results.
					if len(p.Evaluations) > 0 {
						measurements[query.Topic] = eval.Evaluate(p.Evaluations, &trecResults, p.EvaluationFormatters.EvaluationQrels, query.Topic)
					}

					// MeasurementOutput the trec results.
					if len(p.OutputTrec.Path) > 0 {
						c <- pipeline.Result{
							Topic:       query.Topic,
							TrecResults: &trecResults,
							Type:        pipeline.TrecResult,
						}
					}

					// Send the transformation through the channel.
					c <- pipeline.Result{
						Transformation: pipeline.QueryResult{Name: query.Name, Topic: query.Topic, Transformation: query.Query},
						Type:           pipeline.Transformation,
					}

					log.Printf("completed topic %v\n", query.Topic)

					docIds = nil
					trecResults = nil
					runtime.GC()
				}(i, q)
			}

			// Wait until the last goroutine has read from the semaphore.
			for i := 0; i < cap(sem); i++ {
				sem <- true
			}

			// MeasurementOutput the evaluation results.
			evaluations := make([]string, len(p.EvaluationFormatters.EvaluationFormatters))
			// Now we can finally get to formatting the evaluation results.
			for i, f := range p.EvaluationFormatters.EvaluationFormatters {
				r, err := f(measurements)
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				evaluations[i] = r
			}

			// And send the through the channel.
			c <- pipeline.Result{
				Evaluations: evaluations,
				Type:        pipeline.Evaluation,
			}
		}
	}

	if p.Model != nil {
		if p.ModelConfiguration.Generate {
			log.Println("generating features for model")
			err := p.Model.Generate()
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
		}
		if p.ModelConfiguration.Train {
			log.Println("training model")
			err := p.Model.Train()
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
		}
		if p.ModelConfiguration.Test {
			log.Println("testing model")
			err := p.Model.Test()
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}

		}
	}

	// Return the formatted results.
	c <- pipeline.Result{
		Type: pipeline.Done,
	}
	return
}
