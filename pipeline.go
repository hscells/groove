// Package pipeline provides a framework for constructing reproducible query experiments.
package groove

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/formulation"
	"github.com/hscells/groove/learning"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/rank"
	"github.com/hscells/groove/stats"
	"github.com/hscells/headway"
	"github.com/hscells/transmute"
	"github.com/hscells/trecresults"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"time"
)

// Pipeline contains all the information for executing a pipeline for query analysis.
type Pipeline struct {
	QueryPath             string
	PubDatesFile          string
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
	QueryFormulator       formulation.Formulator

	CLF rank.CLFOptions
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
//noinspection GoNilness
func (p Pipeline) Execute(c chan pipeline.Result) {
	defer close(c)
	log.Println("starting groove pipeline...")

	// TODO this method needs some serious refactoring done to it.

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		c <- pipeline.Result{
			Error: err,
			Type:  pipeline.Error,
		}
		return
	}

	// Configure caches.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     path.Join(cacheDir, "groove", "statistics_cache"),
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	if p.QueryCache == nil {
		p.QueryCache = combinator.NewFileQueryCache(path.Join(cacheDir, "groove", "file_cache"))
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

		//if len(p.PubDatesFile) > 0 {
		//	log.Println("adding date restrictions to queries...")
		//	for i, cq := range queries {
		//		log.Println(cq.Topic)
		//		q := preprocess.DateRestrictions(p.PubDatesFile)(cq.Query, cq.Topic)()
		//		queries[i].Query = q
		//	}
		//}

		log.Println("sorting queries by complexity...")

		// Sort the transformed queries by size.
		sort.Slice(queries, func(i, j int) bool {
			return len(analysis.QueryBooleanQueries(queries[i].Query)) < len(analysis.QueryBooleanQueries(queries[j].Query))
		})

		for _, q := range queries {
			fmt.Printf("%s ", q.Topic)
		}
		fmt.Println()

		// This means preprocessing the query.
		measurementQueries := make([]pipeline.Query, len(queries))
		topics := make([]string, len(queries))
		for i, q := range queries {
			topics[i] = q.Topic
			// Ensure there is a processed query.

			// And apply the processing if there is any.
			for _, p := range p.Preprocess {
				q = pipeline.NewQuery(q.Name, q.Topic, preprocess.ProcessQuery(q.Query, p))
			}

			// Apply any transformations.
			for i, t := range p.Transformations.BooleanTransformations {
				fmt.Println(q.Topic, i)
				q = pipeline.NewQuery(q.Name, q.Topic, t(q.Query, q.Topic)())
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

		// Compute measurements for each of the queries.
		// The measurements are computed in parallel.
		N := len(p.Measurements)
		headers := make([]string, N)
		data := make([][]float64, N)

		for i, measure := range p.Measurements {
			headers[i] = measure.Name()
		}

		// Only perform the measurements if there are some measurement formatters to output them to.
		if len(p.MeasurementFormatters) > 0 {
			for _, m := range measurementQueries {
				measurements, err := p.MeasurementExecutor.Execute(m, p.StatisticsSource, p.Measurements...)
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				for i, measurement := range measurements {
					data[i] = append(data[i], measurement)
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

		var hw *headway.Client
		loghw := false
		if len(p.CLF.HeadwayServer) > 0 {
			hw = headway.NewClient(p.CLF.HeadwayServer, fmt.Sprintf("@harry groove pipeline [#%d]", time.Now().Unix()))
			if hw != nil {
				loghw = true
			}
		}

		if (len(p.OutputTrec.Path) > 0 || len(p.EvaluationFormatters.EvaluationFormatters) > 0) && p.CLF.CLF {
			// Store the measurements to be output later.

			f, err := os.OpenFile(p.OutputTrec.Path, os.O_RDONLY, 0664)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}

			r, err := trecresults.ResultsFromReader(f)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			f.Close()

			measurements := make(map[string]map[string]float64)
			for i, q := range measurementQueries {
				if _, ok := r.Results[q.Topic]; ok {
					log.Printf("already completed topic %v, so skipping it\n", q.Topic)
					continue
				}
				log.Printf("starting topic %v\n", q.Topic)
				results, err := rank.CLF(q, p.StatisticsSource.(stats.EntrezStatisticsSource), p.CLF)
				if err != nil {
					if loghw {
						err = hw.Send(float64(i), float64(len(measurementQueries)), err.Error())
						if err != nil {
							log.Println(err)
						}
					}
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				if loghw {
					err = hw.Send(float64(i), float64(len(measurementQueries)), fmt.Sprintf("[measurement] topic %s", q.Topic))
					if err != nil {
						log.Println(err)
					}
				}
				// Set the evaluation results.
				if len(p.Evaluations) > 0 {
					measurements[q.Topic] = eval.Evaluate(p.Evaluations, &results, p.EvaluationFormatters.EvaluationQrels, q.Topic)
				}

				// MeasurementOutput the trec results.
				if len(p.OutputTrec.Path) > 0 {
					c <- pipeline.Result{
						Topic:       q.Topic,
						TrecResults: &results,
						Type:        pipeline.TrecResult,
					}
				}

				// Send the transformation through the channel.
				c <- pipeline.Result{
					Transformation: pipeline.QueryResult{Name: q.Name, Topic: q.Topic, Transformation: q.Query},
					Type:           pipeline.Transformation,
				}

				log.Printf("completed topic %v\n", q.Topic)
			}
			if loghw {
				_ = hw.Send(float64(len(measurementQueries)), float64(len(measurementQueries)), "[measurement] done!")
			}

		} else if len(p.OutputTrec.Path) > 0 || len(p.EvaluationFormatters.EvaluationFormatters) > 0 {
			// This section is run concurrently, since the results can sometimes get quite large and we don't want to eat ram.

			// Store the measurements to be output later.
			measurements := make(map[string]map[string]float64)

			// Set the limit to how many goroutines can be run.
			// http://jmoiron.net/blog/limiting-concurrency-in-go/
			concurrency := 1 //runtime.NumCPU()

			log.Println(p.OutputTrec)

			log.Printf("starting to execute queries with %d goroutines\n", concurrency)

			f, err := os.OpenFile(p.OutputTrec.Path, os.O_RDONLY, 0664)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}

			r, err := trecresults.ResultsFromReader(f)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			f.Close()

			sem := make(chan bool, concurrency)
			for i, q := range measurementQueries {
				sem <- true
				go func(idx int, query pipeline.Query) {
					defer func() { <-sem }()
					if _, ok := r.Results[q.Topic]; ok {
						log.Printf("already completed topic %v, so skipping it\n", q.Topic)
						return
					}
					log.Printf("starting topic %v\n", query.Topic)
					//
					//tree, cache, err := combinator.NewLogicalTree(query, p.StatisticsSource, p.QueryCache)
					//if err != nil {
					//	c <- pipeline.Result{
					//		Topic: query.Topic,
					//		Error: err,
					//		Type:  pipeline.Error,
					//	}
					//	return
					//}
					//docIds := tree.Documents(cache)
					//if err != nil {
					//	c <- pipeline.Result{
					//		Topic: query.Topic,
					//		Error: err,
					//		Type:  pipeline.Error,
					//	}
					//	return
					//}
					//trecResults := docIds.Results(query, query.Name)
					trecResults, err := p.StatisticsSource.Execute(query, p.StatisticsSource.SearchOptions())
					if err != nil {
						if loghw {
							_ = hw.Send(float64(i), float64(len(measurementQueries)), err.Error())
						}
						panic(err)
					}

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

	// This part of the pipeline handles query formulation.
	if p.QueryFormulator != nil {
		// Perform the query formulation.
		queries, sup, err := p.QueryFormulator.Formulate()
		if err != nil {
			c <- pipeline.Result{
				Error: err,
				Type:  pipeline.Error,
			}
			return
		}
		for _, s := range sup {
			// Create the folder the data will be contained in.
			err := os.MkdirAll(path.Join(p.QueryFormulator.Method(), s.Name), 0777)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}

			for _, d := range s.Data {
				fmt.Printf("writing supplimentary file %s\n", path.Join(p.QueryFormulator.Method(), s.Name, d.Name))
				// Create and open the file that will contain the data.
				f, err := os.OpenFile(path.Join(p.QueryFormulator.Method(), s.Name, d.Name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				// Marshal the data into bytes for writing to disk.
				b, err := d.Value.Marshal()
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				// Write those bytes to disk.
				_, err = f.Write(b)
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
				// Close that file.
				err = f.Close()
				if err != nil {
					c <- pipeline.Result{
						Error: err,
						Type:  pipeline.Error,
					}
					return
				}
			}
		}

		// Create the folder that will contain the formulated query/queries.
		err = os.MkdirAll(p.QueryFormulator.Method(), 0777)
		if err != nil {
			c <- pipeline.Result{
				Error: err,
				Type:  pipeline.Error,
			}
			return
		}
		for i, q := range queries {
			fmt.Println(q)
			err := os.MkdirAll(path.Join(p.QueryFormulator.Method(), strconv.Itoa(i)), 0777)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			// Compile the query to CQR.
			s, err := transmute.CompileCqr2PubMed(q)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			// Open the file that will contain the query.
			f, err := os.OpenFile(path.Join(p.QueryFormulator.Method(), strconv.Itoa(i), p.QueryFormulator.Topic()), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			// Write the query to disk.
			_, err = f.WriteString(s)
			if err != nil {
				c <- pipeline.Result{
					Error: err,
					Type:  pipeline.Error,
				}
				return
			}
			// Close the file.
			err = f.Close()
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
