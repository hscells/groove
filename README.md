<img height="200px" src="gopher.png" alt="gopher" align="right"/>

# groove

[![GoDoc](https://godoc.org/github.com/hscells/groove?status.svg)](https://godoc.org/github.com/hscells/groove)
[![Go Report Card](https://goreportcard.com/badge/github.com/hscells/groove)](https://goreportcard.com/report/github.com/hscells/groove)
[![gocover](http://gocover.io/_badge/github.com/hscells/groove)](https://gocover.io/github.com/hscells/groove)

_Query analysis pipeline framework_

groove is a library for pipeline construction for query analysis. The groove pipeline comprises a query source (the
format of the queries), a statistic source (a source for computing information retrieval statistics), preprocessing
steps, any measurements to make, and any output formats.

The groove library is primarily used in [boogie](https://github.com/hscells/boogie) which is a front-end DSL for groove.
If using groove as a Go library, refer to the simple example below which loads Medline queries and analyses them using
Elasticsearch and finally outputs the result into a JSON file.

## API Usage

In the below example, we would like to use Elasticsearch to measure some query performance predictors on some Medline
queries. For the experiment, we would like to pre-process the queries by making each one only contain alpha-numeric
characters, and in lowercase. Finally, we would like to output the results of the measures into a JSON file.

```go
// Construct the pipeline.
pipelineChannel := make(chan groove.PipelineResult)
p := pipeline.NewGroovePipeline(
	query.NewTransmuteQuerySource(query.MedlineTransmutePipeline),
	stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://localhost:9200"),
		stats.ElasticsearchIndex("medline"),
		stats.ElasticsearchField("abstract"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{
			Size:    10000,
			RunName: "qpp",
		})),
	pipeline.Measurement(preqpp.AvgICTF, preqpp.SumIDF, preqpp.AvgIDF, preqpp.MaxIDF, preqpp.StdDevIDF, postqpp.ClarityScore),
	pipeline.Evaluation(eval.PrecisionEvaluator, eval.RecallEvaluator),
	pipeline.MeasurementOutput(output.JsonMeasurementFormatter),
	pipeline.EvaluationOutput("medline.qrels", output.JsonEvaluationFormatter),
	pipeline.TrecOutput("medline_qpp.results"))

// Execute it on a directory of queries. A pipeline executes queries in parallel.
go p.Execute("./medline", pipelineChannel)

for {
	// Continue until completed.
	result := <-pipelineChannel
	if result.Type == groove.Done {
		break
	}
	switch result.Type {
	case groove.Measurement:
		// Process the measurement outputs.
		err := ioutil.WriteFile("medline_qpp.json", bytes.NewBufferString(result.Measurements[0]).Bytes(), 0644)
		if err != nil {
			log.Fatal(err)
		}
	case groove.Evaluation:
		// Process the evaluation outputs.
		err := ioutil.WriteFile("medline_qpp_eval.json", bytes.NewBufferString(result.Evaluations[0]).Bytes(), 0644)
		if err != nil {
			log.Fatal(err)
		}
	}
}
```

## Logo

The Go gopher was created by [Renee French](https://reneefrench.blogspot.com/), licensed under
[Creative Commons 3.0 Attributions license](https://creativecommons.org/licenses/by/3.0/).