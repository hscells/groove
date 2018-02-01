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
p := NewGroovePipeline(
    query.NewTransmuteQuerySource(query.MedlineTransmutePipeline),
    stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://example.com:9200"),
                                           stats.ElasticsearchIndex("pubmed"),
                                           stats.ElasticsearchField("abstract"),
                                           stats.ElasticsearchDocumentType("doc")),
    Preprocess(preprocess.AlphaNum, preprocess.Lowercase),
    Measurement(analysis.AvgIDF, preqpp.AvgICTF, postqpp.ClarityScore),
    Output(output.JsonFormatter),
)

// Execute it on a directory of queries.
go p.Execute("./medline", pipelineChannel)
for {
    result := <-pipelineChannel
    if result.Type == groove.Done {
        break
    }
    switch result.Type {
    case groove.Measurement:
        // Process the measurement outputs. Only one type of measurement (JSON).
        err := ioutil.WriteFile("results.json", bytes.NewBufferString(result.Measurements[0]).Bytes(), 0644)
        if err != nil {
            log.Fatal(err)
        }
        return
    case groove.Error:
        log.Fatal(result.Error)
        return
    }
}
```

## Logo

The Go gopher was created by [Renee French](https://reneefrench.blogspot.com/), licensed under
[Creative Commons 3.0 Attributions license](https://creativecommons.org/licenses/by/3.0/).