# groove

[![GoDoc](https://godoc.org/github.com/hscells/groove?status.svg)](https://godoc.org/github.com/hscells/groove)
[![Go Report Card](https://goreportcard.com/badge/github.com/hscells/groove)](https://goreportcard.com/report/github.com/hscells/groove)
[![gocover](http://gocover.io/_badge/github.com/hscells/groove)](https://gocover.io/github.com/hscells/groove)

_Query analysis pipeline framework_

## API Usage

```go
p := pipeline.NewGroovePipeline(
    query.NewTransmuteQuerySource(query.MedlineTransmutePipeline),
    stats.NewElasticsearchStatisticsSource(),
    []analysis.Measurement{analysis.QueryComplexity{}, analysis.TermCount{}},
    []output.Formatter{output.JsonFormatter},
)
s, err := p.Execute("../../transmute/medline")
if err != nil {
    log.Fatal(err)
}

log.Println(s[0])
```