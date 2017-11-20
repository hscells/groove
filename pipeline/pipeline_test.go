package pipeline

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"log"
	"testing"
)

func TestName(t *testing.T) {
	p := NewGroovePipeline(
		query.NewTransmuteQuerySource(query.MedlineTransmutePipeline),
		stats.NewElasticsearchStatisticsSource(),
		[]preprocess.QueryProcessor{preprocess.AlphaNum},
		[]analysis.Measurement{analysis.QueryComplexity{}, analysis.TermCount{}},
		[]output.Formatter{output.JsonFormatter},
	)
	s, err := p.Execute("../../transmute/medline")
	if err != nil {
		t.Fatal(err)
	}

	log.Println(s[0])
}
