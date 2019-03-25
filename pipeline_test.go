package groove_test

import (
	"bytes"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis/postqpp"
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"io/ioutil"
	"log"
	"testing"
)

func TestName(t *testing.T) {

	// Construct the pipeline.
	ss, _ := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://localhost:9200"),
		stats.ElasticsearchIndex("medline"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{
			Size:    10000,
			RunName: "qpp",
		}))
	pipelineChannel := make(chan groove.Result)
	p := pipeline.NewGroovePipeline(
		query.NewTransmuteQuerySource(query.MedlineTransmutePipeline), ss,
		pipeline.Measurement(preqpp.AvgICTF, preqpp.SumIDF, preqpp.AvgIDF, preqpp.StdDevIDF, preqpp.MaxIDF, postqpp.ClarityScore),
		pipeline.Evaluation(eval.Precision, eval.Recall),
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
}
