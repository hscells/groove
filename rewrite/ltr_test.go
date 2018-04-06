package rewrite_test

import (
	"bytes"
	"fmt"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"testing"
)

func TestLTR(t *testing.T) {
	cqrPipeline := pipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})

	rawQuery := `1. MMSE*.ti,ab.
2. sMMSE.ti,ab.
3. Folstein*.ti,ab.
4. MiniMental.ti,ab.
5. mini mental stat*.ti,ab.
6. or/1-5`

	var topic int64 = 1

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadFile("../../boogie/sigir2018medline.qrels")
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
		//stats.ElasticsearchField("_all"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{Size: 10000, RunName: "test"}))

	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}
	gq := groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation))

	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	ltr := rewrite.NewLTRQueryCandidateSelector("precision2.model")
	qc := rewrite.NewQueryChain(ltr, ss, analysis.NewMeasurementExecutor(statisticsCache), rewrite.NewAdjacencyReplacementTransformer(), rewrite.NewAdjacencyRangeTransformer(), rewrite.NewMeSHExplosionTransformer(), rewrite.NewFieldRestrictionsTransformer(), rewrite.NewLogicalOperatorTransformer())
	tq, err := qc.Execute(gq)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("executing queries")

	results1, err := ss.ExecuteFast(gq, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	d1 := make(combinator.Documents, len(results1))
	for i, r := range results1 {
		d1[i] = combinator.Document(r)
	}

	fmt.Println(tq.PipelineQuery)
	results2, err := ss.ExecuteFast(tq.PipelineQuery, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	d2 := make(combinator.Documents, len(results2))
	for i, r := range results2 {
		d2[i] = combinator.Document(r)
	}

	r1 := d1.Results(gq, gq.Name)
	r2 := d2.Results(gq, gq.Name)

	fmt.Println(len(r1), len(r2))

	fmt.Println(repr.(cqr.CommonQueryRepresentation))
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &r1, qrels, topic))
	fmt.Println(tq.PipelineQuery.Query)
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &r2, qrels, topic))

	fmt.Println("chain: ")
	for _, q := range tq.QueryChain {
		fmt.Println(q)
	}
	fmt.Println(tq.PipelineQuery.Query)
}
