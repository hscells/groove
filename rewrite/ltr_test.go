package rewrite_test

import (
	"testing"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/groove"
	"github.com/hscells/cqr"
	"fmt"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/stats"
	"github.com/TimothyJones/trecresults"
	"bytes"
	"io/ioutil"
	"github.com/peterbourgon/diskv"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/analysis"
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

	rawQuery := `1. exp ORTHODONTICS/
2. orthodontic$.mp.
3. or/1-2
4. (retention or retain$).mp.
5. (stabilise$ or stabilize$).mp.
6. (fraenectom$ or frenectom$).mp.
7. (fiberotom$ or fibreotom$).mp.
8. "interproximal stripping".mp.
9. pericision.mp.
10. reproximat$.mp.
11. ((gingiv$ or periodont$).mp. adj4 surg$).mp.
12. (retain or retention).mp.
13. 11 and 12
14. or/4-10
15. 13 or 14
16. 3 and 15`

	var topic int64 = 6

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

	ltr := rewrite.NewLTRQueryCandidateSelector("precision.model")
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
