package rewrite_test

import (
	"bytes"
	"fmt"
	"github.com/hscells/trecresults"
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

func TestOracleQueryChainSelector_Select(t *testing.T) {
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

	topic := "1"

	cq, err := cqrPipeline.Execute(rawQuery)
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

	b, err := ioutil.ReadFile("../../boogie/sigir2018medline.qrels")
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	cache := combinator.NewDiskvQueryCache(diskv.New(diskv.Options{
		BasePath:     "../cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	}))

	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	selector := rewrite.NewOracleQueryChainCandidateSelector(ss, qrels, cache)

	chain := rewrite.NewQueryChain(selector, ss, analysis.NewDiskMeasurementExecutor(statisticsCache), rewrite.NewLogicalOperatorTransformer(), rewrite.NewAdjacencyReplacementTransformer(), rewrite.NewAdjacencyRangeTransformer(), rewrite.NewMeSHExplosionTransformer(), rewrite.NewFieldRestrictionsTransformer())
	//fmt.Printf("Rewriting query with %v possible transformations\n", len(chain.Transformations))
	q, err := chain.Execute(groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation)))
	if err != nil {
		t.Fatal(err)
	}

	results1, err := ss.Execute(groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation)), ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	results2, err := ss.Execute(q.PipelineQuery, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(repr.(cqr.CommonQueryRepresentation))
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results1, qrels, topic))
	fmt.Println(q.PipelineQuery.Query)
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results2, qrels, topic))

}
