package learning_test

import (
	"bytes"
	"encoding/gob"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/learning"
	pipeline2 "github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/quickumlsrest"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/trecresults"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"log"
	"os"
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

	rawQuery := `1. MMSE*.ab.
2. sMMSE.ab.
3. Folstein*.ab.
4. MiniMental.ab.
5. mini mental stat*.ab.
6. or/1-5`

	topic := "1"

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}

	log.Println(repr.(cqr.CommonQueryRepresentation))
	ss, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
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

	cache := combinator.NewFileQueryCache("../file_cache")
	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	selector := learning.ReinforcementQueryCandidateSelector{Depth: 5} //NewOracleQueryChainCandidateSelector(ss, qrels, cache)

	f, err := os.OpenFile("/Users/harryscells/Repositories/cui2vec/testdata/cui2vec_precomputed.bin", os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	m, err := cui2vec.LoadCUIMapping("/Users/harryscells/Repositories/cui2vec/cuis.csv")
	if err != nil {
		t.Fatal(err)
	}
	p, err := cui2vec.NewPrecomputedEmbeddings(f)
	if err != nil {
		t.Fatal(err)
	}

	f, err = os.OpenFile("../analysis/quiche.cache", os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	var quicheCache quickumlsrest.Cache
	err = gob.NewDecoder(f).Decode(&quicheCache)
	if err != nil {
		t.Fatal(err)
	}

	chain := learning.NewQueryChain(
		selector,
		ss,
		analysis.NewDiskMeasurementExecutor(statisticsCache),
		[]analysis.Measurement{
			analysis.BooleanNonAtomicClauses,
			analysis.BooleanAndCount,
			analysis.BooleanOrCount,
			analysis.BooleanNotCount,
			analysis.BooleanFields,
			analysis.BooleanFieldsTitle,
			analysis.BooleanFieldsAbstract,
			analysis.BooleanFieldsMeSH,
			analysis.BooleanFieldsOther,
			analysis.TermCount,
			analysis.BooleanKeywords,
			analysis.MeshKeywordCount,
			analysis.MeshExplodedCount,
			analysis.MeshAvgDepth,
			analysis.MeshMaxDepth,
			preqpp.RetrievalSize,
		},
		learning.NewLogicalOperatorTransformer(),
		learning.NewMeSHExplosionTransformer(),
		learning.NewMeshParentTransformer(),
		learning.NewFieldRestrictionsTransformer(),
		learning.Newcui2vecExpansionTransformer(p, m, quicheCache),
		learning.NewClauseRemovalTransformer(),
	)
	chain.GenerationExplorer = learning.NewBreadthFirstExplorer(chain, learning.NewRandomSampler(20, 0.1), learning.DepthStoppingCondition)

	//chain.Sampler = learning.NewEvaluationSampler(10, 0.1, eval.PrecisionEvaluator, qrels, cache, ss)
	//chain.GenerationFile = "../evaluation.features"
	//chain.Sampler = learning.NewGreedySampler(10, 0.1, eval.PrecisionEvaluator, qrels, cache, ss)
	//chain.GenerationFile = "../greedy.features"
	//chain.Sampler = learning.NewTransformationSampler(10, 0.1)
	//chain.GenerationFile = "../transformation.features"
	chain.GenerationFile = "../random.features"
	chain.QueryCacher = cache
	chain.QrelsFile = qrels
	chain.Evaluators = []eval.Evaluator{
		eval.PrecisionEvaluator,
		eval.RecallEvaluator,
		eval.F05Measure,
		eval.F1Measure,
		eval.F3Measure,
	}
	chain.Queries = []pipeline2.Query{pipeline2.NewQuery(topic, topic, repr.(cqr.CommonQueryRepresentation))}
	log.Println(chain.Queries)
	//fmt.Printf("Rewriting query with %v possible transformations\n", len(chain.Transformations))
	log.Println(chain.Generate())
	//_, err = chain.Execute(p.NewQuery("test", topic, repr.(cqr.CommonQueryRepresentation)))
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//results1, err := ss.Execute(p.NewQuery("test", topic, repr.(cqr.CommonQueryRepresentation)), ss.SearchOptions())
	//if err != nil {
	//	t.Fatal(err)
	//}
	//fmt.Println(repr.(cqr.CommonQueryRepresentation))
	//fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &results1, qrels, topic))

}
