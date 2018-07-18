package learning_test

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/peterbourgon/diskv"
	"testing"
	"github.com/hscells/groove/learning"
	"log"
)

func TestLogicalOperatorReplacement_Apply(t *testing.T) {
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
16. 3 and 15
`

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}

	ss, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	candidates, err := learning.Variations(learning.NewCandidateQuery(repr.(cqr.CommonQueryRepresentation), nil), ss, analysis.NewDiskMeasurementExecutor(statisticsCache), []analysis.Measurement{analysis.BooleanClauses}, learning.NewLogicalOperatorTransformer())

	//queries, err := LogicalOperatorReplacement.Apply(repr.(cqr.CommonQueryRepresentation))
	//if err != nil {
	//	t.Fatal(err)
	//}
	log.Println("------------------")
	for i, q := range candidates {
		log.Println(i, q.Query, q.Features)
	}

}
