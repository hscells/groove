package learning

import (
	"fmt"
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

	rawQuery := `1 Lymphoma/
2 Hodgkin Disease/
3 (cancer adj8 neoplasm).tw.
4 or/1-3
`

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	repr, err := cq.Representation()
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

	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	candidates, err := Variations(NewCandidateQuery(repr.(cqr.CommonQueryRepresentation), nil), ss, analysis.NewDiskMeasurementExecutor(statisticsCache), NewLogicalOperatorTransformer(), NewAdjacencyRangeTransformer(), NewMeSHExplosionTransformer(), NewFieldRestrictionsTransformer(), NewAdjacencyReplacementTransformer())

	//queries, err := LogicalOperatorReplacement.Apply(repr.(cqr.CommonQueryRepresentation))
	//if err != nil {
	//	t.Fatal(err)
	//}
	fmt.Println("------------------")
	for i, q := range candidates {
		fmt.Println(i, q.Query, q.Features)
	}

}
