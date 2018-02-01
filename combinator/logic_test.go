package combinator

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"testing"
)

func TestLogicalTree(t *testing.T) {
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
5. retain.ti,ab.
6. or/1-5`

	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{Size: 10000, RunName: "test"}))

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}
	query := groove.NewPipelineQuery("0", 1, repr.(cqr.CommonQueryRepresentation))

	tree, _, err := NewLogicalTree(query, ss, make(map[uint64]LogicalTreeNode))
	if err != nil {
		t.Fatal(err)
	}

	r, err := ss.Execute(query, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(ss.RetrievalSize(query.Query))
	fmt.Println(len(r))
	fmt.Println(len(tree.Documents()))
}
