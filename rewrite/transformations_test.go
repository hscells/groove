package rewrite

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
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

	candidates, err := Variations(repr.(cqr.CommonQueryRepresentation), nil, NewLogicalOperatorTransformer(), NewAdjacencyRangeTransformer(), NewMeSHExplosionTransformer(), NewFieldRestrictionsTransformer(), NewAdjacencyReplacementTransformer())

	//queries, err := LogicalOperatorReplacement.Apply(repr.(cqr.CommonQueryRepresentation))
	//if err != nil {
	//	t.Fatal(err)
	//}
	fmt.Println("------------------")
	for i, q := range candidates {
		fmt.Println(i, q.Query, q.Features)
	}

}
