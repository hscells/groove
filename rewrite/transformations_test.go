package rewrite

import (
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/cqr"
	"testing"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/transmute/parser"
	"fmt"
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
4. (retention adj3 retain$).mp.
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

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}

	queries, err := FieldRestrictions.Apply(repr.(cqr.CommonQueryRepresentation))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("------------------")
	for _, q := range queries {
		fmt.Println(q)
	}

}
