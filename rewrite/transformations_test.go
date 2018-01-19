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

	rawQuery := `1. Diabetic Ketoacidosis/
2. Diabetic Coma/
3. ((hyperglyc?emic or diabet*).tw. adj emergenc*).tw.
4. (diabet* and (keto* or acidos* or coma).tw.).tw.
5. DKA.tw.
6. or/1-5
7. Insulin Lispro/
8. Insulin Aspart/
9. Insulin, Short-Acting/
10. (glulisine or apidra).tw.
11. (humulin or novolin).tw.
12. (lispro or aspart).tw.
13. (novolog or novorapid).tw.
14. (insulin* adj3 analogue*).tw.
15. acting insulin*.tw.
16. or/7-15
17. 6 and 16
18. (humans/ not animals/)
19. 17 and 18
`

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
