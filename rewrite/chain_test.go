package rewrite

import (
	"testing"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/transmute/parser"
	"github.com/TimothyJones/trecresults"
	"io/ioutil"
	"bytes"
	"github.com/hscells/groove"
	"github.com/hscells/cqr"
	"fmt"
	"github.com/hscells/groove/eval"
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

	rawQuery := `1. exp Ovarian Neoplasms/
2. (ovar*.mp. adj5 (cancer* or tumor* or tumour* or neoplas* or carcinoma* or adenocarcinoma* or malignan*).mp.).mp.
3. 1 or 2
4. surgery.fs.
5. exp Gynecologic Surgical Procedures/
6. (cytoreduct* or debulk*).mp.
7. 4 or 5 or 6
8. Tranexamic Acid/
9. (tranexamic acid or amchafibrin or anvitoff or cyclokapron or cyklokapron or exacyl or kabi 2161 or lysteda or spotof or t-amcha or tranhexamic acid or transamin or ugurol or xp12b).mp.
10. 8 or 9
11. 3 and 7 and 10`

	var topic int64 = 16

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
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

	selector := NewOracleQueryChainCandidateSelector(ss, qrels)

	chain := NewQueryChain(selector, AdjacencyRange, LogicalOperatorReplacement, MeSHExplosion, FieldRestrictions)
	fmt.Printf("Rewriting query with %v possible transformations\n", len(chain.Transformations))
	q, err := chain.Execute(groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation)).SetTransformed(func() cqr.CommonQueryRepresentation {
		return repr.(cqr.CommonQueryRepresentation)
	}))
	if err != nil {
		t.Fatal(err)
	}

	results1, err := ss.Execute(groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation)).SetTransformed(func() cqr.CommonQueryRepresentation {
		return repr.(cqr.CommonQueryRepresentation)
	}), ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	results2, err := ss.Execute(q, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(repr.(cqr.CommonQueryRepresentation))
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel}, &results1, qrels, topic))
	fmt.Println(q.Transformed())
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel}, &results2, qrels, topic))

}
