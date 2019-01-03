package seed

import (
	"github.com/biogo/ncbi/entrez"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"gopkg.in/jdkato/prose.v2"
	"testing"
)

func TestFeedbackExample(t *testing.T) {
	doc, err := prose.NewDocument(`Galactomannan detection for invasive aspergillosis in immunocompromised`)
	if err != nil {
		t.Fatal(err)
	}

	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]bool)
	var terms []string

	for _, tok := range doc.Tokens() {
		t.Log(tok.Text, tok.Tag)
		if _, ok := seen[tok.Text]; ok {
			continue
		}
		seen[tok.Text] = true
		switch tok.Tag {
		case "JJ", "JJR", "JJS", "NN", "NNP", "NNPS", "NNS",
			"RB", "RBR", "RBS", "RP", "VB", "VBD", "VBG",
			"VBN", "VPP", "VPZ", "VBP":
			terms = append(terms, tok.Text)
		default:
			continue
		}
	}

	query := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{})
	for _, term := range terms {
		query.Children = append(query.Children, cqr.NewKeyword(term, fields.TitleAbstract))
	}

	query = cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
		query,
		sensitivityFilter,
	})
	t.Log(terms)

	q, err := transmute.CompileCqr2PubMed(query)
	if err != nil {
		t.Fatal(err)
	}

	ids, err := e.Search(q, func(p *entrez.Parameters) {
		p.RetMax = 10
	})
	if err != nil {
		t.Fatal(err)
	}

	docs, err := e.Fetch(ids)
	if err != nil {
		t.Fatal(err)
	}

	scores := make([]float64, len(docs))
	docIDs := make([]string, len(docs))
	for i, v := range docs {
		docIDs[i] = v.ID
	}

	t.Log(ids)
	t.Log(scores)
	t.Log(docIDs)
	lm, err := stats.NewLanguageModel(e, docIDs, scores, "TIAB")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(lm.CollectionTermProbability("galactomannan"))
	t.Log(lm.CollectionTermProbability("aspergillosis"))
	t.Log(lm.KLDivergence(0.5, stats.JelinekMercerTermProbability(0.5)))
}
