package seed

import (
	"gopkg.in/jdkato/prose.v2"
	"testing"
)

func TestFeedbackExample(t *testing.T) {
	doc, err := prose.NewDocument("Red flags to screen for vertebral fracture in patients presenting with low-back pain")
	if err != nil {
		t.Fatal(err)
	}

	var (
		terms [][]string
		i     int
	)
	terms = append(terms, []string{})
	for _, tok := range doc.Tokens() {
		t.Log(tok.Text, tok.Tag)
		switch tok.Tag {
		case "JJ", "JJR", "JJS", "NN", "NNP", "NNPS", "NNS", ",", "(", ")":
			terms[i] = append(terms[i], tok.Text)
		default:
			i++
			terms = append(terms, []string{})
		}
	}

	t.Log(terms)
}
