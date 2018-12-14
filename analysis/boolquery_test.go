package analysis_test

import (
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/query"
	"os"
	"regexp"
	"testing"
)

func TestBoolQuery(t *testing.T) {

	queries, err := query.NewTransmuteQuerySource(query.MedlineTransmutePipeline).Load("../../boogie/medline")
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile("keywords.txt", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	remRe := regexp.MustCompile(`[$*~"'?]*`)
	numRe := regexp.MustCompile(`[0-9]+`)

	seen := make(map[string]bool)

	for _, q := range queries {
		keywords := analysis.QueryKeywords(q.Query)
		for _, keyword := range keywords {
			v := remRe.ReplaceAll([]byte(keyword.QueryString+"\n"), []byte(""))
			if numRe.Match(v) {
				continue
			}
			if _, ok := seen[string(v)]; !ok {
				f.Write(v)
				seen[string(v)] = true
			}
		}
	}
}
