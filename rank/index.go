package rank

import (
	"fmt"
	"github.com/hscells/guru"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/jdkato/prose.v2"
	"strings"
)

type Posting struct {
	Index   map[string]map[string]map[string]float64
	DocLens map[string]map[string]float64
}

func Index(documents guru.MedlineDocuments) (*Posting, error) {
	ii := make(map[string]map[string]map[string]float64)
	dl := make(map[string]map[string]float64)
	fmt.Println("indexing documents")
	bar := pb.New(len(documents))
	bar.Start()
	for _, doc := range documents {
		pmid := doc.PMID

		dl[pmid] = make(map[string]float64)

		// Extract tokens for the title and abstract.
		ti, err := prose.NewDocument(strings.ToLower(doc.TI), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
		if err != nil {
			return nil, err
		}
		dl[pmid]["ti"] = float64(len(ti.Tokens()))

		ab, err := prose.NewDocument(strings.ToLower(doc.TI), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
		if err != nil {
			return nil, err
		}
		dl[pmid]["ab"] = float64(len(ab.Tokens()))

		// Compute the term frequency values for the title.
		tiTf := make(map[string]float64)
		for _, token := range ti.Tokens() {
			if _, ok := ii[token.Text]; !ok {
				ii[token.Text] = make(map[string]map[string]float64)
				ii[token.Text]["ti"] = make(map[string]float64)
				ii[token.Text]["ab"] = make(map[string]float64)
				ii[token.Text]["mh"] = make(map[string]float64)
			}
			tiTf[token.Text]++
		}

		// Compute the term frequency values for the abstract.
		abTf := make(map[string]float64)
		for _, token := range ab.Tokens() {
			if _, ok := ii[token.Text]; !ok {
				ii[token.Text] = make(map[string]map[string]float64)
				ii[token.Text]["ti"] = make(map[string]float64)
				ii[token.Text]["ab"] = make(map[string]float64)
				ii[token.Text]["mh"] = make(map[string]float64)
			}
			abTf[token.Text]++
		}

		// Add the title terms to the index.
		for token, count := range tiTf {
			ii[token]["ti"][pmid] = count
		}

		// Add the abstract terms to the index.
		for token, count := range abTf {
			ii[token]["ab"][pmid] = count
		}

		for _, mh := range doc.MH {
			mh = strings.ToLower(mh)
			if _, ok := ii[mh]; !ok {
				ii[mh] = make(map[string]map[string]float64)
				ii[mh]["ti"] = make(map[string]float64)
				ii[mh]["ab"] = make(map[string]float64)
				ii[mh]["mh"] = make(map[string]float64)
			}
			ii[mh]["mh"][pmid] = 1
		}
		dl[pmid]["mh"] = float64(len(doc.MH))
		bar.Increment()
	}
	bar.Finish()

	return &Posting{
		Index:   ii,
		DocLens: dl,
	}, nil
}

func (p Posting) Tf(term, field, pmid string) float64 {
	if _, ok := p.Index[term]; !ok {
		return 0
	}
	if _, ok := p.Index[term][field]; !ok {
		return 0
	}
	if _, ok := p.Index[term][field][pmid]; !ok {
		return 0
	}
	return p.Index[term][field][pmid]
}

func (p Posting) DocLen(field, pmid string) float64 {
	return p.DocLens[pmid][field]
}

func (p Posting) AvgDocLen(field string) float64 {
	var dl float64
	for k := range p.DocLens {
		dl += p.DocLens[k][field]
	}
	return dl / float64(len(p.DocLens))
}
