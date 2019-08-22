package rank

import (
	"fmt"
	"github.com/hscells/guru"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/jdkato/prose.v2"
	"hash/fnv"
	"strings"
	"time"
)

type Statistics struct {
	Tf  float64
	Pos float64
}

type Posting struct {
	// Term -> Field -> PMID -> TF
	Index map[uint32]map[uint32]map[uint32]Statistics
	// PMID -> Field -> DocLen
	DocLens   map[string]map[uint32]float64
	DocDates  map[uint32]int64
	TermIdx   map[uint32]int
	MaxDocLen float64
}

var (
	H        = fnv.New32a()
	suffixes = []string{
		// Noun suffixes.
		"acy", "al", "ance", "ence", "dom", "er", "or", "ism", "ist", "ity", "ty", "ment", "ness", "ship", "st", "ty", "sion", "tion",
		// Verb suffixes.
		"ate", "en", "ify", "fy", "ise", "ize", "se", "ze",
		// Adjective suffixes.
		"able", "ible", "al", "esque", "ful", "ic", "ical", "ious", "ous", "us", "ish", "sh", "ive", "ve", "e", "less", "y",
	}
)

func hash(s string) uint32 {
	defer H.Reset()
	_, err := H.Write([]byte(s))
	if err != nil {
		panic(err)
	}
	return H.Sum32()
}

func Index(documents guru.MedlineDocuments) (*Posting, error) {
	ii := make(map[uint32]map[uint32]map[uint32]Statistics)
	dl := make(map[string]map[uint32]float64, len(documents))
	da := make(map[uint32]int64, len(documents))
	fmt.Println("indexing documents")
	bar := pb.New(len(documents))
	bar.Start()

	TI := hash("ti")
	AB := hash("ab")
	MH := hash("mh")

	for _, doc := range documents {
		pmid := doc.PMID

		if len(doc.DCOM) > 0 {
			t, err := time.Parse("20060102", doc.DCOM)
			if err != nil {
				panic(err)
			}
			da[hash(pmid)] = t.Unix()
		} else {
			da[hash(pmid)] = 0
		}

		dl[pmid] = make(map[uint32]float64)

		// Extract tokens for the title and abstract.
		ti, err := prose.NewDocument(strings.ToLower(doc.TI), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
		if err != nil {
			return nil, err
		}
		dl[pmid][TI] = float64(len(ti.Tokens()))

		ab, err := prose.NewDocument(strings.ToLower(doc.AB), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
		if err != nil {
			return nil, err
		}
		dl[pmid][AB] = float64(len(ab.Tokens()))

		// Compute the term frequency values for the title.
		tiTf := make(map[uint32]float64)
		tiPos := make(map[uint32]float64)
		for i, tok := range ti.Tokens() {
			t := hash(tok.Text)
			if _, ok := ii[t]; !ok {
				ii[t] = make(map[uint32]map[uint32]Statistics)
				ii[t][TI] = make(map[uint32]Statistics)
				ii[t][AB] = make(map[uint32]Statistics)
				ii[t][MH] = make(map[uint32]Statistics)
			}
			tiTf[t]++
			if _, ok := tiPos[t]; !ok {
				tiPos[t] = 1 + (1 - (float64(i) / float64(len(ti.Tokens()))))
			}
		}

		// Compute the term frequency values for the abstract.
		abTf := make(map[uint32]float64)
		abPos := make(map[uint32]float64)
		for i, sent := range ab.Sentences() {
			toks, err := prose.NewDocument(strings.ToLower(sent.Text), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
			if err != nil {
				return nil, err
			}
			for _, tok := range toks.Tokens() {
				t := hash(tok.Text)
				if _, ok := ii[t]; !ok {
					ii[t] = make(map[uint32]map[uint32]Statistics)
					ii[t][TI] = make(map[uint32]Statistics)
					ii[t][AB] = make(map[uint32]Statistics)
					ii[t][MH] = make(map[uint32]Statistics)
				}
				abTf[t]++
				if _, ok := abPos[t]; !ok {
					abPos[t] = 1 + (1 - (float64(i) / float64(len(ab.Sentences()))))
				}
			}
		}

		// Add the title terms to the newPosting.
		for token, count := range tiTf {
			ii[token][TI][hash(pmid)] = Statistics{
				Tf:  count,
				Pos: tiPos[token],
			}
		}

		// Add the abstract terms to the newPosting.
		for token, count := range abTf {
			ii[token][AB][hash(pmid)] = Statistics{
				Tf:  count,
				Pos: abPos[token],
			}
		}

		for _, mh := range doc.MH {
			mh = strings.ToLower(mh)
			t := hash(mh)
			if _, ok := ii[t]; !ok {
				ii[t] = make(map[uint32]map[uint32]Statistics)
				ii[t][TI] = make(map[uint32]Statistics)
				ii[t][AB] = make(map[uint32]Statistics)
				ii[t][MH] = make(map[uint32]Statistics)
			}
			ii[t][MH][hash(pmid)] = Statistics{
				Tf:  1,
				Pos: 1,
			}
		}
		dl[pmid][MH] = float64(len(doc.MH))

		bar.Increment()
	}
	bar.Finish()

	var maxL float64
	for _, fl := range dl {
		var ll float64
		for _, l := range fl {
			ll += l
		}
		if ll > maxL {
			maxL = ll
		}
	}

	tm := make(map[uint32]int)
	i := 0
	for t := range ii {
		tm[t] = i
		i++
	}

	return &Posting{
		Index:     ii,
		DocLens:   dl,
		MaxDocLen: maxL,
		TermIdx:   tm,
		DocDates:  da,
	}, nil
}

func (p *Posting) DocumentVector(pmid uint32) []float64 {
	TI := hash("ti")
	AB := hash("ab")
	MH := hash("mh")

	dv := make([]float64, len(p.Index)*3)
	i := 0
	for t, fmap := range p.Index {
		for f, pmap := range fmap {
			var j int
			switch f {
			case TI:
				j = 1
			case AB:
				j = 2
			case MH:
				j = 3
			}
			for pmid2 := range pmap {
				if pmid == pmid2 {
					dv[j*p.TermIdx[t]] = 1
				}
			}
		}
		i++
	}
	return dv
}

func (p *Posting) Tf(term, field, pmid string) float64 {
	//var terms []uint32
	//if strings.Contains(term, "*") {
	//	tt := strings.Replace(term, "*", "", -1)
	//	for _, suff := range suffixes {
	//		t := hash(fmt.Sprintf("%s%s", tt, suff))
	//		if _, ok := p.Index[t]; ok {
	//			terms = append(terms, t)
	//		}
	//	}
	//} else {
	//	terms = append(terms, hash(term))
	//}
	//
	//f := hash(field)
	//d := hash(pmid)
	//var tf float64
	//for _, term := range terms {
	//	if _, ok := p.Index[term]; !ok {
	//		continue
	//	}
	//	if _, ok := p.Index[term][f]; !ok {
	//		continue
	//	}
	//	if _, ok := p.Index[term][f][d]; !ok {
	//		continue
	//	}
	//	tf += p.Index[term][f][d].Tf
	//}
	//
	//return tf
	t := hash(term)
	f := hash(field)
	d := hash(pmid)
	//var pos float64
	//for _, term := range terms {
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f][d]; !ok {
		return 0
	}
	return p.Index[t][f][d].Tf
}

func (p *Posting) Pos(term, field, pmid string) float64 {
	//var terms []uint32
	//if strings.Contains(term, "*") {
	//	tt := strings.Replace(term, "*", "", -1)
	//	for _, suff := range suffixes {
	//		t := hash(fmt.Sprintf("%s%s", tt, suff))
	//		if _, ok := p.Index[t]; ok {
	//			terms = append(terms, t)
	//		}
	//	}
	//} else {
	//	terms = append(terms, hash(term))
	//}

	t := hash(term)
	f := hash(field)
	d := hash(pmid)
	//var pos float64
	//for _, term := range terms {
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f][d]; !ok {
		return 0
	}
	return p.Index[t][f][d].Pos
	//}
	//if len(terms) > 0 {
	//	return pos / float64(len(terms))
	//}
	//return pos
}

func (p *Posting) DocLen(field string, pmid string) float64 {
	return p.DocLens[pmid][hash(field)]
}

func (p *Posting) AvgDocLen(field string) float64 {
	var dl float64
	for k := range p.DocLens {
		dl += p.DocLens[k][hash(field)]
	}
	return dl / float64(len(p.DocLens))
}

func (p *Posting) TTf(term, field string) float64 {
	t := hash(term)
	f := hash(term)
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	var ttf float64
	for _, stat := range p.Index[t][f] {
		ttf += stat.Tf
	}
	return ttf
}

func (p *Posting) VocabSize(field string) float64 {
	var vocab float64
	f := hash(field)
	for _, fieldPosting := range p.Index {
		if documentPosting, ok := fieldPosting[f]; ok {
			for _, stat := range documentPosting {
				vocab += stat.Tf
			}
		}
	}
	return vocab
}

func (p *Posting) DocumentTermProbability(term, field, pmid string) float64 {
	t := hash(term)
	f := hash(term)
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	return p.Tf(term, field, pmid) / p.TTf(term, field)
}

func (p *Posting) CollectionTermProbability(term, field string) float64 {
	t := hash(term)
	f := hash(term)
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	return p.TTf(term, field) / p.VocabSize(field)
}

func (p *Posting) DirichlectTermProbability(term, field, pmid string, mu float64) float64 {
	t := hash(term)
	f := hash(term)
	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}
	return (float64(len(p.Index[t][f])) + mu*p.CollectionTermProbability(term, field)) / (p.DocLen(field, pmid) + mu)
}
