package rank

import (
	"github.com/hscells/groove/stats"
	"gopkg.in/jdkato/prose.v2"
)

type Scorer interface {
	Score(query, pmid string, fields ...string) (float64, error)
	posting(p *Posting)
	entrez(e stats.EntrezStatisticsSource)
}

type TFIDFScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type BM25Scorer struct {
	s  stats.EntrezStatisticsSource
	p  *Posting
	K1 float64
	B  float64
}

var (
	idfCache map[string]float64
)

func fastIDF(term, field string, e stats.EntrezStatisticsSource) (float64, error) {
	if idfCache == nil {
		idfCache = make(map[string]float64)
	}
	if _, ok := idfCache[term+field]; ok {
		return idfCache[term+field], nil
	}
	idf, err := e.InverseDocumentFrequency(term, field)
	if err != nil {
		return 0, err
	}
	idfCache[term+field] = idf
	return idf, nil
}

func (s *TFIDFScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := prose.NewDocument(query, prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
	if err != nil {
		return 0, err
	}
	var sumTf float64
	var sumIdf float64
	for _, token := range tokens.Tokens() {
		for _, field := range fields {
			tf := s.p.Tf(token.Text, field, pmid)
			sumTf += tf
			idf, err := fastIDF(token.Text, field, s.s)
			if err != nil {
				return 0, err
			}
			sumIdf += idf
		}
	}
	return sumTf * sumIdf, nil
}

func (s *TFIDFScorer) posting(p *Posting) {
	s.p = p
}

func (s *TFIDFScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s *BM25Scorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := prose.NewDocument(query, prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
	if err != nil {
		return 0, err
	}

	// Pre-compute the average document lengths for fields.
	dls := make(map[string]float64)
	for _, field := range fields {
		dls[field] = s.p.AvgDocLen(field)
	}

	var score float64
	for _, token := range tokens.Tokens() {
		for _, field := range fields {
			idf, err := fastIDF(token.Text, field, s.s)
			if err != nil {
				return 0, err
			}
			tf := s.p.Tf(token.Text, field, pmid)
			dl := s.p.DocLen(field, pmid)
			score += idf * ((tf * (s.K1 + 1)) / (tf + s.K1*(1-s.B+s.B*(dl/dls[field]))))
		}
	}
	return score, nil
}

func (s *BM25Scorer) posting(p *Posting) {
	s.p = p
}

func (s *BM25Scorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}
