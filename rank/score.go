package rank

import (
	"github.com/hscells/groove/stats"
	"gopkg.in/jdkato/prose.v2"
	"strings"
	"sync"
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
	idfCache       map[string]float64
	idfCacheMu     sync.Mutex
	avgDocLenCache map[string]float64
	tokensCache    map[string][]prose.Token
)

func fastDocLen(posting *Posting, fields []string) map[string]float64 {
	if avgDocLenCache == nil {
		// Pre-compute the average document lengths for fields.
		avgDocLenCache = make(map[string]float64)
		for _, field := range fields {
			avgDocLenCache[field] = posting.AvgDocLen(field)
		}
	}
	for _, field := range fields {
		if _, ok := avgDocLenCache[field]; !ok {
			avgDocLenCache[field] = posting.AvgDocLen(field)
		}
	}
	return avgDocLenCache
}

func fastTokenise(query string, fields ...string) ([]prose.Token, error) {
	query = strings.ToLower(query)
	if tokensCache == nil {
		tokensCache = make(map[string][]prose.Token)
	}
	if _, ok := tokensCache[query]; !ok {
		if len(fields) == 1 {
			if fields[0] == "mh" {
				tokensCache[query] = []prose.Token{{Text: query}}
			}
		} else {
			tokens, err := prose.NewDocument(query, prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
			if err != nil {
				return nil, err
			}
			tokensCache[query] = tokens.Tokens()
		}
	}
	return tokensCache[query], nil
}

func fastIDF(term, field string, e stats.EntrezStatisticsSource) (float64, error) {
	idfCacheMu.Lock()
	defer idfCacheMu.Unlock()
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
	tokens, err := fastTokenise(query)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	var sumIdf float64
	for _, token := range tokens {
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
	tokens, err := fastTokenise(query, fields...)
	if err != nil {
		return 0, err
	}
	dls := fastDocLen(s.p, fields)

	var score float64
	for _, field := range fields {
		docLen := s.p.DocLen(field, pmid)
		for _, token := range tokens {
			idf, err := fastIDF(token.Text, field, s.s)
			if err != nil {
				return 0, err
			}
			tf := s.p.Tf(token.Text, field, pmid)
			score += idf * ((tf * (s.K1 + 1)) / (tf + s.K1*(1-s.B+s.B*(docLen/dls[field]))))
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
