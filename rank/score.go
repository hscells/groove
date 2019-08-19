package rank

import (
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/stats"
	"math"
	"strings"
	"sync"
)

type Scorer interface {
	Score(query, pmid string, fields ...string) (float64, error)
	posting(p *Posting)
	entrez(e stats.EntrezStatisticsSource)
}

type TitleAbstractScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type TFIDFScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type IDFScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type BM25Scorer struct {
	s  stats.EntrezStatisticsSource
	p  *Posting
	K1 float64
	B  float64
}

type documentLengthScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type VectorSpaceScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type PosScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

func (s PosScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(query)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	for _, token := range tokens {
		for _, field := range fields {
			sumTf += s.p.Pos(token, field, pmid)
		}
	}
	return sumTf, nil
}

func (s PosScorer) posting(p *Posting) {
	s.p = p
}

func (s PosScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s VectorSpaceScorer) Score(query, pmid string, fields ...string) (float64, error) {
	terms, err := fastTokenise(query)
	if err != nil {
		return 0, err
	}

	TI := hash("ti")
	AB := hash("ab")
	MH := hash("mh")

	qv := make([]float64, len(s.p.Index)*3)
	for _, term := range terms {
		for _, field := range fields {
			var j int
			switch hash(field) {
			case TI:
				j = 1
			case AB:
				j = 2
			case MH:
				j = 3
			}

			qv[j*s.p.TermIdx[hash(term)]] = 1
		}
	}

	dv := s.p.DocumentVector(hash(pmid))

	return cui2vec.Cosine(qv, dv)

}

func (s VectorSpaceScorer) posting(p *Posting) {
	s.p = p
}

func (s VectorSpaceScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s TitleAbstractScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(query)
	if err != nil {
		return 0, err
	}

	var score float64
	for _, token := range tokens {
		ti := s.p.Tf(token, "ti", pmid)
		ab := s.p.Tf(token, "ab", pmid)
		mh := s.p.Tf(token, "mh", pmid)
		if ti > 0 && ab > 0 && mh > 0 {
			score += 50
		} else if ti > 0 && ab > 0 {
			score += 10
		} else if ti > 0 && mh > 0 {
			score += 10
		} else if ab > 0 && mh > 0 {
			score += 5
		}
	}
	return score / float64(len(tokens)), nil
}

func (s TitleAbstractScorer) posting(p *Posting) {
	s.p = p
}

func (s TitleAbstractScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s documentLengthScorer) Score(query, pmid string, fields ...string) (float64, error) {
	var l float64
	for _, field := range fields {
		l += s.p.MaxDocLen - (s.p.MaxDocLen - s.p.DocLens[pmid][hash(field)])
	}
	return l / float64(len(fields)), nil
}

func (s documentLengthScorer) posting(p *Posting) {
	s.p = p
}

func (s documentLengthScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

type DirichlectTermProbScorer struct {
	s  stats.EntrezStatisticsSource
	p  *Posting
	Mu float64
}

func (s DirichlectTermProbScorer) Score(query, pmid string, fields ...string) (float64, error) {
	terms, err := fastTokenise(query, fields...)
	if err != nil {
		return 0, err
	}
	var sumProb float64
	for _, term := range terms {
		for _, field := range fields {
			sumProb += s.p.DirichlectTermProbability(term, field, pmid, s.Mu)
		}
	}
	return sumProb, nil
}

func (s DirichlectTermProbScorer) posting(p *Posting) {
	s.p = p
}

func (s DirichlectTermProbScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
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
			tf := s.p.Tf(token, field, pmid)
			sumTf += tf
			idf, err := fastIDF(token, field, s.s, s.p)
			if err != nil {
				return 0, err
			}
			sumIdf += idf
		}
	}
	if sumTf == 0 {
		return 0, nil
	}
	return math.Log10(sumTf * sumIdf), nil
}

func (s *TFIDFScorer) posting(p *Posting) {
	s.p = p
}

func (s *TFIDFScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s *IDFScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(query)
	if err != nil {
		return 0, err
	}
	var sumIdf float64
	for _, token := range tokens {
		for _, field := range fields {
			idf, err := fastIDF(token, field, s.s, s.p)
			if err != nil {
				return 0, err
			}
			sumIdf += idf
		}
	}
	return sumIdf, nil
}

func (s *IDFScorer) posting(p *Posting) {
	s.p = p
}

func (s *IDFScorer) entrez(e stats.EntrezStatisticsSource) {
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
			idf, err := fastIDF(token, field, s.s, s.p)
			if err != nil {
				return 0, err
			}
			tf := s.p.Tf(token, field, pmid) + s.p.Pos(token, field, pmid)
			if tf == 0 {
				continue
			}
			score += idf * ((tf * (s.K1 + 1)) / (tf + s.K1*(1-s.B+s.B*(docLen/dls[hash(field)]))))
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

var (
	idfCache       map[uint32]float64
	idfCacheMu     sync.Mutex
	avgDocLenCache map[uint32]float64
	tokensCache    map[uint32][]string
)

func fastDocLen(posting *Posting, fields []string) map[uint32]float64 {
	if avgDocLenCache == nil {
		// Pre-compute the average document lengths for fields.
		avgDocLenCache = make(map[uint32]float64, len(fields))
		for _, field := range fields {
			avgDocLenCache[hash(field)] = posting.AvgDocLen(field)
		}
	}
	for _, field := range fields {
		f := hash(field)
		if _, ok := avgDocLenCache[f]; !ok {
			avgDocLenCache[f] = posting.AvgDocLen(field)
		}
	}
	return avgDocLenCache
}

func fastTokenise(query string, fields ...string) ([]string, error) {
	query = strings.ReplaceAll(strings.ToLower(query), `"`, "")
	q := hash(query)
	if tokensCache == nil {
		tokensCache = make(map[uint32][]string)
	}
	if _, ok := tokensCache[q]; !ok {
		if len(fields) == 1 {
			if fields[0] == "mh" {
				tokensCache[q] = []string{query}
			}
		} else {
			tokensCache[q] = strings.Split(query, " ")
		}
	}
	return tokensCache[q], nil
}

func fastIDF(term, field string, e stats.EntrezStatisticsSource, p *Posting) (float64, error) {
	idfCacheMu.Lock()
	defer idfCacheMu.Unlock()
	t := hash(term)
	f := hash(field)
	if idfCache == nil {
		idfCache = make(map[uint32]float64)
	}
	if _, ok := idfCache[t*f]; ok {
		return idfCache[t*f], nil
	}

	if _, ok := p.Index[t]; !ok {
		idfCache[t*f] = 0
		return 0, nil
	}

	if _, ok := p.Index[t][f]; !ok {
		idfCache[t*f] = 0
		return 0, nil
	}

	nt := float64(len(p.Index[t][f]))
	idf := math.Log((e.N + 1) / (nt + 1))
	idfCache[t*f] = idf
	return idf, nil
}
