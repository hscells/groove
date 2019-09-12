package rank

import (
	"fmt"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/stats"
	"github.com/hscells/meshexp"
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

type DocLenScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type PubDateScorer struct {
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

type SumIDFScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type LnL2Scorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

type DirichletTermProbScorer struct {
	s  stats.EntrezStatisticsSource
	p  *Posting
	Mu float64
}

type SumTFScorer struct {
	s stats.EntrezStatisticsSource
	p *Posting
}

func (s SumTFScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var tf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		for _, field := range fields {
			tf += s.p.Tf(token, field, pmid)
		}
	}
	return tf, nil
}

func (s SumTFScorer) posting(p *Posting) {
	s.p = p
}

func (s SumTFScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s LnL2Scorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var lnl2 float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		for _, field := range fields {
			tf := s.p.Tf(token, field, pmid)
			norm := 1 / (tf + 1)
			lnl2 += tf * idfDFR(token, field, pmid, s.p, s.s) * norm
		}
	}
	return lnl2, nil
}

func (s LnL2Scorer) posting(p *Posting) {
	s.p = p
}

func (s LnL2Scorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func idfDFR(term, field, pmid string, p *Posting, e stats.EntrezStatisticsSource) float64 {
	t := hash(term)
	f := hash(field)

	if _, ok := p.Index[t]; !ok {
		return 0
	}
	if _, ok := p.Index[t][f]; !ok {
		return 0
	}

	df := float64(len(p.Index[t][f]))
	return (math.Log(e.N+1) / (df + 0.5)) * (1 / math.Log(2))
}

func (s SumIDFScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var idf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		for _, field := range fields {
			v, err := fastIDF(token, field, s.s, s.p)
			if err != nil {
				return 0, err
			}
			idf += v
		}
	}
	return idf, nil
}

func (s SumIDFScorer) posting(p *Posting) {
	s.p = p
}

func (s SumIDFScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s PubDateScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		for _, field := range fields {
			sumTf += s.p.Tf(token, field, pmid)
		}
	}
	if sumTf == 0 {
		return 0, nil
	}
	return float64(s.p.DocDates[hash(pmid)]), nil
}

func (s PubDateScorer) posting(p *Posting) {
	s.p = p
}

func (s PubDateScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s PosScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
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
	terms, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}

	TI := hash("ti")
	AB := hash("ab")
	MH := hash("mh")

	qv := make([]float64, len(s.p.Index)*3)
	for _, term := range terms {
		if _, ok := s.p.Index[hash(term)]; !ok {
			continue
		}
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
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}

	//t := 0.0
	var score float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		//t++
		ti := s.p.Tf(token, "ti", pmid)
		ab := s.p.Tf(token, "ab", pmid)
		mh := s.p.Tf(token, "mh", pmid)

		if ti > 0 && ab > 0 && mh > 0 {
			score += 100
		} else if mh == 0 && ti > 0 && ab > 0 {
			score += 90
		} else if ab == 0 && ti > 0 && mh > 0 {
			score += 10
		} else if ti == 0 && ab > 0 && mh > 0 {
			score += 10
		} else if ti > 0 && ab == 0 && mh == 0 {
			score += 20
		} else if ab > 0 && ti == 0 && mh == 0 {
			score += 20
		} else if ab == 0 && ti == 0 && mh > 0 {
			score += 90
		} else {
			score -= 100
		}
	}
	//score *=
	return score, nil
}

func (s TitleAbstractScorer) posting(p *Posting) {
	s.p = p
}

func (s TitleAbstractScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s DocLenScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
		for _, field := range fields {
			sumTf += s.p.Tf(token, field, pmid)
		}
	}
	if sumTf == 0 {
		return 0, nil
	}
	var l float64
	for _, field := range fields {
		//l += s.p.MaxDocLen - (s.p.MaxDocLen - s.p.DocLens[pmid][hash(field)])
		l += s.p.DocLens[pmid][hash(field)]
	}
	return l, nil
}

func (s DocLenScorer) posting(p *Posting) {
	s.p = p
}

func (s DocLenScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s DirichletTermProbScorer) Score(query, pmid string, fields ...string) (float64, error) {
	terms, err := fastTokenise(s.p, query, fields...)
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

func (s DirichletTermProbScorer) posting(p *Posting) {
	s.p = p
}

func (s DirichletTermProbScorer) entrez(e stats.EntrezStatisticsSource) {
	s.s = e
}

func (s *TFIDFScorer) Score(query, pmid string, fields ...string) (float64, error) {
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	var sumTf float64
	var sumIdf float64
	for _, token := range tokens {
		if _, ok := s.p.Index[hash(token)]; !ok {
			continue
		}
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
	tokens, err := fastTokenise(s.p, query, fields...)
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
	tokens, err := fastTokenise(s.p, query, fields...)
	if err != nil {
		return 0, err
	}
	dls := fastDocLen(s.p, fields)

	var score float64
	for _, field := range fields {
		docLen := s.p.DocLen(field, pmid)
		for _, token := range tokens {
			if _, ok := s.p.Index[hash(token)]; !ok {
				continue
			}
			idf, err := fastIDF(token, field, s.s, s.p)
			if err != nil {
				return 0, err
			}
			tf := s.p.Tf(token, field, pmid)
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
	avgDocLenMu    sync.Mutex
	tokensCache    map[uint32][]string
	tokensMu       sync.Mutex
)

func fastDocLen(posting *Posting, fields []string) map[uint32]float64 {
	avgDocLenMu.Lock()
	defer avgDocLenMu.Unlock()
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

var MESH, _ = meshexp.Default()

func fastTokenise(p *Posting, query string, fields ...string) ([]string, error) {
	tokensMu.Lock()
	defer tokensMu.Unlock()
	query = strings.ReplaceAll(strings.ToLower(query), `"`, "")
	q := hash(query)
	if tokensCache == nil {
		tokensCache = make(map[uint32][]string)
	}
	if _, ok := tokensCache[q]; !ok {
		for _, field := range fields {
			if field == "mh" {
				if query[len(query)-1] == '#' {
					qq := strings.Replace(query, `#`, "", -1)
					tokensCache[q] = append(MESH.Explode(qq), qq)
				} else {
					tokensCache[q] = []string{query}
				}
				break
			}
		}
		terms := strings.Split(query, " ")
		var toks []string
		for _, term := range terms {
			if strings.Contains(term, "*") {
				wc := strings.ReplaceAll(term, "*", "")
				for _, t := range suffixes {
					toks = append(toks, fmt.Sprintf("%s%s", wc, t))
				}
				toks = append(toks, wc)
			} else {
				toks = append(toks, term)
			}
		}
		tokensCache[q] = toks

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
