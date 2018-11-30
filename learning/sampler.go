package learning

import (
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"math"
	"math/rand"
	"sort"
)

type Sampler interface {
	Sample(candidates []CandidateQuery) ([]CandidateQuery, error)
}

// TransformationSampler samples candidate queries based on the transformation that was applied.
// It uses stratified sampling by ensuring a minimum of n candidates are sampled, plus an
// additional delta-%.
type TransformationSampler struct {
	n     int
	delta float64
}

func (s TransformationSampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta * float64(len(candidates)))

	// If there are not at least n candidates, set the number of candidates to sample to n.
	// If there are not enough candidates in n, set the number of candidates to sample
	// as the total number of candidates.
	if N < s.n {
		N = s.n
	}

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// Sort the candidates by transformation ID.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].TransformationID < candidates[j].TransformationID
	})

	c := make([]CandidateQuery, N)
	seen := make(map[int]bool)
	var (
		j       int // The index of un-added candidates.
		prevTID int // The ID of the previous transformation seen.
	)
	// Continue adding unseen candidate queries using stratified sampling until the
	// conditions for stopping are met.
	for j < N {
		for i, child := range candidates {
			if _, ok := seen[i]; !ok {
				if prevTID != child.TransformationID {
					seen[i] = true

					prevTID = child.TransformationID

					c[j] = child
					j++
					if j >= N {
						break
					}
				}
			}
		}
	}

	return c, nil
}

func NewTransformationSampler(n int, delta float64) TransformationSampler {
	return TransformationSampler{
		n:     n,
		delta: delta,
	}
}

// RandomSampler samples candidates randomly. It will reduce the candidates to delta-% and
// sample at least n candidates (if there are at least n to sample from).
type RandomSampler struct {
	n     int
	delta float64
}

func (s RandomSampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Shuffle the candidates to sample.
	l := rand.Perm(len(candidates))

	// Sample delta-% candidates from shuffled slice.
	c := make([]CandidateQuery, int(s.delta*float64(len(candidates))))
	for i := 0; i < len(c); i++ {
		c[i] = candidates[l[i]]
	}

	// Continue to sample until there are at least n candidates sampled.
	i := len(c)
	for len(c) < s.n {
		if i >= len(candidates) {
			break
		}
		c = append(c, candidates[i])
		i++
	}
	return c, nil
}

func NewRandomSampler(n int, delta float64) RandomSampler {
	return RandomSampler{
		n:     n,
		delta: delta,
	}
}

// EvaluationSampler samples candidate queries based on some evaluation measure. It
// uses stratified sampling by ensuring a minimum of n candidates are sampled, plus an
// additional delta-%.
type EvaluationSampler struct {
	n       int
	delta   float64
	measure eval.Evaluator
	qrels   trecresults.QrelsFile
	cache   combinator.QueryCacher
	ss      stats.StatisticsSource
}

func (s EvaluationSampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta * float64(len(candidates)))

	// If there are not at least n candidates, set the number of candidates to sample to n.
	// If there are not enough candidates in n, set the number of candidates to sample
	// as the total number of candidates.
	if N < s.n {
		N = s.n
	}

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// ScoredCandidateQuery contains a candidate query and some score.
	type ScoredCandidateQuery struct {
		CandidateQuery
		score float64
	}

	// Score all of the candidates.
	c := make([]ScoredCandidateQuery, len(candidates))
	for i, child := range candidates {
		pq := pipeline.NewQuery(child.Topic, child.Topic, child.Query)
		t, _, err := combinator.NewLogicalTree(pq, s.ss, s.cache)
		if err != nil {
			return nil, err
		}
		results := t.Documents(s.cache).Results(pq, "")
		v := s.measure.Score(&results, s.qrels.Qrels[child.Topic])
		c[i] = ScoredCandidateQuery{
			CandidateQuery: child,
			score:          v,
		}
	}

	// Sort all of the candidates based on score.
	sort.Slice(c, func(i, j int) bool {
		return c[i].score > c[j].score
	})

	// Compute the step size for stratified sampling.
	stepSize := int(math.Ceil(float64(len(candidates)) / float64(N)))

	// Perform stratified sampling over the scored candidates.
	x := make([]CandidateQuery, N)
	var (
		j int // Index of the sampled candidates.
		k int // Index of the candidates (increased using step size).
		l int // Number of times the step count has "wrapped".
	)
	for j < len(x) {
		if k >= len(c) {
			l++
			k = l
		}
		x[j] = c[k].CandidateQuery
		k += stepSize
		j++
	}

	return x, nil
}

func NewEvaluationSampler(n int, delta float64, measure eval.Evaluator, qrels trecresults.QrelsFile, cache combinator.QueryCacher, ss stats.StatisticsSource) EvaluationSampler {
	return EvaluationSampler{
		n:       n,
		delta:   delta,
		measure: measure,
		qrels:   qrels,
		cache:   cache,
		ss:      ss,
	}
}

// GreedySampler samples candidate queries based on both the number of retrieved documents
// and some evaluation measure. It uses stratified sampling by ensuring a minimum of n
// candidates are sampled, plus an additional delta-%.
type GreedySampler struct {
	n       int
	delta   float64
	measure eval.Evaluator
	qrels   trecresults.QrelsFile
	cache   combinator.QueryCacher
	ss      stats.StatisticsSource
}

func (s GreedySampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta * float64(len(candidates)))

	// If there are not at least n candidates, set the number of candidates to sample to n.
	// If there are not enough candidates in n, set the number of candidates to sample
	// as the total number of candidates.
	if N < s.n {
		N = s.n
	}

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// ScoredCandidateQuery contains a candidate query and some score.
	type ScoredCandidateQuery struct {
		CandidateQuery
		score  float64
		numRet int
	}

	// Score all of the candidates.
	c := make([]ScoredCandidateQuery, len(candidates))
	for i, child := range candidates {
		pq := pipeline.NewQuery(child.Topic, child.Topic, child.Query)
		t, _, err := combinator.NewLogicalTree(pq, s.ss, s.cache)
		if err != nil {
			return nil, err
		}
		results := t.Documents(s.cache).Results(pq, "")
		v := s.measure.Score(&results, s.qrels.Qrels[child.Topic])
		c[i] = ScoredCandidateQuery{
			CandidateQuery: child,
			score:          v,
			numRet:         len(results),
		}
	}

	// Sort all of the candidates based on numRet and score.
	sort.Slice(c, func(i, j int) bool {
		return c[i].numRet < c[j].numRet && c[i].score > c[j].score
	})

	// Sample the queries from lowest numRet to highest until N.
	x := make([]CandidateQuery, N)
	for i, child := range c {
		if i >= len(x) {
			break
		}
		x[i] = child.CandidateQuery
	}

	return x, nil
}

func NewGreedySampler(n int, delta float64, measure eval.Evaluator, qrels trecresults.QrelsFile, cache combinator.QueryCacher, ss stats.StatisticsSource) GreedySampler {
	return GreedySampler{
		n:       n,
		delta:   delta,
		measure: measure,
		qrels:   qrels,
		cache:   cache,
		ss:      ss,
	}
}
