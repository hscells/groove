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

// TransformationStrategy samples candidates using a transformation strategy.
type TransformationStrategy func(candidates []CandidateQuery, N int) []CandidateQuery

// TransformationSampler samples candidate queries based on the transformation that was applied.
// It uses stratified sampling by ensuring a minimum of n candidates are sampled, plus an
// additional delta-%.
type TransformationSampler struct {
	n     int
	delta float64
	TransformationStrategy
}

func BalancedTransformationStrategy(candidates []CandidateQuery, N int) []CandidateQuery {
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
	// Continue adding unseen candidate queries using balanced sampling until the
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
	return c
}

func StratifiedTransformationStrategy(candidates []CandidateQuery, N int) []CandidateQuery {
	factor := math.Floor(float64(len(candidates)) / float64(N))

	// Create a distribution of the transformation IDs in the candidates.
	dist := make(map[int][]CandidateQuery)
	for _, candidate := range candidates {
		dist[candidate.TransformationID] = append(dist[candidate.TransformationID], candidate)
	}

	// Perform stratified sampling on the transformations.
	var c []CandidateQuery
	for k, v := range dist {
		n := math.Floor(float64(len(v)) / factor)
		w := make([]CandidateQuery, len(v))
		copy(w, v)
		for i, candidate := range w {
			if n <= 0 || len(c) >= N || i >= len(dist[k]) {
				break
			}
			c = append(c, candidate)
			dist[k] = append(dist[k][:i], dist[k][i+1:]...)
			n--
		}
	}

	// Return early if there is a perfectly sampled set.
	if len(c) >= N {
		return c
	}

	// Find the maximum distribution size.
	for len(c) < N {
		var max int
		for _, v := range dist {
			if len(v) > max {
				max = len(v)
			}
		}

		// Otherwise continue adding from the set of the maximum size.
		for _, v := range dist {
			if len(v) == max {
				for _, candidate := range v {
					if len(c) >= N {
						break
					}
					c = append(c, candidate)
				}
			}
		}
	}

	return c
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

	c := s.TransformationStrategy(candidates, N)

	return c, nil
}

func NewTransformationSampler(n int, delta float64, strategy TransformationStrategy) TransformationSampler {
	return TransformationSampler{
		n:                      n,
		delta:                  delta,
		TransformationStrategy: strategy,
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
	scores  map[string]float64
	measure eval.Evaluator
	qrels   trecresults.QrelsFile
	cache   combinator.QueryCacher
	ss      stats.StatisticsSource
	ScoredStrategy
}

// ScoredCandidateQuery contains a candidate query and some Score.
type ScoredCandidateQuery struct {
	CandidateQuery
	Score float64
}

// ScoredStrategy samples scored candidates.
type ScoredStrategy func(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery

func BalancedScoredStrategy(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery {
	// Sort all of the candidates based on Score.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
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
		if k >= len(candidates) {
			l++
			k = l
		}
		x[j] = candidates[k].CandidateQuery
		k += stepSize
		j++
	}
	return x
}

func StratifiedScoredStrategy(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery {
	var (
		better, worse []ScoredCandidateQuery
		c             []CandidateQuery
	)

	// Identify which candidates score higher and lower than the comparison scores in `scores`.
	for _, candidate := range candidates {
		if score, ok := scores[candidate.Topic]; ok {
			if candidate.Score >= score {
				better = append(better, candidate)
			} else {
				worse = append(worse, candidate)
			}
		}
	}

	// Compute just how many candidates should be sampled from each set of better or worse candidates.
	betterNum := math.Ceil(float64(len(better)) / float64(len(candidates)) * float64(N))
	worseNum := math.Ceil(float64(len(worse)) / float64(len(candidates)) * float64(N))

	// Otherwise, add candidates from the better sample.
	for i, j := 0.0, 0; i < betterNum; i, j = i+1, j+1 {
		if len(c) >= N {
			return c
		}
		c = append(c, better[j].CandidateQuery)
	}

	// And from the worse sample.
	for i, j := 0.0, 0; i < worseNum; i, j = i+1, j+1 {
		if len(c) >= N {
			return c
		}
		c = append(c, worse[j].CandidateQuery)
	}

	return c
}

// PositiveBiasScoredStrategy samples up to N candidates that improve over the comparison score in `scores`.
func PositiveBiasScoredStrategy(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery {
	var (
		c []CandidateQuery
	)

	for _, candidate := range candidates {
		if score, ok := scores[candidate.Topic]; ok {
			if candidate.Score >= score {
				if len(c) > N {
					return c
				}
				c = append(c, candidate.CandidateQuery)
			}
		}
	}

	return c
}

// NegativeBiasScoredStrategy samples up to N candidates that are worse than the comparison score in `scores`.
func NegativeBiasScoredStrategy(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery {
	var (
		c []CandidateQuery
	)

	for _, candidate := range candidates {
		if score, ok := scores[candidate.Topic]; ok {
			if candidate.Score < score {
				if len(c) > N {
					return c
				}
				c = append(c, candidate.CandidateQuery)
			}
		}
	}

	return c
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
			Score:          v,
		}
	}

	return s.ScoredStrategy(c, s.scores, N), nil
}

func NewEvaluationSampler(n int, delta float64, measure eval.Evaluator, qrels trecresults.QrelsFile, cache combinator.QueryCacher, ss stats.StatisticsSource, scores map[string]float64, strategy ScoredStrategy) EvaluationSampler {
	return EvaluationSampler{
		n:              n,
		delta:          delta,
		measure:        measure,
		qrels:          qrels,
		cache:          cache,
		ss:             ss,
		scores:         scores,
		ScoredStrategy: strategy,
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

	// ScoredCandidateQuery contains a candidate query and some Score.
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

	// Sort all of the candidates based on numRet and Score.
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
