package learning

import (
	"github.com/bugra/kmeans"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"log"
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
	N := int(s.delta*float64(len(candidates))) + s.n

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
	// Compute the number of candidates to sample.
	N := int(s.delta*float64(len(candidates))) + s.n

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// Shuffle the candidates to sample.
	l := rand.Perm(len(candidates))

	// Sample candidates from shuffled slice.
	c := make([]CandidateQuery, N)
	for i := 0; i < len(c); i++ {
		c[i] = candidates[l[i]]
	}

	// Continue to sample until there are N candidates sampled.
	i := len(c)
	for len(c) < N {
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

func BalancedScoredStrategy(candidates []ScoredCandidateQuery, _ map[string]float64, N int) []CandidateQuery {
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

// MaximalMarginalRelevanceScoredStrategy samples candidates according to a diversity-based strategy. Diversity
// functions commonly employ a notion of sim(d,q) where d is a document and q is a query. In this case
// (diversifying queries), this is replaced with the evaluation score of a query (for a particular
// evaluation measure). The sim(q1,q2) - i.e., the similarity between two queries, remains the same.
// Queries are sampled according to Maximal Marginal Relevance (Carbonell '98).
func MaximalMarginalRelevanceScoredStrategy(lambda float64, similarity func(x, y []float64) (float64, error)) ScoredStrategy {
	return func(candidates []ScoredCandidateQuery, scores map[string]float64, N int) []CandidateQuery {
		// Sort all of the candidates based on Score.
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Score > candidates[j].Score
		})

		mmr := func(r float64, unranked, selected []ScoredCandidateQuery) (int, ScoredCandidateQuery) {
			var (
				pos   int
				score float64
				query ScoredCandidateQuery
			)
			for i, u := range unranked {
				sim := 0.0
				for _, s := range selected {
					curr, err := similarity(u.Features.Scores(ChainFeatures+len(s.Chain)), s.Features.Scores(ChainFeatures+len(s.Chain)))
					if err != nil {
						panic(err)
					}
					if curr > sim {
						sim = curr
					}
				}
				curr := lambda*r - (1-lambda)*sim
				if curr > score {
					score = curr
					query = u
					pos = i
				}
			}
			return pos, query
		}

		unranked := candidates[1:]
		selected := []ScoredCandidateQuery{candidates[0]}
		c := make([]CandidateQuery, N)

		for i, q := range unranked {
			if i >= N {
				return c
			}

			pos, query := mmr(q.Score, unranked, selected)
			// Add the query to selected.
			selected = append(selected, query)
			// Remove the query from unranked.
			unranked = append(unranked[:pos], unranked[pos+1:]...)
			// Add the query to the final list.
			c[i] = query.CandidateQuery
		}

		return c
	}
}

func (s EvaluationSampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta*float64(len(candidates))) + s.n

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
	GreedyStrategy
}

type GreedyStrategy func(candidates []GreedyCandidateQuery, N int) []CandidateQuery

// ScoredCandidateQuery contains a candidate query and some Score.
type GreedyCandidateQuery struct {
	CandidateQuery
	score  float64
	numRet int
}

func RankedGreedyStrategy(candidates []GreedyCandidateQuery, N int) []CandidateQuery {
	// Sort all of the candidates based on numRet and Score.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].numRet < candidates[j].numRet && candidates[i].score > candidates[j].score
	})

	// Sample the queries from lowest numRet to highest until N.
	x := make([]CandidateQuery, N)
	for i, child := range candidates {
		if i >= len(x) {
			break
		}
		if child.numRet == 0 {
			continue
		}
		x[i] = child.CandidateQuery
	}

	return x
}

func MaximalMarginalRelevanceGreedyStrategy(scores map[string]float64, lambda float64, similarity func(x, y []float64) (float64, error)) GreedyStrategy {
	return func(candidates []GreedyCandidateQuery, N int) []CandidateQuery {
		scored := make([]ScoredCandidateQuery, len(candidates))
		for i, candidate := range candidates {
			scored[i] = ScoredCandidateQuery{Score: float64(candidate.numRet), CandidateQuery: candidate.CandidateQuery}
		}

		return MaximalMarginalRelevanceScoredStrategy(lambda, similarity)(scored, scores, N)
	}
}

func (s GreedySampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta*float64(len(candidates))) + s.n

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// Score all of the candidates.
	c := make([]GreedyCandidateQuery, len(candidates))
	for i, child := range candidates {
		pq := pipeline.NewQuery(child.Topic, child.Topic, child.Query)
		t, _, err := combinator.NewLogicalTree(pq, s.ss, s.cache)
		if err != nil {
			return nil, err
		}
		results := t.Documents(s.cache).Results(pq, "")
		v := s.measure.Score(&results, s.qrels.Qrels[child.Topic])
		c[i] = GreedyCandidateQuery{
			CandidateQuery: child,
			score:          v,
			numRet:         len(results),
		}
	}

	return s.GreedyStrategy(c, N), nil
}

func NewGreedySampler(n int, delta float64, measure eval.Evaluator, qrels trecresults.QrelsFile, cache combinator.QueryCacher, ss stats.StatisticsSource, strategy GreedyStrategy) GreedySampler {
	return GreedySampler{
		n:              n,
		delta:          delta,
		measure:        measure,
		qrels:          qrels,
		cache:          cache,
		ss:             ss,
		GreedyStrategy: strategy,
	}
}

type ClusterSampler struct {
	n     int
	delta float64
	k     int
}

func (s ClusterSampler) Sample(candidates []CandidateQuery) ([]CandidateQuery, error) {
	// Compute the number of candidates to sample.
	N := int(s.delta*float64(len(candidates))) + s.n

	if len(candidates) <= N {
		// We can return early here because there are not enough candidates to satisfy
		// the sampling conditions.
		return candidates, nil
	}

	// Create a two-dimensional matrix to store the feature vectors of the queries.
	data := make([][]float64, len(candidates))
	for i, candidate := range candidates {
		data[i] = candidate.Features.Scores(ChainFeatures + len(candidate.Chain))
	}

	// Perform k-means++ to cluster the queries.
	labels, err := kmeans.Kmeans(data, s.k, kmeans.EuclideanDistance, 10)
	if err != nil {
		return nil, err
	}

	// Round-robin from the clusters, sampling a query from each cluster.
	var (
		prevLabel int
		c         []CandidateQuery
	)
	seen := make(map[int]bool)
	for len(c) < N {
		for i := 0; i < len(candidates); i++ {
			if len(c) >= N {
				return c, nil
			}
			if _, ok := seen[i]; !ok {
				if labels[i] != prevLabel {
					seen[i] = true
					prevLabel = labels[i]
					c = append(c, candidates[i])
				}
			}
		}
	}

	return c, nil
}

func NewClusterSampler(n int, delta float64, k int) ClusterSampler {
	if k <= 0 {
		log.Println("k was less than 1, setting to default k=5")
		k = 5
	}
	return ClusterSampler{
		n:     n,
		delta: delta,
		k:     k,
	}
}
