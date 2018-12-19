package learning

import (
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"log"
	"math/rand"
)

type GenerationResult struct {
	error
	CandidateQuery
}

// QueryChainGenerationExplorer explores the space of candidate queries from an initial "seed"
// query, sending results through the supplied channel as it explores the possible space of queries.
// Note that the implementations of an explorer must provide the facility for sampling, as
// the method of sampling may apply to different methods of traversal.
type QueryChainGenerationExplorer interface {
	Traverse(seed CandidateQuery, c chan GenerationResult)
}

// BreadthFirstExplorer explores the space of candidates breadth-first. It generates a set of
// variations for each candidate query, and pools these together to be sampled.
type BreadthFirstExplorer struct {
	depth int
	chain *QueryChain
	Sampler
	BreadthFirstStoppingCondition
}

func NewBreadthFirstExplorer(chain *QueryChain, sampler Sampler, condition BreadthFirstStoppingCondition) BreadthFirstExplorer {
	return BreadthFirstExplorer{
		chain:                         chain,
		Sampler:                       sampler,
		BreadthFirstStoppingCondition: condition,
	}
}

// BreadthFirstStoppingCondition controls at what depth in the chain the
// breadth-first explorer should stop.
type BreadthFirstStoppingCondition func(depth int, candidates []CandidateQuery) bool

// DepthStoppingCondition uses the depth of the chain to determine when to stop.
func DepthStoppingCondition(d int) BreadthFirstStoppingCondition {
	return func(depth int, candidates []CandidateQuery) bool {
		if len(candidates) == 0 || len(candidates[0].Chain) == 0 {
			return true
		}
		return depth < d
	}
}

// DriftStoppingCondition TODO
func DriftStoppingCondition(depth int, candidates []CandidateQuery) bool {
	return false
}

func (e BreadthFirstExplorer) Traverse(candidate CandidateQuery, c chan GenerationResult) {
	var nextCandidates []CandidateQuery
	candidates := []CandidateQuery{candidate}

	for e.BreadthFirstStoppingCondition(e.depth, candidates) {
		nextCandidates = []CandidateQuery{}
		log.Printf("loop #%v with %v candidate(s)", e.depth, len(candidates))

		for i, q := range candidates {
			log.Println("this is topic", q.Topic)

			// Generate variations.
			log.Println(len(candidates)-i, "to go")
			log.Println(len(q.Chain), "long chain")
			log.Println("generating variations...")

			vars, err := Variations(q, e.chain.StatisticsSource, e.chain.MeasurementExecutor, e.chain.Measurements, e.chain.Transformations...)
			if err != nil {
				c <- GenerationResult{error: err}
				close(c)
				return
			}

			nextCandidates = append(nextCandidates, vars...)
			log.Println("generated", len(vars), "candidates")
			log.Println("generated", len(nextCandidates), "candidates so far")
		}

		log.Println("sampling", len(nextCandidates), "candidates...")
		sampled, err := e.Sampler.Sample(nextCandidates)
		if err != nil {
			c <- GenerationResult{error: err}
			close(c)
			return
		}

		log.Println("sampled down to", len(sampled), "candidates")
		candidates = []CandidateQuery{}
		for _, candidate := range sampled {
			candidates = append(candidates, candidate)
			c <- GenerationResult{CandidateQuery: candidate}
		}
		e.depth++
	}
	close(c)
}

// DepthFirstExplorer explores the query space depth-first. It traverses one query at a time,
// backtracking further if necessary. The breadth-first approach uses two conditions to control
// (1) when the explorer should stop and backtrack, and (2) when a query should be sampled.
type DepthFirstExplorer struct {
	chain  *QueryChain
	budget *int
	n      int
	DepthFirstSamplingCriteria
}

func NewDepthFirstExplorer(chain *QueryChain, sampling DepthFirstSamplingCriteria, budget int) DepthFirstExplorer {
	return DepthFirstExplorer{
		n:                          budget,
		budget:                     &budget,
		chain:                      chain,
		DepthFirstSamplingCriteria: sampling,
	}
}

// ExplorationSamplingCriteria controls when the explorer should sample a query.
type DepthFirstSamplingCriteria func(query CandidateQuery) bool

// ProbabilisticSamplingCriteria samples queries using a likelihood.
func ProbabilisticSamplingCriteria(likelihood float64) DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		return rand.Float64() < likelihood
	}
}

// BalancedTransformationSamplingCriteria ensures the candidate query potentially
// being sampled has approximately equal transformations applied to it.
func BalancedTransformationSamplingCriteria(numTransformations int) DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		if len(query.Chain) > numTransformations {
			return false
		}
		seen := make(map[int]bool)
		for _, candidate := range query.Chain {
			seen[candidate.TransformationID] = true
		}

		// Special case if the length of the chain is the length of the queries seen.
		// This catches queries where the size of the chain is less than the number of
		// transformations, but all of the transformations applied are different.
		if len(seen) == len(query.Chain) {
			return true
		}

		// Otherwise, all transformations have been applied to this query.
		return len(seen) == numTransformations
	}
}

// BiasedTransformationSamplingCriteria samples candidate queries when all of the transformations
// applied to a query are the same.
func BiasedTransformationSamplingCriteria() DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		var id int
		for i, candidate := range query.Chain {
			if i == 0 {
				id = candidate.TransformationID
			} else if candidate.TransformationID != id {
				return false
			}
		}
		return true
	}
}


// PositiveBiasedEvaluationSamplingCriteria samples using an evaluation measure to determine
// if the candidate query improves over the seed query for that measure.
func BalancedEvaluationSamplingCriteria(measure eval.Evaluator, scores map[string]float64, chain *QueryChain) DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		pq := pipeline.NewQuery(query.Topic, query.Topic, query.Query)
		t, _, err := combinator.NewLogicalTree(pq, chain.StatisticsSource, chain.QueryCacher)
		if err != nil {
			panic(err)
		}
		results := t.Documents(chain.QueryCacher).Results(pq, "")
		v := measure.Score(&results, chain.QrelsFile.Qrels[query.Topic])
		return v >= scores[query.Topic]
	}
}

// PositiveBiasedEvaluationSamplingCriteria samples using an evaluation measure to determine
// if the candidate query improves over the seed query for that measure.
func PositiveBiasedEvaluationSamplingCriteria(measure eval.Evaluator, scores map[string]float64, chain *QueryChain) DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		pq := pipeline.NewQuery(query.Topic, query.Topic, query.Query)
		t, _, err := combinator.NewLogicalTree(pq, chain.StatisticsSource, chain.QueryCacher)
		if err != nil {
			panic(err)
		}
		results := t.Documents(chain.QueryCacher).Results(pq, "")
		v := measure.Score(&results, chain.QrelsFile.Qrels[query.Topic])
		return v >= scores[query.Topic]
	}
}

// NegativeBiasedEvaluationSamplingCriteria samples using an evaluation measure to determine
// if the candidate query is worse than the seed query for that measure.
func NegativeBiasedEvaluationSamplingCriteria(measure eval.Evaluator, scores map[string]float64, chain *QueryChain) DepthFirstSamplingCriteria {
	return func(query CandidateQuery) bool {
		pq := pipeline.NewQuery(query.Topic, query.Topic, query.Query)
		t, _, err := combinator.NewLogicalTree(pq, chain.StatisticsSource, chain.QueryCacher)
		if err != nil {
			panic(err)
		}
		results := t.Documents(chain.QueryCacher).Results(pq, "")
		v := measure.Score(&results, chain.QrelsFile.Qrels[query.Topic])
		return v < scores[query.Topic]
	}
}

func (e DepthFirstExplorer) Traverse(query CandidateQuery, c chan GenerationResult) {
	if *e.budget <= 0 {
		return
	}

	if e.DepthFirstSamplingCriteria(query) {
		*e.budget--
		log.Printf("sampled query from topic %s at depth %d (budget %d/%d)\n", query.Topic, len(query.Chain), *e.budget, e.n)
		c <- GenerationResult{CandidateQuery: query}
	} else {
		return
	}

	log.Println("generating variations...")
	vars, err := Variations(query, e.chain.StatisticsSource, e.chain.MeasurementExecutor, e.chain.Measurements, e.chain.Transformations...)
	if err != nil {
		c <- GenerationResult{error: err}
		close(c)
		return
	}
	rand.Shuffle(len(vars), func(i, j int) {
		vars[i], vars[j] = vars[j], vars[i]
	})
	log.Println("generated and shuffled", len(vars), "candidates")

	for _, q := range vars {
		e.Traverse(q, c)
	}

	log.Printf("completed depth %d\n", len(query.Chain))
	if len(query.Chain) == 0 {
		*e.budget = e.n
		close(c)
		return
	}
}
