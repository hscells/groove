package learning

import (
	"fmt"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"log"
	"math/rand"
	"strings"
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
	depth           int
	ss              stats.StatisticsSource
	me              analysis.MeasurementExecutor
	measurements    []analysis.Measurement
	transformations []Transformation
	Sampler
	BreadthFirstStoppingCondition
}

func NewBreadthFirstExplorer(ss stats.StatisticsSource, me analysis.MeasurementExecutor, measurements []analysis.Measurement, transformations []Transformation, sampler Sampler, condition BreadthFirstStoppingCondition) BreadthFirstExplorer {
	return BreadthFirstExplorer{
		ss:                            ss,
		me:                            me,
		measurements:                  measurements,
		transformations:               transformations,
		Sampler:                       sampler,
		BreadthFirstStoppingCondition: condition,
	}
}

// BreadthFirstStoppingCondition controls at what depth in the chain the
// breadth-first explorer should stop.
type BreadthFirstStoppingCondition func(depth int, candidates []CandidateQuery) bool

// DepthStoppingCondition uses the depth of the chain to determine when to stop.
func DepthStoppingCondition(depth int, candidates []CandidateQuery) bool {
	if len(candidates) == 0 || len(candidates[0].Chain) == 0 {
		return true
	}
	return !(depth < len(candidates[0].Chain))
}

// DriftStoppingCondition TODO
func DriftStoppingCondition(depth int, candidates []CandidateQuery) bool {
	return false
}

func (e BreadthFirstExplorer) Traverse(candidate CandidateQuery, c chan GenerationResult) {
	var nextCandidates []CandidateQuery
	candidates := []CandidateQuery{candidate}
	log.Println("hello")
	for e.BreadthFirstStoppingCondition(e.depth, candidates) {
		log.Printf("loop #%v with %v candidate(s)", e.depth, len(candidates))

		for i, q := range candidates {

			log.Println("this is topic", q.Topic)

			// Generate variations.
			log.Println(len(candidates)-i, "to go")
			log.Println(len(q.Chain), "long chain")
			log.Println("generating variations...")

			vars, err := Variations(q, e.ss, e.me, e.measurements, e.transformations...)
			if err != nil {
				c <- GenerationResult{error: err}
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
		log.Println("sampled down to", len(c), "candidates")
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
	ss              stats.StatisticsSource
	me              analysis.MeasurementExecutor
	measurements    []analysis.Measurement
	transformations []Transformation
	DepthFirstStoppingCriteria
	ExplorationSamplingCriteria
}

// DepthFirstStoppingCriteria controls when the explorer should backtrack.
type DepthFirstStoppingCriteria func(query CandidateQuery) bool

// ExplorationSamplingCriteria controls when the explorer should sample a query.
type ExplorationSamplingCriteria func(query CandidateQuery) bool

// DepthStoppingCriteria ensures a query backtracks at a certain depth.
func ProbabalisticDepthStoppingCriteria(prob float64) DepthFirstStoppingCriteria {
	return func(query CandidateQuery) bool {
		depth := float64(len(query.Chain))
		likelihood := (1 - (1 / depth)) * prob
		return rand.Float64() < likelihood
	}
}

// StratifiedTransformationSamplingCriteria ensures the candidate query potentially
// being sampled has approximately equal transformations applied to it.
func StratifiedTransformationSamplingCriteria(numTransformations int) DepthFirstStoppingCriteria {
	return func(query CandidateQuery) bool {
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
func BiasedTransformationSamplingCriteria() DepthFirstStoppingCriteria {
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

func (e DepthFirstExplorer) Traverse(query CandidateQuery, c chan GenerationResult) {
	fmt.Println(strings.Repeat("-", len(query.Chain)) + "q")

	if e.ExplorationSamplingCriteria(query) {
		c <- GenerationResult{CandidateQuery: query}
	}

	if e.DepthFirstStoppingCriteria(query) {
		return
	}

	vars, err := Variations(query, e.ss, e.me, e.measurements, e.transformations...)
	if err != nil {
		c <- GenerationResult{error: err}
		close(c)
		return
	}

	for _, q := range vars {
		e.Traverse(q, c)
	}
}
