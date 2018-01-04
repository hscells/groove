package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"fmt"
)

type Transformation interface {
	Apply(query cqr.CommonQueryRepresentation) (queries []cqr.BooleanQuery)
}

type LogicalOperatorReplacement struct{}
type AdjacencyRange struct{}

var (
	logicalOperatorReplacement = LogicalOperatorReplacement{}
	adjacencyRange             = AdjacencyRange{}
)

func (LogicalOperatorReplacement) invert(q cqr.BooleanQuery) cqr.BooleanQuery {
	switch q.Operator {
	case "and":
		q.Operator = "or"
	case "or":
		q.Operator = "and"
	}
	return q
}

func (lor LogicalOperatorReplacement) permutations(query cqr.CommonQueryRepresentation) (queries []cqr.BooleanQuery) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		// Create the two initial seed inversions.
		var invertedQueries []cqr.BooleanQuery
		invertedQueries = append(invertedQueries, q)
		invertedQueries = append(invertedQueries, lor.invert(q))

		// If it's not an AND or OR query, just return.
		if invertedQueries[0].Operator == invertedQueries[1].Operator {
			return
		}

		// For each of the two initial queries.
		for _, queryCopy := range invertedQueries {
			// And for each of their children.
			for j, child := range queryCopy.Children {
				// Apply this transformation.
				for _, applied := range lor.permutations(child) {

					children := make([]cqr.CommonQueryRepresentation, len(queryCopy.Children))
					copy(children, queryCopy.Children)
					tmp := queryCopy
					tmp.Children = children
					tmp.Children[j] = applied
					queries = append(queries, tmp)
				}
			}
		}

		if len(queries) == 0 {
			queries = invertedQueries
		}
	}
	return
}

func (lor LogicalOperatorReplacement) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.BooleanQuery) {
	permutations := lor.permutations(query)
	subQueries := analysis.QueryBooleanQueries(query)
	for _, permutation := range permutations {
		permutationSubQueries := analysis.QueryBooleanQueries(permutation)
		numDifferent := 0
		for i := range subQueries {
			if subQueries[i].Operator != permutationSubQueries[i].Operator {
				numDifferent++
			}
		}
		if numDifferent <= 1 {
			queries = append(queries, permutation)
		}
	}
	fmt.Println(len(permutations), len(queries))
	return
}

func (AdjacencyRange) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.BooleanQuery) {
	panic("implement me")
}
