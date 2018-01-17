package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"strings"
	"strconv"
	"fmt"
	"github.com/hscells/meshexp"
)

type Transformation interface {
	Apply(query cqr.CommonQueryRepresentation) (queries []CandidateQuery, err error)
}

type logicalOperatorReplacement struct{}
type adjacencyRange struct{}
type meshExplosion struct{}
type fieldRestrictions struct{}

var (
	LogicalOperatorReplacement = logicalOperatorReplacement{}
	AdjacencyRange             = adjacencyRange{}
	MeSHExplosion              = meshExplosion{}
	FieldRestrictions          = fieldRestrictions{}
)

// invert switches logical operators for a Boolean query.
func (logicalOperatorReplacement) invert(q cqr.BooleanQuery) cqr.BooleanQuery {
	switch q.Operator {
	case "and":
		q.Operator = "or"
	case "or":
		q.Operator = "and"
	}
	return q
}

// permutations generates all possible permutations of the logical operators.
func (lor logicalOperatorReplacement) permutations(query cqr.CommonQueryRepresentation, depth float64) (queries []CandidateQuery) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		// Create the two initial seed inversions.
		var invertedQueries []cqr.BooleanQuery
		invertedQueries = append(invertedQueries, q)
		invertedQueries = append(invertedQueries, lor.invert(q))

		// For each of the two initial queries.
		for _, queryCopy := range invertedQueries {
			// And for each of their children.
			for j, child := range queryCopy.Children {
				// Apply this transformation.
				for _, applied := range lor.permutations(child, depth+1) {
					children := make([]cqr.CommonQueryRepresentation, len(queryCopy.Children))
					copy(children, queryCopy.Children)
					tmp := queryCopy
					tmp.Children = children
					tmp.Children[j] = applied.Query
					queries = append(queries, NewCandidateQuery(tmp, applied.FeatureFamily))
				}
			}
		}
		for _, iq := range invertedQueries {
			var replacement float64
			if q.Operator == iq.Operator {
				replacement = 0
			} else if q.Operator == "and" && iq.Operator == "or" {
				replacement = 1
			} else {
				replacement = 2
			}

			ff := FeatureFamily{NewFeature16(0x0, 0x0, replacement), NewFeature16(0x0, 0x1, depth)}
			queries = append(queries, NewCandidateQuery(iq, ff))
		}
	}
	return
}

func (lor logicalOperatorReplacement) Apply(query cqr.CommonQueryRepresentation) (queries []CandidateQuery, err error) {
	// Generate permutations.
	permutations := lor.permutations(query, 0.0)
	// Get all the sub-queries for the original query.
	subQueries := analysis.QueryBooleanQueries(query)

	queryMap := map[string]CandidateQuery{}

	for _, permutation := range permutations {
		// Get the sub-queries for each permutation.
		permutationSubQueries := analysis.QueryBooleanQueries(permutation.Query)

		// The "key" to the permutation.
		path := ""

		// Count the differences between operators for each query.
		numDifferent := 0
		for i := range subQueries {
			path += permutationSubQueries[i].Operator
			if subQueries[i].Operator != permutationSubQueries[i].Operator {
				numDifferent++
			}
		}

		// This is an applicable transformation.
		if numDifferent <= 2 {
			queryMap[path] = permutation
		}
	}

	for _, permutation := range queryMap {
		queries = append(queries, permutation)
	}

	return
}

func (ar adjacencyRange) permutations(query cqr.CommonQueryRepresentation, depth float64) (queries []CandidateQuery, err error) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		var rangeQueries []cqr.BooleanQuery
		var changes []float64
		if strings.Contains(q.Operator, "adj") {

			addition := q
			subtraction := q

			operator := q.Operator
			var number int
			if len(operator) == 3 {
				number = 1
			} else {
				number, err = strconv.Atoi(operator[3:])
				if err != nil {
					return nil, err
				}
			}

			addition.Operator = fmt.Sprintf("adj%v", number+1)
			subtraction.Operator = fmt.Sprintf("adj%v", number-1)

			rangeQueries = append(rangeQueries, addition)
			changes = append(changes, float64(number+1))
			if number > 1 {
				rangeQueries = append(rangeQueries, subtraction)
				changes = append(changes, float64(number-1))
			}
		} else {
			rangeQueries = append(rangeQueries, q)
			changes = append(changes, 0)
		}
		// For each of the two initial queries.
		for _, queryCopy := range rangeQueries {
			// And for each of their children.
			for j, child := range queryCopy.Children {
				// Apply this transformation.
				appliedQueries, err := ar.permutations(child, depth+1)
				if err != nil {
					return nil, err
				}
				for _, applied := range appliedQueries {
					children := make([]cqr.CommonQueryRepresentation, len(queryCopy.Children))
					copy(children, queryCopy.Children)
					tmp := queryCopy
					tmp.Children = children
					tmp.Children[j] = applied.Query
					queries = append(queries, NewCandidateQuery(tmp, applied.FeatureFamily))
				}
			}
		}

		for i, rq := range rangeQueries {
			ff := FeatureFamily{NewFeature16(0x10, 0x0, changes[i]), NewFeature16(0x10, 0x1, depth)}
			queries = append(queries, NewCandidateQuery(rq, ff))
		}
	}
	return
}

func (ar adjacencyRange) Apply(query cqr.CommonQueryRepresentation) (queries []CandidateQuery, err error) {
	// Generate permutations.
	permutations, err := ar.permutations(query, 0.0)
	if err != nil {
		return nil, err
	}
	// Get all the sub-queries for the original query.
	subQueries := analysis.QueryBooleanQueries(query)

	queryMap := map[string]CandidateQuery{}

	for _, permutation := range permutations {
		// Get the sub-queries for each permutation.
		permutationSubQueries := analysis.QueryBooleanQueries(permutation.Query)

		// The "key" to the permutation.
		path := ""

		// Count the differences between operators for each query.
		numDifferent := 0
		for i := range subQueries {
			path += permutationSubQueries[i].Operator
			if subQueries[i].Operator != permutationSubQueries[i].Operator {
				numDifferent++
			}
		}

		// This is an applicable transformation.
		if numDifferent <= 2 && numDifferent > 0 {
			queryMap[path] = permutation
		}
	}

	for _, permutation := range queryMap {
		queries = append(queries, permutation)
	}

	return
}

// permutations generates all possible permutations of the logical operators.
func (m meshExplosion) permutations(query cqr.CommonQueryRepresentation, depth float64) (queries []CandidateQuery) {
	d, _ := meshexp.Default()
	//fmt.Println(query)
	switch q := query.(type) {
	case cqr.BooleanQuery:
		// Create the two initial seed inversions.
		for i, child := range q.Children {
			switch c := child.(type) {
			case cqr.Keyword:
				for _, field := range c.Fields {
					if field == "mesh_headings" {
						if exploded, ok := c.Options["exploded"].(bool); ok {
							// Copy the outer parents children.
							children := make([]cqr.CommonQueryRepresentation, len(q.Children))
							copy(children, q.Children)

							// Make a copy of the options.
							options := make(map[string]interface{})
							for k, v := range q.Children[i].(cqr.Keyword).Options {
								options[k] = v
							}

							// Make a complete copy of the query and the children.
							tmp := q
							tmp.Children = children

							// Flip the explosion.
							if exploded {
								options["exploded"] = false
							} else {
								options["exploded"] = true
							}

							// Copy the new options map the query copy.
							switch ch := tmp.Children[i].(type) {
							case cqr.Keyword:
								ch.Options = options
								tmp.Children[i] = ch
							}

							// Append it.
							ff := FeatureFamily{NewFeature16(0x20, 0x0, float64(d.Depth(c.QueryString))), NewFeature16(0x20, 0x1, depth)}
							queries = append(queries, NewCandidateQuery(tmp, ff))
						}
						break
					}
				}
			case cqr.BooleanQuery:
				for j, child := range q.Children {
					// Apply this transformation.
					for _, applied := range m.permutations(child, depth+1) {
						children := make([]cqr.CommonQueryRepresentation, len(q.Children))
						copy(children, q.Children)
						tmp := q
						tmp.Children = children
						tmp.Children[j] = applied.Query
						queries = append(queries, NewCandidateQuery(tmp, applied.FeatureFamily))
					}
				}
			}
		}
	}
	return
}

func (m meshExplosion) Apply(query cqr.CommonQueryRepresentation) (queries []CandidateQuery, err error) {
	// Generate permutations.
	permutations := m.permutations(query, 0.0)

	// Get all the sub-queries for the original query.
	subQueries := analysis.QueryKeywords(query)

	queryMap := map[string]CandidateQuery{}

	for _, permutation := range permutations {
		// Get the sub-queries for each permutation.
		permutationSubQueries := analysis.QueryKeywords(permutation.Query)

		// The "key" to the permutation.
		path := ""

		// Count the differences between operators for each query.
		for i := range subQueries {
			path += fmt.Sprintf("%v%v", permutationSubQueries[i].QueryString, permutationSubQueries[i].Options["exploded"].(bool))
		}

		queryMap[path] = permutation
	}

	for _, permutation := range queryMap {
		queries = append(queries, permutation)
	}

	return
}

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func (fr fieldRestrictions) restrictionType(fields []string) float64 {
	if len(fields) == 1 {

		if fields[0] == "text" {
			return 1.0
		} else if fields[0] == "title" {
			return 2.0
		}
	} else if len(fields) == 2 {
		if (fields[0] == "text" && fields[1] == "title") || (fields[0] == "title" && fields[1] == "text") {
			return 3.0
		}
	}
	return 0.0
}

// permutations generates all possible permutations of the logical operators.
func (fr fieldRestrictions) permutations(query cqr.CommonQueryRepresentation, depth float64) (queries []CandidateQuery) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		// Create the two initial seed inversions.
		for i, child := range q.Children {
			var (
				f1 []string
				f2 []string
			)
			switch c := child.(type) {
			case cqr.Keyword:
				hasTitle, hasAbstract, posTitle, posAbstract := false, false, 0, 0
				for j, field := range c.Fields {
					if field == "title" {
						hasTitle = true
						posTitle = j
					} else if field == "text" {
						hasAbstract = true
						posAbstract = j
					}
				}

				if hasTitle && !hasAbstract {
					// Copy the outer parents children.
					children1 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children1, q.Children)
					children2 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children2, q.Children)

					// Make a complete copy of the query and the children.
					tmp1 := q
					tmp1.Children = children1

					// Copy the new fields to the query copy.
					switch ch := tmp1.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields[posTitle] = "text"
						ch.Fields = fields
						tmp1.Children[i] = ch
						f1 = fields
					}

					tmp2 := q
					tmp2.Children = children2

					// Copy the new fields to the query copy.
					switch ch := tmp2.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields = append(fields, "text")
						ch.Fields = fields
						tmp2.Children[i] = ch
						f2 = fields
					}

					// Append it.
					queries = append(queries, NewCandidateQuery(tmp1, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f1)), NewFeature16(0x30, 0x1, depth)}))
					queries = append(queries, NewCandidateQuery(tmp2, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f2)), NewFeature16(0x30, 0x1, depth)}))
				} else if !hasTitle && hasAbstract {
					// Copy the outer parents children.
					children1 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children1, q.Children)
					children2 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children2, q.Children)

					// Make a complete copy of the query and the children.
					tmp1 := q
					tmp1.Children = children1

					// Copy the new fields to the query copy.
					switch ch := tmp1.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields[posAbstract] = "title"
						tmp1.Children[i] = ch
						f1 = fields
					}

					tmp2 := q
					tmp2.Children = children2

					// Copy the new fields to the query copy.
					switch ch := tmp2.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields = append(fields, "title")
						ch.Fields = fields
						tmp2.Children[i] = ch
						f2 = fields
					}

					// Append it.
					queries = append(queries, NewCandidateQuery(tmp1, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f1)), NewFeature16(0x30, 0x1, depth)}))
					queries = append(queries, NewCandidateQuery(tmp2, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f2)), NewFeature16(0x30, 0x1, depth)}))
				} else if hasTitle && hasAbstract {
					// Copy the outer parents children.
					children1 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children1, q.Children)
					children2 := make([]cqr.CommonQueryRepresentation, len(q.Children))
					copy(children2, q.Children)

					// Make a complete copy of the query and the children.
					tmp1 := q
					tmp1.Children = children1

					tmp2 := q
					tmp2.Children = children2

					// Copy the new fields to the query copy.
					switch ch := tmp1.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields = remove(fields, posTitle)
						ch.Fields = fields
						tmp1.Children[i] = ch
						f1 = fields
					}

					// Copy the new fields to the query copy.
					switch ch := tmp2.Children[i].(type) {
					case cqr.Keyword:
						fields := make([]string, len(ch.Fields))
						copy(fields, ch.Fields)
						fields = remove(fields, posAbstract)
						ch.Fields = fields
						tmp2.Children[i] = ch
						f2 = fields
					}

					// Append it.
					queries = append(queries, NewCandidateQuery(tmp1, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f1)), NewFeature16(0x30, 0x1, depth)}))
					queries = append(queries, NewCandidateQuery(tmp2, FeatureFamily{NewFeature16(0x30, 0x0, fr.restrictionType(f2)), NewFeature16(0x30, 0x1, depth)}))
				}

			case cqr.BooleanQuery:
				for j, child := range q.Children {
					// Apply this transformation.
					for _, applied := range fr.permutations(child, depth+1) {
						children := make([]cqr.CommonQueryRepresentation, len(q.Children))
						copy(children, q.Children)
						tmp := q
						tmp.Children = children
						tmp.Children[j] = applied.Query
						queries = append(queries, NewCandidateQuery(tmp, applied.FeatureFamily))
					}
				}
			}
		}
	}
	return
}

func (fr fieldRestrictions) Apply(query cqr.CommonQueryRepresentation) (queries []CandidateQuery, err error) {
	// Generate permutations.
	permutations := fr.permutations(query, 0.0)

	// Get all the sub-queries for the original query.
	subQueries := analysis.QueryKeywords(query)

	queryMap := map[string]cqr.CommonQueryRepresentation{}

	ff := FeatureFamily{}

	for _, permutation := range permutations {
		// Get the sub-queries for each permutation.
		permutationSubQueries := analysis.QueryKeywords(permutation.Query)

		// The "key" to the permutation.
		path := ""

		// Count the differences between operators for each query.
		for i := range subQueries {
			path += fmt.Sprintf("%v%v", permutationSubQueries[i].QueryString, permutationSubQueries[i].Fields)
		}

		queryMap[path] = permutation.Query
	}

	for _, permutation := range queryMap {
		queries = append(queries, NewCandidateQuery(permutation, ff))
	}

	return
}
