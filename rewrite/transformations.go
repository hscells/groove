package rewrite

import (
	"github.com/hscells/cqr"
	"github.com/hscells/meshexp"
	"strings"
	"strconv"
	"fmt"
	"github.com/hscells/groove/combinator"
)

// Transformer is applied to a query to generate a set of query candidates.
type Transformer interface {
	Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error)
	Applicable(query cqr.CommonQueryRepresentation) bool
	Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features
	Name() string
}

type Transformation struct {
	Transformer
}

type logicalOperatorReplacement struct {
	replacementType float64
}
type adjacencyRange struct {
	distanceChange []float64
	distance       []float64
	n              int
}
type meshExplosion struct {
	meshDepth float64
}
type fieldRestrictions struct {
	restrictionType float64
}
type adjacencyReplacement struct{}

func NewLogicalOperatorTransformer() Transformation {
	return Transformation{logicalOperatorReplacement{}}
}
func NewAdjacencyRangeTransformer() Transformation {
	return Transformation{&adjacencyRange{}}
}
func NewMeSHExplosionTransformer() Transformation {
	return Transformation{meshExplosion{}}
}
func NewFieldRestrictionsTransformer() Transformation {
	return Transformation{fieldRestrictions{}}
}
func NewAdjacencyReplacementTransformer() Transformation {
	return Transformation{adjacencyReplacement{}}
}

var (
	d, _ = meshexp.Default()
)

func variations(query cqr.CommonQueryRepresentation, context TransformationContext, transformations ...Transformer) ([]CandidateQuery, error) {
	var candidates []CandidateQuery
	switch q := query.(type) {
	case cqr.BooleanQuery: // First we look at the variations for a Boolean query (that most likely has children).
		var queries []CandidateQuery
		context = context.
			AddDepth(1).
			SetClauseType(booleanClause).
			SetChildrenCount(float64(len(q.Children)))
		// In order to generate variations for a Boolean query, we must generate all the variations for children,
		// and then overwrite the child with the particular permutation. This ensures that exactly one permutation is
		// generated and no variations combine with each other.
		// In general: for each of the query's children, generate all variations of that child, then make a copy of
		// the parent query and update the child with the generated permutation.
		for j, child := range q.Children {
			// Apply this transformation.
			perms, err := variations(child, context, transformations...)
			if err != nil {
				return nil, err
			}
			for _, applied := range perms {
				children := make([]cqr.CommonQueryRepresentation, len(q.Children))
				copy(children, q.Children)
				tmp := q
				tmp.Children = children
				tmp.Children[j] = applied.Query
				queries = append(queries, NewCandidateQuery(tmp, applied.Features))
			}
		}

		// Apply the transformations to the current Boolean query.
		for _, transformation := range transformations {
			if transformation.Applicable(q) {
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				for _, applied := range c {
					ff := ContextFeatures(context)
					ff = append(ff, transformation.Features(query, context)...)
					queries = append(queries, NewCandidateQuery(applied, ff))
				}
			}
		}

		// Finally, if there have been any duplicate variations produced, this step filters them out.
		queryMap := make(map[uint64]CandidateQuery)
		for _, permutation := range queries {
			// This is an applicable transformation.
			queryMap[combinator.HashCQR(permutation.Query)] = permutation
		}
		for k, permutation := range queryMap {
			candidates = append(candidates, permutation)
			delete(queryMap, k)
		}
	case cqr.Keyword: // In the second case for keywords there is no recursion and we only generate variations.
		context = context.
			AddDepth(1).
			SetClauseType(keywordClause).
			SetChildrenCount(0)
		candidates = append(candidates, NewCandidateQuery(q, Features{}))
		// First, apply the transformations to the current Boolean query.
		for _, transformation := range transformations {
			if transformation.Applicable(q) {
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				for _, applied := range c {
					ff := ContextFeatures(context)
					ff = append(ff, transformation.Features(query, context)...)
					candidates = append(candidates, NewCandidateQuery(applied, ff))
				}
			}
		}
	}

	return candidates, nil
}

// Variations creates query variations of the input query using the specified transformations. Permute will only generate
// query variations that modify the query in one single place. This means that no transformation is applied twice to an
// already modified query.
func Variations(query cqr.CommonQueryRepresentation, transformations ...Transformer) ([]CandidateQuery, error) {
	c, err := variations(query, TransformationContext{}, transformations...)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (r logicalOperatorReplacement) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(logicalReplacementTypeFeature, r.replacementType)}
}

func (r logicalOperatorReplacement) Apply(query cqr.CommonQueryRepresentation) (candidate []cqr.CommonQueryRepresentation, err error) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		switch q.Operator {
		case "and", "AND":
			q.Operator = "or"
			r.replacementType = 1
		case "or", "OR":
			q.Operator = "and"
			r.replacementType = 2
		}
		return []cqr.CommonQueryRepresentation{q}, nil
	}
	return
}

func (logicalOperatorReplacement) Applicable(query cqr.CommonQueryRepresentation) bool {
	return cqr.IsBoolean(query)
}

func (logicalOperatorReplacement) Name() string {
	return "LogicalOperatorReplacement"
}

func (r *adjacencyRange) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	var rangeQueries []cqr.CommonQueryRepresentation
	var changes []float64
	switch q := query.(type) {
	case cqr.BooleanQuery:
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

			r.distanceChange = append(r.distanceChange, 1)
			r.distance = append(r.distance, float64(number+1))
			if number > 1 {
				rangeQueries = append(rangeQueries, subtraction)
				changes = append(changes, float64(number-1))
				r.distance = append(r.distance, float64(number-1))
				r.distanceChange = append(r.distanceChange, -1)
			}
			rangeQueries = append(rangeQueries, addition)
			changes = append(changes, float64(number+1))
			r.n = len(changes)
		}
	}
	return rangeQueries, nil
}

func (*adjacencyRange) Applicable(query cqr.CommonQueryRepresentation) bool {
	return cqr.IsBoolean(query)
}

func (r *adjacencyRange) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	var f Features
	if r.n > 0 {
		f = append(f, NewFeature(adjacencyChangeFeature, r.distanceChange[r.n-1]), NewFeature(adjacencyDistanceFeature, r.distance[r.n-1]))
	}
	r.n--
	return f
}

func (*adjacencyRange) Name() string {
	return "AdjacencyRange"
}

func (r meshExplosion) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	var candidates []cqr.CommonQueryRepresentation
	switch q := query.(type) {
	case cqr.Keyword:
		nq := cqr.NewKeyword(q.QueryString, q.Fields...)
		for k, v := range q.Options {
			nq.Options[k] = v
		}
		for _, field := range q.Fields {
			if field == "mesh_headings" {
				if exploded, ok := q.Options["exploded"].(bool); ok {
					// Flip the explosion.
					if exploded {
						nq.SetOption("exploded", false)
					} else {
						nq.SetOption("exploded", true)
					}
					r.meshDepth = float64(d.Depth(q.QueryString))
					return []cqr.CommonQueryRepresentation{nq}, nil
				}
				return candidates, nil
			}
		}
	}
	return candidates, nil
}

func (meshExplosion) Applicable(query cqr.CommonQueryRepresentation) bool {
	return !cqr.IsBoolean(query)
}

func (r meshExplosion) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(meshDepthFeature, r.meshDepth)}
}

func (meshExplosion) Name() string {
	return "MeshExplosion"
}

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func (r fieldRestrictions) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	var candidates []cqr.CommonQueryRepresentation
	switch q := query.(type) {
	case cqr.Keyword:
		hasTitle, hasAbstract, posTitle, posAbstract := false, false, 0, 0
		for j, field := range q.Fields {
			if strings.Contains(field, "title") {
				hasTitle = true
				posTitle = j
			} else if strings.Contains(field, "text") {
				hasAbstract = true
				posAbstract = j
			} else if hasTitle && hasAbstract {
				break
			}
		}

		if hasTitle && !hasAbstract {
			q1 := cqr.CopyKeyword(q)
			q1.Fields[posTitle] = "text"
			q2 := cqr.CopyKeyword(q)
			q2.Fields = append(q2.Fields, "text")
			queries = append(queries, q1, q2)
			r.restrictionType = 1
			return
		} else if !hasTitle && hasAbstract {
			q1 := cqr.CopyKeyword(q)
			q1.Fields[posAbstract] = "title"
			q2 := cqr.CopyKeyword(q)
			q2.Fields = append(q2.Fields, "title")
			queries = append(queries, q1, q2)
			r.restrictionType = 2
			return
		} else if hasTitle && hasAbstract {
			q1 := cqr.CopyKeyword(q)
			q1.Fields = remove(q1.Fields, posTitle)
			q2 := cqr.CopyKeyword(q)
			q2.Fields = remove(q2.Fields, posAbstract)
			queries = append(queries, q1, q2)
			r.restrictionType = 3
			return
		}
	}
	return candidates, nil
}

func (fieldRestrictions) Applicable(query cqr.CommonQueryRepresentation) bool {
	return !cqr.IsBoolean(query)
}

func (r fieldRestrictions) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(restrictionTypeFeature, r.restrictionType)}
}

func (fieldRestrictions) Name() string {
	return "FieldRestrictions"
}

func (adjacencyReplacement) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		// Create the two initial seed inversions.
		var invertedQueries []cqr.BooleanQuery
		invertedQueries = append(invertedQueries, q)

		if strings.Contains(q.Operator, "adj") {
			nq := q
			nq.Operator = "and"
			return []cqr.CommonQueryRepresentation{nq}, nil
		}
	}
	return
}

func (adjacencyReplacement) Applicable(query cqr.CommonQueryRepresentation) bool {
	return cqr.IsBoolean(query)
}

func (adjacencyReplacement) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{}
}

func (adjacencyReplacement) Name() string {
	return "AdjacencyReplacement"
}
