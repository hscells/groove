package rewrite

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/stats"
	"github.com/hscells/meshexp"
	"strconv"
	"strings"
	"sync"
	"gopkg.in/olivere/elastic.v5"
)

const (
	logicalOperatorTransformation      = iota
	adjacencyRangeTransformation
	meshExplosionTransformation
	fieldRestrictionsTransformation
	adjacencyReplacementTransformation
)

// Transformer is applied to a query to generate a set of query candidates.
type Transformer interface {
	Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error)
	Applicable(query cqr.CommonQueryRepresentation) bool
	Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features
	Name() string
}

// Transformation is the implementation of a transformer.
type Transformation struct {
	ID int
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

// NewLogicalOperatorTransformer creates a logical operator transformation.
func NewLogicalOperatorTransformer() Transformation {
	return Transformation{logicalOperatorTransformation, logicalOperatorReplacement{}}
}

// NewAdjacencyRangeTransformer creates an adjacency range transformation.
func NewAdjacencyRangeTransformer() Transformation {
	return Transformation{adjacencyRangeTransformation, &adjacencyRange{}}
}

// NewMeSHExplosionTransformer creates a mesh explosion transformer.
func NewMeSHExplosionTransformer() Transformation {
	return Transformation{meshExplosionTransformation, meshExplosion{}}
}

// NewFieldRestrictionsTransformer creates a field restrictions transformer.
func NewFieldRestrictionsTransformer() Transformation {
	return Transformation{fieldRestrictionsTransformation, fieldRestrictions{}}
}

// NewAdjacencyReplacementTransformer creates an adjacency replacement transformer.
func NewAdjacencyReplacementTransformer() Transformation {
	return Transformation{adjacencyReplacementTransformation, adjacencyReplacement{}}
}

var (
	d, _ = meshexp.Default()
)

// variations creates the variations of an input candidate query in the transformation chain using the specified
// transformations.
func variations(query CandidateQuery, context TransformationContext, ss stats.StatisticsSource, me analysis.MeasurementExecutor, transformations ...Transformation) ([]CandidateQuery, error) {
	var candidates []CandidateQuery

	// Compute features (and pre-transformation features) for the original Boolean query.
	preDeltas, err := deltas(query.Query, ss, me)
	if err != nil {
		return nil, err
	}
	preFeatures := contextFeatures(context)
	for feature, score := range preDeltas {
		preFeatures = append(preFeatures, NewFeature(feature, score))
	}

	switch q := query.Query.(type) {
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
			perms, err := variations(NewCandidateQuery(child, nil), context, ss, me, transformations...)
			if err != nil {
				return nil, err
			}
			for _, applied := range perms {
				children := make([]cqr.CommonQueryRepresentation, len(q.Children))
				copy(children, q.Children)
				tmp := q
				tmp.Children = children
				tmp.Children[j] = applied.Query

				features := applied.Features
				if len(applied.Features) == 0 {
					// Context features.
					features = contextFeatures(context)

					// Optional keyword features.
					switch applied.Query.(type) {
					case cqr.Keyword:
						features = append(features, keywordFeatures(applied.Query.(cqr.Keyword))...)
					}

					// Boolean features.
					features = append(features, booleanFeatures(tmp)...)

					// Features about the entire Boolean query.
					foundTotal := false
					for _, feature := range features {
						if feature.ID == totalFieldsFeature {
							foundTotal = true
							break
						}
					}
					if !foundTotal {
						features = append(features,
							NewFeature(totalFieldsFeature, float64(len(analysis.QueryFields(tmp)))),
							NewFeature(totalKeywordsFeature, float64(len(analysis.QueryKeywords(tmp)))),
							NewFeature(totalTermsFeature, float64(len(analysis.QueryTerms(tmp)))),
							NewFeature(totalExplodedFeature, float64(len(analysis.ExplodedKeywords(tmp)))),
							NewFeature(totalTruncatedFeature, float64(len(analysis.TruncatedKeywords(tmp)))),
							NewFeature(totalClausesFeature, float64(len(analysis.QueryBooleanQueries(tmp)))))
					}

					deltas, err := deltas(tmp, ss, me)
					if err != nil {
						return nil, err
					}

					for feature, score := range deltas {
						features = append(features, NewFeature(feature, score))
					}
					features = append(features, computeDeltas(preDeltas, deltas)...)
				}

				queries = append(queries, NewCandidateQuery(tmp, features).SetTransformationID(applied.TransformationID).Append(query))
			}
		}

		// Apply the transformations to the current Boolean query.
		for _, transformation := range transformations {
			if transformation.Applicable(q) {
				query.TransformationID = transformation.ID
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				for _, applied := range c {
					features := contextFeatures(context)
					features = append(features, booleanFeatures(applied.(cqr.BooleanQuery))...)
					features = append(features, transformation.Features(applied, context)...)
					features = append(features, transformationFeature(transformation.Transformer))
					// Features about the entire Boolean query.
					foundTotal := false
					for _, feature := range features {
						if feature.ID == totalFieldsFeature {
							foundTotal = true
							break
						}
					}
					if !foundTotal {
						features = append(features,
							NewFeature(totalFieldsFeature, float64(len(analysis.QueryFields(q)))),
							NewFeature(totalKeywordsFeature, float64(len(analysis.QueryKeywords(q)))),
							NewFeature(totalTermsFeature, float64(len(analysis.QueryTerms(q)))),
							NewFeature(totalExplodedFeature, float64(len(analysis.ExplodedKeywords(q)))),
							NewFeature(totalTruncatedFeature, float64(len(analysis.TruncatedKeywords(q)))),
							NewFeature(totalClausesFeature, float64(len(analysis.QueryBooleanQueries(q)))))
					}

					deltas, err := deltas(applied, ss, me)
					if err != nil {
						return nil, err
					}

					for feature, score := range deltas {
						features = append(features, NewFeature(feature, score))
					}
					features = append(features, computeDeltas(preDeltas, deltas)...)

					queries = append(queries, NewCandidateQuery(applied, features).SetTransformationID(transformation.ID).Append(query))
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
		// Populate the context.
		context = context.
			AddDepth(1).
			SetClauseType(keywordClause).
			SetChildrenCount(0)

		// Add the original query to the list of candidates.
		candidates = append(candidates, NewCandidateQuery(q, preFeatures).Append(query))

		// Next, apply the transformations to the current query.
		for _, transformation := range transformations {
			if transformation.Applicable(q) {
				query.TransformationID = transformation.ID
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				for _, applied := range c {
					features := contextFeatures(context)
					deltas, err := deltas(applied, ss, me)
					if err != nil {
						return nil, err
					}

					for feature, score := range deltas {
						features = append(features, NewFeature(feature, score))
					}

					features = append(features, computeDeltas(preDeltas, deltas)...)
					features = append(features, keywordFeatures(applied.(cqr.Keyword))...)
					features = append(features, transformation.Features(applied, context)...)
					features = append(features, transformationFeature(transformation.Transformer))
					candidates = append(candidates, NewCandidateQuery(applied, features).SetTransformationID(transformation.ID).Append(query))
				}
			}
		}
	}

	return candidates, nil
}

// Variations creates query variations of the input query using the specified transformations. Permute will only generate
// query variations that modify the query in one single place. This means that no transformation is applied twice to an
// already modified query.
func Variations(query CandidateQuery, ss stats.StatisticsSource, me analysis.MeasurementExecutor, transformations ...Transformation) ([]CandidateQuery, error) {
	var vars []CandidateQuery
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, transformation := range transformations {
		wg.Add(1)
		go func(t Transformation) {
			defer wg.Done()
		vars:
			c, err := variations(query, TransformationContext{}, ss, me, t)
			if err != nil {
				if elastic.IsConnErr(err) {
					fmt.Println(err, "...retrying...")
					goto vars
				}
				panic(err)
			}
			mu.Lock()
			vars = append(vars, c...)
			mu.Unlock()
		}(transformation)
	}

	wg.Wait()

	return vars, nil
}

func (r logicalOperatorReplacement) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(logicalReplacementTypeFeature, r.replacementType)}
}

func (r logicalOperatorReplacement) Apply(query cqr.CommonQueryRepresentation) (candidate []cqr.CommonQueryRepresentation, err error) {
	r.replacementType = 0
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

	r.distance = []float64{}
	r.distanceChange = []float64{}

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

		r.restrictionType = 0

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
