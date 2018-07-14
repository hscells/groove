package learning

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
	"github.com/hscells/cui2vec"
	"github.com/hscells/metawrap"
	"sort"
	"time"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"github.com/go-errors/errors"
)

const (
	LogicalOperatorTransformation      = iota
	AdjacencyRangeTransformation
	MeshExplosionTransformation
	FieldRestrictionsTransformation
	AdjacencyReplacementTransformation
	ClauseRemovalTransformation
	Cui2vecExpansionTransformation
)

// Transformer is applied to a query to generate a set of query candidates.
type Transformer interface {
	Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error)
	BooleanApplicable() bool
	Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features
	Name() string
}

type BooleanTransformer interface {
	BooleanFeatures(query cqr.CommonQueryRepresentation, context TransformationContext) []Features
}

// Transformation is the implementation of a transformer.
type Transformation struct {
	ID int
	Transformer
	BooleanTransformer
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

type clauseRemoval struct{}

type cui2vecExpansion struct {
	vector  cui2vec.Vector
	mapping cui2vec.Mapping
	metamap metawrap.MetaMap
}

// NewLogicalOperatorTransformer creates a logical operator transformation.
func NewLogicalOperatorTransformer() Transformation {
	return Transformation{ID: LogicalOperatorTransformation, Transformer: logicalOperatorReplacement{}, BooleanTransformer: logicalOperatorReplacement{}}
}

// NewAdjacencyRangeTransformer creates an adjacency range transformation.
func NewAdjacencyRangeTransformer() Transformation {
	a := &adjacencyRange{}
	return Transformation{ID: AdjacencyRangeTransformation, Transformer: a, BooleanTransformer: a}
}

// NewMeSHExplosionTransformer creates a mesh explosion transformer.
func NewMeSHExplosionTransformer() Transformation {
	return Transformation{ID: MeshExplosionTransformation, Transformer: meshExplosion{}}
}

// NewFieldRestrictionsTransformer creates a field restrictions transformer.
func NewFieldRestrictionsTransformer() Transformation {
	return Transformation{ID: FieldRestrictionsTransformation, Transformer: fieldRestrictions{}}
}

// NewAdjacencyReplacementTransformer creates an adjacency replacement transformer.
func NewAdjacencyReplacementTransformer() Transformation {
	return Transformation{ID: AdjacencyReplacementTransformation, Transformer: adjacencyReplacement{}, BooleanTransformer: adjacencyReplacement{}}
}

// NewClauseRemovalTransformer creates a clause removal transformer.
func NewClauseRemovalTransformer() Transformation {
	return Transformation{ID: ClauseRemovalTransformation, Transformer: clauseRemoval{}, BooleanTransformer: clauseRemoval{}}
}

// NewClauseRemovalTransformer creates a clause removal transformer.
func Newcui2vecExpansionTransformer(vector cui2vec.Vector, mapping cui2vec.Mapping, metamap metawrap.MetaMap) Transformation {
	return Transformation{
		ID: Cui2vecExpansionTransformation,
		Transformer: cui2vecExpansion{
			vector:  vector,
			mapping: mapping,
			metamap: metamap,
		},
	}
}

var (
	d, _ = meshexp.Default()
)

// variations creates the variations of an input candidate query in the transformation chain using the specified
// transformations.
func variations(query CandidateQuery, context TransformationContext, ss stats.StatisticsSource, me analysis.MeasurementExecutor, measurements []analysis.Measurement, transformations ...Transformation) ([]CandidateQuery, error) {
	var candidates []CandidateQuery

	// Compute features (and pre-transformation features) for the original Boolean query.
	preDeltas, err := deltas(query.Query, ss, measurements, me)
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
			perms, err := variations(NewCandidateQuery(child, nil).SetTransformationID(query.TransformationID), context, ss, me, measurements, transformations...)
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

					deltas, err := deltas(tmp, ss, measurements, me)
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
			if transformation.BooleanApplicable() {
				query.TransformationID = transformation.ID
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				boolFeatures := transformation.BooleanFeatures(q, context)

				for i, applied := range c {
					features := contextFeatures(context)
					features = append(features, booleanFeatures(applied.(cqr.BooleanQuery))...)
					features = append(features, transformationFeature(transformation.Transformer))
					if i < len(boolFeatures) {
						features = append(features, boolFeatures[i]...)
					}

					deltas, err := deltas(applied, ss, measurements, me)
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
		candidates = append(candidates, NewCandidateQuery(q, preFeatures).SetTransformationID(query.TransformationID).Append(query))

		// Next, apply the transformations to the current query.
		for _, transformation := range transformations {
			if !transformation.BooleanApplicable() {
				query.TransformationID = transformation.ID
				c, err := transformation.Apply(q)
				if err != nil {
					return nil, err
				}

				for _, applied := range c {
					features := contextFeatures(context)
					deltas, err := deltas(applied, ss, measurements, me)
					if err != nil {
						return nil, err
					}

					for feature, score := range deltas {
						features = append(features, NewFeature(feature, score))
					}

					features = append(features, computeDeltas(preDeltas, deltas)...)
					switch appliedQuery := applied.(type) {
					case cqr.Keyword:
						features = append(features, keywordFeatures(appliedQuery)...)
					case cqr.BooleanQuery:
						features = append(features, booleanFeatures(appliedQuery)...)
					}
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
func Variations(query CandidateQuery, ss stats.StatisticsSource, me analysis.MeasurementExecutor, measurements []analysis.Measurement, transformations ...Transformation) ([]CandidateQuery, error) {
	var vars []CandidateQuery
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, transformation := range transformations {
		wg.Add(1)
		go func(t Transformation) {
			defer wg.Done()
		v:
			c, err := variations(query, TransformationContext{}, ss, me, measurements, t)
			if elastic.IsConnErr(err) || elastic.IsTimeout(err) {
				log.Println("Elasticsearch error -- retrying...")
				time.Sleep(5 * time.Second)
				goto v
			}
			if err != nil {
				if strings.Contains(err.Error(), "can't assign requested address") {
					log.Println("can't connect -- retrying...")
					//errors.Wrap(err, "error")
					fmt.Println(err.(*errors.Error).ErrorStack())
					time.Sleep(5 * time.Second)
					goto v
				}
				log.Println(err)
			}
			mu.Lock()
			vars = append(vars, c...)
			mu.Unlock()
		}(transformation)
	}

	wg.Wait()

	return vars, nil
}

func (r logicalOperatorReplacement) BooleanFeatures(query cqr.CommonQueryRepresentation, context TransformationContext) []Features {
	return []Features{r.Features(query, context)}
}

func (r logicalOperatorReplacement) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(LogicalReplacementTypeFeature, r.replacementType)}
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

func (logicalOperatorReplacement) BooleanApplicable() bool {
	return true
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

func (*adjacencyRange) BooleanApplicable() bool {
	return true
}

func (r *adjacencyRange) BooleanFeatures(query cqr.CommonQueryRepresentation, context TransformationContext) []Features {
	if r.n == 0 {
		return []Features{}
	}
	f := make([]Features, r.n)
	for i := r.n - 1; i >= 0; i-- {
		f[i] = Features{
			NewFeature(AdjacencyReplacementFeature, r.distanceChange[i]),
			NewFeature(AdjacencyDistanceFeature, r.distance[i]),
		}
	}
	r.n = 0
	return f
}

func (r *adjacencyRange) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return nil
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

func (meshExplosion) BooleanApplicable() bool {
	return false
}

func (r meshExplosion) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(MeshDepthFeature, r.meshDepth)}
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

func (fieldRestrictions) BooleanApplicable() bool {
	return false
}

func (r fieldRestrictions) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{NewFeature(RestrictionTypeFeature, r.restrictionType)}
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

func (adjacencyReplacement) BooleanApplicable() bool {
	return true
}

func (adjacencyReplacement) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return Features{}
}

func (adjacencyReplacement) BooleanFeatures(query cqr.CommonQueryRepresentation, context TransformationContext) []Features {
	return []Features{{NewFeature(AdjacencyReplacementFeature, 1)}}
}

func (adjacencyReplacement) Name() string {
	return "AdjacencyReplacement"
}

func (clauseRemoval) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		removed := make([]cqr.CommonQueryRepresentation, len(q.Children))
		for i := range q.Children {
			copied := make([]cqr.CommonQueryRepresentation, len(q.Children))
			copy(copied, q.Children)
			copied = append(copied[:i], copied[i+1:]...)
			removed[i] = cqr.NewBooleanQuery(q.Operator, copied)
		}
		return removed, nil
	}
	return []cqr.CommonQueryRepresentation{}, nil
}

func (clauseRemoval) BooleanApplicable() bool {
	return true
}

func (clauseRemoval) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	return nil
}

func (clauseRemoval) BooleanFeatures(query cqr.CommonQueryRepresentation, context TransformationContext) []Features {
	switch q := query.(type) {
	case cqr.BooleanQuery:
		features := make([]Features, len(q.Children))
		for i, child := range q.Children {
			switch c := child.(type) {
			case cqr.BooleanQuery:
				features[i] = booleanFeatures(c)
			case cqr.Keyword:
				features[i] = keywordFeatures(c)
			}
			features[i] = append(features[i], NewFeature(ClauseRemovalFeature, 1))
		}
		return features
	}
	return []Features{{NewFeature(ClauseRemovalFeature, 1)}}
}

func (clauseRemoval) Name() string {
	return "KeywordRemoval"
}

func (c cui2vecExpansion) Apply(query cqr.CommonQueryRepresentation) (queries []cqr.CommonQueryRepresentation, err error) {
	switch q := query.(type) {
	case cqr.Keyword:
		// Use MetaMap to get the preferred cui.
		candidates, err := c.metamap.PreferredCandidates(q.QueryString)
		if err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			return []cqr.CommonQueryRepresentation{}, nil
		}

		// Next, parse and sort the candidates from MetaMap.
		type concept struct {
			cui   string
			score int64
		}
		cuis := make([]concept, len(candidates))
		for i, candidate := range candidates {
			// We need to parse the score from MetaMap.
			score, err := strconv.ParseInt(candidate.CandidateScore, 10, 64)
			if err != nil {
				return nil, err
			}

			// Scores from MetaMap come out as negative and we want to negate that.
			score = -score

			cuis[i] = concept{cui: candidate.CandidateCUI, score: score}
		}
		sort.Slice(cuis, func(i, j int) bool {
			return cuis[i].score < cuis[j].score
		})

		// Now we know the target cui that is most associated with the input term.
		target := cuis[0].cui

		// We can get cuis that are similar to each other from cui2vec.
		similar, err := c.vector.Similar(target)
		if err != nil {
			return nil, err
		}

		if len(similar) == 0 {
			return []cqr.CommonQueryRepresentation{}, nil
		}

		// Only continue to expand keywords until the score is less than a tenth of a percent (0.001) similar.
		// Or to a maximum of 5 expansions.
		maxScore := similar[0].Value
		var expansions []string
		for _, cui := range similar {
			if maxScore-cui.Value > 0.001 {
				break
			}

			if term, ok := c.mapping[cui.CUI]; ok {
				expansions = append(expansions, term)
			}

			if len(expansions) >= 5 {
				break
			}
		}

		// Now create keywords from the expanded terms.
		children := make([]cqr.CommonQueryRepresentation, len(expansions)+1)
		for i, expansion := range expansions {
			children[i] = cqr.NewKeyword(expansion, q.Fields...)
		}
		children[len(children)-1] = q

		return []cqr.CommonQueryRepresentation{cqr.NewBooleanQuery("or", children)}, nil
	}
	return nil, nil
}

func (cui2vecExpansion) BooleanApplicable() bool {
	return false
}

func (cui2vecExpansion) Features(query cqr.CommonQueryRepresentation, context TransformationContext) Features {
	var features Features
	switch q := query.(type) {
	case cqr.BooleanQuery:
		features = booleanFeatures(q)
	case cqr.Keyword:
		features = keywordFeatures(q)
	}
	terms := analysis.QueryTerms(query)
	return append(features, NewFeature(Cui2vecExpansionFeature, 1), NewFeature(Cui2vecNumExpansionsFeature, float64(len(terms))))
}

func (cui2vecExpansion) Name() string {
	return "cui2vecExpansion"
}