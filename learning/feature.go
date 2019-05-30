package learning

import (
	"bufio"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/analysis/preqpp"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/xtgo/set"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Feature is some value that is applicable to a query transformation.
type Feature struct {
	ID    int
	Score float64
}

// Set sets the `Score` of the feature.
func (f Feature) Set(score float64) Feature {
	f.Score = score
	return f
}

type deltaFeatures map[int]float64

const (
	// Context Features.
	nilFeature = iota
	DepthFeature
	ClauseTypeFeature  // This isn't the operator type, it's the type of the clause (keyword query/Boolean query).
	ChildrenCountFeature

	// Transformation-based Features.
	TransformationTypeFeature
	LogicalReplacementTypeFeature
	AdjacencyReplacementFeature
	AdjacencyDistanceFeature
	MeshDepthFeature
	MeshParentFeature
	RestrictionTypeFeature
	ClauseRemovalFeature
	Cui2vecExpansionFeature
	Cui2vecNumExpansionsFeature

	// Keyword specific Features.
	IsExplodedFeature
	IsTruncatedFeature
	NumFieldsFeature

	// Boolean specific Features.
	OperatorTypeFeature

	// Measurement Features.
	measurementFeatures

	// Protocol Query Type (when generated automatically from a protocol).
	ProtocolQueryTypeFeature

	// Chain of transformations !!THIS MUST BE THE LAST FEATURE IN THE LIST!!
	chainFeatures
)

// MeasurementFeatureKeys contains a mapping of applicable measurement to a feature.
var MeasurementFeatureKeys = map[string]int{
	analysis.BooleanFields.Name():           measurementFeatures,
	analysis.BooleanClauses.Name():          measurementFeatures + 1,
	analysis.BooleanKeywords.Name():         measurementFeatures + 2,
	analysis.BooleanTruncated.Name():        measurementFeatures + 3,
	preqpp.RetrievalSize.Name():             measurementFeatures + 4,
	preqpp.QueryScope.Name():                measurementFeatures + 5,
	analysis.MeshKeywordCount.Name():        measurementFeatures + 6,
	analysis.MeshExplodedCount.Name():       measurementFeatures + 7,
	analysis.MeshNonExplodedCount.Name():    measurementFeatures + 8,
	analysis.MeshAvgDepth.Name():            measurementFeatures + 9,
	analysis.MeshMaxDepth.Name():            measurementFeatures + 10,
	analysis.BooleanNonAtomicClauses.Name(): measurementFeatures + 11,
	analysis.BooleanAndCount.Name():         measurementFeatures + 12,
	analysis.BooleanOrCount.Name():          measurementFeatures + 13,
	analysis.BooleanNotCount.Name():         measurementFeatures + 14,
	analysis.BooleanFieldsTitle.Name():      measurementFeatures + 15,
	analysis.BooleanFieldsAbstract.Name():   measurementFeatures + 16,
	analysis.BooleanFieldsMeSH.Name():       measurementFeatures + 17,
	analysis.BooleanFieldsOther.Name():      measurementFeatures + 18,
	analysis.TermCount.Name():               measurementFeatures + 19,
}

// Chain of transformations !!THIS MUST BE THE LAST FEATURE IN THE LIST!!
var ChainFeatures = chainFeatures + len(MeasurementFeatureKeys)*2

// NewFeature creates a new feature with the specified ID and `Score`.
func NewFeature(id int, score float64) Feature {
	return Feature{id, score}
}

// Features is the group of Features used to learn or predict a Score.
type Features []Feature

func (ff Features) Len() int           { return len(ff) }
func (ff Features) Swap(i, j int)      { ff[i], ff[j] = ff[j], ff[i] }
func (ff Features) Less(i, j int) bool { return ff[i].ID < ff[j].ID }

func (ff Features) Scores(max int) []float64 {
	v := make([]float64, max)
	for _, f := range ff {
		if f.ID >= len(v) {
			//fmt.Printf("[!] %d is larger than feature size %d\n", f.ID, len(v))
			continue
		}
		v[f.ID] = f.Score
	}
	return v
}

// LearntFeature contains the Features that were used to produce a particular Score.
type LearntFeature struct {
	Features
	Scores  []float64
	Topic   string
	Comment string
}

// CandidateQuery is a possible transformation a query can take.
type CandidateQuery struct {
	TransformationID int
	Topic            string
	Query            cqr.CommonQueryRepresentation
	Chain            []CandidateQuery
	Features
}

func LoadFeatures(reader io.Reader) ([]LearntFeature, error) {
	var lfs []LearntFeature
	s := bufio.NewScanner(reader)
	for s.Scan() {
		var (
			comment  string
			topic    string
			features Features
			scores   []float64

			rest string
		)
		l := s.Text()

		// {line} # [comment]
		a := strings.Split(l, "#")
		if len(a) == 2 {
			comment = a[1]
			rest = a[0]
		} else {
			rest = l
		}

		// [Score] qid:[topic] {features}
		b := strings.Split(strings.TrimSpace(rest), " ")
		s, err := strconv.ParseFloat(strings.TrimSpace(b[0]), 64)
		if err != nil {
			return nil, err
		}
		scores = []float64{s}

		topic = strings.Split(b[1], ":")[1]

		features = make(Features, len(b[2:]))
		for i, v := range b[2:] {
			f := strings.Split(v, ":")
			id, err := strconv.Atoi(f[0])
			if err != nil {
				return nil, err
			}
			score, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				return nil, err
			}
			features[i] = Feature{
				ID:    id,
				Score: score,
			}
		}

		lfs = append(lfs, LearntFeature{
			Topic:    topic,
			Comment:  comment,
			Features: features,
			Scores:   scores,
		})
	}
	return lfs, nil
}

func LoadReinforcementFeatures(reader io.Reader) ([]LearntFeature, error) {
	var lfs []LearntFeature
	s := bufio.NewScanner(reader)
	for s.Scan() {
		var (
			comment  string
			topic    string
			features Features
			scores   []float64

			rest string
		)
		l := s.Text()

		// {line} # [comment]
		a := strings.Split(l, "#")
		if len(a) == 2 {
			comment = a[1]
			rest = a[0]
		} else {
			rest = l
		}

		// [topic] * {scores} * {Features}
		b := strings.Split(rest, "*")
		topic = b[0]

		// [Score {scores}]
		c := strings.Split(strings.TrimSpace(b[1]), " ")
		scores = make([]float64, len(c))
		for i, v := range c {
			var err error
			scores[i], err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
		}

		// [feature {Features}]
		d := strings.Split(strings.TrimSpace(b[2]), " ")
		features = make(Features, len(d))
		for i, v := range d {
			f := strings.Split(v, ":")
			id, err := strconv.Atoi(f[0])
			if err != nil {
				return nil, err
			}
			score, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				return nil, err
			}
			features[i] = Feature{
				ID:    id,
				Score: score,
			}
		}

		lfs = append(lfs, LearntFeature{
			Topic:    topic,
			Comment:  comment,
			Features: features,
			Scores:   scores,
		})
	}
	return lfs, nil
}

func keywordFeatures(q cqr.Keyword) Features {
	var features Features

	// Exploded.
	// 0 - not applicable.
	// 1 - not exploded.
	// 2 - exploded.
	explodedFeature := NewFeature(IsExplodedFeature, 0)
	if _, ok := q.Options["exploded"]; ok {
		if exploded, ok := q.Options["exploded"].(bool); ok && exploded {
			explodedFeature.Score = 2
		} else {
			explodedFeature.Score = 1
		}
	}

	// Truncation. Same feature Values as exploded.
	truncatedFeature := NewFeature(IsTruncatedFeature, 0)
	if _, ok := q.Options["truncated"]; ok {
		if truncated, ok := q.Options["truncated"].(bool); ok && truncated {
			truncatedFeature.Score = 2
		} else {
			truncatedFeature.Score = 1
		}
	}

	// Number of fields the query has.
	numFields := NewFeature(NumFieldsFeature, float64(len(q.Fields)))

	features = append(features, explodedFeature, truncatedFeature, numFields)
	return features
}

func booleanFeatures(q cqr.BooleanQuery) Features {
	var features Features

	// Operator type feature.
	// 0 - n/a
	// 1 - or
	// 2 - and
	// 3 - not
	// 4 - adj
	operatorType := NewFeature(OperatorTypeFeature, 0)
	switch q.Operator {
	case "or", "OR":
		operatorType.Score = 1
	case "and", "AND":
		operatorType.Score = 2
	case "not", "NOT":
		operatorType.Score = 3
	default:
		if strings.Contains(q.Operator, "adj") {
			operatorType.Score = 4
		}

	}
	features = append(features, operatorType)

	return features
}

func contextFeatures(context TransformationContext) Features {
	var features Features

	return append(features,
		NewFeature(DepthFeature, context.Depth),
		NewFeature(ClauseTypeFeature, context.ClauseType),
		NewFeature(ChildrenCountFeature, context.ChildrenCount))
}

// deltas computes delta Features for a query.
func deltas(query cqr.CommonQueryRepresentation, ss stats.StatisticsSource, measurements []analysis.Measurement, me analysis.MeasurementExecutor) (deltaFeatures, error) {
	deltas := make(deltaFeatures)

	gq := pipeline.NewQuery("qpp", "test", query)
	m, err := me.Execute(gq, ss, measurements...)
	if err != nil {
		return nil, err
	}
	for i, measurement := range measurements {
		if v, ok := MeasurementFeatureKeys[measurement.Name()]; ok {
			deltas[v] = m[i]
		} else {
			return nil, errors.New(fmt.Sprintf("%s is not registered as a feature in MeasurementFeatureKeys", measurement.Name()))
		}
	}

	return deltas, nil
}

func calcDelta(feature int, score float64, features deltaFeatures) float64 {
	if y, ok := features[feature]; ok {
		return score - y
	}
	return 0
}

func computeDeltas(preTransformation deltaFeatures, postTransformation deltaFeatures) Features {
	var features Features
	for feature, x := range preTransformation {
		deltaFeature := feature + len(MeasurementFeatureKeys)
		features = append(features, NewFeature(deltaFeature, calcDelta(feature, x, postTransformation)))
	}
	return features
}

// transformationFeature is a feature representing the previous feature that was applied to a query.
func transformationFeature(transformer Transformer) Feature {
	transformationType := NewFeature(TransformationTypeFeature, -1)
	switch transformer.(type) {
	case logicalOperatorReplacement:
		transformationType.Score = LogicalOperatorTransformation
	case *adjacencyRange:
		transformationType.Score = AdjacencyRangeTransformation
	case meshExplosion:
		transformationType.Score = MeshExplosionTransformation
	case fieldRestrictions:
		transformationType.Score = FieldRestrictionsTransformation
	case adjacencyReplacement:
		transformationType.Score = AdjacencyReplacementTransformation
	case clauseRemoval:
		transformationType.Score = ClauseRemovalTransformation
	case cui2vecExpansion:
		transformationType.Score = Cui2vecExpansionTransformation
	case meshParent:
		transformationType.Score = MeshParentTransformation
	}
	return transformationType
}

// String returns the string of a Feature family.
func (ff Features) String() string {
	sort.Sort(ff)
	size := set.Uniq(ff)
	tmp := ff[:size]
	s := make([]string, len(ff))
	for i, f := range tmp {
		s[i] = fmt.Sprintf("%v:%v", f.ID, f.Score)
	}
	return strings.Join(s, " ")
}


// WriteLibSVM writes a LIBSVM compatible line to a writer.
func (lf LearntFeature) WriteLibSVM(writer io.Writer, comment ...interface{}) (int, error) {
	sort.Sort(lf.Features)
	size := set.Uniq(lf.Features)
	ff := lf.Features[:size]
	line := fmt.Sprintf("%v", lf.Scores[0])
	for _, f := range ff {
		line += fmt.Sprintf(" %v:%v", f.ID, f.Score)
	}
	if len(comment) > 0 {
		line += " #"
		for _, c := range comment {
			line += fmt.Sprintf(" %v", c)
		}
	}

	return writer.Write([]byte(line + "\n"))
}

// WriteLibSVMRank writes a LIBSVM^rank compatible line to a writer.
func (lf LearntFeature) WriteLibSVMRank(writer io.Writer) (int, error) {
	sort.Sort(lf.Features)
	size := set.Uniq(lf.Features)
	ff := lf.Features[:size]
	line := fmt.Sprintf("%v qid:%v", lf.Scores[0], lf.Topic)
	for _, f := range ff {
		line += fmt.Sprintf(" %v:%v", f.ID, f.Score)
	}
	line += " # " + lf.Comment

	return writer.Write([]byte(line + "\n"))
}

// AverageScore compute the average Feature Score for a group of Features.
func (ff Features) AverageScore() float64 {
	if len(ff) == 0 {
		return 0
	}

	totalScore := 0.0
	for _, f := range ff {
		totalScore += f.Score
	}

	if totalScore == 0 {
		return 0
	}

	return totalScore / float64(len(ff))
}

// NewLearntFeature creates a new learnt feature with a Score and a set of Features.
func NewLearntFeature(features Features) LearntFeature {
	return LearntFeature{
		Features: features,
	}
}

// NewCandidateQuery creates a new candidate query.
func NewCandidateQuery(query cqr.CommonQueryRepresentation, topic string, ff Features) CandidateQuery {
	return CandidateQuery{
		Topic:            topic,
		Features:         ff,
		Query:            query,
		TransformationID: -1,
	}
}

// SetTransformationID sets the transformation id to the candidate query.
func (c CandidateQuery) SetTransformationID(id int) CandidateQuery {
	c.TransformationID = id
	return c
}

// Append adds the previous query to the chain of transformations so far so we can keep track of which transformations
// have been applied up until this point, and for Features about the query.
func (c CandidateQuery) Append(query CandidateQuery) CandidateQuery {

	query.Chain = append(query.Chain, query)
	c.Chain = append(c.Chain, query.Chain...)

	prevID := ChainFeatures
	var features Features
	for _, feature := range c.Features {
		if feature.ID < ChainFeatures {
			features = append(features, feature)
			continue
		}

		if prevID <= feature.ID {
			continue
		}

		features = append(features, feature)
		prevID = feature.ID
	}

	c.Features = features

	// Chain Features is the minimum possible index for these Features.
	idx := ChainFeatures
	for i, candidate := range c.Chain {
		c.Features = append(c.Features, NewFeature(idx+i, float64(candidate.TransformationID)))
	}

	return c
}
