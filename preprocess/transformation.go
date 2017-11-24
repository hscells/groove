package preprocess

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
)

// Transformation is an major modification to a query.
type Transformation func(q cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation

// Transformations is a collection of transformation operations.
type Transformations []Transformation

// TransformedQueries is a collection of transformed queries.
type TransformedQueries []cqr.CommonQueryRepresentation

// QueryTransformations is the information needed to perform query transformations.
type QueryTransformations struct {
	Transformations Transformations
	Output          string
}

func Simplify(query cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation {
	//return cqr.NewBooleanQuery("or", nil)
	switch q := query.(type) {
	case cqr.Keyword:
		return q
	case cqr.BooleanQuery:
		if q.Operator != "and" {
			q.Operator = "or"
		}
		for i, child := range q.Children {
			q.Children[i] = Simplify(child)
		}
		return q
	default:
		return q
	}
}

// Apply applies a set of transformations to a query.
func (ts Transformations) Apply(query groove.PipelineQuery) cqr.CommonQueryRepresentation {
	tmpQuery := query.Original()
	for _, transformation := range ts {
		tmpQuery = transformation(tmpQuery)
		query.SetTransformed(tmpQuery)
	}
	return query.Transformed()
}
