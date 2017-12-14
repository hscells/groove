package preprocess

import (
	"github.com/hscells/cqr"
)

// Transformation is an major modification to a query.
type Transformation func() cqr.CommonQueryRepresentation

// BooleanTransformation is a transformation that can be made to a Boolean query.
type BooleanTransformation func(q cqr.CommonQueryRepresentation) Transformation

// Transformations is a collection of transformation operations.
type Transformations []Transformation

// TransformedQueries is a collection of transformed queries.
type TransformedQueries []cqr.CommonQueryRepresentation

// QueryTransformations is the information needed to perform query transformations.
type QueryTransformations struct {
	BooleanTransformations       []BooleanTransformation
	ElasticsearchTransformations []ElasticsearchTransformation
	Output                       string
}

// Simplify replaces anything that is not an `and` operator with and `or` operator.
func Simplify(query cqr.CommonQueryRepresentation) Transformation {
	return func() cqr.CommonQueryRepresentation {
		//return cqr.NewBooleanQuery("or", nil)
		switch q := query.(type) {
		case cqr.Keyword:
			return q
		case cqr.BooleanQuery:
			if q.Operator != "and" {
				q.Operator = "or"
			}
			for i, child := range q.Children {
				q.Children[i] = Simplify(child)()
			}
			return q
		default:
			return q
		}
	}
}

// AndSimplify replaces all operators with `and`.
func AndSimplify(query cqr.CommonQueryRepresentation) Transformation {
	return func() cqr.CommonQueryRepresentation {
		//return cqr.NewBooleanQuery("or", nil)
		switch q := query.(type) {
		case cqr.Keyword:
			return q
		case cqr.BooleanQuery:
			q.Operator = "and"
			for i, child := range q.Children {
				q.Children[i] = AndSimplify(child)()
			}
			return q
		default:
			return q
		}
	}
}

// OrSimplify replaces all operators with `or`.
func OrSimplify(query cqr.CommonQueryRepresentation) Transformation {
	return func() cqr.CommonQueryRepresentation {
		//return cqr.NewBooleanQuery("or", nil)
		switch q := query.(type) {
		case cqr.Keyword:
			return q
		case cqr.BooleanQuery:
			q.Operator = "or"
			for i, child := range q.Children {
				q.Children[i] = OrSimplify(child)()
			}
			return q
		default:
			return q
		}
	}
}
