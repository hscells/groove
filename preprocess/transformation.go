package preprocess

import (
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/fields"
	"strings"
)

// Transformation is an major modification to a query.
type Transformation func() cqr.CommonQueryRepresentation

// BooleanTransformation is a transformation that can be made to a Boolean query.
type BooleanTransformation func(q cqr.CommonQueryRepresentation, topic string) Transformation

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
func Simplify(query cqr.CommonQueryRepresentation, topic string) Transformation {
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
				q.Children[i] = Simplify(child, topic)()
			}
			return q
		default:
			return q
		}
	}
}

// RelaxPhrases replaces phrase queries with OR clauses.
func RelaxPhrases(query cqr.CommonQueryRepresentation, topic string) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.Keyword:
			switch q.Fields[0] {
			case fields.MeshHeadings, fields.MeSHMajorTopic, fields.FloatingMeshHeadings, fields.MeSHSubheading, fields.MajorFocusMeshHeading:
				return q
			}
			t := strings.Split(strings.Replace(q.QueryString, `"`, "", -1), " ")
			kw := make([]cqr.CommonQueryRepresentation, len(t))
			for i, term := range t {
				kw[i] = cqr.NewKeyword(term, q.Fields...)
			}
			return cqr.NewBooleanQuery(cqr.OR, kw)
		case cqr.BooleanQuery:
			for i, child := range q.Children {
				q.Children[i] = RelaxPhrases(child, topic)()
			}
			return q
		default:
			return q
		}
	}
}

// RemoveExplosionMeSH removes the explosion from MeSH terms.
func RemoveExplosionMeSH(query cqr.CommonQueryRepresentation, topic string) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.Keyword:
			q.SetOption(cqr.ExplodedString, false)
			return q
		case cqr.BooleanQuery:
			for i, child := range q.Children {
				q.Children[i] = RemoveExplosionMeSH(child, topic)()
			}
			return q
		default:
			return q

		}
	}
}

// AndSimplify replaces all operators with `and`.
func AndSimplify(query cqr.CommonQueryRepresentation, topic string) Transformation {
	return func() cqr.CommonQueryRepresentation {
		//return cqr.NewBooleanQuery("or", nil)
		switch q := query.(type) {
		case cqr.Keyword:
			return q
		case cqr.BooleanQuery:
			q.Operator = "and"
			for i, child := range q.Children {
				q.Children[i] = AndSimplify(child, topic)()
			}
			return q
		default:
			return q
		}
	}
}

// OrSimplify replaces all operators with `or`.
func OrSimplify(query cqr.CommonQueryRepresentation, topic string) Transformation {
	return func() cqr.CommonQueryRepresentation {
		//return cqr.NewBooleanQuery("or", nil)
		switch q := query.(type) {
		case cqr.Keyword:
			return q
		case cqr.BooleanQuery:
			q.Operator = "or"
			for i, child := range q.Children {
				q.Children[i] = OrSimplify(child, topic)()
			}
			return q
		default:
			return q
		}
	}
}

// RCTFilter adds a randomised controlled trials filter to queries.
// randomized controlled trial.pt.
// controlled clinical trial.pt.
// randomized.ab.
// placebo.ab.
// clinical trials as topic.sh.
// randomly.ab.
// trial.ti.
func RCTFilter(query cqr.CommonQueryRepresentation, _ string) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.BooleanQuery:
			q = cqr.NewBooleanQuery("and",
				[]cqr.CommonQueryRepresentation{
					q, // Original query.
					cqr.NewBooleanQuery("or", []cqr.CommonQueryRepresentation{ // RCT filter.
						cqr.NewKeyword("Randomized Controlled Trials As Topic", "mesh_headings").SetOption("exploded", true),
						cqr.NewKeyword("Randomized Controlled Trial", "publication_types"),
						cqr.NewKeyword("Controlled Clinical Trial", "publication_types"),
						cqr.NewKeyword("randomized", "text"),
						cqr.NewKeyword("placebo", "text"),
						cqr.NewKeyword("randomly", "text"),
						cqr.NewKeyword("trial", "title"),
					}),
				},
			)
			return q
		}
		return query
	}
}
