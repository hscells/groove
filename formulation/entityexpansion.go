package formulation

import (
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
)

// EntityExpander takes as input a keyword that has been annotated with entities in the entity extraction
// step and expands it.
type EntityExpander interface {
	Expand(q cqr.Keyword) ([]cqr.CommonQueryRepresentation, error)
}

// Cui2VecEntityExpander expands entities using cui2vec embeddings.
type Cui2VecEntityExpander struct {
	embeddings cui2vec.PrecomputedEmbeddings
}

func NewCui2VecEntityExpander(embeddings cui2vec.PrecomputedEmbeddings) *Cui2VecEntityExpander {
	return &Cui2VecEntityExpander{embeddings: embeddings}
}

func (c Cui2VecEntityExpander) Expand(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
	concepts, err := c.embeddings.Similar(keyword.GetOption(Entity).(string))
	if err != nil {
		return nil, err
	}
	keywords := make([]cqr.CommonQueryRepresentation, len(concepts))
	for i, concept := range concepts {
		keywords[i] = cqr.NewKeyword(keyword.QueryString, keyword.Fields...).SetOption(Entity, concept.CUI)
	}
	return keywords, nil
}

// EntityExpansion performs entity expansion on a query using a specified expander.
func EntityExpansion(query cqr.CommonQueryRepresentation, expander EntityExpander) (cqr.CommonQueryRepresentation, error) {
	switch q := query.(type) {
	case cqr.Keyword:
		keywords, err := expander.Expand(q)
		if err != nil {
			panic(err)
		}
		if len(keywords) == 0 {
			return q, nil
		} else if len(keywords) == 1 {
			return keywords[0], nil
		}
		return cqr.NewBooleanQuery(cqr.OR, keywords), nil
	case cqr.BooleanQuery:
		bq := cqr.NewBooleanQuery(q.Operator, nil)
		for _, child := range q.Children {
			c, err := EntityExpansion(child, expander)
			if err != nil {
				return nil, err
			}
			bq.Children = append(bq.Children, c)
		}
		return bq, nil
	}
	return query, nil
}
