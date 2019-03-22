package formulation

import (
	"github.com/hscells/cqr"
	"github.com/hscells/metawrap"
)

const Entity = "entity"

// EntityExtractor extracts entities from queries. These could be, for example, CUIs.
// Each Entity Extractor implementation adds the Entity option on queries which is used later in the keyword mapping step.
type EntityExtractor interface {
	Extract(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error)
}

// MetaMapEntityExtractor extracts CUI entities from queries.
type MetaMapEntityExtractor struct {
	client metawrap.HTTPClient
}

func (m MetaMapEntityExtractor) Extract(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error) {
	switch q := query.(type) {
	case cqr.Keyword:
		candidates, err := m.client.Candidates(q.QueryString)
		if err != nil {
			return nil, err
		}

		keywords := make([]cqr.CommonQueryRepresentation, len(candidates))
		for i, c := range candidates {
			keywords[i] = cqr.NewKeyword(c.CandidateMatched, q.Fields...).SetOption(Entity, c.CandidateCUI)
		}

		if len(keywords) == 1 {
			return keywords[0], nil
		}
		return cqr.NewBooleanQuery(cqr.OR, keywords), nil
	case cqr.BooleanQuery:
		for i, child := range q.Children {
			var err error
			q.Children[i], err = m.Extract(child)
			if err != nil {
				return nil, err
			}
		}
		return q, nil
	}
	return query, nil
}
