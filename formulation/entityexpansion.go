package formulation

import (
	"github.com/biogo/ncbi/entrez"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/transmute/fields"
	"strconv"
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

// MedGenEntityExpander expands entities using the MedGen API.
type MedGenEntityExpander struct {
	e stats.EntrezStatisticsSource
}

func (m MedGenEntityExpander) Expand(q cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
	ids, err := m.e.Search(q.QueryString)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	sids := make([]string, len(ids))
	for i, id := range ids {
		sids[i] = strconv.Itoa(id)
	}
	var summary guru.CeSummaryResult
	err = m.e.Summary(sids, &summary, func(p *entrez.Parameters) {
		p.Sort = "relevance"
	})
	if err != nil {
		return nil, err
	}

	var keywords []cqr.CommonQueryRepresentation
	for _, docSum := range summary.CDocumentSummarySet.CDocumentSummary {
		for j, name := range docSum.CConceptMeta.CNames.CName {
			if j > 2 {
				break
			}
			query := cqr.NewKeyword(name.Value, fields.TitleAbstract)
			// Add MeSH field restrictions to the query if it comes from a MeSH source.
			if name.AttrSAB == "MSH" {
				query.Fields = []string{fields.MeshHeadings}
				query = query.SetOption(cqr.ExplodedString, true).(cqr.Keyword)
			}
			keywords = append(keywords, query)
		}
		if len(keywords) > 5 {
			break
		}
	}
	return keywords, nil
}

func (m MedGenEntityExpander) CUIs(q cqr.Keyword) ([]string, error) {
	ids, err := m.e.Search(q.QueryString)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	sids := make([]string, len(ids))
	for i, id := range ids {
		sids[i] = cui2vec.Int2CUI(id)
	}
	return sids, nil
}

func NewMedGenExpander(e stats.EntrezStatisticsSource) *MedGenEntityExpander {
	return &MedGenEntityExpander{e: e.SetDB("medgen")}
}

func NewCui2VecEntityExpander(embeddings cui2vec.PrecomputedEmbeddings) *Cui2VecEntityExpander {
	return &Cui2VecEntityExpander{embeddings: embeddings}
}

func (c Cui2VecEntityExpander) Expand(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
	if keyword.GetOption(Entity) == nil {
		return []cqr.CommonQueryRepresentation{}, nil
	}
	concepts, err := c.embeddings.Similar(keyword.GetOption(Entity).(string))
	if err != nil {
		return nil, err
	}
	var keywords []cqr.CommonQueryRepresentation
	for _, concept := range concepts {
		if len(concept.CUI) == 0 {
			continue
		}
		keywords = append(keywords, cqr.NewKeyword(keyword.QueryString, keyword.Fields...).SetOption(Entity, concept.CUI))
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
		if keywords == nil || len(keywords) == 0 {
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
