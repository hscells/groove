package formulation

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/guru"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute/fields"
	"strings"
)

// KeywordMapper transforms entities (e.g., CUIs into keywords).
type KeywordMapper interface {
	Map(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error)
}

// MetaMapMapper maps candidates from MetaMap to one or more keywords.
type MetaMapMapper func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error)

// MetaMapKeywordMapper uses MetaMap to map entities (CUIs) to keywords.
type MetaMapKeywordMapper struct {
	client metawrap.HTTPClient
	mapper MetaMapMapper
}

// Matched uses the Matched entity from MetaMap.
func Matched() MetaMapMapper {
	return func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
		return []cqr.CommonQueryRepresentation{keyword}, nil
	}
}

// Preferred uses the Preferred entity from MetaMap.
func Preferred(client guru.UMLSClient) MetaMapMapper {
	return func(keyword cqr.Keyword) (representations []cqr.CommonQueryRepresentation, err error) {
		preferred, err := client.Preferred(keyword.GetOption(Entity).(string))
		if err != nil {
			return
		}
		return []cqr.CommonQueryRepresentation{cqr.NewKeyword(preferred, keyword.Fields...)}, nil
	}
}

// Frequent identifies the most frequently used term for the concept in the UMLS meta-thesaurus.
func Frequent(mapping cui2vec.Mapping) MetaMapMapper {
	return func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
		if v, ok := mapping[keyword.GetOption(Entity).(string)]; ok {
			return []cqr.CommonQueryRepresentation{cqr.NewKeyword(v, keyword.Fields...)}, nil
		}
		return []cqr.CommonQueryRepresentation{keyword}, nil
	}
}

// Frequent identifies all of the TermStatistics for the concept in the UMLS meta-thesaurus.
func Alias(mapping cui2vec.AliasMapping) MetaMapMapper {
	return func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
		if v, ok := mapping[keyword.GetOption(Entity).(string)]; ok {
			var mappings []cqr.CommonQueryRepresentation
			for _, s := range v {
				mappings = append(mappings, cqr.NewKeyword(s, keyword.Fields...))
			}
			return mappings, nil
		}
		return []cqr.CommonQueryRepresentation{keyword}, nil
	}
}

// Map maps text to several concepts.
func (m MetaMapKeywordMapper) Map(keyword cqr.Keyword) (keywords []cqr.CommonQueryRepresentation, err error) {
	return m.mapper(keyword)
}

// NewMetaMapKeywordMapper creates a new keyword mapper that uses MetaMap.
func NewMetaMapKeywordMapper(client metawrap.HTTPClient, mapper MetaMapMapper) MetaMapKeywordMapper {
	return MetaMapKeywordMapper{
		client: client,
		mapper: mapper,
	}
}

// MapKeywords takes as input a proto-query from a newly logically composed query and maps concepts in it to keywords
// using the specified mapper.
func MapKeywords(r cqr.CommonQueryRepresentation, mapper KeywordMapper) (v cqr.CommonQueryRepresentation, err error) {
	switch q := r.(type) {
	case cqr.Keyword:
		if len(strings.TrimSpace(q.QueryString)) == 0 {
			return
		}
		keywords, err := mapper.Map(q)
		if err != nil {
			return
		}
		if len(keywords) == 0 {
			return
		} else if len(keywords) == 1 {
			return keywords[0], nil
		}
		b := cqr.NewBooleanQuery(cqr.OR, nil)
		for _, keyword := range keywords {
			b.Children = append(b.Children, keyword)
		}
		return b, nil
	case cqr.BooleanQuery:
		b := cqr.NewBooleanQuery(q.Operator, nil)
		var qs []string
		for _, child := range q.Children {
			switch v := child.(type) {
			case cqr.Keyword:
				qs = append(qs, v.QueryString)
			case cqr.BooleanQuery:
				m, err := MapKeywords(child, mapper)
				if err != nil {
					return
				}
				if m != nil {
					b.Children = append(b.Children, m)
				}
			}
		}

		k := cqr.NewKeyword(fmt.Sprintf(`"%s"`, strings.Join(qs, " ")), fields.TitleAbstract)
		if len(qs) == len(q.Children) {
			return MapKeywords(k, mapper)
		} else if len(qs) > 0 {
			v, err := MapKeywords(k, mapper)
			if err != nil {
				return
			}
			if v != nil {
				b.Children = append(b.Children, v)
			}
		}
		return b, nil
	}
	return
}
