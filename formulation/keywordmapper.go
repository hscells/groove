package formulation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/guru"
	"github.com/hscells/meshexp"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute/fields"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
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

// Preferred uses the Preferred entity from UMLS.
func Preferred(client guru.UMLSClient) MetaMapMapper {
	return func(keyword cqr.Keyword) (representations []cqr.CommonQueryRepresentation, err error) {
	lookup:
		preferred, err := client.Preferred(keyword.GetOption(Entity).(string))
		if err != nil {
			fmt.Println(err)
			time.Sleep(5 * time.Second)
			goto lookup
		}
		if len(preferred) == 0 {
			return []cqr.CommonQueryRepresentation{keyword}, nil
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

// Frequent identifies all of the terms for the concept in the UMLS meta-thesaurus.
func Alias(mapping cui2vec.AliasMapping) MetaMapMapper {
	return func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
		seen := make(map[string]struct{})
		if v, ok := mapping[keyword.GetOption(Entity).(string)]; ok {
			var mappings []cqr.CommonQueryRepresentation
			for _, s := range v {
				if _, ok := seen[s]; !ok {
					mappings = append(mappings, cqr.NewKeyword(fmt.Sprintf(`"%s"`, s), keyword.Fields...))
					seen[s] = struct{}{}
				}
			}
			return mappings, nil
		}
		return []cqr.CommonQueryRepresentation{keyword}, nil
	}
}

func ElasticUMLS(c *elastic.Client) MetaMapMapper {
	return func(keyword cqr.Keyword) ([]cqr.CommonQueryRepresentation, error) {
		res, err := c.Search("umls").Query(elastic.NewMatchQuery("cui", keyword.GetOption(Entity))).Do(context.Background())
		if err != nil {
			return nil, err
		}
		seen := make(map[string]struct{})
		mappings := []cqr.CommonQueryRepresentation{keyword}
		if res.Hits != nil {
			for i := range res.Hits.Hits {
				b, _ := res.Hits.Hits[i].Source.MarshalJSON()
				body := make(map[string]interface{})
				err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&body)
				if err != nil {
					return nil, err
				}
				thesaurus := body["thesaurus"].([]interface{})
				for _, item := range thesaurus {
					t := item.(map[string]interface{})
					if t["MRCONSO_LAT"] == "ENG" && t["MRCONSO_ISPREF"] == "Y" && t["MRCONSO_TS"] == "P" && t["MRCONSO_STT"] == "PF" {
						s := strings.ToLower(t["MRCONSO_STR"].(string))
						if strings.ContainsAny(s, ",[]()") {
							s = fmt.Sprintf(`"%s"`, s)
						}
						if t["MRCONSO_SAB"] == "MSH" {
							mappings = append(mappings, cqr.NewKeyword(s, fields.MeshHeadings).SetOption(cqr.ExplodedString, true))
						} else if _, ok := seen[s]; !ok {
							mappings = append(mappings, cqr.NewKeyword(s, keyword.Fields...))
						}
						seen[s] = struct{}{}
					}
				}
			}
		}
		return mappings, nil
	}
}

// MeSHMapper uses the output of another MetaMap mapper to assign MeSH terms.
func MeSHMapper(mapper MetaMapMapper) MetaMapMapper {
	return func(keyword cqr.Keyword) (representations []cqr.CommonQueryRepresentation, e error) {
		mt, _ := meshexp.Default()
		keywords, err := mapper(keyword)
		if e != nil {
			return nil, err
		}
		for i := 0; i < len(keywords); i++ {
			switch q := keywords[i].(type) {
			case cqr.Keyword:
				if mt.Contains(q.QueryString) {
					q.Fields = []string{fields.MeSHTerms}
					keywords[i] = q
				}
			}
		}
		return keywords, nil
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
func MapKeywords(r cqr.CommonQueryRepresentation, mapper KeywordMapper) (cqr.CommonQueryRepresentation, error) {
	switch q := r.(type) {
	case cqr.Keyword:
		// Don't process the query if it has no content.
		if len(strings.TrimSpace(q.QueryString)) == 0 {
			return q, nil
		}
		// Don't process the query if was never assigned an entity.
		if q.GetOption(Entity) == nil {
			return q, nil
		}
		// Otherwise, proceed to map the entities in the query to keywords.
		keywords, err := mapper.Map(q)
		if err != nil {
			return nil, err
		}
		if len(keywords) == 0 {
			return nil, nil
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
		var children []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			kws, err := MapKeywords(child, mapper)
			if err != nil {
				return nil, err
			}
			children = append(children, kws)
		}

		return cqr.NewBooleanQuery(b.Operator, children), nil
	}
	return r, nil
}
