package formulation

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"strings"
)

// PostProcess applies any post-formatting to a query.
type PostProcess func(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error)

// Stem uses already stemmed terms from the original query to
// replace terms from the query that requires post-processing.
func Stem(original cqr.CommonQueryRepresentation) PostProcess {
	return func(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error) {
		stemDict := make(map[string]bool)
		for _, kw := range analysis.QueryKeywords(original) {
			if v, ok := kw.Options["truncated"]; ok {
				if v.(bool) == true {
					stemDict[kw.QueryString] = true
				}
			}
		}
		return stemQuery(query, stemDict, make(map[string]bool)), nil
	}
}

func stemQuery(query cqr.CommonQueryRepresentation, d map[string]bool, seen map[string]bool) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		for k := range d {
			if strings.Contains(strings.ToLower(q.QueryString), strings.Replace(strings.ToLower(k), "*", "", -1)) {
				q.QueryString = k
				if _, ok := seen[k]; !ok {
					q.SetOption("truncated", true)
					seen[k] = true
					return q
				} else {
					return nil
				}
			}
		}
		return q
	case cqr.BooleanQuery:
		var c []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			s := stemQuery(child, d, seen)
			if s != nil {
				c = append(c, s)
			}
		}
		q.Children = c
		return q
	default:
		return q
	}
}
