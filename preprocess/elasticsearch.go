package preprocess

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/stats"
	"strings"
)

// ElasticsearchTransformation is a specific transformation that uses an Elasticsearch statistics source.
type ElasticsearchTransformation func(query cqr.CommonQueryRepresentation, source *stats.ElasticsearchStatisticsSource) Transformation

// Analyse runs the specified Elasticsearch analyser on a query and returns a new, analysed query.
func Analyse(query cqr.CommonQueryRepresentation, source *stats.ElasticsearchStatisticsSource) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.Keyword:
			tokens, err := source.Analyse(q.QueryString, source.Analyser)
			if err != nil {
				panic(err)
			}
			return cqr.NewKeyword(strings.Join(tokens, " "), q.Fields...)
		case cqr.BooleanQuery:
			for i, child := range q.Children {
				q.Children[i] = Analyse(child, source)()
			}
			return q
		default:
			return q
		}
	}
}

// SetAnalyseField sets the text and title fields to be analysed by the specified analyser.
func SetAnalyseField(query cqr.CommonQueryRepresentation, source *stats.ElasticsearchStatisticsSource) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.Keyword:
			fields := make([]string, len(q.Fields))
			copy(fields, q.Fields)
			if truncated, ok := q.Options["truncated"].(bool); ok && truncated {
				for i, field := range q.Fields {
					if field == "text" || field == "title" {
						fields[i] = fmt.Sprintf("%s.%s", field, source.AnalyseField)
					}
				}
			} else if !truncated {
				for i, field := range q.Fields {
					fields[i] = strings.Replace(field, ".stemmed", "", -1)
				}
			}
			q.Fields = fields
			return q
		case cqr.BooleanQuery:
			numTruncated := 0
			if strings.Contains(q.Operator, "adj") {
				keywords := analysis.QueryKeywords(q)
				for _, keyword := range keywords {
					if truncated, ok := keyword.Options["truncated"].(bool); ok && truncated {
						numTruncated++
					}
				}
				if numTruncated != len(keywords) {
					return q
				}
			}
			for i, child := range q.Children {
				q.Children[i] = SetAnalyseField(child, source)()
			}
			return q
		default:
			return q
		}
	}
}
