package preprocess

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"log"
	"strings"
	"fmt"
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
				log.Fatal(err)
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

func SetAnalyseField(query cqr.CommonQueryRepresentation, source *stats.ElasticsearchStatisticsSource) Transformation {
	return func() cqr.CommonQueryRepresentation {
		switch q := query.(type) {
		case cqr.Keyword:
			if truncated, ok := q.Options["truncated"].(bool); ok {
				if truncated {
					for i, field := range q.Fields {
						if !strings.Contains(field, source.AnalyseField) && !strings.Contains(field, "mesh_headings") {
							q.Fields[i] = fmt.Sprintf("%s.%s", field, source.AnalyseField)
						}
					}
				}
			}
			return q
		case cqr.BooleanQuery:
			for i, child := range q.Children {
				q.Children[i] = SetAnalyseField(child, source)()
			}
			return q
		default:
			return q
		}
	}
}
