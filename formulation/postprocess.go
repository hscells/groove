package formulation

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/transmute/fields"
	"strings"
)

// PostProcess applies any post-formatting to a query.
type PostProcess func(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error)

// Stem uses already stemmed TermStatistics from the original query to
// replace TermStatistics from the query that requires post-processing.
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


var (
	/*
	#1 randomized controlled trial [pt]
	#2 controlled clinical trial [pt]
	#3 randomized [tiab]
	#4 placebo [tiab]
	#5 drug therapy [sh]
	#6 randomly [tiab]
	#7 trial [tiab]
	#8 groups [tiab]
	#9 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7 OR #8
	#10 animals [mh] NOT humans [mh]
	#11 #9 NOT #10
	 */
	SensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("drug therapy", fields.FloatingMeshHeadings),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.TitleAbstract),
			cqr.NewKeyword("groups", fields.TitleAbstract),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})

	/*
	#1 randomized controlled trial [pt]
	#2 controlled clinical trial [pt]
	#3 randomized [tiab]
	#4 placebo [tiab]
	#5 clinical trials as topic [mesh: noexp]
	#6 randomly [tiab]
	#7 trial [ti]
	#8 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7
	#9 animals [mh] NOT humans [mh]
	#10 #8 NOT #9
	 */
	PrecisionSensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("clinical trials as topic", fields.MeshHeadings).SetOption(cqr.ExplodedString, false),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.Title),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})
)