package rank

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/merging"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"os"
	"path"
	"sort"
)

func boolCOMB(query pipeline.Query, posting *Posting, cacher combinator.QueryCacher, scorer Scorer, merger merging.Merger, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	switch q := query.Query.(type) {
	case cqr.BooleanQuery:
		r := make([]trecresults.ResultList, len(q.Children))
		lists := make([]merging.Items, len(r))
		for i, child := range q.Children {
			var err error
			r[i], err = boolCOMB(pipeline.NewQuery(query.Name, query.Topic, child), posting, cacher, scorer, merger, e)
			if err != nil {
				return nil, err
			}
		}

		for i, result := range r {
			lists[i] = merging.FromTRECResults(result)
		}
		fmt.Printf("merging %d lists, each list contains:\n", len(lists))
		for i, l := range lists {
			fmt.Printf("%d. %d items\n", i, len(l))
		}
		items := merger.Merge(lists)
		fmt.Println(items[0].Score, items[len(items)-1].Score)
		fmt.Println("lists merged!")
		return items.TRECResults(query.Topic), nil
	case cqr.Keyword:
		var list trecresults.ResultList
		//fmt.Println("here", q.QueryString, q.Fields[0])
		for pmid := range posting.DocLens {
			var score float64
			var err error

			switch q.Fields[0] {
			case fields.TitleAbstract, fields.TextWord:
				score, err = scorer.Score(q.QueryString, pmid, "ti", "ab")
			case fields.Title:
				score, err = scorer.Score(q.QueryString, pmid, "ti")
			case fields.Abstract:
				score, err = scorer.Score(q.QueryString, pmid, "ab")
			case fields.MeshHeadings, fields.MeSHTerms, fields.MeSHSubheading, fields.MeSHMajorTopic, fields.FloatingMeshHeadings:
				score, err = scorer.Score(q.QueryString, pmid, "mh")
			default:
				score, err = scorer.Score(q.QueryString, pmid, "ti", "ab", "mh")
			}
			if err != nil {
				return nil, err
			}
			list = append(list, &trecresults.Result{
				Topic:     query.Topic,
				Iteration: "0",
				DocId:     pmid,
				Score:     score,
				RunName:   query.Topic,
			})
		}
		sort.Slice(list, func(i, j int) bool {
			return list[i].Score > list[j].Score
		})
		fmt.Println(len(list), list[0].Score, q.QueryString, q.Fields[0])
		if list[0].Score == 0 {
			return trecresults.ResultList{}, nil
		}
		for i := range list {
			list[i].Rank = int64(i)
		}
		return list, nil
	}
	return nil, nil
}

func BoolCOMB(query pipeline.Query, cacher combinator.QueryCacher, scorer Scorer, merger merging.Merger, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	q, err := transmute.CompileCqr2PubMed(query.Query)
	if err != nil {
		return nil, err
	}
	cd, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	indexPath := path.Join(cd, "groove_rank")
	posting, err := index(q, indexPath, e)
	if err != nil {
		return nil, err
	}
	scorer.posting(posting)
	scorer.entrez(e)
	return boolCOMB(query, posting, cacher, scorer, merger, e)
}
