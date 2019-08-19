package rank

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/merging"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"gopkg.in/cheggaaa/pb.v1"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
)

func boolCOMB(query pipeline.Query, posting *Posting, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	norm := merging.MinMaxNorm
	switch q := query.Query.(type) {
	case cqr.BooleanQuery:
		r := make([]trecresults.ResultList, len(q.Children))
		lists := make([]merging.Items, len(r))
		for i, child := range q.Children {
			var err error
			r[i], err = boolCOMB(pipeline.NewQuery(query.Name, query.Topic, child), posting, e)
			if err != nil {
				return nil, err
			}
		}

		fmt.Printf("merging %d lists", len(lists))

		for i, result := range r {
			lists[i] = merging.FromTRECResults(result)
		}

		for range lists {
			fmt.Print(".")
		}

		var items merging.Items
		if q.Operator == cqr.AND {
			items = merging.CombMNZ{}.Merge(lists)
		} else {
			items = merging.CombSUM{}.Merge(lists)
		}
		norm.Init(items)
		items = merging.Normalise(norm, items)

		sort.Slice(items, func(i, j int) bool {
			return items[i].Score > items[j].Score
		})
		list := items.TRECResults(query.Topic)
		for i := range list {
			list[i].Rank = int64(i + 1)
		}

		fmt.Println("lists merged!")
		return list, nil
	case cqr.Keyword:
		//bm25 := &BM25Scorer{K1: 1.2, B: 0.3, s: e, p: posting}
		scorers := []Scorer{&TFIDFScorer{s: e, p: posting}, &TitleAbstractScorer{s: e, p: posting}}
		lists := make([]trecresults.ResultList, len(scorers))
		fmt.Printf("%s %v", q.QueryString, q.Fields)
		defer func() { fmt.Println(" [√]") }()

		for i, scorer := range scorers {
			for pmid := range posting.DocLens {
				var score float64
				var err error
				switch q.Fields[0] {
				case fields.Title:
					score, err = scorer.Score(q.QueryString, pmid, "ti")
				case fields.Abstract:
					score, err = scorer.Score(q.QueryString, pmid, "ab")
				case fields.MeshHeadings, fields.MeSHTerms, fields.MeSHSubheading, fields.MeSHMajorTopic, fields.FloatingMeshHeadings:
					score, err = scorer.Score(q.QueryString, pmid, "mh")
				case fields.TitleAbstract, fields.TextWord:
					score, err = scorer.Score(q.QueryString, pmid, "ti", "ab")
				case fields.AllFields:
					score, err = scorer.Score(q.QueryString, pmid, "ti", "ab", "mh")
				default:
					score = 0.0
				}
				if err != nil {
					return nil, err
				}
				if score == 0 {
					continue
				}
				lists[i] = append(lists[i], &trecresults.Result{
					Topic:     query.Topic,
					Iteration: "0",
					DocId:     pmid,
					Score:     score,
					RunName:   query.Topic,
				})
			}

			l := merging.FromTRECResults(lists[i])
			norm.Init(l)
			list := merging.Normalise(norm, l).TRECResults(query.Topic)
			//list := lists[i]

			sort.Slice(list, func(i, j int) bool {
				return list[i].Score > list[j].Score
			})

			for i := range list {
				list[i].Rank = int64(i + 1)
			}
			fmt.Printf(".")
			//return list, nil
		}

		items := make([]merging.Items, len(lists))
		for i, list := range lists {
			items[i] = merging.FromTRECResults(list)
		}

		merger := merging.CombMNZ{}
		res := merger.Merge(items).TRECResults(query.Topic)
		return res, nil
	}
	return nil, nil
}

func vecOR(x, y []float64) []float64 {
	z := make([]float64, len(x))
	for i := range x {
		if x[i] == 1 {
			z[i] = 1
		}
		if y[i] == 1 {
			z[i] = 1
		}
	}
	return z
}

func boolVECCOMB(query pipeline.Query, posting *Posting, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	var (
		lists []trecresults.ResultList
		qvec  []float64
	)

	fmt.Println(len(posting.DocLens), len(posting.DocLens))

	TI := hash("ti")
	AB := hash("ab")
	MH := hash("mh")

	switch q := query.Query.(type) {
	case cqr.BooleanQuery:
		for _, child := range q.Children {
			switch c := child.(type) {
			case cqr.BooleanQuery:
				l, err := boolVECCOMB(pipeline.NewQuery(query.Name, query.Topic, child), posting, e)
				if err != nil {
					return nil, err
				}
				lists = append(lists, l)
			case cqr.Keyword:
				terms, err := fastTokenise(c.QueryString)
				if err != nil {
					return nil, err
				}

				qv := make([]float64, len(posting.Index)*3)
				var f []string

				switch c.Fields[0] {
				case fields.Title:
					f = []string{"ti"}
				case fields.Abstract:
					f = []string{"ab"}
				case fields.MeshHeadings, fields.MeSHTerms, fields.MeSHSubheading, fields.MeSHMajorTopic, fields.FloatingMeshHeadings:
					f = []string{"mh"}
				default:
					f = []string{"ti", "ab"}
				}

				for _, term := range terms {
					for _, field := range f {
						var j int
						switch hash(field) {
						case TI:
							j = 1
						case AB:
							j = 2
						case MH:
							j = 3
						}
						qv[j*posting.TermIdx[hash(term)]] = 1
					}
				}

				if qvec == nil || len(qvec) == 0 {
					qvec = qv
				} else {
					qvec = vecOR(qvec, qv)
				}
			}
		}
	}

	if len(qvec) > 0 {
		l := make(trecresults.ResultList, len(posting.DocLens))
		i := 0
		fmt.Println(query.Query)
		bar := pb.New(len(posting.DocLens))
		bar.Start()
		for pmid := range posting.DocLens {
			dv := posting.DocumentVector(hash(pmid))

			sim, err := cui2vec.Cosine(qvec, dv)
			if err != nil {
				return nil, err
			}

			l[i] = &trecresults.Result{
				Topic: query.Topic,
				DocId: pmid,
				Score: sim,
			}
			i++
			bar.Add(1)
		}

		sort.Slice(l, func(i, j int) bool {
			return l[i].Score > l[j].Score
		})

		for i := range l {
			l[i].Rank = int64(i + 1)
		}

		lists = append(lists, l)
		bar.Finish()
	}

	items := make([]merging.Items, len(lists))
	for i, list := range lists {
		items[i] = merging.FromTRECResults(list)
	}

	return merging.CombMNZ{}.Merge(items).TRECResults(query.Topic), nil
}

func BoolCOMB(query pipeline.Query, cacher combinator.QueryCacher, scorer Scorer, merger merging.Merger, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	cd, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	indexPath := path.Join(cd, "groove_rank")

	var pmids []int
	b, err := ioutil.ReadFile(fmt.Sprintf("/Users/s4558151/go/src/github.com/hscells/groove/scripts/testing_task2_pmids/%s", query.Topic))
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(bytes.NewBuffer(b))
	for s.Scan() {
		pmid, err := strconv.Atoi(s.Text())
		if err != nil {
			return nil, err
		}
		pmids = append(pmids, pmid)
	}
	posting, err := newPostingFromPMIDS(pmids, query.Topic, indexPath, e)

	//c := query.Query.(cqr.BooleanQuery).Children
	//c = append(c, cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
	//	cqr.NewKeyword("sensitivity", fields.TitleAbstract),
	//	cqr.NewKeyword("specificity", fields.TitleAbstract),
	//	cqr.NewKeyword("diagnos*", fields.TitleAbstract),
	//	cqr.NewKeyword("diagnosis", fields.TitleAbstract),
	//	cqr.NewKeyword("predictive", fields.TitleAbstract),
	//	cqr.NewKeyword("accuracy", fields.TitleAbstract),
	//}))
	//c = append(c, cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
	//	cqr.NewKeyword("sensitivity and specificity", fields.MeSHTerms),
	//	cqr.NewKeyword("mass screening", fields.MeSHTerms),
	//	cqr.NewKeyword("reference values", fields.MeSHTerms),
	//	cqr.NewKeyword("false positive reactions", fields.MeSHTerms),
	//	cqr.NewKeyword("false negative reactions", fields.MeSHTerms),
	//	cqr.NewKeyword("specificit*", fields.TitleAbstract),
	//	cqr.NewKeyword("screening", fields.TitleAbstract),
	//	cqr.NewKeyword("false positive*", fields.TitleAbstract),
	//	cqr.NewKeyword("false negative*", fields.TitleAbstract),
	//	cqr.NewKeyword("accuracy", fields.TitleAbstract),
	//	cqr.NewKeyword("predictive value*", fields.TitleAbstract),
	//	cqr.NewKeyword("reference value*", fields.TitleAbstract),
	//	cqr.NewKeyword("roc*", fields.TitleAbstract),
	//	cqr.NewKeyword("likelihood ratio*", fields.TitleAbstract),
	//	cqr.NewKeyword("predictive value*", fields.TitleAbstract),
	//}))

	//title, err := ioutil.ReadFile(fmt.Sprintf("/Users/s4558151/go/src/github.com/hscells/groove/scripts/testing_task2_titles/%s", query.Topic))
	//if err != nil {
	//	return nil, err
	//}
	//fmt.Println(string(title))
	//
	//titleParsed, err := prose.NewDocument(stopwords.CleanString(string(title), "en", false), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
	//if err != nil {
	//	return nil, err
	//}
	//titleKeywords := make([]cqr.CommonQueryRepresentation, len(titleParsed.Tokens()))
	//for i, tok := range titleParsed.Tokens() {
	//	titleKeywords[i] = cqr.NewKeyword(tok.Text, fields.TitleAbstract)
	//}
	//fmt.Println(titleKeywords)
	//c = append(c, cqr.NewBooleanQuery(cqr.AND, titleKeywords))
	//
	//q := query.Query.(cqr.BooleanQuery)
	//q.Children = c
	//query.Query = q

	scorer.posting(posting)
	scorer.entrez(e)
	return boolCOMB(query, posting, e)
}
