package rank

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/bbalet/stopwords"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/learning"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/merging"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"gopkg.in/jdkato/prose.v2"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"time"
)

var cm = merging.CoordinationLevelMatching{
	Occurances: make(map[string]float64),
}

// clf is the actual implementation of coordination level fusion. The exported function is simply a wrapper.
func clf(query pipeline.Query, posting *Posting, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	norm := merging.MinMaxNorm
	switch q := query.Query.(type) {
	case cqr.BooleanQuery:
		r := make([]trecresults.ResultList, len(q.Children))
		lists := make([]merging.Items, len(r))
		for i, child := range q.Children {
			var err error
			r[i], err = clf(pipeline.NewQuery(query.Name, query.Topic, child), posting, e)
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
			items = merging.CombSUM{}.Merge(lists)
		} else {
			items = merging.CombMNZ{}.Merge(lists)
		}
		norm.Init(items)
		items = merging.Normalise(norm, items)

		sort.Slice(items, func(i, j int) bool {
			if items[i].Score != items[j].Score {
				return items[i].Score > items[j].Score
			}
			// Break ties by publication date.
			n := float64(time.Now().Unix())
			a := (n - float64(posting.DocDates[hash(items[i].Id)]+1)) / n
			b := (n - float64(posting.DocDates[hash(items[j].Id)]+1)) / n
			return a+items[i].Score > b+items[i].Score
		})
		list := items.TRECResults(query.Topic)
		for i := range list {
			list[i].Rank = int64(i + 1)
		}

		fmt.Println("lists merged!")
		return list, nil
	case cqr.Keyword:
		scorers := []Scorer{
			&BM25Scorer{s: e, p: posting, B: 0.3, K1: 1.2},
			//&TFIDFScorer{s: e, p: posting},
			&TitleAbstractScorer{s: e, p: posting},
			//&PubDateScorer{s: e, p: posting},
			&PosScorer{s: e, p: posting},
			&DocLenScorer{s: e, p: posting},
		}
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

			list := lists[i]
			R := merging.FromTRECResults(list)
			norm.Init(R)
			list = merging.Normalise(norm, R).TRECResults(query.Topic)

			sort.Slice(list, func(i, j int) bool {
				return list[i].Score > list[j].Score
			})

			for i := range list {
				list[i].Rank = int64(i + 1)
			}

			if len(list) > 1000 {
				list = list[:1000]
			}

			lists[i] = list
			fmt.Printf(".")
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

func boolCOMB_CoordinationLevelMatching(query pipeline.Query, posting *Posting, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	//norm := merging.MinMaxNorm
	switch q := query.Query.(type) {
	case cqr.BooleanQuery:
		r := make([]trecresults.ResultList, len(q.Children))
		lists := make([]merging.Items, len(r))
		for i, child := range q.Children {
			var err error
			r[i], err = clf(pipeline.NewQuery(query.Name, query.Topic, child), posting, e)
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

		items := cm.Merge(lists)

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
		scorer := AppearScorer{s: e, p: posting}
		fmt.Printf("%s %v", q.QueryString, q.Fields)
		var list trecresults.ResultList
		defer func() { fmt.Println(" [√]") }()

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
			list = append(list, &trecresults.Result{
				Topic:     query.Topic,
				Iteration: "0",
				DocId:     pmid,
				Score:     score,
				RunName:   query.Topic,
			})
		}

		//l := merging.FromTRECResults(lists[i])
		//norm.Init(l)
		//list := merging.Normalise(norm, l).TRECResults(query.Topic)

		sort.Slice(list, func(i, j int) bool {
			return list[i].Score > list[j].Score
		})

		for i := range list {
			list[i].Rank = int64(i + 1)
		}
		fmt.Printf(".")
		return list, nil

	}
	return nil, nil
}

func writeResults(list trecresults.ResultList, dir string) error {
	f, err := os.OpenFile(dir, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, res := range list {
		_, err := f.WriteString(fmt.Sprintf("%s 0 %s %d %f %s\n", res.Topic, res.DocId, res.Rank, res.Score, res.RunName))
		if err != nil {
			return err
		}
	}
	return nil
}

func clfVariations(query cqr.CommonQueryRepresentation, topic string, idealPosting *Posting, e stats.EntrezStatisticsSource) error {
	candidates, err := learning.Variations(learning.CandidateQuery{
		TransformationID: -1,
		Topic:            topic,
		Query:            query,
		Chain:            nil,
	}, e, analysis.NewMemoryMeasurementExecutor(), nil,
		learning.NewLogicalOperatorTransformer(),
		learning.NewFieldRestrictionsTransformer(),
		learning.NewMeshParentTransformer(),
		learning.NewClauseRemovalTransformer())
	if err != nil {
		return err
	}

	cd, err := os.UserCacheDir()
	if err != nil {
		return err
	}
	indexPath := path.Join(cd, "groove_rank_variations")

	p := "tar18t2_variations"
	err = os.MkdirAll(path.Join(p, topic), 0777)
	if err != nil {
		return err
	}

	N, err := e.RetrievalSize(query)
	if err != nil {
		return err
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	for i, candidate := range candidates {
		fmt.Printf("[%s] variation %d/%d\n", topic, i+1, len(candidates))

		// String-ify the query.
		s, err := transmute.CompileCqr2PubMed(candidate.Query)
		if err != nil {
			return err
		}
	r:
		// Skip this candidate if it retrieves more than the original query.
		n, err := e.RetrievalSize(candidate.Query)
		if err != nil {
			fmt.Println(err)
			goto r
		}
		if n < N/2 || n > N*2 || n == 0 {
			fmt.Printf("skipping variation %d, retrieved no documents\n", i+1)
			fmt.Println(s)
			continue
		}
	s:
		// Obtain list of pmids.
		pmids, err := e.Search(s)
		if err != nil {
			fmt.Println(err)
			goto s
		}
		// Create posting list for query.
	f:
		posting, err := newPostingFromPMIDS(pmids, topic+"_"+strconv.Itoa(int(hash(s))), indexPath, e)
		if err != nil {
			fmt.Println(err)
			goto f
		}
		// Use fusion technique to rank retrieved results and write results to file.
		res, err := clf(pipeline.NewQuery(topic, topic, candidate.Query), posting, e)
		if err != nil {
			return err
		}
		err = writeResults(res, path.Join(p, topic, strconv.Itoa(int(hash(s)))+".res.retrieved"))
		if err != nil {
			return err
		}
		// Use fusion technique to rank only the relevant results and write to file.
		idealRes, err := clf(pipeline.NewQuery(topic, topic, candidate.Query), idealPosting, e)
		if err != nil {
			return err
		}
		err = writeResults(idealRes, path.Join(p, topic, strconv.Itoa(int(hash(s)))+".res.ideal"))
		if err != nil {
			return err
		}
		// Write the query to file for posterity.
		f, err := os.OpenFile(path.Join(p, topic, strconv.Itoa(int(hash(s)))+".qry"), os.O_CREATE|os.O_WRONLY, 0664)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		_, err = f.WriteString(s)
		if err != nil {
			return err
		}
		err = f.Close()
	}
	return nil
}

// CLF performs coordination-level fusion given a query. It ranks the documents retrieved for a query according to ...TODO?
// This wrapper function performs some pre-processing steps before actually ranking the documents for the query.
func CLF(query pipeline.Query, cacher combinator.QueryCacher, scorer Scorer, merger merging.Merger, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	cd, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	indexPath := path.Join(cd, "groove_rank")
	idealIndexPath := path.Join(cd, "groove_rank_ideal")

	var pmids []int
	//b, err := ioutil.ReadFile(fmt.Sprintf("/Users/s4558151/go/src/github.com/hscells/groove/scripts/tar17_testing_pmids/%s", query.Topic))
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

	QueryExpansion := false
	CLFVariations := false
	RankCLF := true

	if QueryExpansion {
		c := query.Query.(cqr.BooleanQuery).Children
		c = append(c, cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("sensitivity", fields.TitleAbstract),
			cqr.NewKeyword("specificity", fields.TitleAbstract),
			cqr.NewKeyword("diagnos*", fields.TitleAbstract),
			cqr.NewKeyword("diagnosis", fields.TitleAbstract),
			cqr.NewKeyword("predictive", fields.TitleAbstract),
			cqr.NewKeyword("accuracy", fields.TitleAbstract),
		}))
		c = append(c, cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("sensitivity and specificity", fields.MeSHTerms),
			cqr.NewKeyword("mass screening", fields.MeSHTerms),
			cqr.NewKeyword("reference values", fields.MeSHTerms),
			cqr.NewKeyword("false positive reactions", fields.MeSHTerms),
			cqr.NewKeyword("false negative reactions", fields.MeSHTerms),
			cqr.NewKeyword("specificit*", fields.TitleAbstract),
			cqr.NewKeyword("screening", fields.TitleAbstract),
			cqr.NewKeyword("false positive*", fields.TitleAbstract),
			cqr.NewKeyword("false negative*", fields.TitleAbstract),
			cqr.NewKeyword("accuracy", fields.TitleAbstract),
			cqr.NewKeyword("predictive value*", fields.TitleAbstract),
			cqr.NewKeyword("reference value*", fields.TitleAbstract),
			cqr.NewKeyword("roc*", fields.TitleAbstract),
			cqr.NewKeyword("likelihood ratio*", fields.TitleAbstract),
			cqr.NewKeyword("predictive value*", fields.TitleAbstract),
		}))

		title, err := ioutil.ReadFile(fmt.Sprintf("/Users/s4558151/go/src/github.com/hscells/groove/scripts/testing_task2_titles/%s", query.Topic))
		//title, err := ioutil.ReadFile(fmt.Sprintf("/Users/s4558151/go/src/github.com/hscells/groove/scripts/tar17_testing_titles/%s", query.Topic))
		if err != nil {
			return nil, err
		}
		fmt.Println(string(title))

		titleParsed, err := prose.NewDocument(stopwords.CleanString(string(title), "en", false), prose.WithTagging(false), prose.WithExtraction(false), prose.WithSegmentation(false))
		if err != nil {
			return nil, err
		}
		titleKeywords := make([]cqr.CommonQueryRepresentation, len(titleParsed.Tokens()))
		for i, tok := range titleParsed.Tokens() {
			titleKeywords[i] = cqr.NewKeyword(tok.Text, fields.TitleAbstract)
		}
		fmt.Println(titleKeywords)
		c = append(c, cqr.NewBooleanQuery(cqr.AND, titleKeywords))

		q := query.Query.(cqr.BooleanQuery)
		q.Children = c
		query.Query = q
	}

	if CLFVariations {
		scorer.posting(posting)
		scorer.entrez(e)

		f, err := os.OpenFile("/Users/s4558151/Repositories/tar/2018-TAR/Task2/Testing/qrels/qrel_abs_task2", os.O_RDONLY, 0664)
		if err != nil {
			return nil, err
		}
		qrels, err := trecresults.QrelsFromReader(f)
		if err != nil {
			return nil, err
		}
		rels := qrels.Qrels[query.Topic]
		var pmidsIdeal []int
		for _, rel := range rels {
			if rel.Score > 0 {
				i, err := strconv.Atoi(rel.DocId)
				if err != nil {
					return nil, err
				}
				pmidsIdeal = append(pmids, i)
			}
		}
		idealPosting, err := newPostingFromPMIDS(pmidsIdeal, query.Topic, idealIndexPath, e)
		if err != nil {
			return nil, err
		}

		if _, err := os.Stat("/Users/s4558151/go/src/github.com/hscells/groove/scripts/tar18t2_variations/" + query.Topic); os.IsNotExist(err) {
			res, err := e.Execute(query, e.SearchOptions())
			if err != nil {
				return nil, err
			}
			err = writeResults(res, path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/tar18t2_orig/"+query.Topic))
			if err != nil {
				return nil, err
			}
			return nil, clfVariations(query.Query, query.Topic, idealPosting, e)
		} else {
			fmt.Printf("skipping topic %s, already exists\n", query.Topic)
		}
	}

	if RankCLF {
		return clf(query, posting, e)
	}

	return nil, err
}
