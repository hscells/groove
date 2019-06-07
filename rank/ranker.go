package rank

import (
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
)

type ScoredDocument struct {
	PMID  string
	Score float64
	Rank  float64
}

type ScoredDocuments struct {
	Docs []ScoredDocument
}

func Rank(query string, topic string, scorer Scorer, e stats.EntrezStatisticsSource) (trecresults.ResultList, error) {
	runner := NewRunner("groove_rank", []string{query}, []string{"ti", "ab", "mh"}, e, scorer)
	docs, err := runner.Run()
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	list := make(trecresults.ResultList, len(docs[0].Docs))
	for i, d := range docs[0].Docs {
		list[i] = &trecresults.Result{
			Topic:     topic,
			Iteration: "0",
			DocId:     d.PMID,
			Rank:      int64(d.Rank),
			Score:     d.Score,
			RunName:   topic,
		}
	}
	return list, nil
}
