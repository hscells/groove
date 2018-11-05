package learning

import (
	"fmt"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"io"
	"sort"
)

type RankOracleCandidateSelector struct {
	qrels    trecresults.QrelsFile
	ss       stats.StatisticsSource
	measure  eval.Evaluator
	maxDepth int
	depth    int
}

type oracleQuery struct {
	score float64
	query CandidateQuery
}

func (r RankOracleCandidateSelector) Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	ranked := make([]oracleQuery, len(transformations))
	for i, candidate := range transformations {
		results, err := r.ss.Execute(pipeline.NewQuery(query.Topic, query.Topic, candidate.Query), r.ss.SearchOptions())
		if err != nil {
			return CandidateQuery{}, nil, err
		}
		qrels := r.qrels.Qrels[query.Topic]
		score := r.measure.Score(&results, qrels)
		ranked[i] = oracleQuery{score, candidate}
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].score < ranked[j].score
	})

	r.depth++

	return ranked[0].query, r, nil
}

func (RankOracleCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	panic("implement me")
}

func (RankOracleCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (r RankOracleCandidateSelector) StoppingCriteria() bool {
	return r.depth > r.maxDepth
}

func NewRankOracleCandidateSelector(ss stats.StatisticsSource, qrels trecresults.QrelsFile, measure eval.Evaluator, maxDepth int) *QueryChain {
	return &QueryChain{
		CandidateSelector: RankOracleCandidateSelector{
			ss:       ss,
			qrels:    qrels,
			measure:  measure,
			maxDepth: maxDepth,
		},
	}
}
