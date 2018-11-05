package learning

import (
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"io"
	"log"
	"sort"
	"sync"
)

type RankOracleCandidateSelector struct {
	qrels    trecresults.QrelsFile
	ss       stats.StatisticsSource
	cache    combinator.QueryCacher
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
	var wg sync.WaitGroup
	for i, candidate := range transformations {
		wg.Add(1)
		go func(q CandidateQuery, j int) {
			defer wg.Done()
			pq := pipeline.NewQuery(query.Topic, query.Topic, q.Query)
			tree, _, err := combinator.NewLogicalTree(pq, r.ss, r.cache)
			if err != nil {
				panic(err)
			}
			results := tree.Documents(r.cache).Results(pq, pq.Topic)
			qrels := r.qrels.Qrels[query.Topic]
			score := r.measure.Score(&results, qrels)
			ranked[j] = oracleQuery{score, q}
		}(candidate, i)
	}

	wg.Wait()

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].score < ranked[j].score
	})

	ret, err := r.ss.RetrievalSize(query.Query)
	if err != nil {
		return CandidateQuery{}, nil, err
	}
	if ret == 0 {
		log.Println("stopping early")
		r.depth = r.maxDepth
		return query, r, nil
	}
	log.Printf("numret: %f\n", ret)

	r.depth++

	if query.Query.String() == ranked[0].query.String() {
		r.depth = r.maxDepth
	}

	return ranked[0].query, r, nil
}

func (RankOracleCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	panic("implement me")
}

func (RankOracleCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (r RankOracleCandidateSelector) StoppingCriteria() bool {
	return r.depth >= r.maxDepth
}

func NewRankOracleCandidateSelector(ss stats.StatisticsSource, qrels trecresults.QrelsFile, measure eval.Evaluator, maxDepth int) *QueryChain {
	return &QueryChain{
		CandidateSelector: RankOracleCandidateSelector{
			ss:       ss,
			qrels:    qrels,
			measure:  measure,
			maxDepth: maxDepth,
			cache:    combinator.NewFileQueryCache("file_cache"),
		},
	}
}
