package cmd

import (
	"bytes"
	"fmt"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/rewrite"
	"io/ioutil"
	"log"
	"sort"
)

// LoadExcludingQuery loads queries from a directory while excluding a specific topic.
func LoadExcludingQuery(directory, measure string, topic int64) map[int64][]Feature {
	queries := make(chan Query)

	rank := make(map[int64][]Feature)

	go func() {
		for {
			q, more := <-queries
			if more {
				if q.Error != nil {
					log.Fatal(q.Error)
				}

				var m float64
				if measure == "F05" {
					m = Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 0.5)
				} else if measure == "F1" {
					m = Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 1)
				} else if measure == "F3" {
					m = Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 3)
				} else if measure == "WSS" {
					m = WSS(N, q.Query.Eval[eval.NumRet.Name()], q.Query.Eval[eval.RecallEvaluator.Name()])
				} else {
					m = q.Query.Eval[measure]
				}

				if q.Query.Topic != topic {
					f := Feature{
						Topic:    q.Query.Topic,
						Depth:    q.Query.Depth,
						FileName: q.FileName,
						LearntFeature: rewrite.LearntFeature{
							Features: q.Query.Candidate.Features,
							Score:    m,
						},
						Eval: q.Query.Eval,
					}

					rank[q.Query.Topic] = append(rank[q.Query.Topic], f)
				}

			} else {
				return
			}
		}
	}()

	LoadQueries(directory, queries)

	for topic, queries := range rank {
		var maxDepth int64
		for _, query := range queries {
			if query.Depth > maxDepth {
				maxDepth = query.Depth
			}
		}

		var ff rewrite.Features
		for depth := 0; int64(depth) < maxDepth; depth++ {
			query := BestFeatureAt(int64(depth), queries)
			ff = query.Features
			for j, innerQuery := range queries {
				if innerQuery.Depth == int64(depth+1) {
					rank[topic][j].Features = append(innerQuery.Features, ff...)
				}
			}
		}
	}

	return rank
}

// LoadAllQueriesForRanking loads queries from a directory without excluding any topics.
func LoadAllQueriesForRanking(directory, measure string) map[int64][]Feature {
	return LoadExcludingQuery(directory, measure, -1)
}

// WriteFeatures writes features to a file suitable for SVM rank.
func WriteFeatures(rank map[int64][]Feature, featureFile string) error {
	topics := make([]int64, len(rank))
	i := 0
	for k := range rank {
		q := rank[k]

		q = SortFeatures(q)

		rank[k] = q
		topics[i] = k
		i++
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i] < topics[j]
	})

	buff := bytes.NewBufferString("")

	for i := 0; i < len(topics); i++ {
		for j, lf := range rank[topics[i]] {
			lf.WriteLibSVMRank(buff, topics[i], fmt.Sprintf("%v_%v %v", topics[i], j+1, lf.FileName))
		}
	}

	return ioutil.WriteFile(featureFile, buff.Bytes(), 0644)
}

// SortFeatures sorts a slice of features so that is suitable for use in an SVM rank file.
func SortFeatures(q []Feature) []Feature {
	sort.Slice(q, func(i, j int) bool {
		return q[i].Score > q[j].Score
	})

	for i, lf := range q {
		sort.Slice(lf.Features, func(i, j int) bool {
			return lf.Features[i].ID+lf.Features[i].Index < lf.Features[j].ID+lf.Features[j].Index
		})

		var ff rewrite.Features
		for _, feature := range lf.Features {
			found := false
			for _, f := range ff {
				if f.ID+f.Index == feature.ID+feature.Index {
					found = true
				}
			}
			if !found {
				ff = append(ff, feature)
			}
		}
		q[i].Features = ff
	}
	return q
}
