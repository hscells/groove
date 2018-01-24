package main

import (
	"github.com/alexflint/go-arg"
	"log"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove/eval"
	"bytes"
	"io/ioutil"
	"sort"
	"fmt"
	"github.com/hscells/groove/cmd"
)

type args struct {
	Queries     string `arg:"help:Path to queries.,required"`
	FeatureFile string `arg:"help:File to output features to.,required"`
}

func (args) Version() string {
	return "Query Chain SVM (qcsvm) 23.Jan.2018"
}

func (args) Description() string {
	return `Train an SVM model for predicting query chain transformations.`
}

func main() {
	// Parse the command line arguments.
	var args args
	arg.MustParse(&args)

	queries := make(chan cmd.Query)

	rank := make(map[int64][]rewrite.LearntFeature)

	go func() {
		for {
			q, more := <-queries
			if more {
				if q.Error != nil {
					log.Fatal(q.Error)
				}

				lf := rewrite.LearntFeature{
					FeatureFamily: q.Query.Candidate.FeatureFamily,
					Score:         q.Query.Eval[eval.PrecisionEvaluator.Name()],
				}

				rank[q.Query.Topic] = append(rank[q.Query.Topic], lf)
			} else {
				return
			}
		}
	}()

	cmd.LoadQueries(args.Queries, queries)

	topics := make([]int64, len(rank))
	i := 0
	for k := range rank {
		q := rank[k]
		sort.Slice(q, func(i, j int) bool {
			return q[i].Score > q[j].Score
		})

		for _, lf := range q {
			sort.Slice(lf.FeatureFamily, func(i, j int) bool {
				return lf.FeatureFamily[i].Id+lf.FeatureFamily[i].Index < lf.FeatureFamily[j].Id+lf.FeatureFamily[j].Index
			})
		}

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
			lf.WriteLibSVMRank(buff, topics[i], fmt.Sprintf("%v_%v", topics[i], j+1))
		}
	}

	ioutil.WriteFile(args.FeatureFile, buff.Bytes(), 0644)

}
