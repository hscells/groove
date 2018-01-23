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

	queries := make(chan Query)

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

	LoadQueries(args.Queries, queries)

	topics := make([]int64, len(rank))
	i := 0
	for k := range rank {
		q := rank[k]
		sort.Slice(q, func(i, j int) bool {
			return q[i].Score > q[j].Score
		})
		rank[k] = q
		topics[i] = k
		i++
	}

	buff := bytes.NewBufferString("")

	for i := 0; i < len(topics); i++ {
		for _, lf := range rank[topics[i]] {
			fmt.Println(topics[i], lf.Score)
			lf.WriteLibSVM(buff)
		}
	}

	ioutil.WriteFile(args.FeatureFile, buff.Bytes(), 0644)

}
