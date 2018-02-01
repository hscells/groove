package main

import (
	"bytes"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/trecrun"
	"io/ioutil"
	"log"
	"os"
)

type args struct {
	Queries     string  `arg:"help:Path to queries.,required"`
	RunFile     string  `arg:"help:Path to trec_eval run file.,required"`
	Measure     string  `arg:"help:Measure to optimise.,required"`
	FeatureFile string  `arg:"help:File to output features to.,required"`
	N           float64 `arg:"help:Number of documents in collection (default is 26758795)."`
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

	if args.N > 0 {
		cmd.N = args.N
	}

	buff := bytes.NewBufferString("")

	f, err := os.Open(args.RunFile)
	if err != nil {
		log.Fatal(err)
	}
	rf, err := trecrun.RunsFromReader(f)
	if err != nil {
		log.Fatal(err)
	}

	queries := make(chan cmd.Query)

	topics := make(map[int64][]cmd.Feature)

	go func() {
		for {
			q, more := <-queries
			if more {
				if q.Error != nil {
					log.Fatal(q.Error)
				}

				score := 0.0

				if run, ok := rf.Runs[q.Query.Topic]; ok {
					score = cmd.ScoreMeasurement(args.Measure, q.Query.Eval, run)
				}

				lf := cmd.Feature{
					Topic:    q.Query.Topic,
					Depth:    q.Query.Depth,
					FileName: q.FileName,
					LearntFeature: rewrite.LearntFeature{
						FeatureFamily: q.Query.Candidate.FeatureFamily,
						Score:         score,
					},
				}
				topics[q.Query.Topic] = append(topics[q.Query.Topic], lf)
			} else {
				return
			}
		}
	}()

	cmd.LoadQueries(args.Queries, queries)

	for _, features := range topics {
		// Find the max depth that this query has.
		var maxDepth int64
		for _, f := range features {
			if f.Depth > maxDepth {
				maxDepth = f.Depth
			}
		}

		var ff rewrite.FeatureFamily
		for depth := 0; int64(depth) < maxDepth; depth++ {
			f := cmd.BestFeatureAt(int64(depth), features)
			ff = f.FeatureFamily
			fmt.Println(depth, ff)
			for i, f := range features {
				if f.Depth == int64(depth+1) {
					features[i].FeatureFamily = append(f.FeatureFamily, ff...)
				}
			}
		}
	}

	for _, features := range topics {
		for _, f := range features {
			f.WriteLibSVM(buff, f.FileName, f.Topic)
		}
	}

	ioutil.WriteFile(args.FeatureFile, buff.Bytes(), 0644)
}
