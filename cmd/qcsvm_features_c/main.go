package main

import (
	"github.com/alexflint/go-arg"
	"log"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove/eval"
	"bytes"
	"io/ioutil"
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

	buff := bytes.NewBufferString("")

	go func() {
		for {
			q, more := <-queries
			if more {
				if q.Error != nil {
					log.Fatal(q.Error)
				}

				score := 0.0
				if q.Query.Eval[eval.NumRelRet.Name()] == q.Query.Eval[eval.NumRel.Name()] {
					score = 1.0
				}

				lf := rewrite.LearntFeature{
					FeatureFamily: q.Query.Candidate.FeatureFamily,
					Score:         score,
				}
				lf.WriteLibSVM(buff)
			} else {
				return
			}
		}
	}()

	LoadQueries(args.Queries, queries)

	ioutil.WriteFile(args.FeatureFile, buff.Bytes(), 0644)
}
