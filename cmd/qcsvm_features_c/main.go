package main

import (
	"github.com/alexflint/go-arg"
	"log"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/groove/eval"
	"bytes"
	"io/ioutil"
	"github.com/hscells/trecrun"
	"os"
	"github.com/hscells/groove/cmd"
)

type args struct {
	Queries     string `arg:"help:Path to queries.,required"`
	RunFile     string `arg:"help:Path to trec_eval run file.,required"`
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

	buff := bytes.NewBufferString("")

	f, err := os.Open(args.RunFile)
	if err != nil {
		log.Fatal(err)
	}
	rf, err := trecrun.RunsFromReader(f)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			q, more := <-queries
			if more {
				if q.Error != nil {
					log.Fatal(q.Error)
				}

				score := 0.0

				if run, ok := rf.Runs[q.Query.Topic]; ok {
					var (
						relRet float64
						ret    float64
					)
					if relRet, ok = run.Measurement["num_rel_ret"]; !ok {
						log.Fatalf("no num_rel_ret in for topic %v", q.Query.Topic)
					}
					if ret, ok = run.Measurement["num_rel"]; !ok {
						log.Fatalf("no num_rel in for topic %v", q.Query.Topic)
					}

					if q.Query.Eval[eval.NumRelRet.Name()] <= relRet && q.Query.Eval[eval.NumRet.Name()] >= ret {
						score = 1.0
					}
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

	cmd.LoadQueries(args.Queries, queries)

	ioutil.WriteFile(args.FeatureFile, buff.Bytes(), 0644)
}
