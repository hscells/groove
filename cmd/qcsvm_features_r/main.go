// Package qcsvm_features_r creates a SVM rank file from queries produced as a by-product of the greedy query
// chain process in order to create rankers for query chains.
package main

import (
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd"
)

type args struct {
	Queries     string  `arg:"help:Path to queries.,required"`
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

	rank := cmd.LoadAllQueriesForRanking(args.Queries, args.Measure)
	cmd.WriteFeatures(rank, args.FeatureFile)
}
