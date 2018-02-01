package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd"
	"github.com/hscells/trecrun"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
)

type result struct {
	Topic     int64
	Measure   float64
	Baseline  float64
	NumRet    float64
	NumRel    float64
	NumRelRet float64
}

type ranking struct {
	Rank float64
	cmd.Feature
}

type args struct {
	FeatureFile string  `arg:"help:File containing features.,required"`
	ResultFile  string  `arg:"help:File to output results to.,required"`
	Queries     string  `arg:"help:Path to queries.,required"`
	Measure     string  `arg:"help:Measure to optimise.,required"`
	RunFile     string  `arg:"help:Path to trec_eval run file.,required"`
	FeatureDir  string  `arg:"help:Directory to output features to.,required"`
	N           float64 `arg:"help:Number of documents in collection (default is 26758795)."`
}

func (args) Version() string {
	return "Query Chain SVM (qcsvm) 23.Jan.2018"
}

func (args) Description() string {
	return `Train an SVM model for predicting query chain transformations.`
}

type param struct {
	begin float64
	end   float64
	step  float64
}

var (
	C     = param{-5, 15, 0.5}
	Gamma = param{3, -15, -0.5}
)

func main() {
	// Parse the command line arguments.
	var args args
	arg.MustParse(&args)

	if args.N > 0 {
		cmd.N = args.N
	}

	topics := cmd.LoadAllQueriesForRanking(args.Queries, args.Measure)
	r, err := os.Open(args.RunFile)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	rf, err := trecrun.RunsFromReader(r)
	if err != nil {
		log.Fatal(err)
	}

	for i := range topics {
		testF, err := os.OpenFile(path.Join(args.FeatureDir, fmt.Sprintf("%v.%v", strconv.FormatInt(i, 10), "test")), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		testBuff := bytes.NewBufferString("")
		train := make(map[int64][]cmd.Feature)
		for k, queries := range topics {
			if k != i {
				train[k] = queries
			} else {
				for j, q := range cmd.SortFeatures(queries) {
					q.WriteLibSVMRank(testBuff, q.Topic, fmt.Sprintf("%v_%v %v", i, j+1, q.FileName))
				}
			}
		}

		cmd.WriteFeatures(train, path.Join(args.FeatureDir, fmt.Sprintf("%v.%v", strconv.FormatInt(i, 10), "train")))
		fmt.Println(i)
		testF.Write(testBuff.Bytes())
		testF.Close()
	}

	os.Exit(1)
	results := make([]result, len(topics))

	for topic, features := range topics {
		log.Println(topic)

		log.Println("learning")
		ex := exec.Command("svm_rank_learn", "-c", "1", path.Join(args.FeatureDir, fmt.Sprintf("%v.train", topic)), path.Join(args.FeatureDir, fmt.Sprintf("%v.model", topic)))
		err := ex.Run()
		if err != nil {
			log.Fatal(err)
		}

		log.Println("predicting")
		ex = exec.Command("svm_rank_classify", path.Join(args.FeatureDir, fmt.Sprintf("%v.test", topic)), path.Join(args.FeatureDir, fmt.Sprintf("%v.model", topic)), path.Join(args.FeatureDir, fmt.Sprintf("%v.predictions", topic)))
		err = ex.Run()
		if err != nil {
			log.Fatal(err)
		}

		log.Println("analysing")
		// Load the file containing the learnt feature.
		b, err := ioutil.ReadFile(path.Join(args.FeatureDir, fmt.Sprintf("%v.predictions", topic)))
		if err != nil {
			log.Fatal(err)
		}

		scanner := bufio.NewScanner(bytes.NewBuffer(b))
		i := 0
		ranks := make([]ranking, len(features))
		for scanner.Scan() {
			r, err := strconv.ParseFloat(scanner.Text(), 64)
			if err != nil {
				log.Fatal(err)
			}
			ranks[i] = ranking{
				r,
				features[i],
			}
			i++
		}

		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].Rank > ranks[j].Rank
		})

		measure := ranks[0].Score
		baseline := cmd.GetMeasurement(args.Measure, rf.Runs[topic])

		e := ranks[0].Eval
		results = append(results, result{topic, measure, baseline,
			e["NumRet"], e["NumRel"], e["NumRelRet"]})

		fmt.Println("--------------------------------------------------------")
		fmt.Printf("topic:                 %v\n", topic)
		fmt.Printf("baseline:              %v\n", baseline)
		fmt.Printf("measure:               %v\n", measure)
	}

	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		if err != nil {
			log.Fatal(err)
		}
	}
	err = ioutil.WriteFile(args.ResultFile, b, 0644)
	if err != nil {
		log.Fatal(err)
	}

}
