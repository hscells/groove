// Package qcsvm_train_c trains an SVM classifier for query chain rewriting.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/ewalker544/libsvm-go"
	"github.com/hscells/groove/cmd"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/trecrun"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
)

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

type query struct {
	Target     float64
	Prediction float64
	Query      string
}

type Result struct {
	Tp        int
	Tn        int
	Fp        int
	Fn        int
	Topic     int64
	Precision float64
	Recall    float64
	NumRel    float64
	NumRet    float64
	NumRelRet float64
	Measure   float64
	Queries   []query
}

func (r Result) String() string {
	return fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v", r.Topic, r.Tp, r.Tn, r.Fp, r.Fn, r.Precision, r.Recall, r.NumRet, r.NumRelRet, r.Measure)
}

// https://github.com/cjlin1/libsvm/blob/master/tools/grid.py
var (
	C     = param{1, 5, 2}
	Gamma = param{3, 1, -2}
)

func extractMeasures(reader io.Reader, directory string) (map[int64][]cmd.Query, error) {
	topics := make(map[int64][]cmd.Query)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		// Read the line.
		line := scanner.Text()
		l := strings.Split(line, "#")
		if len(l) != 2 {
			return nil, errors.New("line does not contain comment")
		}

		// Extract the info portion.
		info := strings.Split(strings.TrimSpace(l[1]), " ")
		if len(info) != 2 {
			return nil, errors.New("info does not contain two elements")
		}

		// Parse the info portion.
		filename := info[0]
		topic, err := strconv.ParseInt(info[1], 10, 64)
		if err != nil {
			return nil, err
		}

		// Open the file and read the measurement.
		source, err := ioutil.ReadFile(path.Join(directory, filename))
		if err != nil {
			return nil, err
		}

		var m map[string]interface{}
		err = json.Unmarshal(source, &m)
		if err != nil {
			return nil, err
		}

		// Marshall the query.
		q := cmd.ValueQuery(m, filename)

		// Append the measurement.
		topics[topic] = append(topics[topic], q)

	}
	return topics, nil
}

func setParam(c, gamma float64, problem *libSvm.Parameter) {
	problem.C = c
	problem.Gamma = gamma
}

func main() {
	// Parse the command line arguments.
	var args args
	arg.MustParse(&args)

	if args.N > 0 {
		cmd.N = args.N
	}

	// Load the query information.
	f, err := os.Open(args.FeatureFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	topics, err := extractMeasures(f, args.Queries)
	if err != nil {
		log.Fatal(err)
	}

	r, err := os.Open(args.RunFile)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	rf, err := trecrun.RunsFromReader(r)
	if err != nil {
		log.Fatal(err)
	}

	for topic, queries := range topics {
		var maxDepth int64
		for _, query := range queries {
			if query.Query.Depth > maxDepth {
				maxDepth = query.Query.Depth
			}
		}

		var ff rewrite.Features
		for depth := 0; int64(depth) < maxDepth; depth++ {
			query := cmd.BestQueryAt(int64(depth), queries, args.Measure)
			ff = query.Query.Candidate.Features
			for j, innerQuery := range queries {
				if innerQuery.Query.Depth == int64(depth+1) {
					topics[topic][j].Query.Candidate.Features = append(innerQuery.Query.Candidate.Features, ff...)
				}
			}
		}
	}

	for i := range topics {
		f, err := os.OpenFile(path.Join(args.FeatureDir, strconv.FormatInt(i, 10)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		buff := bytes.NewBufferString("")
		for k, queries := range topics {
			if k != i {
				for _, q := range queries {
					var score float64
					if run, ok := rf.Runs[q.Query.Topic]; ok {
						score = cmd.ScoreMeasurement(args.Measure, q.Query.Eval, run)
					} else {
						fmt.Println("Topic not found", q.Query.Topic)
					}

					rewrite.LearntFeature{q.Query.Candidate.Features, score}.WriteLibSVM(buff)
				}
			}
		}
		fmt.Println(i)
		f.Write(buff.Bytes())
		f.Close()
	}

	param := libSvm.NewParameter()
	param.SvmType = libSvm.C_SVC
	param.KernelType = libSvm.RBF
	param.NumCPU = runtime.NumCPU()

	results := make([]Result, len(topics))

	topicN := 0
	for topic, queries := range topics {
		bestMSQ := math.Inf(1)
		bestC, bestGamma := 1.0, 1.0
		gamma := 1.0

		log.Println(topic)
		// The set of all topics minus T_i.
		problem, err := libSvm.NewProblem(path.Join(args.FeatureDir, strconv.FormatInt(topic, 10)), param)
		if err != nil {
			log.Fatal(err)
		}

		for c := C.begin; c < C.end; c += C.step {
			//for gamma := Gamma.end; gamma < Gamma.begin; gamma -= Gamma.step {
			setParam(c, gamma, param)

			fmt.Printf("C: %v Gamma: %v\n", param.C, param.Gamma)

			targets := libSvm.CrossValidation(problem, param, 5)
			squareErr := libSvm.NewSquareErrorComputer()

			var i = 0
			for problem.Begin(); !problem.Done(); problem.Next() {
				y, _ := problem.GetLine()
				v := targets[i]
				squareErr.Sum(v, y)
				i++
			}

			fmt.Printf("Cross Validation Mean squared error = %.6g\n", squareErr.MeanSquareError())
			fmt.Printf("Cross Validation Squared correlation coefficient = %.6g\n", squareErr.SquareCorrelationCoeff())
			if squareErr.MeanSquareError() < bestMSQ {
				bestMSQ = squareErr.MeanSquareError()
				bestC = c
				bestGamma = gamma
			}
			//}
		}

		param.C = bestC
		param.Gamma = bestGamma

		model := libSvm.NewModel(param)
		model.Train(problem)

		measure := 0.0
		p, r := 0.0, 0.0
		ret, relRet, rel := 0.0, 0.0, 0.0
		tp, tn, fp, fn := 0, 0, 0, 0

		var selected []query
		for _, l := range queries {
			features := make(map[int]float64)
			for _, feature := range l.Query.Candidate.Features {
				features[int(rewrite.CompactFeatureSVM(feature.ID, feature.Index, feature.MaxFeatures))] = feature.Score
			}
			prediction := model.Predict(features)
			score := cmd.ScoreMeasurement(args.Measure, l.Query.Eval, rf.Runs[l.Query.Topic])

			var m float64
			if args.Measure == "F05" {
				m = cmd.Fb(l.Query.Eval[eval.PrecisionEvaluator.Name()], l.Query.Eval[eval.RecallEvaluator.Name()], 0.5)
			} else if args.Measure == "F1" {
				m = cmd.Fb(l.Query.Eval[eval.PrecisionEvaluator.Name()], l.Query.Eval[eval.RecallEvaluator.Name()], 1)
			} else if args.Measure == "F3" {
				m = cmd.Fb(l.Query.Eval[eval.PrecisionEvaluator.Name()], l.Query.Eval[eval.RecallEvaluator.Name()], 3)
			} else if args.Measure == "WSS" {
				m = cmd.WSS(cmd.N, l.Query.Eval[eval.NumRet.Name()], l.Query.Eval[eval.RecallEvaluator.Name()])
			} else {
				m = l.Query.Eval[args.Measure]
			}

			if prediction > 0 && score > 0 {
				tp++
				measure += m
				p += l.Query.Eval[eval.PrecisionEvaluator.Name()]
				r += l.Query.Eval[eval.RecallEvaluator.Name()]
				ret += l.Query.Eval[eval.NumRet.Name()]
				rel += l.Query.Eval[eval.NumRel.Name()]
				relRet += l.Query.Eval[eval.NumRelRet.Name()]
			} else if prediction > 0 && score < 1 {
				fp++
				measure += m
				p += l.Query.Eval[eval.PrecisionEvaluator.Name()]
				r += l.Query.Eval[eval.RecallEvaluator.Name()]
				ret += l.Query.Eval[eval.NumRet.Name()]
				rel += l.Query.Eval[eval.NumRel.Name()]
				relRet += l.Query.Eval[eval.NumRelRet.Name()]
			} else if prediction < 1 && score > 0 {
				fn++
			} else if prediction < 1 && score < 1 {
				tn++
			}

			selected = append(selected, query{score, prediction, l.FileName})
		}

		totalPositive := float64(tp + fp)

		precision := float64(tp) / float64(tp+fp)
		if tp+fp == 0 {
			precision = 0.0
		}

		recall := float64(tp) / float64(tp+fn)
		if float64(tp+fn) == 0 {
			recall = 0.0
		}

		m := float64(measure) / totalPositive
		P := p / totalPositive
		R := r / totalPositive
		Rel := rel / totalPositive
		Ret := ret / totalPositive
		RelRet := relRet / totalPositive
		if totalPositive == 0 {
			m = 0
			P = 0
			R = 0
			Ret = 0
			Rel = 0
			RelRet = 0
		}

		results[topicN] = Result{tp, tn, fp, fn,
			topic,
			P,
			R,
			Rel,
			Ret,
			RelRet,
			m,
			selected,
		}
		topicN++
		fmt.Println("--------------------------------------------------------")
		fmt.Printf("best msq: %v best c: %v best gamma: %v\n", bestMSQ, param.C, param.Gamma)
		fmt.Printf("topic:             %v\n", topic)
		fmt.Printf("true positives:    %v\n", tp)
		fmt.Printf("true negatives:    %v\n", tn)
		fmt.Printf("false positives:   %v\n", fp)
		fmt.Printf("false negatives:   %v\n", fn)
		fmt.Printf("total:             %v\n", tp+tn+fp+fn)
		fmt.Printf("precision:         %v\n", precision)
		fmt.Printf("recall:            %v\n", recall)
		fmt.Printf("measure:           %v\n", m)
		fmt.Printf("queries precision: %v\n", P)
		fmt.Printf("queries recall:    %v\n", R)
	}

	fmt.Println("Topic,Tp,Tn,Fp,Fn,Precision,Recall,NumRet,NumRelRet,Measure")
	for _, result := range results {
		fmt.Println(result)
	}

	precision := 0.0
	recall := 0.0
	accuracy := 0.0
	measure := 0.0
	for _, r := range results {
		precision += r.Precision
		recall += r.Recall
		measure += r.Measure
	}
	fmt.Println(measure/float64(len(results)), precision/float64(len(results)), recall/float64(len(results)), accuracy/float64(len(results)))

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
