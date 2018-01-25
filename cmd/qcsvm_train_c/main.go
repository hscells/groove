package main

import (
	"github.com/alexflint/go-arg"
	"log"
	"github.com/ewalker544/libsvm-go"
	"fmt"
	"math"
	"runtime"
	"io"
	"bufio"
	"strings"
	"errors"
	"strconv"
	"io/ioutil"
	"path"
	"encoding/json"
	"github.com/hscells/groove/cmd"
	"os"
	"bytes"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/trecrun"
	"github.com/hscells/groove/eval"
)

type args struct {
	FeatureFile string `arg:"help:File containing features.,required"`
	ResultFile  string `arg:"help:File to output results to.,required"`
	Queries     string `arg:"help:Path to queries.,required"`
	Measure     string `arg:"help:Measure to optimise.,required"`
	RunFile     string `arg:"help:Path to trec_eval run file.,required"`
	FeatureDir  string `arg:"help:Directory to output features to.,required"`
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

type result struct {
	Tp        int
	Tn        int
	Fp        int
	Fn        int
	Topic     int64
	Precision float64
	Recall    float64
	Accuracy  float64
	Measure   float64
}

// https://github.com/cjlin1/libsvm/blob/master/tools/grid.py
var (
	C     = param{1, 7, 2}
	Gamma = param{5, 1, -2}
)

func toTrecEval(measurement string, e map[string]float64, run trecrun.Run) float64 {
	var (
		relRet float64
		ret    float64
		rel    float64
		ok     bool
	)
	if relRet, ok = run.Measurement["num_rel_ret"]; !ok {
		log.Fatalf("no num_rel_ret in for Topic %v", run.Topic)
	}
	if ret, ok = run.Measurement["num_ret"]; !ok {
		log.Fatalf("no num_rel in for Topic %v", run.Topic)
	}
	if rel, ok = run.Measurement["num_rel"]; !ok {
		log.Fatalf("no num_rel in for Topic %v", run.Topic)
	}

	precision := relRet / ret
	recall := relRet / rel

	switch measurement {
	default:
		return 0
	case "NumRelRet":
		if e[eval.NumRelRet.Name()] <= relRet && e[eval.NumRet.Name()] >= ret {
			return 1
		}
	case "Precision":
		if precision >= e[eval.PrecisionEvaluator.Name()] {
			return 1
		}
	case "Recall":
		if recall >= e[eval.RecallEvaluator.Name()] {
			return 1
		}
	}
	return 0
}

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

	for i := range topics {
		f, err := os.OpenFile(path.Join(args.FeatureDir, strconv.FormatInt(i, 10)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		buff := bytes.NewBufferString("")
		for k, v := range topics {
			if k != i {
				for _, q := range v {
					var score float64
					if run, ok := rf.Runs[q.Query.Topic]; ok {
						score = toTrecEval(args.Measure, q.Query.Eval, run)
					} else {
						fmt.Println("Topic not found", q.Query.Topic)
					}
					rewrite.LearntFeature{q.Query.Candidate.FeatureFamily, score}.WriteLibSVM(buff)
				}
			}
		}
		f.Write(buff.Bytes())
		f.Close()
	}

	param := libSvm.NewParameter()
	param.SvmType = libSvm.C_SVC
	param.KernelType = libSvm.RBF
	param.NumCPU = runtime.NumCPU()

	bestMSQ := math.Inf(1)
	bestC, bestGamma := 0.0, 0.0

	results := make([]result, len(topics))

	topicN := 0
	for topic, v := range topics {
		log.Println(topic)
		// The set of all topics minus T_i.
		problem, err := libSvm.NewProblem(path.Join(args.FeatureDir, strconv.FormatInt(topic, 10)), param)
		if err != nil {
			log.Fatal(err)
		}

		for c := C.begin; c < C.end; c += C.step {
			for gamma := Gamma.end; gamma < Gamma.begin; gamma -= Gamma.step {
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
			}
		}

		param.C = bestC
		param.Gamma = bestGamma

		model := libSvm.NewModel(param)
		model.Train(problem)

		measure := 0.0
		actual := 0.0
		tp, tn, fp, fn := 0, 0, 0, 0
		for _, l := range v {
			features := make(map[int]float64)
			for _, feature := range l.Query.Candidate.FeatureFamily {
				features[int(rewrite.CompactFeatureSVM(feature.Id, feature.Index, feature.MaxFeatures))] = feature.Score
			}
			prediction := model.Predict(features)
			score := toTrecEval(args.Measure, l.Query.Eval, rf.Runs[l.Query.Topic])
			if prediction > 0 && score > 0 {
				tp++
				measure += l.Query.Eval[args.Measure]
			} else if prediction > 0 && score < 1 {
				fp++
				measure += l.Query.Eval[args.Measure]
			} else if prediction < 1 && score > 0 {
				fn++
			} else if prediction < 1 && score < 1 {
				tn++
			}
			actual += l.Query.Eval[args.Measure]
		}

		results[topicN] = result{tp, tn, fp, fn, topic,
			float64(tp) / float64(tp+fp),
			float64(tp) / float64(tp+fn),
			float64(tp+tn) / float64(tp+tn+fp+fn),
			float64(measure) / float64(tp+tn+fp+fn),
		}
		fmt.Println("--------------------------------------------------------")
		fmt.Printf("best msq: %v best c: %v best gamma: %v\n", bestMSQ, param.C, param.Gamma)
		fmt.Printf("topic:                 %v\n", topic)
		fmt.Printf("true positives:        %v\n", tp)
		fmt.Printf("true negatives:        %v\n", tn)
		fmt.Printf("false positives:       %v\n", fp)
		fmt.Printf("false negatives:       %v\n", fn)
		fmt.Printf("total:                 %v\n", tp+tn+fp+fn)
		fmt.Printf("accuracy:              %v\n", float64(tp+tn)/float64(tp+tn+fp+fn))
		fmt.Printf("precision:             %v\n", float64(tp)/float64(tp+fp))
		fmt.Printf("recall:                %v\n", float64(tp)/float64(tp+fn))
		fmt.Printf("measure:               %v\n", float64(measure)/float64(tp+tn+fp+fn))
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

	precision := 0.0
	recall := 0.0
	accuracy := 0.0
	measure := 0.0
	for _, r := range results {
		precision += r.Precision
		recall += r.Recall
		accuracy += r.Accuracy
		measure += r.Measure
	}
	fmt.Println(measure/float64(len(results)), precision/float64(len(results)), recall/float64(len(results)), accuracy/float64(len(results)))
}
