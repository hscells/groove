package main

import (
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd/qrel_server/qrelrpc"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/output"
	"github.com/hscells/groove/retrieval"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/trecresults"
	"gonum.org/v1/gonum/stat"
	"log"
	"net/rpc"
	"os"
	"path"
	"strings"
)

var (
	name    = "entrez_eval"
	version = "06.Jan.2020"
	author  = "Harry Scells"
)

type args struct {
	RelevanceGrade   int64    `help:"Minimum level of relevance to consider" arg:"-l"`
	Evaluation       []string `help:"Which evaluation measures to use" arg:"-e,separate"`
	ResultHandlers   []string `help:"Which run handlers to use" arg:"-r,separate"`
	RunOutput        string   `help:"Name of processed run file" arg:"-o"`
	EvaluationOutput string   `help:"Name of results file" arg:"-q"`
	Summary          bool     `help:"Only output summary information" arg:"-s"`
	Topic            string   `help:"Topic to evaluate (only when loading qrels using RPC)" arg:"-t"`
	EstimateN        float64  `help:"Estimate number of documents" arg:"-n"`
	QrelsFile        string   `help:"Path to qrels file" arg:"required,positional"`
	RunFile          string   `help:"Path to run file" arg:"required,positional"`
}

func (args) Version() string {
	return version
}

func (args) Description() string {
	return fmt.Sprintf(`%s
@ %s
# %s`, name, author, version)
}

type config struct {
	Entrez struct {
		Email string `toml:"email"`
		Tool  string `toml:"tool"`
		Key   string `toml:"key"`
	} `toml:"entrez"`
}

func main() {
	var args args
	arg.MustParse(&args)

	if len(args.Evaluation) == 0 && len(args.ResultHandlers) == 0 {
		log.Fatalln("nothing to do, quitting")
		os.Exit(1)
	}

	dir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalln(err)
	}

	f, err := os.OpenFile(path.Join(dir, ".entrez_eval"), os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		log.Fatalln(err)
	}

	var c config
	_, err = toml.DecodeReader(f, &c)
	if err != nil {
		log.Fatalln(err)
	}

	resultsHandlers := make(map[string]retrieval.ResultsHandler)
	evaluationMeasures := make(map[string]eval.Evaluator)

	var N float64
	if args.EstimateN == 0 {
		e, err := stats.NewEntrezStatisticsSource(
			stats.EntrezTool(c.Entrez.Tool),
			stats.EntrezAPIKey(c.Entrez.Key),
			stats.EntrezEmail(c.Entrez.Email),
			stats.EntrezOptions(stats.SearchOptions{
				Size:    100000,
				RunName: "entrez_eval",
			}))
		if err != nil {
			log.Fatalln(err)
		}

		N, err = e.CollectionSize()
		if err != nil {
			log.Fatalln(err)
		}
		resultsHandlers["deduplicate"] = retrieval.NewDeduplicator(e)

	} else {
		N = args.EstimateN
	}

	evaluationMeasures["precision"] = eval.Precision
	evaluationMeasures["precision_res"] = eval.NewResidualEvaluator(eval.Precision)
	evaluationMeasures["precision_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.Precision)
	evaluationMeasures["recall"] = eval.Recall
	evaluationMeasures["recall_res"] = eval.NewResidualEvaluator(eval.Recall)
	evaluationMeasures["recall_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.Recall)
	evaluationMeasures["f1"] = eval.F1Measure
	evaluationMeasures["f1_res"] = eval.NewResidualEvaluator(eval.F1Measure)
	evaluationMeasures["f1_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.F1Measure)
	evaluationMeasures["f0.5"] = eval.F05Measure
	evaluationMeasures["f0.5_res"] = eval.NewResidualEvaluator(eval.F05Measure)
	evaluationMeasures["f0.5_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.F05Measure)
	evaluationMeasures["f3"] = eval.F3Measure
	evaluationMeasures["f3_res"] = eval.NewResidualEvaluator(eval.F3Measure)
	evaluationMeasures["f3_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.F3Measure)
	evaluationMeasures["nnr"] = eval.NNR
	evaluationMeasures["wss"] = eval.NewWSSEvaluator(N)
	evaluationMeasures["wss_res"] = eval.NewResidualEvaluator(eval.NewWSSEvaluator(N))
	evaluationMeasures["wss_mle"] = eval.NewMaximumLikelihoodEvaluator(eval.NewWSSEvaluator(N))
	evaluationMeasures["num_ret"] = eval.NumRet
	evaluationMeasures["num_rel"] = eval.NumRel
	evaluationMeasures["num_rel_ret"] = eval.NumRelRet
	evaluationMeasures["ap"] = eval.AP
	evaluationMeasures["p@10"] = eval.PrecisionAtK{K: 10}
	evaluationMeasures["p@1000"] = eval.PrecisionAtK{K: 1000}
	evaluationMeasures["ndcg"] = eval.NDCG{}
	evaluationMeasures["ndcg@5"] = eval.NDCG{K: 5}
	evaluationMeasures["ndcg@10"] = eval.NDCG{K: 10}
	evaluationMeasures["ndcg@100"] = eval.NDCG{K: 100}
	evaluationMeasures["ndcg@200"] = eval.NDCG{K: 200}
	evaluationMeasures["ndcg@500"] = eval.NDCG{K: 500}

	eval.RelevanceGrade = args.RelevanceGrade

	r, err := os.OpenFile(args.RunFile, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatalln(err)
	}

	var results *trecresults.ResultFile
	if strings.Contains(args.RunFile, ".xres") || strings.Contains(args.RunFile, ".xrun") {
		results, err = guru.ReadCompressedTrecResultFile(r)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		rr, err := trecresults.ResultsFromReader(r)
		if err != nil {
			log.Fatalln(err)
		}
		results = &rr
	}

	for topic, run := range results.Results {
		newTopic := ""
		if strings.Contains(topic, "_") {
			parts := strings.Split(topic, "_")
			newTopic = parts[len(parts)-1]
			newRun := make(trecresults.ResultList, len(run))
			copy(newRun, run)
			results.Results[newTopic] = newRun
			delete(results.Results, topic)
			fmt.Printf("whoops! topic %s has been corrected automatically to %s\n", topic, parts[len(parts)-1])
		} else {
			continue
		}
		// Also rename the topic in each element of thee run.
		for i, el := range run {
			if strings.Contains(el.Topic, "_") {
				run[i].Topic = newTopic
				run[i].RunName = newTopic
			}
		}
	}

	var qrels trecresults.QrelsFile
	if strings.Contains(args.QrelsFile, ":8004") {
		client, err := rpc.Dial("tcp", args.QrelsFile)
		if err != nil {
			log.Fatalln(err)
		}
		q := new(qrelrpc.Response)
		err = client.Call("QrelsRPC.GetQrels", args.Topic, &q)
		if err != nil {
			log.Fatalln(err)
		}
		qrels = q.Qrels
	} else {
		q, err := os.OpenFile(args.QrelsFile, os.O_RDONLY, 0664)
		if err != nil {
			log.Fatalln(err)
		}
		qrels, err = trecresults.QrelsFromReader(q)
		if err != nil {
			log.Fatalln(err)
		}
	}

	evaluation := make(map[string]map[string]float64)
	size := 0
	for k, v := range results.Results {
		// Process all the results handlers first.
		for _, h := range args.ResultHandlers {
			size += v.Len()
			if handler, ok := resultsHandlers[h]; ok {
				err := handler.Handle(&v)
				if err != nil {
					log.Fatalln(err)
				}
				results.Results[k] = v
			}
		}
		// Then move on to perform the evaluation.
		evaluation[k] = make(map[string]float64)
		for _, ev := range args.Evaluation {
			if m, ok := evaluationMeasures[ev]; ok {
				score := m.Score(&v, qrels.Qrels[k])
				evaluation[k][evaluationMeasures[ev].Name()] = score
			}
		}
	}

	if size > 0 {
		t, err := os.OpenFile(args.RunOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
		if err != nil {
			log.Fatalln(err)
		}

		l := make([]string, size)
		i := 0
		for _, list := range results.Results {
			for _, line := range list {
				l[i] = line.String()
			}
		}

		_, err = t.Write([]byte(strings.Join(l, "\n") + "\n"))
		if err != nil {
			log.Fatalln(err)
		}
	}

	if args.Summary {
		summary := make(map[string][]float64)
		for _, evals := range evaluation {
			for measure, value := range evals {
				summary[measure] = append(summary[measure], value)
			}
		}
		avgs := make(map[string]float64)
		for measure, values := range summary {
			avgs[measure] = stat.Mean(values, nil)
		}
		avgs["NumQ"] = float64(len(evaluation))
		v, err := json.Marshal(avgs)
		if err != nil {
			log.Fatalln(err)
		}
		if len(args.EvaluationOutput) > 0 {
			o, err := os.OpenFile(args.EvaluationOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
			if err != nil {
				log.Fatalln(err)
			}
			_, err = o.Write(v)
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			_, err = os.Stdout.Write(v)
			if err != nil {
				log.Fatalln(err)
			}
		}
	} else {
		v, err := output.JsonEvaluationFormatter(evaluation)
		if err != nil {
			log.Fatalln(err)
		}
		if len(args.EvaluationOutput) > 0 {
			o, err := os.OpenFile(args.EvaluationOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
			if err != nil {
				log.Fatalln(err)
			}
			_, err = o.WriteString(v)
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			_, err = os.Stdout.WriteString(v)
			if err != nil {
				log.Fatalln(err)
			}
		}
	}
}
