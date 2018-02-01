package cmd

import (
	"encoding/json"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/rewrite"
	"github.com/hscells/trecrun"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"log"
	"math"
	"path"
)

// Query is used to communicate the deserialized query being sent over a channel.
type Query struct {
	FileName string
	Query    rewrite.LearntCandidateQuery
	Error    error
}

type Feature struct {
	Topic    int64
	Depth    int64
	FileName string
	Eval     map[string]float64
	rewrite.LearntFeature
}

var (
	N float64 = 26758795
)

// Fb computes the F-beta measurement. This is converted from the Python function below:
//
// # F1 = 2 * (precision * recall) / (precision + recall)
// # 3, 1, 0.5
// f_b = lambda p, r, b: (1 + math.pow(b, 2)) * (p * r) / ((math.pow(b, 2) * p) + r) if (p + r) > 0 else 0
func Fb(p, r, b float64) float64 {
	if (p + r) > 0 {
		return (1 + math.Pow(b, 2)) * (p * r) / ((math.Pow(b, 2) * p) + r)
	}
	return 0
}

// WSS computes work saved over sampling. This is converted from the Python function below:
//
// # TN: total_col - num_ret - (num_rel - rel_ret)
// # FN: (num_rel - rel_ret)
// # WSS = (total_col - num_ret / total_colN) - (1 - recall)
// wss = lambda N, num_ret, rel_ret, recall: ((N - num_ret) / N) - (1 - recall)
func WSS(N, ret, recall float64) float64 {
	return ((N - ret) / N) - (1.0 - recall)
}

// ScoreMeasurement computes a score for a particular measurement.
//
// This method will return a float64 value of a 1 or a 0.
//
// measurement can be either `Precision`, `Recall`, `F05`, `F1`, `F3`, or `WSS`. Any other measurement specified will
// return a value of 0.
func ScoreMeasurement(measurement string, e map[string]float64, run trecrun.Run) float64 {
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
	f05 := Fb(precision, recall, 5)
	f1 := Fb(precision, recall, 1)
	f3 := Fb(precision, recall, 3)
	wss := WSS(N, ret, recall)

	switch measurement {
	default:
		return 0
	case "NumRelRet":
		if relRet >= e[eval.NumRelRet.Name()] && ret <= e[eval.NumRet.Name()] {
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
	case "F05":
		if f05 >= Fb(e[eval.PrecisionEvaluator.Name()], e[eval.RecallEvaluator.Name()], 0.5) {
			return 1
		}
	case "F1":
		if f1 >= Fb(e[eval.PrecisionEvaluator.Name()], e[eval.RecallEvaluator.Name()], 1) {
			return 1
		}
	case "F3":
		if f3 >= Fb(e[eval.PrecisionEvaluator.Name()], e[eval.RecallEvaluator.Name()], 3) {
			return 1
		}
	case "WSS":
		if wss >= WSS(N, e[eval.NumRet.Name()], e[eval.RecallEvaluator.Name()]) {
			return 1
		}
	}
	return 0
}

// GetMeasurement computes the specified measurement from a run.
//
// measurement can be either `Precision`, `Recall`, `F05`, `F1`, `F3`, or `WSS`. Any other measurement specified will
// return a value of 0.
func GetMeasurement(measurement string, run trecrun.Run) float64 {
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
	case "Precision":
		return precision
	case "Recall":
		return recall
	case "F05":
		return Fb(precision, recall, 0.5)
	case "F1":
		return Fb(precision, recall, 1)
	case "F3":
		return Fb(precision, recall, 3)
	case "WSS":
		return WSS(N, ret, recall)
	}
	return 0
}

// BestFeatureAt gets the best feature are the specified depth.
func BestFeatureAt(depth int64, features []Feature) Feature {
	var best Feature
	var score float64
	for _, feature := range features {
		if feature.Depth == depth {
			if feature.Score > score {
				best = feature
				score = feature.Score
			}
		}
	}
	return best
}

// BestQueryAt gets the best query at the specified depth.
func BestQueryAt(depth int64, queries []Query, measurement string) Query {
	var best Query
	var score float64
	for _, q := range queries {
		if q.Query.Depth == depth {
			if measurement == "F05" {
				f05 := Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 0.5)
				if f05 > score {
					best = q
					score = f05
				}
			} else if measurement == "F1" {
				f1 := Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 1)
				if f1 > score {
					best = q
					score = f1
				}
			} else if measurement == "F3" {
				f3 := Fb(q.Query.Eval[eval.PrecisionEvaluator.Name()], q.Query.Eval[eval.RecallEvaluator.Name()], 3)
				if f3 > score {
					best = q
					score = f3
				}
			} else if measurement == "WSS" {
				wss := WSS(N, q.Query.Eval[eval.NumRet.Name()], q.Query.Eval[eval.RecallEvaluator.Name()])
				if wss > score {
					best = q
					score = wss
				}
			} else {
				if q.Query.Eval[measurement] > score {
					best = q
					score = q.Query.Eval[measurement]
				}
			}
		}
	}
	return best
}

// LoadQueries loads queries in a directory. The queries are "lazy-loaded" as some directories may contain hundreds of
// thousands of queries.
//
// q must be passed in, and the receiver must switch on the type contained (error or learnt query candidate).
//
// This function is a bit of a hack in that it closes the channel, but take a look at qcsvm_features_c for an example
// of how to use it.
func LoadQueries(directory string, q chan Query) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		q <- ErrorQuery(err)
	}

	for _, f := range files {
		log.Println(f.Name())
		source, err := ioutil.ReadFile(path.Join(directory, f.Name()))
		if err != nil {
			q <- ErrorQuery(err)
		}

		var m map[string]interface{}
		err = json.Unmarshal(source, &m)
		if err != nil {
			q <- ErrorQuery(err)
		}

		if _, ok := m["topic"]; ok {
			q <- ValueQuery(m, f.Name())
		}

	}
	close(q)
}

// ErrorQuery is a wrapper for an error.
func ErrorQuery(err error) Query {
	return Query{
		Error: err,
	}
}

// ValueQuery is a wrapper for a query. This method will actually construct a query from a map[string]interface{},
// since it contains a cqr.
func ValueQuery(m map[string]interface{}, filename string) Query {
	var ff rewrite.FeatureFamily
	for _, feature := range m["candidate"].(map[string]interface{})["FeatureFamily"].([]interface{}) {
		var f rewrite.Feature
		mapstructure.Decode(feature, &f)
		ff = append(ff, f)
	}

	ev := make(map[string]float64)
	for k, v := range m["eval"].(map[string]interface{}) {
		ev[k] = v.(float64)
	}

	lq := rewrite.LearntCandidateQuery{
		Topic: int64(m["topic"].(float64)),
		Depth: int64(m["depth"].(float64)),
		Eval:  ev,
		Candidate: rewrite.CandidateQuery{
			FeatureFamily: ff,
		},
	}
	return Query{
		FileName: filename,
		Query:    lq,
	}
}
