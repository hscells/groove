package main

import (
	"github.com/hscells/groove/rewrite"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"encoding/json"
	"path"
	"log"
)

type Query struct {
	Query rewrite.LearntCandidateQuery
	Error error
}

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
			q <- ValueQuery(m)
		}

	}
	close(q)
}

func ErrorQuery(err error) Query {
	return Query{
		Error: err,
	}
}

func ValueQuery(m map[string]interface{}) Query {
	var ff rewrite.FeatureFamily
	for _, feature := range m["candidate"].(map[string]interface{})["FeatureFamily"].([]interface{}) {
		var f rewrite.Feature
		mapstructure.Decode(feature, &f)
		ff = append(ff, f)
	}

	var ev map[string]float64
	mapstructure.Decode(m["eval"], &ev)

	lq := rewrite.LearntCandidateQuery{
		Topic: int64(m["topic"].(float64)),
		Depth: int64(m["depth"].(float64)),
		Eval:  ev,
		Candidate: rewrite.CandidateQuery{
			FeatureFamily: ff,
		},
	}
	return Query{
		Query: lq,
	}
}
