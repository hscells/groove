package cmd

import (
	"github.com/hscells/groove/rewrite"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"encoding/json"
	"path"
	"log"
)

// Query is used to communicate the deserialized query being sent over a channel.
type Query struct {
	FileName string
	Query    rewrite.LearntCandidateQuery
	Error    error
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
