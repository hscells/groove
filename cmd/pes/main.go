package main

import (
	"encoding/json"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd/pes/pes"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"os"
)

var (
	name    = "pes"
	version = "25.Mar.2020"
	author  = "Harry Scells"
)

type args struct {
	Script string `help:"pes script to load" arg:"required,positional"`
}

func (args) Version() string {
	return version
}

func (args) Description() string {
	return fmt.Sprintf(`%s
@ %s
# %s`, name, author, version)
}

func main() {
	var args args
	arg.MustParse(&args)

	f, err := os.Open(args.Script)
	if err != nil {
		panic(err)
	}

	script, err := pes.Parse(f)
	if err != nil {
		panic(err)
	}

	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey(script.Statistic.Key),
		stats.EntrezEmail(script.Statistic.Email),
		stats.EntrezTool(script.Statistic.Tool),
		stats.EntrezOptions(stats.SearchOptions{Size: 10000}))
	if err != nil {
		panic(err)
	}

	var docs guru.MedlineDocuments
	for i := 0; i < len(script.PMIDS); i += 10000 {
		end := i + 10000
		if end >= len(script.PMIDS) {
			end = len(script.PMIDS) - 1
		}

		d, err := e.Fetch(script.PMIDS[i:end])
		if err != nil {
			panic(err)
		}
		docs = append(docs, d...)
	}

	titles := make(map[string]string, len(docs))
	for _, doc := range docs {
		titles[doc.PMID] = doc.TI
	}

	err = json.NewEncoder(os.Stdout).Encode(titles)
	if err != nil {
		panic(err)
	}
}
