package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/go-errors/errors"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/transmute"
	"io/ioutil"
	"os"
)

var (
	name    = "sherlock"
	version = "16.Jan.2020"
	author  = "Harry Scells"
)

type args struct {
	Query  string `help:"query to load" arg:"-q"`
	Format string `help:"format of the query (pubmed/medline)" arg:"-f"`
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

	f, err := os.OpenFile(args.Query, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	q := bytes.NewBuffer(b).String()

	m, err := Analyse(q, args.Format)
	if err != nil {
		panic(err)
	}

	o, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.Write(o)
	if err != nil {
		panic(err)
	}
}

func Analyse(query string, format string) ([]float64, error) {
	var q cqr.CommonQueryRepresentation
	var err error
	switch format {
	case "pubmed":
		q, err = transmute.CompilePubmed2Cqr(query)
	case "medline":
		q, err = transmute.CompileMedline2Cqr(query)
	default:
		err = errors.New("unrecognised format")
	}
	if err != nil {
		return nil, err
	}

	return analysis.NewMemoryMeasurementExecutor().Execute(pipeline.NewQuery("", "", q), nil,
		analysis.BooleanClauses,
		analysis.BooleanKeywords,
		analysis.BooleanFields,
		analysis.BooleanTruncated,
		analysis.BooleanNonAtomicClauses,
		analysis.BooleanFieldsAbstract,
		analysis.BooleanFieldsTitle,
		analysis.BooleanFieldsMeSH,
		analysis.BooleanFieldsOther,
		analysis.BooleanAndCount,
		analysis.BooleanOrCount,
		analysis.BooleanNotCount,
		analysis.MeshKeywordCount,
		analysis.MeshExplodedCount,
		analysis.MeshNonExplodedCount,
		analysis.MeshAvgDepth,
		analysis.MeshMaxDepth,
	)
}
