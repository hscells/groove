package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/cqr"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"io/ioutil"
	"os"
)

var (
	name    = "mesh_remover"
	version = "04.Feb.2020"
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

	m, kws, err := RemoveMeSH(q, args.Format)
	if err != nil {
		panic(err)
	}

	o, err := transmute.CompileCqr2PubMed(m)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.WriteString(o)
	if err != nil {
		panic(err)
	}

	for _, kw := range kws {
		s, err := transmute.CompileCqr2String(kw)
		if err != nil {
			panic(err)
		}
		_, err = os.Stdout.WriteString(fmt.Sprintf("%s\n", s))
		if err != nil {
			panic(err)
		}
	}
}

func RemoveMeSH(query string, format string) (cqr.CommonQueryRepresentation, []cqr.Keyword, error) {
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
		return nil, nil, err
	}
	return removeMeSH(q)
}

func removeMeSH(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, []cqr.Keyword, error) {
	var kw []cqr.Keyword
	switch q := query.(type) {
	case cqr.Keyword:
		for _, field := range q.Fields {
			// Remove the keyword if it is a MeSH subheading.
			switch field {
			case fields.MeshHeadings, fields.MeSHSubheading, fields.MeSHMajorTopic, fields.MeSHTerms, fields.MajorFocusMeshHeading, fields.FloatingMeshHeadings:
				kw = append(kw, q)
				return nil, kw, nil
			}
		}
		return q, kw, nil
	case cqr.BooleanQuery:
		var children []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			c, kws, _ := removeMeSH(child)
			if c != nil {
				children = append(children, c)
			}
			kw = append(kw, kws...)
		}
		q.Children = children
		return q, kw, nil
	default:
		return nil, nil, nil
	}
}
