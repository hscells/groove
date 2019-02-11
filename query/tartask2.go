package query

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type TARTask2QueriesSource struct {
}

func (TARTask2QueriesSource) Load(directory string) ([]pipeline.Query, error) {
	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	// Next, read all files, generating queries for each file.
	var queries []pipeline.Query
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if len(f.Name()) == 0 {
			continue
		}

		source, err := ioutil.ReadFile(directory + "/" + f.Name())
		if err != nil {
			return nil, err
		}

		s := bufio.NewScanner(bytes.NewBuffer(source))
		n := 0
		lines := 0
		inQuery := false
		var (
			topic, title, query string
			q                   cqr.CommonQueryRepresentation
		)
		for s.Scan() {
			line := s.Text()
			if n == 3 {
				break
			}
			if (len(line) > 0 && line[0] == '\n') || len(line) == 0 {
				n++
				continue
			}
			if !inQuery {
				t := strings.Split(line, ":")
				if len(t) > 1 {
					switch t[0] {
					case "Topic":
						topic = strings.TrimSpace(strings.Join(t[1:], " "))
					case "Title":
						title = strings.TrimSpace(strings.Join(t[1:], " "))
					case "Query":
						inQuery = true
					}
				}
			} else {
				query += fmt.Sprintln(line)
				lines++
			}
		}
		fmt.Println(topic)
		query = strings.Replace(query, `“`, `"`, -1)
		query = strings.Replace(query, `”`, `"`, -1)
		if lines < 3 {
			fmt.Println("pubmed")
			q, _ = transmute.CompilePubmed2Cqr(query)
		} else {
			fmt.Println("medline")
			q, _ = transmute.CompileMedline2Cqr(query)
		}
		//fmt.Println(transmute.CompileCqr2Medline(q))
		queries = append(queries, pipeline.NewQuery(title, topic, q))
	}
	return queries, nil
}

func simplifyOriginal(query cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		if len(q.Fields) == 1 {
			switch q.Fields[0] {
			case fields.MeshHeadings, fields.MeSHMajorTopic, fields.MeSHSubheading, fields.MeSHTerms, fields.FloatingMeshHeadings, fields.MajorFocusMeshHeading:
				return nil
			}
		} else {
			q.Fields = []string{fields.TitleAbstract}
		}
		return q
	case cqr.BooleanQuery:
		var c []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			s := simplifyOriginal(child)
			if s != nil {
				c = append(c, s)
			}
		}
		q.Children = c
		return q
	}
	return query
}

func (t TARTask2QueriesSource) ReadAndWriteQueries(queriesDir, queriesBinFile, queryOutputDir string) []pipeline.Query {
	gob.Register(pipeline.Query{})
	gob.Register(cqr.BooleanQuery{})
	gob.Register(cqr.Keyword{})

	queries, err := t.Load(queriesDir)
	if err != nil {
		panic(err)
	}
	for _, q := range queries {
		q1, _ := transmute.CompileCqr2Medline(q.Query)
		err = os.MkdirAll(path.Join(queryOutputDir, "original"), 0777)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join(queryOutputDir, "original", q.Topic), []byte(q1), 0644)
		if err != nil {
			panic(err)
		}
		q2, _ := transmute.CompileCqr2Medline(simplifyOriginal(q.Query))
		err = os.MkdirAll(path.Join(queryOutputDir, "original_simplified"), 0777)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join(queryOutputDir, "original_simplified", q.Topic), []byte(q2), 0644)
		if err != nil {
			panic(err)
		}
	}
	f, err := os.OpenFile(queriesBinFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = gob.NewEncoder(f).Encode(queries)
	if err != nil {
		panic(err)
	}
	return queries
}

func (t TARTask2QueriesSource) LoadQueriesFromBin(queriesBinFile string) []pipeline.Query {
	gob.Register(pipeline.Query{})
	gob.Register(cqr.BooleanQuery{})
	gob.Register(cqr.Keyword{})

	f, err := os.OpenFile(queriesBinFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var queries []pipeline.Query
	err = gob.NewDecoder(f).Decode(&queries)
	if err != nil {
		panic(err)
	}
	return queries
}

/*
Topic: CD009551

Title: Polymerase chain reaction blood tests for the diagnosis of invasive aspergillosis in immunocompromised people

Query:
exp Aspergillosis/
exp Pulmonary Aspergillosis/
exp Aspergillus/
(aspergillosis or aspergillus or aspergilloma or "A.fumigatus" or "A. flavus" or "A. clavatus" or "A. terreus" or "A. niger").ti,ab.
or/1-4
exp Nucleic Acid Amplification Techniques/
pcr.ti,ab.
"polymerase chain reaction*".ti,ab.
or/6-8
5 and 9
exp Animals/ not Humans/
10 not 11

Pids:
    25815649
    26065322
    26047046
    26036769
    26028521
    26024441
    25960612
    25918347
    25917801
    25896772
    25883525
 */
