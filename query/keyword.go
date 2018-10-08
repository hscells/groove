package query

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"io/ioutil"
)

// KeywordQuerySource is a source of queries that contain only one "string".
type KeywordQuerySource struct {
	fields []string
}

// Load takes a directory of queries and parses them "as is".
func (kw KeywordQuerySource) Load(directory string) ([]pipeline.Query, error) {
	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return []pipeline.Query{}, err
	}

	// Next, load each query into a CQR keyword query.
	queries := make([]pipeline.Query, len(files))
	for i, f := range files {
		source, err := ioutil.ReadFile(directory + "/" + f.Name())
		if err != nil {
			return []pipeline.Query{}, err
		}

		cqrQuery := cqr.Keyword{QueryString: string(source), Fields: kw.fields}

		topic := f.Name()

		queries[i] = pipeline.NewQuery(f.Name(), topic, cqrQuery)
	}

	// Finally, return the queries.
	return queries, nil
}

// NewKeywordQuerySource creates a new keyword query source with the specified fields.
func NewKeywordQuerySource(fields ...string) KeywordQuerySource {
	return KeywordQuerySource{fields: fields}
}
