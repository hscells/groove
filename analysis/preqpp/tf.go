package preqpp

import (
	"fmt"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"strconv"
	"strings"
)

type TF struct{}

func (tf TF) Name() string {
	return "TermFrequency"
}

func (tf TF) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {

	e, ok := s.(stats.EntrezStatisticsSource)
	if !ok {
		return 0, nil
	}

	results, err := e.Execute(q, s.SearchOptions())
	if err != nil {
		return 0, err
	}
	pmids := make([]int, len(results))
	for i, result := range results {
		pmids[i], err = strconv.Atoi(result.DocId)
		if err != nil {
			return 0, err
		}
	}

	docs, err := e.Fetch(pmids)
	if err != nil {
		return 0, err
	}

	terms := analysis.QueryTerms(q.Query)
	sumTf := 0.0
	for _, term := range terms {
		for _, doc := range docs {
			if strings.Contains(term, fmt.Sprintf("%s. %s", doc.TI, doc.AB)) {
				sumTf++
			}
		}
	}
	return sumTf, nil
}
