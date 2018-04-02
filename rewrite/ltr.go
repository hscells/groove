package rewrite

import (
	"github.com/hscells/svmrank"
	"io/ioutil"
	"bytes"
	"bufio"
	"strconv"
	"os"
	"sort"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"fmt"
	"math"
)

type LTRQueryCandidateSelector struct {
	depth     int
	modelFile string
}

type ranking struct {
	rank  float64
	query cqr.CommonQueryRepresentation
}

func getRanking(filename string, candidates []CandidateQuery) (cqr.CommonQueryRepresentation, error) {
	if candidates == nil || len(candidates) == 0 {
		return nil, nil
	}

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(b))
	i := 0
	ranks := make([]ranking, len(candidates))
	for scanner.Scan() {
		r, err := strconv.ParseFloat(scanner.Text(), 64)
		if err != nil {
			return nil, err
		}
		fmt.Println(candidates[i])
		ranks[i] = ranking{
			r,
			candidates[i].Query,
		}
		i++
	}

	sort.Slice(ranks, func(i, j int) bool {
		return ranks[i].rank > ranks[j].rank
	})

	if len(ranks) == 0 {
		return nil, nil
	}

	return ranks[0].query, nil
}

func (sel LTRQueryCandidateSelector) Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error) {
	f, err := os.OpenFile("tmp.features", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, applied := range transformations {
		f.WriteString(fmt.Sprintf("%v%v", applied.Features.String(), "\n"))
	}
	svmrank.Predict("tmp.features", sel.modelFile, "tmp.output")
	candidate, err := getRanking("tmp.output", transformations)
	if err != nil {
		return TransformedQuery{}, nil, err
	}
	if candidate == nil {
		sel.depth = int(math.Inf(1))
		return query, sel, nil
	}

	sel.depth++
	f.Truncate(0)
	f.Seek(0, 0)
	err = os.Remove("tmp.output")
	if err != nil {
		return TransformedQuery{}, nil, err
	}
	return query.Append(groove.NewPipelineQuery(query.PipelineQuery.Name, query.PipelineQuery.Topic, candidate)), sel, nil
}

func (sel LTRQueryCandidateSelector) StoppingCriteria() bool {
	return sel.depth >= 5
}

func NewLTRQueryCandidateSelector(modelFile string) LTRQueryCandidateSelector {
	return LTRQueryCandidateSelector{
		modelFile: modelFile,
	}
}
