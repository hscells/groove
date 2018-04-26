package rewrite

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/svmrank"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
)

type LTRQueryCandidateSelector struct {
	depth     int32
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

	sel.depth++
	f.Truncate(0)
	f.Seek(0, 0)
	err2 := os.Remove("tmp.features")
	if err2 != nil {
		return TransformedQuery{}, nil, err2
	}

	if err != nil {
		return TransformedQuery{}, nil, err
	}
	if candidate == nil {
		sel.depth = math.MaxInt32
		return query, sel, nil
	}
	if query.PipelineQuery.Query.String() == candidate.String() {
		sel.depth = math.MaxInt32
	}

	return query.Append(groove.NewPipelineQuery(query.PipelineQuery.Name, query.PipelineQuery.Topic, candidate)), sel, nil
}

func (sel LTRQueryCandidateSelector) StoppingCriteria() bool {
	return sel.depth >= 1
}

func NewLTRQueryCandidateSelector(modelFile string) LTRQueryCandidateSelector {
	return LTRQueryCandidateSelector{
		modelFile: modelFile,
	}
}
