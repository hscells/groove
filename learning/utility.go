package learning

import (
	"encoding/gob"
	"fmt"
	"github.com/hscells/cui2vec"
	"io"
	"math"
	"os"
	"sort"
)

type DivDistQueryCandidateSelector struct {
	depth     int
	stopDepth int // How far should the model go before stopping?
	modelName string
	model     divDistModel
}

func (u DivDistQueryCandidateSelector) closest(n float64, f []float64) int {
	if len(f) == 0 {
		return 0
	}
	var idx int
	c := f[0]
	for i, v := range f {
		if math.Abs(n-v) < math.Abs(n-c) {
			c = v
			idx = i
		}
	}
	return idx
}

type scoredDivergenceQuery struct {
	divergence float64
	query      CandidateQuery
}

func (u DivDistQueryCandidateSelector) Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	queries := make([]scoredDivergenceQuery, len(transformations))
	for k, t := range transformations {
		scores := t.Features.Scores(60)
		minDivergence := math.MaxFloat64
		divergencePredict := 0.0
		for i, f := range u.model.Features {
			for len(scores) < len(f.Scores(60)) {
				scores = append(scores, 0.0)
			}
			for len(f.Scores(60)) < len(scores) {
				scores = append(scores, 0.0)
			}
			distance, err := cui2vec.Cosine(scores, f.Scores(60))
			if err != nil {
				return CandidateQuery{}, nil, err
			}
			j := u.closest(distance, f.Scores(60))
			divergencePredict = u.model.Values[i][j].Divergence
			if divergencePredict < minDivergence {
				minDivergence = divergencePredict
			}
		}
		queries[k] = scoredDivergenceQuery{
			divergence: divergencePredict,
			query:      t,
		}
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].divergence < queries[j].divergence
	})

	u.depth++

	return queries[0].query, u, nil
}

func (u DivDistQueryCandidateSelector) maximumScore(topic string, lfs []LearntFeature) (float64, Features) {
	max := 0.0
	var features Features
	for _, f := range lfs {
		if f.Topic == topic {
			if f.Scores[0] > max {
				max = f.Scores[0]
				features = f.Features
			}
		}
	}
	return max, features
}

type divDist struct {
	Divergence float64
	Distance   float64
}

type divDistModel struct {
	Features []Features
	Values   [][]divDist
}

func (u DivDistQueryCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	var (
		topic      string
		maxScore   float64
		bestScores []float64
		dd         divDistModel
		idx        int
	)
	idx = -1
	for _, f := range lfs {
		// Update the topic, and therefore the expected utility.
		if f.Topic != topic {
			topic = f.Topic
			var vec Features
			maxScore, vec = u.maximumScore(topic, lfs)
			bestScores = vec.Scores(60)
			dd.Features = append(dd.Features, vec)
			dd.Values = append(dd.Values, []divDist{})
			idx++
			fmt.Printf("%s [%f]\n", f.Topic, maxScore)
		}

		score := f.Scores[0]
		divergence := maxScore - score
		scores := f.Features.Scores(60)

		distance, err := cui2vec.Cosine(bestScores, scores)
		if err != nil {
			return nil, err
		}

		dd.Values[idx] = append(dd.Values[idx], divDist{
			Divergence: divergence,
			Distance:   distance,
		})
	}
	u.model = dd

	f, err := os.OpenFile(u.modelName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	gob.Register(divDistModel{})
	err = gob.NewEncoder(f).Encode(dd)
	return nil, err
}

func (u DivDistQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (u DivDistQueryCandidateSelector) StoppingCriteria() bool {
	return u.depth >= u.stopDepth
}

func DivDistLoadModel(file string) func(c *DivDistQueryCandidateSelector) {
	return func(c *DivDistQueryCandidateSelector) {
		f, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		m := divDistModel{}
		gob.Register(divDistModel{})
		err = gob.NewDecoder(f).Decode(&m)
		if err != nil {
			panic(err)
		}
		c.model = m
	}
}

func DivDistModelName(file string) func(c *DivDistQueryCandidateSelector) {
	return func(c *DivDistQueryCandidateSelector) {
		c.modelName = file
	}
}

func DivDistDepth(depth int) func(c *DivDistQueryCandidateSelector) {
	return func(c *DivDistQueryCandidateSelector) {
		c.stopDepth = depth
	}
}

func NewDivDistCandidateSelector(options ...func(c *DivDistQueryCandidateSelector)) DivDistQueryCandidateSelector {
	u := &DivDistQueryCandidateSelector{}
	for _, o := range options {
		o(u)
	}
	return *u
}
