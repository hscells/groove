package learning

import (
	"encoding/gob"
	"fmt"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/stats"
	"io"
	"log"
	"math"
	"os"
	"sort"
)

var (
	NNFeaturesN = ChainFeatures
)

type NearestNeighbourQueryCandidateSelector struct {
	depth     int
	stopDepth int // How far should the model go before stopping?
	modelName string
	model     divDistModel
	topics    map[string]float64
	s         stats.StatisticsSource
}

func (u NearestNeighbourQueryCandidateSelector) closest(n float64, f []float64) int {
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

func (u NearestNeighbourQueryCandidateSelector) Select(query CandidateQuery, transformations []CandidateQuery) (CandidateQuery, QueryChainCandidateSelector, error) {
	queries := make([]scoredDivergenceQuery, len(transformations))
	if _, ok := u.topics[query.Topic]; !ok {
		u.topics[query.Topic] = math.MaxFloat64
	}
	for k, t := range transformations {
		scores := t.Features.Scores(NNFeaturesN)
		divergencePredict := 0.0
		minDivergence := math.MaxFloat64
		minScore := 0.0

		for i, f := range u.model.Features {
			for len(scores) < len(f.Scores(NNFeaturesN)) {
				scores = append(scores, 0.0)
			}
			for len(f.Scores(NNFeaturesN)) < len(scores) {
				scores = append(scores, 0.0)
			}
			distance, err := cui2vec.Cosine(scores, f.Scores(NNFeaturesN))
			if err != nil {
				return CandidateQuery{}, nil, err
			}
			j := u.closest(distance, f.Scores(NNFeaturesN))
			divergencePredict = u.model.Values[i][j].Divergence
			if divergencePredict < minDivergence && u.model.Scores[i] > minScore {
				minDivergence = divergencePredict
				minScore = u.model.Scores[i]
			}
		}

		queries[k] = scoredDivergenceQuery{
			divergence: minDivergence,
			query:      t,
		}
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].divergence < queries[j].divergence
	})

	if len(queries) > 1 {
		log.Printf("best: %f, worst: %f\n", queries[0].divergence, queries[len(queries)-1].divergence)
	}

	ret, err := u.s.RetrievalSize(queries[0].query.Query)
	if err != nil {
		return CandidateQuery{}, nil, err
	}
	if ret == 0 {
		u.depth = u.stopDepth
		return query, u, nil
	}
	log.Printf("numret: %f\n", ret)

	prevDivergence := u.topics[query.Topic]
	if prevDivergence == math.MaxFloat64 {
		prevDivergence = queries[0].divergence
	}

	u.topics[query.Topic] = queries[0].divergence
	u.depth++

	log.Printf("previous div: %f, current div: %f\n", prevDivergence, queries[0].divergence)
	if prevDivergence > u.topics[query.Topic] {
		u.depth = u.stopDepth
		return query, u, nil
	}

	return queries[0].query, u, nil
}

func (u NearestNeighbourQueryCandidateSelector) maximumScore(topic string, lfs []LearntFeature) (float64, Features) {
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
	Scores   []float64
	Values   [][]divDist
}

func (u NearestNeighbourQueryCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
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
			if maxScore == 0 {
				continue
			}
			bestScores = vec.Scores(NNFeaturesN)
			dd.Features = append(dd.Features, vec)
			dd.Values = append(dd.Values, []divDist{})
			dd.Scores = append(dd.Scores, maxScore)
			idx++
			fmt.Printf("%s [%f]\n", f.Topic, maxScore)
		}

		score := f.Scores[0]
		divergence := maxScore - score
		scores := f.Features.Scores(NNFeaturesN)

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

func (u NearestNeighbourQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (u NearestNeighbourQueryCandidateSelector) StoppingCriteria() bool {
	return u.depth >= u.stopDepth
}

func NearestNeighbourLoadModel(file string) func(c *NearestNeighbourQueryCandidateSelector) {
	return func(c *NearestNeighbourQueryCandidateSelector) {
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

func NearestNeighbourModelName(file string) func(c *NearestNeighbourQueryCandidateSelector) {
	return func(c *NearestNeighbourQueryCandidateSelector) {
		c.modelName = file
	}
}

func NearestNeighbourDepth(depth int) func(c *NearestNeighbourQueryCandidateSelector) {
	return func(c *NearestNeighbourQueryCandidateSelector) {
		c.stopDepth = depth
	}
}

func NearestNeighbourStatisticsSource(s stats.StatisticsSource) func(c *NearestNeighbourQueryCandidateSelector) {
	return func(c *NearestNeighbourQueryCandidateSelector) {
		c.s = s
	}
}

func NewNearestNeighbourCandidateSelector(options ...func(c *NearestNeighbourQueryCandidateSelector)) NearestNeighbourQueryCandidateSelector {
	u := &NearestNeighbourQueryCandidateSelector{}
	for _, o := range options {
		o(u)
	}
	u.topics = make(map[string]float64)
	return *u
}
