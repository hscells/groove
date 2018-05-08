package probability

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"math"
)

// MeSHExplosionsRatio measures and predicts how mesh explosions affect retrieval performance.
type MeSHExplosionsRatio struct{}

var meshExplosionRatio = NewProbabilisticMeasurement(MeSHExplosionsRatio{})

// ComputeAdditionProbability NOT IMPLEMENTED.
func (MeSHExplosionsRatio) ComputeAdditionProbability(m float64) PredictionPair {
	return NewPredictionPair(-math.Log(m), 1 - -math.Log(m))
}

// ComputeReductionProbability NOT IMPLEMENTED.
func (MeSHExplosionsRatio) ComputeReductionProbability(m float64) PredictionPair {
	return NewPredictionPair(1 - -math.Log(m), -math.Log(m))
}

// Name NOT IMPLEMENTED.
func (MeSHExplosionsRatio) Name() string {
	return "MeSHExplosionsRatio"
}

func countExplodedMeSHTerms(query cqr.CommonQueryRepresentation) (numMeSH, numExpMeSH float64) {
	switch q := query.(type) {
	case cqr.Keyword:
		for _, field := range q.Fields {
			if field == "mesh_headings" {
				numMeSH++
			}
		}
		if exploded, ok := q.Options["exploded"].(bool); ok && exploded {
			numExpMeSH++
		}
		return
	case cqr.BooleanQuery:
		for _, child := range q.Children {
			x, y := countExplodedMeSHTerms(child)
			numMeSH += x
			numExpMeSH += y
		}
	}
	return
}

// Execute returns the ratio of exploded MeSH queries to the number of total MeSH queries in the search strategy.
func (MeSHExplosionsRatio) Execute(pq groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	numExpMeSH, numMeSH := countExplodedMeSHTerms(pq.Query)
	if numMeSH == 0 {
		return 0.0, nil
	}
	return numExpMeSH / numMeSH, nil
}
