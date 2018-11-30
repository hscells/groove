package analysis

import (
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/meshexp"
	"github.com/hscells/transmute/fields"
	"log"
	"strings"
)

var MeSHTree, _ = meshexp.Default()

var (
	MeshKeywordCount     = meshKeywordCount{}
	MeshExplodedCount    = meshExplodedCount{}
	MeshNonExplodedCount = meshNonExplodedCount{}
	MeshAvgDepth         = meshAvgDepth{}
	MeshMaxDepth         = meshMaxDepth{}
)

func normalise(q string) string {
	q = strings.Replace(q, "*", "", -1)
	q = strings.Replace(q, `"`, "", -1)
	return q
}

type meshKeywordCount struct{}

func (meshKeywordCount) Name() string {
	return "MeshKeywordCount"
}

func (meshKeywordCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(KeywordsWithField(q.Query, fields.MeshHeadings))), nil
}

type meshExplodedCount struct{}

func (meshExplodedCount) Name() string {
	return "MeshExplodedKeywordCount"
}

func (meshExplodedCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(MeshExplodedKeywords(q.Query))), nil
}

type meshNonExplodedCount struct{}

func (meshNonExplodedCount) Name() string {
	return "MeshNonExplodedKeywordCount"
}

func (meshNonExplodedCount) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	return float64(len(MeshNonExplodedKeywords(q.Query))), nil
}

type meshAvgDepth struct{}

func (meshAvgDepth) Name() string {
	return "MeshAvgDepth"
}

func (meshAvgDepth) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	keywords := KeywordsWithField(q.Query, fields.MeshHeadings)
	if len(keywords) == 0 {
		return 0, nil
	}
	var sum int64
	for _, kw := range keywords {
		sum += MeSHTree.Depth(normalise(kw.QueryString))
	}
	return float64(sum) / float64(len(keywords)), nil
}

type meshMaxDepth struct{}

func (meshMaxDepth) Name() string {
	return "MeshMaxDepth"
}

func (meshMaxDepth) Execute(q pipeline.Query, s stats.StatisticsSource) (float64, error) {
	log.Println(q)
	keywords := KeywordsWithField(q.Query, fields.MeshHeadings)
	if len(keywords) == 0 {
		return 0, nil
	}
	var max int64
	for _, kw := range keywords {
		d := MeSHTree.Depth(normalise(kw.QueryString))
		if d > max {
			max = d
		}
	}
	return float64(max), nil
}
