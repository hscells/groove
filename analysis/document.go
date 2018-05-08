package analysis

import (
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
	"github.com/xtgo/set"
	"sort"
)

type docs []string

func (d docs) Len() int {
	return len(d)
}

func (d docs) Less(i, j int) bool {
	return d[i] < d[j]
}

func (d docs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// DocumentOverlap computes term overlap betweent the document and query.
type DocumentOverlap struct {
	document docs
}

// Name is the name for the document overlap measurement.
func (DocumentOverlap) Name() string {
	return "DocumentOverlap"
}

// Execute computes document overlap between query and document.
func (d DocumentOverlap) Execute(q groove.PipelineQuery, s stats.StatisticsSource) (float64, error) {
	t := docs(QueryTerms(q.Query))
	sort.Sort(t)
	sort.Sort(d.document)

	pivot := len(d.document)
	x := append(d.document, t...)
	size := set.Inter(docs(x), pivot)

	return float64(len(x[:size])) / float64(len(d.document)), nil
}

// NewDocumentOverlapMeasurement creates an overlap measurement.
func NewDocumentOverlapMeasurement(document []string) DocumentOverlap {
	return DocumentOverlap{
		document: docs(document),
	}
}
