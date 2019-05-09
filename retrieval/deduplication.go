package retrieval

import (
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/trecresults"
	"log"
	"strconv"
	"time"
)

// Deduplicator removes duplicate documents from a result list.
type Deduplicator struct {
	e stats.EntrezStatisticsSource
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (d Deduplicator) Handle(list *trecresults.ResultList) error {
	// Obtain the pmids from the result list.
	pmids := make([]int, list.Len())
	for i, doc := range *list {
		var err error
		pmids[i], err = strconv.Atoi(doc.DocId)
		if err != nil {
			return err
		}
	}

	log.Println("fetching documents")

	var docs []guru.MedlineDocument
reset:
	fetched, err := d.e.Fetch(pmids)
	if err != nil {
		log.Println(err)
		time.Sleep(5 * time.Second)
		goto reset
	}
	docs = append(docs, fetched...)
	log.Println("begin de-duplication")

	var removal []int
	for i := 0; i < len(docs); i++ {
		for j := 0; j < len(docs); j++ {
			if i == j {
				continue
			}
			if docs[i].TI == docs[j].TI {
				removal = append(removal, i)
			}
		}
	}

	a := make(trecresults.ResultList, list.Len())
	copy(a, *list)
	for i := 0; i < a.Len(); i++ {
		copy(a[i:], a[i+1:])
		a[len(a)-1] = nil // or the zero value of T
		a = a[:len(a)-1]
	}
	list = &a
	return nil
}

func NewDeduplicator(e stats.EntrezStatisticsSource) Deduplicator {
	return Deduplicator{
		e: e,
	}
}
