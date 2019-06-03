package rank

import (
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	skiplist "github.com/sean-public/fast-skiplist"
	"gopkg.in/cheggaaa/pb.v1"
	"os"
	"path"
	"sort"
	"strconv"
)

type Runner struct {
	cache   string
	queries []string
	fields  []string
	stats.EntrezStatisticsSource
	scorer Scorer
}

func (r Runner) run(done chan bool, scoredc chan ScoredDocuments, errc chan error) () {
	defer close(scoredc)
	defer close(errc)
	defer close(done)

	gob.Register(skiplist.SkipList{})
	gob.Register(Posting{})

	cd, err := os.UserCacheDir()
	if err != nil {
		errc <- err
		return
	}
	indexPath := path.Join(cd, r.cache)

	for _, query := range r.queries {
		h := sha256.New()
		h.Write([]byte(query))
		id := fmt.Sprintf("%x", h.Sum(nil))

		cachePath := path.Join(indexPath, id)

		var posting *Posting
		if _, err := os.Stat(cachePath); err == nil {
			fmt.Printf("found a cached copy for the cache %s\n", id)
			f, err := os.OpenFile(cachePath, os.O_RDONLY, 0644)
			if err != nil {
				errc <- err
				return
			}
			err = gob.NewDecoder(f).Decode(&posting)
			if err != nil {
				errc <- err
				return
			}
		} else {
			p, err := r.Search(query)
			if err != nil {
				errc <- err
				return
			}
			pmids := make([]string, len(p))
			for i, pmid := range p {
				pmids[i] = strconv.Itoa(pmid)
			}

			fmt.Println(len(pmids))

			var docs guru.MedlineDocuments
			sem := make(chan bool, 3)
			for i, j := 0, 10000; i < len(pmids); i, j = i+10000, j+10000 {
				sem <- true
				go func(k, l int) {
					defer func() { <-sem }()
					fmt.Printf("fetching %d-%d out of %d\n", k, l, len(pmids))
					if l > len(pmids) {
						l = len(pmids) - 1
					}
					d, err := r.Fetch(p[k:l])
					if err != nil {
						errc <- err
						return
					}
					docs = append(docs, d...)
					fmt.Printf("fetched %d-%d out of %d\n", k, l, len(pmids))
				}(i, j)
			}

			// Wait until the last goroutine has read from the semaphore.
			for i := 0; i < cap(sem); i++ {
				sem <- true
			}
			fmt.Println(len(docs))

			posting, err = Index(docs)
			if err != nil {
				errc <- err
				return
			}

			fmt.Println(len(posting.DocLens))

			fmt.Printf("caching a copy using id %s\n", id)
			err = os.MkdirAll(indexPath, 0777)
			if err != nil {
				errc <- err
				return
			}
			f, err := os.OpenFile(cachePath, os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				errc <- err
				return
			}
			err = gob.NewEncoder(f).Encode(posting)
		}

		r.scorer.posting(posting)
		r.scorer.entrez(r.EntrezStatisticsSource)

		fmt.Println("scoring documents")
		scored := make([]ScoredDocument, len(posting.DocLens))
		i := 0
		bar := pb.New(len(posting.DocLens))
		bar.Start()
		for pmid := range posting.DocLens {
			score, err := r.scorer.Score(query, pmid, r.fields...)
			if err != nil {
				errc <- err
				return
			}
			scored[i] = ScoredDocument{
				PMID:  pmid,
				Score: score,
			}
			i++
			bar.Increment()
		}
		bar.Finish()
		fmt.Println("sorting scored docs")
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].Score > scored[j].Score
		})
		for i := range scored {
			scored[i].Rank = float64(i + 1)
		}
		scoredc <- ScoredDocuments{
			Docs: scored,
		}
	}
	done <- true
	return
}

func (r Runner) Run() ([]ScoredDocuments, error) {
	docs := make([]ScoredDocuments, len(r.queries))
	i := 0
	scoredc := make(chan ScoredDocuments)
	errc := make(chan error, 1)
	done := make(chan bool, 1)
	go func() {
		r.run(done, scoredc, errc)
	}()
	running := true
	for running {
		select {
		case err := <-errc:
			return nil, err
		case s := <-scoredc:
			docs[i] = s
			i++
		case <-done:
			running = false
		}
	}
	return docs, nil
}

func NewRunner(cache string, queries, fields []string, e stats.EntrezStatisticsSource, scorer Scorer) Runner {
	return Runner{
		cache:                  cache,
		queries:                queries,
		fields:                 fields,
		EntrezStatisticsSource: e,
		scorer:                 scorer,
	}
}
