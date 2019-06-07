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
	"runtime"
	"sort"
	"strconv"
	"sync"
)

type Runner struct {
	cache   string
	queries []string
	fields  []string
	stats.EntrezStatisticsSource
	scorer Scorer
}

func index(query string, indexPath string, e stats.EntrezStatisticsSource) (*Posting, error) {
	h := sha256.New()
	h.Write([]byte(query))
	id := fmt.Sprintf("%x", h.Sum(nil))

	cachePath := path.Join(indexPath, id)

	var posting *Posting
	if _, err := os.Stat(cachePath); err == nil {
		fmt.Printf("found a cached copy for the cache %s\n", id)
		f, err := os.OpenFile(cachePath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
		err = gob.NewDecoder(f).Decode(&posting)
		if err != nil {
			return nil, err
		}
	} else {
		p, err := e.Search(query)
		if err != nil {
			return nil, err
		}
		pmids := make([]string, len(p))
		for i, pmid := range p {
			pmids[i] = strconv.Itoa(pmid)
		}

		var docs guru.MedlineDocuments
		sem := make(chan bool, 1)
		n := 10000
		bar := pb.New(len(pmids))
		bar.Start()
		for i, j := 0, n; i < len(pmids); i, j = i+n, j+n {
			sem <- true
			go func(k, l int) {
				defer func() { <-sem }()
				if l > len(pmids) {
					l = len(pmids) - 1
				}
				d, err := e.Fetch(p[k:l])
				if err != nil {
					panic(err)
				}
				docs = append(docs, d...)
				bar.Add(n)
			}(i, j)
		}

		// Wait until the last goroutine has read from the semaphore.
		for i := 0; i < cap(sem); i++ {
			sem <- true
		}
		bar.Finish()

		posting, err = Index(docs)
		if err != nil {
			return nil, err
		}

		fmt.Printf("caching a copy using id %s\n", id)
		err = os.MkdirAll(indexPath, 0777)
		if err != nil {
			return nil, err
		}
		f, err := os.OpenFile(cachePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		err = gob.NewEncoder(f).Encode(posting)
	}
	return posting, nil
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
		posting, err := index(query, indexPath, r.EntrezStatisticsSource)
		if err != nil {
			errc <- err
			return
		}
		r.scorer.posting(posting)
		r.scorer.entrez(r.EntrezStatisticsSource)

		fmt.Println("scoring documents")
		scored := make([]ScoredDocument, len(posting.DocLens))
		i := 0
		bar := pb.New(len(posting.DocLens))
		bar.Start()
		sem := make(chan bool, runtime.NumCPU())
		var mu sync.Mutex
		for pmid := range posting.DocLens {
			sem <- true
			go func(p string) {
				defer func() { <-sem }()
				score, err := r.scorer.Score(query, p, r.fields...)
				if err != nil {
					errc <- err
					return
				}
				mu.Lock()
				defer mu.Unlock()
				scored[i] = ScoredDocument{
					PMID:  p,
					Score: score,
				}
				i++
				bar.Increment()
				return
			}(pmid)
		}
		// Wait until the last goroutine has read from the semaphore.
		for i := 0; i < cap(sem); i++ {
			sem <- true
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
		if i == len(docs) {
			break
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
