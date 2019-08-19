package rank_test

import (
	"fmt"
	"github.com/hscells/groove/rank"
	"github.com/hscells/groove/stats"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezLimiter(60*time.Minute),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	runner := rank.NewRunner("groove_rank_test", []string{
		"(physical tests OR shoulder impingement OR local lesions of bursa tendon labrum OR accompany impingement)",
	}, []string{"ti"}, e, &rank.BM25Scorer{K1: 2, B: 0.75})

	scored, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}

	for _, docs := range scored {
		for _, doc := range docs.Docs[:10] {
			fmt.Println(doc.Rank, doc.Score, doc.PMID)
		}
	}

	fmt.Println("done!")
}
