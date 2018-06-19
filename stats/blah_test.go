package stats

import (
	"testing"
	"log"
)

func TestName(t *testing.T) {
	ss := NewEntrezStatisticsSource(
		EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		EntrezEmail("harryscells@gmail.com"),
		EntrezTool("groove"),
		EntrezOptions(SearchOptions{Size: 100000}))

	q := `((ORTHODONTICS[Mesh] OR orthodontic*[All Fields]) AND (((surg*[All Fields] ADJ4 (gingiv*[All Fields] OR periodont*[All Fields])) AND (retain[All Fields] OR retention[All Fields])) OR ("interproximal stripping"[All Fields] OR pericision[All Fields] OR reproximat*[All Fields] OR (stabilise*[All Fields] OR stabilize*[All Fields]) OR (fraenectom*[All Fields] OR frenectom*[All Fields]) OR (fiberotom*[All Fields] OR fibreotom*[All Fields]) OR (retention[All Fields] OR retain*[All Fields]))))`

	p, err := ss.search(q)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(len(p))
}
