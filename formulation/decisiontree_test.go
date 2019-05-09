package formulation

import (
	"fmt"
	"github.com/hscells/groove/stats"
	"testing"
)

func TestDT(t *testing.T) {
	pos := []int{18163419, 21512799, 21177034, 22753863, 18163419, 21512799, 21177034, 22753863, 19770596, 22310655, 11295901, 12438988, 16323384, 17515742, 21168116, 11043871, 10767816}
	neg := []int{6238007, 2245458, 8995049, 1962157, 23220809, 6238007, 2245458, 8995049, 11240079, 1962157, 9762745, 8650509, 21370761, 23411644, 9762745}

	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	positive, err := e.Fetch(pos)
	if err != nil {
		t.Fatal(err)
	}

	negative, err := e.Fetch(neg)
	if err != nil {
		t.Fatal(err)
	}

	dt, err := NewDecisionTreeFormulator("61", positive, negative)
	if err != nil {
		t.Error(err)
	}

	queries, _, err := dt.Formulate()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(queries)
}
