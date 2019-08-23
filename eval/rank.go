package eval

import (
	"fmt"
	"github.com/hscells/trecresults"
	"math"
	"sort"
)

type DCG struct{ K int }
type NDCG struct{ K int }

var (
	AP = ap{}
)

type ap struct{}

func (e ap) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	R := NumRel.Score(results, qrels)
	var sum float64
	for i, res := range *results {
		if _, ok := qrels[res.DocId]; ok {
			if qrels[res.DocId].Score > RelevanceGrade {
				sum += PrecisionAtK{K: i + 1}.Score(results, qrels)
			}
		}
	}
	return sum / R
}

func (e ap) Name() string {
	return "AP"
}

func (e DCG) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	var score float64
	for i, item := range *results {
		// Compute DCG at a cutoff.
		if e.K != 0 && i >= e.K {
			break
		}
		if _, ok := qrels[item.DocId]; ok {
			score += float64(qrels[item.DocId].Score) / math.Log2(float64(i)+2)
		}
	}
	return score
}

func (e DCG) Name() string {
	return "DCG"
}

func (e NDCG) Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64 {
	// Compute ideal discounted cumulative gain.
	ideal := make(trecresults.ResultList, len(qrels))
	i := 0
	for _, rel := range qrels {
		ideal[i] = &trecresults.Result{
			Topic: rel.Topic,
			DocId: rel.DocId,
			Score: float64(rel.Score),
		}
		i++
	}
	sort.Slice(ideal, func(i, j int) bool {
		return ideal[i].Score > ideal[j].Score
	})

	dcg := DCG{K: e.K}.Score(results, qrels)
	idcg := DCG{K: e.K}.Score(&ideal, qrels)
	return dcg / idcg
}

func (e NDCG) Name() string {
	if e.K > 0 {
		return fmt.Sprintf("nDCG@%d", e.K)
	}
	return "nDCG"
}
