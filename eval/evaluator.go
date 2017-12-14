package eval

import (
	"github.com/TimothyJones/trecresults"
)

// Evaluator is an interface for evaluating a retrieved list of documents.
type Evaluator interface {
	Score(results *trecresults.ResultList, qrels trecresults.Qrels) float64
	Name() string
}

// Evaluate scores documents using supplied evaluation measurements.
func Evaluate(evaluators []Evaluator, results *trecresults.ResultList, qrels trecresults.QrelsFile) map[int64]map[string]float64 {
	// First create a map of topic->results.
	resultMap := map[int64]trecresults.ResultList{}
	for _, res := range *results {
		if r, ok := resultMap[res.Topic]; ok {
			resultMap[res.Topic] = append(r, res)
		} else {
			resultMap[res.Topic] = trecresults.ResultList{res}
		}
	}

	// Next create a map of topic->evaluator:score.
	scores := map[int64]map[string]float64{}
	for topic, q := range qrels.Qrels {
		scores[topic] = map[string]float64{}
		// Since we care about all of the topics and not just the ones retrieved, we want to check if any documents
		// were retrieved for a document.
		if resultList, ok := resultMap[topic]; ok {
			for _, evaluator := range evaluators {
				scores[topic][evaluator.Name()] = evaluator.Score(&resultList, q)
			}
		} else {
			// If no documents were retrieved, we score with an empty list.
			for _, evaluator := range evaluators {
				scores[topic][evaluator.Name()] = evaluator.Score(&trecresults.ResultList{}, q)
			}
		}
	}
	return scores
}
