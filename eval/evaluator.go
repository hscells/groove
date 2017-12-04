package eval

import "github.com/TimothyJones/trecresults"

// Evaluator is an interface for evaluating a retrieved list of documents.
type Evaluator interface {
	Score(topic int64, results *trecresults.ResultList, qrel trecresults.Qrels) float64
	Name() string
}

// Evaluate scores documents using supplied evaluation measurements.
func Evaluate(evaluators []Evaluator, results *trecresults.ResultList, qrels trecresults.QrelsFile) map[int64]map[string]float64 {
	// First create a map of topic->results
	resultMap := map[int64]trecresults.ResultList{}
	for _, res := range *results {
		if r, ok := resultMap[res.Topic]; ok {
			resultMap[res.Topic] = append(r, res)
		} else {
			resultMap[res.Topic] = trecresults.ResultList{res}
		}
	}

	// Next create a map of topic->evaluator:score
	scores := map[int64]map[string]float64{}
	for topic, resultList := range resultMap {
		scores[topic] = map[string]float64{}
		for _, evaluator := range evaluators {
			scores[topic][evaluator.Name()] = evaluator.Score(topic, &resultList, qrels.Qrels[topic])
		}
	}

	return scores
}
