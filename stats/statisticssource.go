// Package stats provides implementations of statistic sources.
package stats

import (
	"errors"
	"github.com/TimothyJones/trecresults"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"math"
	"strconv"
)

// SearchOptions are options that the statistics source will use for retrieval.
type SearchOptions struct {
	Size    int
	RunName string
}

// TermVectorTerm is a term inside a term vector.
type TermVectorTerm struct {
	DocumentFrequency  float64
	TotalTermFrequency float64
	TermFrequency      float64
	Term               string
}

// TermVector is a standard format for returning term vectors from statistic sources.
type TermVector []TermVectorTerm

// TermProbability returns a term probability for a term in a language model.
type TermProbability func(model LanguageModel, term string) float64

// StatisticsSource represents the way statistics are calculated for a collection.
type StatisticsSource interface {
	SearchOptions() SearchOptions
	Parameters() map[string]float64

	TermFrequency(term, document string) (float64, error)
	TermVector(document string) (TermVector, error)

	DocumentFrequency(term string) (float64, error)
	TotalTermFrequency(term string) (float64, error)
	InverseDocumentFrequency(term string) (float64, error)
	RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error)
	VocabularySize() (float64, error)
	Execute(query groove.PipelineQuery, options SearchOptions) (trecresults.ResultList, error)
}

func GetDocumentIDs(query groove.PipelineQuery, ss StatisticsSource) ([]uint32, error) {
	var docs []uint32

	// Elasticsearch has a "fast" execute to scroll quickly so we can account for that here.
	switch x := ss.(type) {
	case *ElasticsearchStatisticsSource:
		ids, err := x.ExecuteFast(query, x.SearchOptions())
		if err != nil {
			return nil, err
		}
		docs = make([]uint32, len(ids))
		for i, id := range ids {
			docs[i] = id
		}
	default: // By default, we can just perform a regular search on the other systems.
		results, err := x.Execute(query, x.SearchOptions())
		if err != nil {
			return nil, err
		}

		docs = make([]uint32, len(results))

		for i, result := range results {
			id, err := strconv.ParseInt(result.DocId, 10, 32)
			if err != nil {
				return nil, err
			}
			docs[i] = uint32(id)
		}
	}
	return docs, nil
}

// idf calculates inverse document frequency, or the ratio of of documents in the collection to the number of documents
// the term appears in, logarithmically smoothed.
func idf(N, nt float64) float64 {
	return math.Log((N + 1) / (nt + 1))
}

// LanguageModel is used for query likelihood statistics.
type LanguageModel struct {
	DocIds             []string
	Scores             []float64
	Weights            []float64
	TermCount          map[string]float64
	DocLen             float64
	StatisticsSource   StatisticsSource
	VocabularySize     float64
	TotalTermFrequency map[string]float64
}

// LanguageModelWeights configures a language model to use the specified weights.
func LanguageModelWeights(weights []float64) func(*LanguageModel) {
	return func(lm *LanguageModel) {
		lm.Weights = weights
		return
	}
}

// NewLanguageModel creates a new language model from a statistics source using the specified documents and scores for
// those documents. Optionally, the language model can use weights that can be configured through the functional
// arguments.
func NewLanguageModel(source StatisticsSource, docIds []string, scores []float64, options ...func(model *LanguageModel)) (*LanguageModel, error) {
	// Create the language model with default values.
	weights := new([]float64)
	lm := &LanguageModel{
		DocIds:             docIds,
		Scores:             scores,
		Weights:            *weights,
		StatisticsSource:   source,
		TermCount:          make(map[string]float64),
		TotalTermFrequency: make(map[string]float64),
		DocLen:             0,
	}

	// Pre-calculate the vocabulary size.
	vocab, err := lm.StatisticsSource.VocabularySize()
	if err != nil {
		return nil, err
	}
	lm.VocabularySize = vocab

	// Update the language model with any additional options (e.g. weights).
	for _, option := range options {
		option(lm)
	}

	// If the weights are still uninitialised, set all of the weights to one.
	if lm.Weights == nil || len(lm.Weights) == 0 {
		lm.Weights = make([]float64, len(lm.DocIds))
		for i := 0; i < len(lm.DocIds); i++ {
			lm.Weights[i] = 1.0
		}
	}

	// Assert that the length of all the values are the same.
	if len(docIds) != len(scores) && len(docIds) != len(lm.Weights) {
		return nil, errors.New("cannot create language model; the length of doc ids, scores, and weights must be the same")
	}

	// Update the term count map for all the docs and weights.
	for i := range lm.DocIds {
		lm.updateTermCountMap(lm.DocIds[i], lm.Weights[i])
	}

	return lm, nil
}

// updateTermCountMap updates the count of terms for the language model.
func (lm *LanguageModel) updateTermCountMap(docID string, weight float64) error {
	tv, err := lm.StatisticsSource.TermVector(docID)
	if err != nil {
		return err
	}
	for _, term := range tv {
		// Update term counts.
		if count, ok := lm.TermCount[term.Term]; ok {
			lm.TermCount[term.Term] += count + (term.TermFrequency * weight)
		} else {
			lm.TermCount[term.Term] = term.TermFrequency * weight
		}

		// Update term frequencies.
		if _, ok := lm.TotalTermFrequency[term.Term]; !ok {
			lm.TotalTermFrequency[term.Term] = term.TotalTermFrequency
		}

		// Update total doc length.
		lm.DocLen += term.TermFrequency * weight
	}
	return nil
}

// CollectionTermProbability is the term probability for the background language model.
func (lm *LanguageModel) CollectionTermProbability(term string) float64 {
	if ttf, ok := lm.TotalTermFrequency[term]; ok {
		return ttf / lm.VocabularySize
	}
	return 0.0
}

// DocumentTermProbability is the term probability for the document language model.
func (lm *LanguageModel) DocumentTermProbability(term string) float64 {
	if tf, ok := lm.TermCount[term]; ok {
		return tf / lm.DocLen
	}
	return 0.0
}

// KLDivergence computes the KLDivergence between the background collection and the
func (lm *LanguageModel) KLDivergence(lambda float64, probability TermProbability) (float64, error) {
	div := 0.0
	for term := range lm.TermCount {
		px := probability(*lm, term)
		qx := lm.CollectionTermProbability(term)
		div += px * math.Log(px/qx)
	}
	return div, nil
}

// JelinekMercerTermProbability computes Jelinek-Mercer probability for term in a language model.
func JelinekMercerTermProbability(lambda float64) TermProbability {
	return func(lm LanguageModel, term string) float64 {
		return (lambda * lm.DocumentTermProbability(term)) + (1-lambda)*lm.CollectionTermProbability(term)
	}
}

// DirichletTermProbability computes Dirichlet distribution for term in a language model.
func DirichletTermProbability(mu float64) TermProbability {
	return func(lm LanguageModel, term string) float64 {
		return (lm.TermCount[term] + (mu * lm.CollectionTermProbability(term))) / (lm.DocLen + mu)
	}
}
