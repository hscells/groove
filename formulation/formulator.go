// Package formulation provides a library for automatically formulating queries.
package formulation

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/trecresults"
	"math/rand"
	"strconv"
)

// Formulator formulates queries to some specification.
type Formulator interface {
	Formulate() ([]cqr.CommonQueryRepresentation, []interface{}, error)
}

// ConceptualFormulator formulates queries using the title or string of a systematic review.
type ConceptualFormulator struct {
	title string

	LogicComposer
	EntityExtractor
	EntityExpander
	KeywordMapper
	postProcessing []PostProcess
}

// ObjectiveFormulator formulates queries according to the objective approach.
type ObjectiveFormulator struct {
	seed                                   int
	Folder, Pubdates, SemTypes, MetaMapURL string
	query                                  pipeline.Query
	s                                      stats.EntrezStatisticsSource
	qrels                                  trecresults.Qrels
	population                             []guru.MedlineDocument
	MeSHK                                  []int
	DevK, PopK                             []float64
}

func NewObjectiveFormulator(query pipeline.Query, s stats.EntrezStatisticsSource, qrels trecresults.Qrels, population []guru.MedlineDocument) *ObjectiveFormulator {
	return &ObjectiveFormulator{
		s:          s,
		qrels:      qrels,
		population: population,
		query:      query,
		DevK:       []float64{0.20},
		PopK:       []float64{0.02},
		MeSHK:      []int{20},
		//DevK:       []float64{0.05, 0.10, 0.15, 0.20, 0.25, 0.30},
		//PopK:       []float64{0.001, 0.01, 0.02, 0.05, 0.10, 0.20},
		//MeSHK:      []int{1, 5, 10, 15, 20, 25},
	}
}

func (o ObjectiveFormulator) Formulate() ([]cqr.CommonQueryRepresentation, []interface{}, error) {
	// Identify the relevant studies using relevance assessments.
	var docs []int
	var nonrel []*trecresults.Qrel
	for _, rel := range o.qrels {
		if rel.Score > 0 {
			v, err := strconv.Atoi(rel.DocId)
			if err != nil {
				panic(err)
			}
			docs = append(docs, v)
		} else {
			nonrel = append(nonrel, rel)
		}
	}

	if len(docs) <= 50 {
		return nil, nil, fmt.Errorf("not enough relevant studies (minimmum 50)")
	}

	// Fetch those relevant documents.
	test, err := fetchDocuments(docs, o.s)
	if err != nil {
		panic(err)
	}

	// Split the 'test' set into dev, val, and unseen.
	rand.Seed(1000)
	dev, val, unseen := splitTest(test)
	fmt.Println(len(dev), len(val), len(unseen))

	// Perform 'term frequency analysis' on the development set.
	devDF, err := termFrequencyAnalysis(dev)
	if err != nil {
		panic(err)
	}

	q1, q2, err := deriveQueries(devDF, dev, val, o.population, o.query, o.s, o.DevK, o.PopK, o.MeSHK, o.Folder, o.SemTypes, o.Pubdates, o.MetaMapURL)
	return []cqr.CommonQueryRepresentation{q1, q2}, []interface{}{unseen}, nil
}

func NewConceptualFormulator(title string, logicComposer LogicComposer, entityExtractor EntityExtractor, entityExpander EntityExpander, keywordMapper KeywordMapper, postProcessing ...PostProcess) *ConceptualFormulator {
	return &ConceptualFormulator{title: title, LogicComposer: logicComposer, EntityExtractor: entityExtractor, EntityExpander: entityExpander, KeywordMapper: keywordMapper, postProcessing: postProcessing}
}

func (t ConceptualFormulator) Formulate() ([]cqr.CommonQueryRepresentation, []interface{}, error) {
	// Query Logic Composition.
	q, err := t.LogicComposer.Compose(t.title)
	if err != nil {
		return nil, nil, err
	}

	// Entity Extraction.
	q, err = t.EntityExtractor.Extract(q)
	if err != nil {
		return nil, nil, err
	}

	// Entity Expansion.
	if t.EntityExpander != nil {
		q, err = EntityExpansion(q, t.EntityExpander)
		if err != nil {
			return nil, nil, err
		}
	}

	// Entities to Keywords Mapping.
	q, err = MapKeywords(q, t.KeywordMapper)
	if err != nil {
		return nil, nil, err
	}

	// Post-Processing.
	for _, postProcessor := range t.postProcessing {
		q, err = postProcessor(q)
		if err != nil {
			return nil, nil, err
		}
	}

	return []cqr.CommonQueryRepresentation{q}, nil, nil
}
