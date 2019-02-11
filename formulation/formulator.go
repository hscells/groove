package formulation

import (
	"github.com/hscells/cqr"
)

// Formulator formulates queries to some specification.
type Formulator interface {
	Formulate() (cqr.CommonQueryRepresentation, error)
}

// TitleFormulator formulates queries using the title or string of a systematic review.
type TitleFormulator struct {
	title string

	LogicComposer
	EntityExtractor
	EntityExpander
	KeywordMapper
	postProcessing []PostProcess
}

func NewTitleFormulator(title string, logicComposer LogicComposer, entityExtractor EntityExtractor, entityExpander EntityExpander, keywordMapper KeywordMapper, postProcessing ...PostProcess) *TitleFormulator {
	return &TitleFormulator{title: title, LogicComposer: logicComposer, EntityExtractor: entityExtractor, EntityExpander: entityExpander, KeywordMapper: keywordMapper, postProcessing: postProcessing}
}

func (t TitleFormulator) Formulate() (cqr.CommonQueryRepresentation, error) {
	// Query Logic Composition.
	q, err := t.LogicComposer.Compose(t.title)
	if err != nil {
		return nil, err
	}

	// Entity Extraction.
	q, err = t.EntityExtractor.Extract(q)
	if err != nil {
		return nil, err
	}

	// Entity Expansion.
	if t.EntityExpander != nil {
		q, err = EntityExpansion(q, t.EntityExpander)
		if err != nil {
			return nil, err
		}
	}

	// Entities to Keywords Mapping.
	q, err = MapKeywords(q, t.KeywordMapper)
	if err != nil {
		return nil, err
	}

	// Post-Processing.
	for _, postProcessor := range t.postProcessing {
		q, err = postProcessor(q)
		if err != nil {
			return nil, err
		}
	}

	return q, nil
}
