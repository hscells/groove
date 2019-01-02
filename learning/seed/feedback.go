package seed

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"gopkg.in/jdkato/prose.v2"
)

type PseudoRelevanceFeedbackQueryConstructor struct {
	clinicalQuestion string
	stats.StatisticsSource
}

func (c PseudoRelevanceFeedbackQueryConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
	doc, err := prose.NewDocument(c.clinicalQuestion)
	if err != nil {
		return nil, err
	}

	fmt.Println(doc.Entities())
	fmt.Println(doc.Sentences())
	fmt.Println(doc.Tokens())

	return nil, nil

}

func NewPseudoRelevanceFeedbackQueryConstructor(clinicalQuestion string) PseudoRelevanceFeedbackQueryConstructor {
	return PseudoRelevanceFeedbackQueryConstructor{
		clinicalQuestion: clinicalQuestion,
	}
}
