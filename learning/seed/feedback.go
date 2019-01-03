package seed

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"gopkg.in/jdkato/prose.v2"
	"strings"
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

func ngram(text []string, n int) (grams []string) {
	var curr []string
	var j int
	for j <= len(text)-n {
		for i := j; i < j+n; i++ {
			curr = append(curr, text[i])
		}
		grams = append(grams, strings.Join(curr, " "))
		curr = []string{}
		j++
	}
	return
}
