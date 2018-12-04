package seed

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/hscells/meshexp"
	"github.com/hscells/quickumlsrest"
	"github.com/hscells/transmute/fields"
)

type QuickUMLSProtocolConstructor struct {
	ProtocolConstructor
	similarityThreshold float64
	client              quickumlsrest.Client
	mesh                *meshexp.MeSHTree
	ss                  stats.StatisticsSource
	stopwords           []string
}

type queryFieldPair struct {
	query  string
	fields []string
}

func (c QuickUMLSProtocolConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
	text := []string{
		c.objective,
		c.participants,
		c.indexTests,
		c.targetConditions,
	}

	terms := make([][]queryFieldPair, 4)
	for i, t := range text {
		candidates, err := c.client.Match(t)
		if err != nil {
			return nil, err
		}

		seen := make(map[string]bool)
		for _, candidate := range candidates {
			if _, ok := seen[candidate.Term]; !ok {
				seen[candidate.Term] = true

				if candidate.Similarity < c.similarityThreshold {
					continue
				}

				found := false
				for _, w := range c.stopwords {
					if w == candidate.Term {
						found = true
						break
					}
				}
				if found {
					continue
				}

				var f string
				if c.mesh.Contains(candidate.Term) {
					f = fields.MeshHeadings
				} else {
					f = fields.TitleAbstract
				}
				terms[i] = append(terms[i], queryFieldPair{query: candidate.Term, fields: []string{f}})
			}
		}
	}

	objectiveKeywords := MakeKeywordsStructured(terms[0])
	participantKeywords := MakeKeywordsStructured(terms[1])
	indexTestsKeywords := MakeKeywordsStructured(terms[2])
	targetConditionsKeywords := MakeKeywordsStructured(terms[3])

	keywords := [][]cqr.CommonQueryRepresentation{objectiveKeywords, participantKeywords, indexTestsKeywords, targetConditionsKeywords}
	for i := range keywords {
		x := keywords[i]
		for j := 0; j < len(x); j++ {
			if v, err := c.ss.RetrievalSize(x[j]); err == nil && v == 0 {
				x = append(x[:j], x[j+1:]...)
			}
		}
		keywords[i] = x
	}

	return c.extractQueries(keywords[0], keywords[1], keywords[2], keywords[3], 2), nil
}

func NewQuickUMLSProtocolConstructor(objective, participants, indexTests, targetConditions, url string, threshold float64, ss stats.StatisticsSource) QuickUMLSProtocolConstructor {
	d, _ := meshexp.Default()

	return QuickUMLSProtocolConstructor{
		ProtocolConstructor: ProtocolConstructor{
			objective:        objective,
			participants:     participants,
			indexTests:       indexTests,
			targetConditions: targetConditions,
		},
		client:              quickumlsrest.NewClient(url),
		similarityThreshold: threshold,
		mesh:                d,
		ss:                  ss,
		stopwords:           StopwordsEn,
	}
}
