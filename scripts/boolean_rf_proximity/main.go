package main

import (
	"fmt"
	"github.com/biogo/ncbi/entrez"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/learning/seed"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/jdkato/prose/v2"
	"log"
	"sort"
)

func main() {
	doc, err := prose.NewDocument(`Human papillomavirus testing versus repeat cytology for triage of minor cytological cervical lesions`)
	if err != nil {
		log.Fatal(err)
	}

	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		log.Fatal(err)
	}

	seen := make(map[string]bool)
	var concepts []string

	for _, tok := range doc.Tokens() {
		fmt.Println(tok.Text, tok.Tag)
		if _, ok := seen[tok.Text]; ok {
			continue
		}
		seen[tok.Text] = true
		switch tok.Tag {
		case "JJ", "JJR", "JJS", "NN", "NNP", "NNPS", "NNS",
			"RB", "RBR", "RBS", "RP", "VB", "VBD", "VBG",
			"VBN", "VPP", "VPZ", "VBP":
			concepts = append(concepts, tok.Text)
		default:
			continue
		}
	}

	query := cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{})
	for _, concept := range concepts {
		query.Children = append(query.Children, cqr.NewKeyword(concept, fields.TitleAbstract))
	}

	query = cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
		query,
		seed.SensitivityFilter,
	})
	fmt.Println(concepts)

	q, err := transmute.CompileCqr2PubMed(query)
	if err != nil {
		log.Fatal(err)
	}

	ids, err := e.Search(q, func(p *entrez.Parameters) {
		p.RetMax = 20
		p.Sort = "relevance"
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(ids)

	docs, err := e.Fetch(ids)
	if err != nil {
		log.Fatal(err)
	}

	scores := make([]float64, len(docs))
	docIDs := make([]string, len(docs))
	for i, v := range docs {
		docIDs[i] = v.ID
	}

	// Identify concept termWeights which define a grouping of concepts
	conceptWeights := make(map[string]map[string]float64) // TODO this comes from @dan.
	conceptWeights = map[string]map[string]float64{
		"human": {
			"human":          0,
			"papillomavirus": 1,
			"testing":        10,
		},
		"papillomavirus": {
			"human":          1,
			"papillomavirus": 0,
			"testing":        10,
		},
		"testing": {
			"human":          10,
			"papillomavirus": 10,
			"testing":        0,
		},
	}
	threshold := 5.0
	var (
		conceptGroupings [][]string
	)
	for conceptA, conceptBWeights := range conceptWeights {
		if conceptGroupings == nil {
			conceptGroupings = append(conceptGroupings, []string{conceptA})
		}
		// Determine if the next concept should be grouped with the current one.
		for conceptB, weight := range conceptBWeights {
			if conceptA == conceptB {
				continue
			}
			fmt.Println(conceptA, conceptB, weight)

			var idx int
			for j, grouping := range conceptGroupings {
				for _, concept := range grouping {
					if concept == conceptA {
						idx = j
					} else if concept == conceptB {
						goto skipConcept
					}
				}
			}

			if weight < threshold {
				conceptGroupings[idx] = append(conceptGroupings[idx], conceptB)
			} else {
				conceptGroupings = append(conceptGroupings, []string{conceptB})
				//i++
			}
		}
	skipConcept:
	}

	fmt.Println(conceptGroupings)
	lm, err := stats.NewLanguageModel(e, docIDs, scores, "TIAB")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("------")
	fmt.Println(lm.TermCount)

	type pair struct {
		term  string
		score float64
	}

	// Compute JM probability for each unique concept retrieved.
	var scoredDocumentTerms []pair
	for term := range lm.TermCount {
		scoredDocumentTerms = append(scoredDocumentTerms, pair{
			term,
			stats.JelinekMercerTermProbability(0.75)(*lm, term),
		})
	}

	// Weight the JM probability "score" with proximity
	termWeights := make(map[string]map[string]float64) // TODO this comes from @dan.
	rankedExpansions := make(map[int][]pair)
	for i, grouping := range conceptGroupings {
		for _, scored := range scoredDocumentTerms {
			score := scored.score
			for _, concept := range grouping {
				termWeights[concept] = make(map[string]float64)
				termWeights[concept][scored.term] = 1 //rand.Float64() * 10
				score *= termWeights[concept][scored.term]
			}
			rankedExpansions[i] = append(rankedExpansions[i], pair{
				scored.term,
				score,
			})
		}
	}

	// Display the candidate expansion concept rankings for each of the concepts from the query.
	for i, concepts := range conceptGroupings {
		sort.Slice(rankedExpansions[i], func(x, y int) bool {
			return rankedExpansions[i][x].score > rankedExpansions[i][y].score
		})
		fmt.Println(" + ", concepts)
		for j, rankedTerm := range rankedExpansions[i] {
			fmt.Println(" | ", j, rankedTerm.term, rankedTerm.score)
			if j > 20 {
				break
			}
		}
	}

}
