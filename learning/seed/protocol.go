package seed

import (
	"github.com/hscells/cqr"
	"gopkg.in/neurosnap/sentences.v1"
)

type ProtocolConstructor struct {
	objective        string
	participants     string
	indexTests       string
	targetConditions string
}

func (p ProtocolConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
	// This will store the seed queries.
	var queries []cqr.CommonQueryRepresentation

	// Extract cqr keywords from the protocol text.
	objectiveKeywords := MakeKeywords(p.objective, StopwordsEn, &sentences.DefaultPunctStrings{})
	participantsKeywords := MakeKeywords(p.participants, StopwordsEn, &sentences.DefaultPunctStrings{})
	indexTestsKeywords := MakeKeywords(p.indexTests, StopwordsEn, &sentences.DefaultPunctStrings{})
	targetConditionsKeywords := MakeKeywords(p.targetConditions, StopwordsEn, &sentences.DefaultPunctStrings{})

	objectiveQuery := cqr.NewBooleanQuery(cqr.OR, objectiveKeywords)
	participantsQuery := cqr.NewBooleanQuery(cqr.OR, participantsKeywords)
	indexTestsQuery := cqr.NewBooleanQuery(cqr.OR, indexTestsKeywords)
	targetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, targetConditionsKeywords)

	// Crate a series of valid Boolean queries from the extracted keywords.
	queries = append(queries, objectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, targetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, targetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{indexTestsQuery, targetConditionsQuery}))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, targetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery, targetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery, targetConditionsQuery}))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery, targetConditionsQuery}))

	// Next, generate a number of new queries all with the redundant keywords removed.
	var overlap []string
	for _, objectiveKeyword := range objectiveKeywords {
		for _, participantsKeyword := range participantsKeywords {
			for _, indexTestsKeyword := range indexTestsKeywords {
				for _, targetConditionsKeyword := range targetConditionsKeywords {
					if objectiveKeyword.String() == participantsKeyword.String() &&
						objectiveKeyword.String() == indexTestsKeyword.String() &&
						objectiveKeyword.String() == targetConditionsKeyword.String() {
						overlap = append(overlap, objectiveKeyword.String())
					}
				}
			}
		}
	}
	uniqueObjective := difference(objectiveKeywords, overlap)
	uniqueParticipants := difference(objectiveKeywords, overlap)
	uniqueIndexTests := difference(objectiveKeywords, overlap)
	uniqueTargetConditions := difference(objectiveKeywords, overlap)

	uniqueObjectiveQuery := cqr.NewBooleanQuery(cqr.OR, uniqueObjective)
	uniqueParticipantsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueParticipants)
	uniqueIndexTestsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueIndexTests)
	uniqueTargetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueTargetConditions)

	// Crate a series of valid Boolean queries from the unique keywords.
	queries = append(queries, uniqueObjectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueTargetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueTargetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueIndexTestsQuery, uniqueTargetConditionsQuery}))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueTargetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}))

	return queries, nil
}

func difference(keywords []cqr.CommonQueryRepresentation, overlap []string) []cqr.CommonQueryRepresentation {
	var unique []cqr.CommonQueryRepresentation
	for _, queryKeyword := range keywords {
		include := true
		for _, overlapKeyword := range overlap {
			if queryKeyword.String() == overlapKeyword {
				include = false
				break
			}
		}
		if include {
			unique = append(unique, queryKeyword)
		}
	}
	return unique
}

func NewProtocolConstructor(objective, participants, indexTests, targetConditions string) ProtocolConstructor {
	return ProtocolConstructor{
		objective:        objective,
		participants:     participants,
		indexTests:       indexTests,
		targetConditions: targetConditions,
	}
}
