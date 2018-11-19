package seed

import (
	"github.com/hscells/cqr"
	"gopkg.in/neurosnap/sentences.v1"
	"log"
)

type ProtocolConstructor struct {
	objective        string
	participants     string
	indexTests       string
	targetConditions string
}

type ProtocolQueryType int

const ProtocolOption string = "protocol"

const (
	Objective = iota
	Participants
	IndexTests
	TargetConditions
	ObjectiveParticipants
	ObjectiveIndexTests
	ObjectiveTargetConditions
	ParticipantsIndexTests
	ParticipantsTargetConditions
	IndexTestsTargetConditions
	ObjectiveParticipantsIndexTests
	ObjectiveParticipantsTargetConditions
	ObjectiveIndexTestsTargetConditions
	ParticipantsIndexTestsTargetConditions
	ObjectiveParticipantsIndexTestsTargetConditions
	UniqueObjective
	UniqueParticipants
	UniqueIndexTests
	UniqueTargetConditions
	UniqueObjectiveParticipants
	UniqueObjectiveIndexTests
	UniqueObjectiveTargetConditions
	UniqueParticipantsIndexTests
	UniqueParticipantsTargetConditions
	UniqueIndexTestsTargetConditions
	UniqueObjectiveParticipantsIndexTests
	UniqueObjectiveParticipantsTargetConditions
	UniqueObjectiveIndexTestsTargetConditions
	UniqueParticipantsIndexTestsTargetConditions
	UniqueObjectiveParticipantsIndexTestsTargetConditions
)

func (p ProtocolConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
	// This will store the seed queries.
	var queries []cqr.CommonQueryRepresentation

	// Extract cqr keywords from the protocol text.
	objectiveKeywords := MakeKeywords(p.objective, StopwordsEn, &sentences.DefaultPunctStrings{})
	participantsKeywords := MakeKeywords(p.participants, StopwordsEn, &sentences.DefaultPunctStrings{})
	indexTestsKeywords := MakeKeywords(p.indexTests, StopwordsEn, &sentences.DefaultPunctStrings{})
	targetConditionsKeywords := MakeKeywords(p.targetConditions, StopwordsEn, &sentences.DefaultPunctStrings{})

	objectiveQuery := cqr.NewBooleanQuery(cqr.OR, objectiveKeywords).SetOption(ProtocolOption, Objective)
	participantsQuery := cqr.NewBooleanQuery(cqr.OR, participantsKeywords).SetOption(ProtocolOption, Participants)
	indexTestsQuery := cqr.NewBooleanQuery(cqr.OR, indexTestsKeywords).SetOption(ProtocolOption, IndexTests)
	targetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, targetConditionsKeywords).SetOption(ProtocolOption, TargetConditions)

	// Crate a series of valid Boolean queries from the extracted keywords.
	queries = append(queries, objectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery}).SetOption(ProtocolOption, ObjectiveParticipants))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery}).SetOption(ProtocolOption, ObjectiveIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery}).SetOption(ProtocolOption, ParticipantsIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ParticipantsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, IndexTestsTargetConditions))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveIndexTestsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ParticipantsIndexTestsTargetConditions))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsIndexTestsTargetConditions))

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
	uniqueParticipants := difference(participantsKeywords, overlap)
	uniqueIndexTests := difference(indexTestsKeywords, overlap)
	uniqueTargetConditions := difference(targetConditionsKeywords, overlap)

	uniqueObjectiveQuery := cqr.NewBooleanQuery(cqr.OR, uniqueObjective).SetOption(ProtocolOption, UniqueObjective)
	uniqueParticipantsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueParticipants).SetOption(ProtocolOption, UniqueParticipants)
	uniqueIndexTestsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueIndexTests).SetOption(ProtocolOption, UniqueIndexTests)
	uniqueTargetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueTargetConditions).SetOption(ProtocolOption, UniqueTargetConditions)

	// Crate a series of valid Boolean queries from the unique keywords.
	queries = append(queries, uniqueObjectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipants))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueObjectiveIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueParticipantsIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueParticipantsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueIndexTestsTargetConditions))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsIndexTests))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveIndexTestsTargetConditions))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueParticipantsIndexTestsTargetConditions))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsIndexTestsTargetConditions))

	return queries, nil
}

func difference(keywords []cqr.CommonQueryRepresentation, overlap []string) []cqr.CommonQueryRepresentation {
	var unique []cqr.CommonQueryRepresentation
	for _, queryKeyword := range keywords {
		include := true
		for _, overlapKeyword := range overlap {
			if queryKeyword.String() == overlapKeyword {
				log.Println(queryKeyword.String(), overlapKeyword)
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