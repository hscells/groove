package seed

import (
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/fields"
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
	Objective ProtocolQueryType = iota
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

const last = UniqueObjectiveParticipantsIndexTestsTargetConditions

var (
	/*
	#1 randomized controlled trial [pt]
	#2 controlled clinical trial [pt]
	#3 randomized [tiab]
	#4 placebo [tiab]
	#5 drug therapy [sh]
	#6 randomly [tiab]
	#7 trial [tiab]
	#8 groups [tiab]
	#9 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7 OR #8
	#10 animals [mh] NOT humans [mh]
	#11 #9 NOT #10
	 */
	sensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("drug therapy", fields.FloatingMeshHeadings),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.TitleAbstract),
			cqr.NewKeyword("groups", fields.TitleAbstract),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})

	/*
	#1 randomized controlled trial [pt]
	#2 controlled clinical trial [pt]
	#3 randomized [tiab]
	#4 placebo [tiab]
	#5 clinical trials as topic [mesh: noexp]
	#6 randomly [tiab]
	#7 trial [ti]
	#8 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7
	#9 animals [mh] NOT humans [mh]
	#10 #8 NOT #9
	 */
	precisionSensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("clinical trials as topic", fields.MeshHeadings).SetOption(cqr.ExplodedString, false),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.Title),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})
)

func (p ProtocolConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
	// Extract cqr keywords from the protocol text.
	objectiveKeywords := MakeKeywords(p.objective, StopwordsEn, &sentences.DefaultPunctStrings{})
	participantsKeywords := MakeKeywords(p.participants, StopwordsEn, &sentences.DefaultPunctStrings{})
	indexTestsKeywords := MakeKeywords(p.indexTests, StopwordsEn, &sentences.DefaultPunctStrings{})
	targetConditionsKeywords := MakeKeywords(p.targetConditions, StopwordsEn, &sentences.DefaultPunctStrings{})

	return p.extractQueries(objectiveKeywords, participantsKeywords, indexTestsKeywords, targetConditionsKeywords, 1), nil
}

func (p ProtocolConstructor) extractQueries(objectiveKeywords []cqr.CommonQueryRepresentation, participantsKeywords []cqr.CommonQueryRepresentation, indexTestsKeywords []cqr.CommonQueryRepresentation, targetConditionsKeywords []cqr.CommonQueryRepresentation, offset ProtocolQueryType) []cqr.CommonQueryRepresentation {
	var queries []cqr.CommonQueryRepresentation

	objectiveQuery := cqr.NewBooleanQuery(cqr.OR, objectiveKeywords).SetOption(ProtocolOption, Objective*offset)
	participantsQuery := cqr.NewBooleanQuery(cqr.OR, participantsKeywords).SetOption(ProtocolOption, Participants*offset)
	indexTestsQuery := cqr.NewBooleanQuery(cqr.OR, indexTestsKeywords).SetOption(ProtocolOption, IndexTests*offset)
	targetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, targetConditionsKeywords).SetOption(ProtocolOption, TargetConditions*offset)
	// Crate a series of valid Boolean queries from the extracted keywords.
	queries = append(queries, objectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery}).SetOption(ProtocolOption, ObjectiveParticipants*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery}).SetOption(ProtocolOption, ObjectiveIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery}).SetOption(ProtocolOption, ParticipantsIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ParticipantsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, IndexTestsTargetConditions*offset))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveIndexTestsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{participantsQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ParticipantsIndexTestsTargetConditions*offset))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{objectiveQuery, participantsQuery, indexTestsQuery, targetConditionsQuery}).SetOption(ProtocolOption, ObjectiveParticipantsIndexTestsTargetConditions*offset))
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
	uniqueObjectiveQuery := cqr.NewBooleanQuery(cqr.OR, uniqueObjective).SetOption(ProtocolOption, UniqueObjective*offset)
	uniqueParticipantsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueParticipants).SetOption(ProtocolOption, UniqueParticipants*offset)
	uniqueIndexTestsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueIndexTests).SetOption(ProtocolOption, UniqueIndexTests*offset)
	uniqueTargetConditionsQuery := cqr.NewBooleanQuery(cqr.OR, uniqueTargetConditions).SetOption(ProtocolOption, UniqueTargetConditions*offset)
	// Crate a series of valid Boolean queries from the unique keywords.
	queries = append(queries, uniqueObjectiveQuery)
	queries = append(queries, participantsQuery)
	queries = append(queries, indexTestsQuery)
	queries = append(queries, targetConditionsQuery)
	// Queries with two of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipants*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueObjectiveIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueParticipantsIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueParticipantsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueIndexTestsTargetConditions*offset))
	// Queries with three of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsIndexTests*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveIndexTestsTargetConditions*offset))
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueParticipantsIndexTestsTargetConditions*offset))
	// Final query with all four of the clauses.
	queries = append(queries, cqr.NewBooleanQuery("and", []cqr.CommonQueryRepresentation{uniqueObjectiveQuery, uniqueParticipantsQuery, uniqueIndexTestsQuery, uniqueTargetConditionsQuery}).SetOption(ProtocolOption, UniqueObjectiveParticipantsIndexTestsTargetConditions*offset))
	// Add the sensitivity-maximising rct filter:
	f1 := make([]cqr.CommonQueryRepresentation, len(queries))
	for i, q := range queries {
		f1[i] = cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
			sensitivityFilter,
			q,
		}).SetOption(ProtocolOption, int(last*offset)+i)
	}
	// Add the precision-and-sensitivity-maximising rct filter.
	f2 := make([]cqr.CommonQueryRepresentation, len(queries))
	for i, q := range queries {
		f2[i] = cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
			precisionSensitivityFilter,
			q,
		}).SetOption(ProtocolOption, int(last*offset*2)+i)
	}
	queries = append(queries, f1...)
	queries = append(queries, f2...)
	return queries
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
