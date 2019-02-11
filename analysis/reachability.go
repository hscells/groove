package analysis

import (
	"encoding/gob"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/guru"
	"github.com/hscells/metawrap"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type KeywordReachability struct {
	Title, Objectives, TypeOfStudy, Participants, IndexTests,
	TargetConditions, ReferenceStandards, Concepts int
	Topic string
}

type ConceptReachability struct {
	QueryCount, FieldCount int
	Overlap, OverlapRatio  float64
	Topic                  string
}

type conceptMapping map[string][]string

var textReg = regexp.MustCompile(`[*"]+`)

func StringMatchReachability(queries []pipeline.Query, protocols guru.Protocols) []KeywordReachability {
	var reach []KeywordReachability
	for _, q := range queries {
		var concepts []string

		keywords := QueryKeywords(q.Query)
		for _, keyword := range keywords {
			kw := textReg.ReplaceAllString(keyword.QueryString, "")
			concepts = append(concepts, kw)
		}

		var nT, nO, nC, nI, nP, nR, nS []string
		p := protocols[q.Topic]

		for _, concept := range concepts {
			c := strings.ToLower(concept)
			if len(c) == 0 {
				continue
			}
			if strings.Contains(strings.ToLower(q.Name), c) {
				nT = append(nT, c)
			}
			if strings.Contains(strings.ToLower(p.Objective), c) {
				nO = append(nO, c)
			}
			if strings.Contains(strings.ToLower(p.TargetConditions), c) {
				nC = append(nC, c)
			}
			if strings.Contains(strings.ToLower(p.IndexTests), c) {
				nI = append(nC, c)
			}
			if strings.Contains(strings.ToLower(p.Participants), c) {
				nP = append(nP, c)
			}
			if strings.Contains(strings.ToLower(p.ReferenceStandards), c) {
				nR = append(nR, c)
			}
			if strings.Contains(strings.ToLower(p.TypeOfStudy), c) {
				nS = append(nS, c)
			}
		}

		fmt.Println("  + title:              ", len(nT), float64(len(nT))/float64(len(concepts)))
		for _, c := range nT {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + objective:          ", len(nO), float64(len(nO))/float64(len(concepts)))
		for _, c := range nO {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + target conditions:  ", len(nC), float64(len(nC))/float64(len(concepts)))
		for _, c := range nC {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + index tests:        ", len(nI), float64(len(nI))/float64(len(concepts)))
		for _, c := range nI {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + participants:       ", len(nP), float64(len(nP))/float64(len(concepts)))
		for _, c := range nP {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + reference standards:", len(nR), float64(len(nR))/float64(len(concepts)))
		for _, c := range nR {
			fmt.Printf("  | %s\n", c)
		}
		fmt.Println("  + type of study:      ", len(nS), float64(len(nS))/float64(len(concepts)))
		for _, c := range nS {
			fmt.Printf("  | %s\n", c)
		}

		reach = append(reach, KeywordReachability{
			Title:              len(nT),
			Objectives:         len(nO),
			TargetConditions:   len(nC),
			IndexTests:         len(nI),
			Participants:       len(nP),
			ReferenceStandards: len(nR),
			TypeOfStudy:        len(nS),
			Concepts:           len(concepts),
			Topic:              q.Topic,
		})
	}
	return reach
}

func ConceptMatchReachability(queries []pipeline.Query, protocols guru.Protocols, conceptsBinFile string, client metawrap.HTTPClient) (conceptReachabilityMapping map[string][]ConceptReachability, conceptsNotInTitle map[string]map[string]int, err error) {
	// Load or create the concept mapping file.
	var cm conceptMapping
	if _, err = os.Stat(conceptsBinFile); err != nil && os.IsNotExist(err) {
		cm = make(conceptMapping)
	} else if err != nil {
		return
	} else {
		var f *os.File
		f, err = os.OpenFile(conceptsBinFile, os.O_RDONLY, 0644)
		if err != nil {
			return
		}
		err = gob.NewDecoder(f).Decode(&cm)
		if err != nil {
			return
		}
		f.Close()
	}

	conceptReachabilityMapping = make(map[string][]ConceptReachability)
	conceptsNotInTitle = make(map[string]map[string]int)

	for _, q := range queries {
		var queryConcepts []string
		seen := make(map[string]bool)

		keywords := QueryKeywords(q.Query)
		for _, keyword := range keywords {
			kw := strings.ToLower(textReg.ReplaceAllString(keyword.QueryString, ""))

			// Look the concept up in the cache.
			if c, ok := cm[kw]; ok {
				//fmt.Printf(" | * %s (%d)\n", kw, len(c))
				for _, concept := range c {
					if _, ok := seen[concept]; !ok {
						seen[concept] = true
						queryConcepts = append(queryConcepts, concept)
					}
				}
				continue
			}

			//fmt.Printf(" | ? %s", kw)

			// Otherwise, perform a QuickUMLS lookup.
			candidates, err := client.Candidates(kw)
			if err != nil {
				return nil, nil, err
			}
			var c []string
			for _, candidate := range candidates {
				c = append(c, candidate.CandidateCUI)
			}
			for _, concept := range c {
				if _, ok := seen[concept]; !ok {
					seen[concept] = true
					queryConcepts = append(queryConcepts, concept)
				}
			}
			//fmt.Printf(" (%d)\n", len(c))
			cm[kw] = c
		}

		p := protocols[q.Topic]
		objectives, err := guru.MetaMapCUIs(p.Objective, client)
		if err != nil {
			return nil, nil, err
		}
		typeOfStudy, err := guru.MetaMapCUIs(p.TypeOfStudy, client)
		if err != nil {
			return nil, nil, err
		}
		referenceStandards, err := guru.MetaMapCUIs(p.ReferenceStandards, client)
		if err != nil {
			return nil, nil, err
		}
		participants, err := guru.MetaMapCUIs(p.Participants, client)
		if err != nil {
			return nil, nil, err
		}
		indexTests, err := guru.MetaMapCUIs(p.IndexTests, client)
		if err != nil {
			return nil, nil, err
		}
		targetConditions, err := guru.MetaMapCUIs(p.TargetConditions, client)
		if err != nil {
			return nil, nil, err
		}
		title, err := guru.MetaMapCUIs(q.Name, client)
		if err != nil {
			return nil, nil, err
		}

		overlapFields := map[string][]string{
			"Objectives":          objectives,
			"Type Of Study":       typeOfStudy,
			"Reference Standards": referenceStandards,
			"Participants":        participants,
			"Index Tests":         indexTests,
			"Target Conditions":   targetConditions,
			"Title":               title,
		}

		for _, c1 := range title {
			if _, ok := conceptsNotInTitle["Title"]; !ok {
				conceptsNotInTitle["Title"] = make(map[string]int)
			}
			conceptsNotInTitle["Title"][q.Topic]++
			for k, concepts := range overlapFields {
				if k == "Title" {
					continue
				}
				found := 0
				for _, c2 := range concepts {
					for _, c3 := range queryConcepts {
						if c1 == c2 && c1 == c3 {
							found++
						}
					}
				}
				if _, ok := conceptsNotInTitle[k]; !ok {
					conceptsNotInTitle[k] = make(map[string]int)
				}
				conceptsNotInTitle[k][q.Topic] = len(concepts) - found
			}
		}

		for k, v := range overlapFields {
			//fmt.Printf(" | ? %s\n", k)
			n, ratio, c1, c2 := guru.MetaMapConceptRatio(queryConcepts, v)
			conceptReachabilityMapping[k] = append(conceptReachabilityMapping[k], ConceptReachability{
				Overlap:      n,
				OverlapRatio: ratio,
				QueryCount:   c1,
				FieldCount:   c2,
				Topic:        q.Topic,
			})
		}

	}
	f, err := os.OpenFile(conceptsBinFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	err = gob.NewEncoder(f).Encode(cm)
	if err != nil {
		return
	}
	f.Close()

	return
}

func MetaMapScoreDistributions(queries []pipeline.Query, protocols guru.Protocols, client metawrap.HTTPClient) (inQuery map[string][]float64, notInQuery map[string][]float64, err error) {
	inQuery = make(map[string][]float64)
	notInQuery = make(map[string][]float64)
	for _, q := range queries {
		var objectives, typeOfStudy, referenceStandards, participants, indexTests, targetConditions, title []metawrap.MappingCandidate

		var wg0 sync.WaitGroup
		wg0.Add(1)
		go func() {
			defer wg0.Done()
			var err error
			p := protocols[q.Topic]
			objectives, err = client.Candidates(p.Objective)
			if err != nil {
				return
			}
			typeOfStudy, err = client.Candidates(p.TypeOfStudy)
			if err != nil {
				return
			}
			referenceStandards, err = client.Candidates(p.ReferenceStandards)
			if err != nil {
				return
			}
			participants, err = client.Candidates(p.Participants)
			if err != nil {
				return
			}
			indexTests, err = client.Candidates(p.IndexTests)
			if err != nil {
				return
			}
			targetConditions, err = client.Candidates(p.TargetConditions)
			if err != nil {
				return
			}
			title, err = client.Candidates(q.Name)
			if err != nil {
				return
			}
		}()

		keywords := QueryKeywords(q.Query)
		var queryCandidates []metawrap.MappingCandidate
		var wg1 sync.WaitGroup
		for _, kw := range keywords {
			wg1.Add(1)
			go func(k cqr.Keyword) {
				defer wg1.Done()
				candidates, err := client.Candidates(kw.QueryString)
				if err != nil {
					return
				}
				queryCandidates = append(queryCandidates, candidates...)
			}(kw)
		}

		wg1.Wait()
		wg0.Wait()

		fieldConcepts := map[string][]metawrap.MappingCandidate{
			"Objectives":          objectives,
			"Type Of Study":       typeOfStudy,
			"Reference Standards": referenceStandards,
			"Participants":        participants,
			"Index Tests":         indexTests,
			"Target Conditions":   targetConditions,
			"Title":               title,
		}

		for field, candidates := range fieldConcepts {
			for _, candidate := range candidates {
				v, _ := strconv.Atoi(candidate.CandidateScore)
				score := math.Abs(float64(v))
				found := false
				for _, queryCandidate := range queryCandidates {
					if queryCandidate.CandidateCUI == candidate.CandidateCUI {
						inQuery[field] = append(inQuery[field], score)
						found = true
						break
					}
				}
				if !found {
					notInQuery[field] = append(notInQuery[field], score)
				}
			}
		}
	}
	return
}
