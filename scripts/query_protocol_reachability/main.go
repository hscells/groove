package main

import (
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/quickumlsrest"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
)

const (
	dir              = "/Users/harryscells/gocode/src/github.com/hscells/groove/scripts/query_protocol_reachability/test_data/"
	queriesDir       = dir + "train_t2/"
	protocolsDir     = dir + "train_p2/"
	queriesBinFile   = dir + "queries.bin"
	protocolsBinFile = dir + "protocols.bin"
	conceptsBinFile  = dir + "concepts.bin"

	LoadQueries                = false
	LoadProtocols              = false
	DoStringMatchReachability  = false
	DoConceptMatchReachability = true
)

var reg = regexp.MustCompile(`[*"]+`)

// protocol is a representation of a systematic review protocol in XML.
type protocol struct {
	Objective          string `xml:"objective"`
	TypeOfStudy        string `xml:"type_of_study"`
	Participants       string `xml:"participants"`
	IndexTests         string `xml:"index_tests"`
	TargetConditions   string `xml:"target_conditions"`
	ReferenceStandards string `xml:"reference_standards"`
}

type reachability struct {
	Title, Objectives, TypeOfStudy, Participants, IndexTests,
	TargetConditions, ReferenceStandards, Concepts int
	Topic string
}

type conceptReachability struct {
	Overlap, OverlapRatio  float64
	QueryCount, FieldCount int
	Topic                  string
}

type protocols map[string]protocol

type conceptMapping map[string][]string

//noinspection GoBoolExpressions
func main() {
	gob.Register(pipeline.Query{})
	gob.Register(cqr.BooleanQuery{})
	gob.Register(cqr.Keyword{})
	gob.Register(protocol{})
	gob.Register(protocols{})

	var (
		queries   []pipeline.Query
		protocols protocols
	)

	if LoadQueries {
		queries = readAndWriteQueries()
	} else {
		queries = loadQueries()
	}

	if LoadProtocols {
		protocols = readAndWriteProtocols()
	} else {
		protocols = loadProtocols()
	}

	fmt.Printf("queries: %d, protocols: %d\n", len(queries), len(protocols))
	notFound := make(map[string]bool)
	for _, q := range queries {
		if _, ok := protocols[q.Topic]; !ok {
			fmt.Printf("! topic [%s] not found\n", q.Topic)
			notFound[q.Topic] = true
		}
	}

	if DoStringMatchReachability {
		stringMatchReachability(queries, protocols, notFound)
	}

	if DoConceptMatchReachability {
		conceptMatchReachability(queries, protocols, notFound)
	}

}

func conceptMatchReachability(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
	umls := quickumlsrest.NewClient("http://43.240.96.223:5000")

	// Load or create the concept mapping file.
	var cm conceptMapping
	if _, err := os.Stat(conceptsBinFile); err != nil && os.IsNotExist(err) {
		cm = make(conceptMapping)
	} else if err != nil {
		panic(err)
	} else {
		f, err := os.OpenFile(conceptsBinFile, os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		err = gob.NewDecoder(f).Decode(&cm)
		if err != nil {
			panic(err)
		}
		f.Close()
	}

	data := make(map[string][]conceptReachability)

	for _, q := range queries {
		fmt.Printf("+ [%s]%s\n", q.Topic, q.Name)

		var queryConcepts []string
		seen := make(map[string]bool)

		keywords := analysis.QueryKeywords(q.Query)
		for _, keyword := range keywords {
			kw := strings.ToLower(reg.ReplaceAllString(keyword.QueryString, ""))

			// Look the concept up in the cache.
			if c, ok := cm[kw]; ok {
				fmt.Printf(" | * %s (%d)\n", kw, len(c))
				for _, concept := range c {
					if _, ok := seen[concept]; !ok {
						seen[concept] = true
						queryConcepts = append(queryConcepts, concept)
					}
				}
				continue
			}

			fmt.Printf(" | ? %s", kw)

			// Otherwise, perform a QuickUMLS lookup.
			candidates, err := umls.Match(kw)
			if err != nil {
				panic(err)
			}
			var c []string
			for _, candidate := range candidates {
				if candidate.Preferred == 1 {
					c = append(c, candidate.Term)
				}
			}
			for _, concept := range c {
				if _, ok := seen[concept]; !ok {
					seen[concept] = true
					queryConcepts = append(queryConcepts, concept)
				}
			}
			fmt.Printf(" (%d)\n", len(c))
			cm[kw] = c
		}

		p := protocols[q.Topic]
		objectives := getTextConcepts(umls, p.Objective)
		typeOfStudy := getTextConcepts(umls, p.TypeOfStudy)
		referenceStandards := getTextConcepts(umls, p.ReferenceStandards)
		participants := getTextConcepts(umls, p.Participants)
		indexTests := getTextConcepts(umls, p.IndexTests)
		targetConditions := getTextConcepts(umls, p.TargetConditions)
		title := getTextConcepts(umls, q.Name)

		fields := map[string][]string{
			"Objectives":          objectives,
			"Type Of Study":       typeOfStudy,
			"Reference Standards": referenceStandards,
			"Participants":        participants,
			"Index Tests":         indexTests,
			"Target Conditions":   targetConditions,
			"Title":               title,
		}

		for k, v := range fields {
			fmt.Printf(" | ? %s\n", k)
			n, ratio, c1, c2 := computeConceptRatio(queryConcepts, v)
			data[k] = append(data[k], conceptReachability{
				Overlap:      n,
				OverlapRatio: ratio,
				QueryCount:   c1,
				FieldCount:   c2,
				Topic:        q.Topic,
			})
		}

	}

	f, err := os.OpenFile(path.Join(dir, "conceptReachability.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = json.NewEncoder(f).Encode(data)
	if err != nil {
		panic(err)
	}
	f.Close()

	f, err = os.OpenFile(conceptsBinFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = gob.NewEncoder(f).Encode(cm)
	if err != nil {
		panic(err)
	}
	f.Close()
}

func computeConceptRatio(concepts1 []string, concepts2 []string) (float64, float64, int, int) {
	n := 0.0
	for _, c1 := range concepts1 {
		for _, c2 := range concepts2 {
			if c1 == c2 {
				n++
			}
		}
	}
	return n, n / float64(len(concepts1)), len(concepts1), len(concepts2)
}

func getTextConcepts(umls quickumlsrest.Client, text string) []string {
	candidates, err := umls.Match(text)
	if err != nil {
		panic(err)
	}
	var c []string
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		if candidate.Preferred == 1 {
			if _, ok := seen[candidate.Term]; !ok {
				c = append(c, candidate.Term)
				seen[candidate.Term] = true
			}
		}
	}
	return c
}

func stringMatchReachability(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
	var reach []reachability
	for _, q := range queries {
		// Skip topics that do not have a protocol.
		if _, ok := notFound[q.Topic]; ok {
			continue
		}
		var concepts []string

		fmt.Printf("+ [%s]%s\n", q.Topic, q.Name)

		keywords := analysis.QueryKeywords(q.Query)
		for _, keyword := range keywords {
			kw := reg.ReplaceAllString(keyword.QueryString, "")
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

		reach = append(reach, reachability{
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
	f, err := os.OpenFile(path.Join(dir, "reachability.json"), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = json.NewEncoder(f).Encode(reach)
	if err != nil {
		panic(err)
	}
}

func readAndWriteProtocols() protocols {
	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(protocolsDir)
	if err != nil {
		panic(err)
	}

	protocols := make(protocols)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if len(f.Name()) == 0 {
			continue
		}

		p := path.Join(protocolsDir, f.Name())
		source, err := ioutil.ReadFile(p)
		if err != nil {
			panic(err)
		}

		var protocol protocol
		err = xml.Unmarshal(source, &protocol)
		if err != nil {
			panic(err)
		}

		_, topic := path.Split(p)

		protocols[strings.TrimSpace(topic)] = protocol
	}
	f, err := os.OpenFile(protocolsBinFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = gob.NewEncoder(f).Encode(protocols)
	if err != nil {
		panic(err)
	}
	return protocols
}

func loadProtocols() protocols {
	f, err := os.OpenFile(protocolsBinFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var protocols protocols
	err = gob.NewDecoder(f).Decode(&protocols)
	if err != nil {
		panic(err)
	}
	return protocols
}

func readAndWriteQueries() []pipeline.Query {
	qs := query.TARTask2QuerySource{}
	queries, err := qs.Load(queriesDir)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(queriesBinFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = gob.NewEncoder(f).Encode(queries)
	if err != nil {
		panic(err)
	}
	return queries
}

func loadQueries() []pipeline.Query {
	f, err := os.OpenFile(queriesBinFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var queries []pipeline.Query
	err = gob.NewDecoder(f).Decode(&queries)
	if err != nil {
		panic(err)
	}
	return queries
}
