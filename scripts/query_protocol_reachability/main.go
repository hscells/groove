package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

const (
	cui2vecUncompressedPath = "/Users/harryscells/Repositories/cui2vec/cui2vec_pretrained.csv"
	cui2vecPretrainedPath   = "/Users/harryscells/Repositories/cui2vec/testdata/cui2vec_precomputed.bin"
	cuisPath                = "/Users/harryscells/Repositories/cui2vec/cuis.csv"
	qrelsPath               = "/Users/harryscells/Repositories/tar/2018-TAR/Task1/Training/qrel"
	javaClassPath           = "/Users/harryscells/stanford-parser-full-2018-10-17"
	dir                     = "/Users/harryscells/gocode/src/github.com/hscells/groove/scripts/query_protocol_reachability/test_data/"
	queriesDir              = dir + "train_t2/"
	protocolsDir            = dir + "train_p2/"
	queryOutputDir          = dir + "queries/"
	queriesBinFile          = dir + "queries.bin"
	protocolsBinFile        = dir + "protocols.bin"
	conceptsBinFile         = dir + "concepts.bin"

	LoadQueries                = false
	LoadProtocols              = false
	DoStringMatchReachability  = false
	DoConceptMatchReachability = false
	DoQueryGeneration          = true
	DoMMScoreDistributions     = false
)

var textReg = regexp.MustCompile(`[*"]+`)
var queryReg = regexp.MustCompile(`\(.*\)`)
var mmClient = &http.Client{}
var precomputedEmbeddings *cui2vec.PrecomputedEmbeddings
var uncompressedEmbeddings *cui2vec.UncompressedEmbeddings
var cuiMapping, _ = cui2vec.LoadCUIMapping(cuisPath)

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
	QueryCount, FieldCount int
	Overlap, OverlapRatio  float64
	Topic                  string
}

type uniqueOverlaps struct {
	UniqueTitle, UniqueObjectives,
	UniqueTypeOfStudy, UniqueParticipants,
	UniqueIndexTests, UniqueTargetConditions,
	UniqueReferenceStandards int
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
	//fmt.Println(x)

	if DoStringMatchReachability {
		stringMatchReachability(queries, protocols, notFound)
	}

	if DoConceptMatchReachability {
		conceptMatchReachability(queries, protocols, notFound)
	}

	if DoQueryGeneration {
		//fmt.Printf("! loading cuis...\n")
		//f, err := os.OpenFile(cui2vecUncompressedPath, os.O_RDONLY, 0644)
		//if err != nil {
		//	panic(err)
		//}
		//uncompressedEmbeddings, err = cui2vec.NewUncompressedEmbeddings(f, true)
		//if err != nil {
		//	panic(err)
		//}
		//f.Close()
		f, err := os.OpenFile(cui2vecPretrainedPath, os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		precomputedEmbeddings, err = cui2vec.NewPrecomputedEmbeddings(f)
		if err != nil {
			panic(err)
		}
		f.Close()
		fmt.Printf("! generating queries\n")
		generateQueries(queries, protocols, notFound)
	}

	if DoMMScoreDistributions {
		mmScoreDistributions(queries, protocols, notFound)
	}
}

type ast struct {
	tag      string
	text     string
	children []ast
}

func parseTree(text string) ast {
	// First, lex the text.
	var tokens []string
	var token string
	for _, char := range text {
		if char == '(' {
			if len(token) > 0 {
				tokens = append(tokens, strings.TrimSpace(token))
				token = ""
			}
			tokens = append(tokens, "(")
		} else if char == ')' {
			if len(token) > 0 {
				tokens = append(tokens, strings.TrimSpace(token))
				token = ""
			}
			tokens = append(tokens, ")")
		} else if len(token) > 0 || len(token) == 0 && !unicode.IsSpace(char) {
			token += string(char)
		}
	}

	var parse func(l []string, a ast) ([]string, ast)
	parse = func(l []string, a ast) ([]string, ast) {
		if len(l) <= 2 {
			return l, a
		}
		token := l[0]
		if token == "(" {
			var t ast
			l, t = parse(l[1:], ast{})
			a.children = append(a.children, t)
		} else if token == ")" {
			return l, a
		} else {
			tokens := strings.Split(token, " ")
			if len(tokens) == 2 {
				a.tag = strings.TrimSpace(tokens[0])
				a.text = strings.TrimSpace(tokens[1])
			} else {
				a.tag = token
			}
		}
		return parse(l[1:], a)
	}
	fmt.Println(tokens)
	_, ast := parse(tokens, ast{})
	return ast.children[0]
}

func treeToQuery(a ast) cqr.CommonQueryRepresentation {
	var tree func(a ast, l int) cqr.CommonQueryRepresentation
	tree = func(a ast, l int) cqr.CommonQueryRepresentation {
		if len(strings.TrimSpace(a.text)) > 0 {
			switch a.tag {
			case "NN", "NNP", "NNS", "JJ", "VB", "VBZ", "VBG", "RB":
				return cqr.NewKeyword(a.text, fields.TitleAbstract)
			default:
				return nil
			}
		}

		q := cqr.NewBooleanQuery(cqr.OR, nil)
		if l <= 2 {
			q.Operator = cqr.AND
		}
		for _, child := range a.children {
			c := tree(child, l+1)
			if c != nil {
				q.Children = append(q.Children, c)
			}
		}
		return q
	}
	s, _ := transmute.CompileCqr2Medline(tree(a, 0))
	c, _ := transmute.CompileMedline2Cqr(s)
	return c
}

func treeToSimpleQuery(a ast) cqr.CommonQueryRepresentation {
	var tree func(a ast, l int) cqr.CommonQueryRepresentation
	tree = func(a ast, l int) cqr.CommonQueryRepresentation {
		if len(strings.TrimSpace(a.text)) > 0 {
			switch a.tag {
			case "NN", "NNP", "NNS", "JJ", "VB", "VBZ", "VBG", "RB":
				return cqr.NewKeyword(a.text, fields.TitleAbstract)
			default:
				return nil
			}
		}

		q := cqr.NewBooleanQuery(cqr.OR, nil)
		for _, child := range a.children {
			c := tree(child, l+1)
			if c != nil {
				q.Children = append(q.Children, c)
			}
		}
		return q
	}
	s, _ := transmute.CompileCqr2Medline(tree(a, 0))
	c, _ := transmute.CompileMedline2Cqr(s)
	return simplify(c)
}

func simplify(r cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation {
	switch q := r.(type) {
	case cqr.Keyword:
		return q
	case cqr.BooleanQuery:
		var children []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			switch c := child.(type) {
			case cqr.Keyword:
				if len(c.QueryString) > 0 {
					children = append(children, child)
				}
			case cqr.BooleanQuery:
				if c.Operator == q.Operator {
					for _, child := range c.Children {
						children = append(children, simplify(child))
					}
				} else {
					children = append(children, simplify(c))
				}
			}
		}
		q.Children = children
		return q
	}
	return nil
}

func entityExpansion(query cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		bq := cqr.NewBooleanQuery(cqr.OR, nil)
		cui := q.GetOption("cui").(string)
		fmt.Println("+", cui)
		concepts, err := precomputedEmbeddings.Similar(cui)
		if err != nil {
			panic(err)
		}
		for _, concept := range concepts {
			if v, ok := cuiMapping[concept.CUI]; ok {
				fmt.Println("|", v)
				bq.Children = append(bq.Children, cqr.NewKeyword(v, fields.TitleAbstract))
			}
		}
		if len(bq.Children) == 0 {
			return q
		}
		bq.Children = append(bq.Children, q)
		return bq
	case cqr.BooleanQuery:
		bq := cqr.NewBooleanQuery(q.Operator, nil)
		for _, child := range q.Children {
			bq.Children = append(bq.Children, entityExpansion(child))
		}
		return bq
	}
	return nil
}

func generateQueries(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
	//f, err := os.OpenFile(qrelsPath, os.O_RDONLY, 0644)
	//if err != nil {
	//	panic(err)
	//}
	//qrels, err := trecresults.QrelsFromReader(f)
	//if err != nil {
	//	panic(err)
	//}
	//f.Close()
	//
	//e, err := stats.NewEntrezStatisticsSource(
	//	stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
	//	stats.EntrezEmail("harryscells@gmail.com"),
	//	stats.EntrezTool("groove"),
	//	stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	//if err != nil {
	//	panic(err)
	//}
	//
	//size, err := e.CollectionSize()
	//if err != nil {
	//	panic(err)
	//}

	for _, q := range queries {
		fmt.Printf("+ [%s] %s \n", q.Topic, q.Name)
		// Stop if the p does not have a protocol.
		if _, ok := notFound[q.Topic]; ok {
			continue
		}

		// Parse title: "Query Logic Composition".
		cmd := exec.Command("bash", "-c", fmt.Sprintf(`echo "%s" | java -cp "%s/*" edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat "penn" %s/englishPCFG.ser.gz -`, q.Name, javaClassPath, javaClassPath))
		r, err := cmd.StdoutPipe()
		if err != nil {
			panic(err)
		}

		cmd.Start()

		var buff bytes.Buffer
		s := bufio.NewScanner(bufio.NewReader(r))
		for s.Scan() {
			_, err = buff.Write(s.Bytes())
			if err != nil {
				panic(err)
			}
		}
		//fmt.Println("----")
		//fmt.Println(buff.String())
		//
		p := simplify(treeToQuery(parseTree(buff.String()))) // The magic part.
		//fmt.Println("----")
		//fmt.Println(transmute.CompileCqr2PubMed(p))

		type entity struct {
			score int
			text  string
			cui   string
		}

		// Map the terms in the query to "Concepts".
		// This transforms the query from the previous step by replacing terms with concepts
		// and annotating the query with a CUI for the next step.
		var mapQuery func(r cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation
		mapQuery = func(r cqr.CommonQueryRepresentation) (v cqr.CommonQueryRepresentation) {
			switch q := r.(type) {
			case cqr.Keyword:
				if len(strings.TrimSpace(q.QueryString)) == 0 {
					return nil
				}
				var entities []entity
				seen := make(map[string]bool)
				candidates := metaMapCandidates(q.QueryString)
				for _, c := range candidates {
					score, _ := strconv.Atoi(c.CandidateScore)
					matched := strings.ToLower(c.CandidateMatched)
					if _, ok := seen[matched]; !ok {
						//fmt.Println(matched, "|", q.QueryString)
						for _, semtype := range c.SemTypes {
							switch semtype {
							case "spco":
								goto skipEntity
							}
						}
						entities = append(entities, entity{text: matched, score: score, cui: c.CandidateCUI})
					skipEntity:
						seen[matched] = true
					}
				}
				//sort.Slice(entities, func(i, j int) bool {
				//	return entities[i].score < entities[j].score
				//})
				////fmt.Println(entities)
				//if len(entities) == 0 {
				//	return nil
				//}
				//var concepts []entity
				////concepts = append(concepts, entities[0])
				//for i := 0; i < len(entities)-1; i++ {
				//	//	if entities[i].score == entities[i+1].score {
				//	concepts = append(concepts, entities[i])
				//	//} else {
				//	//	break
				//	//}
				//}
				if len(entities) == 0 {
					return nil
				} else if len(entities) == 1 {
					return cqr.NewKeyword(entities[0].text, q.Fields...).SetOption("cui", entities[0].cui)
				}
				b := cqr.NewBooleanQuery(cqr.OR, nil)
				for _, concept := range entities {
					fmt.Println(concept)
					b.Children = append(b.Children, cqr.NewKeyword(concept.text, q.Fields...).SetOption("cui", concept.cui))
				}
				return b
			case cqr.BooleanQuery:
				b := cqr.NewBooleanQuery(q.Operator, nil)
				var qs []string
				//fmt.Println(q.Children)
				for _, child := range q.Children {
					switch v := child.(type) {
					case cqr.Keyword:
						qs = append(qs, v.QueryString)
					case cqr.BooleanQuery:
						m := mapQuery(child)
						if m != nil {
							b.Children = append(b.Children, m)
						}
					}
				}

				k := cqr.NewKeyword(strings.Join(qs, " "), fields.TitleAbstract)
				if len(qs) == len(q.Children) {
					return mapQuery(k)
				} else if len(qs) > 0 {
					v := mapQuery(k)
					if v != nil {
						b.Children = append(b.Children, v)
					}
				}
				return b
			}
			return
		}

		//r0 := evaluateQuery(e, pipeline.NewQuery(q.Name, q.Topic, treeToSimpleQuery(parseTree(buff.String()))), size, qrels)
		//r0 := evaluateQuery(e, q, size, qrels)
		m := mapQuery(p)
		//r1 := evaluateQuery(e, pipeline.NewQuery(q.Name, q.Topic, m), size, qrels)
		ent := entityExpansion(m)
		//r2 := evaluateQuery(e, pipeline.NewQuery(q.Name, q.Topic, ent), size, qrels)
		//fmt.Println(r0["Precision"], r1["Precision"], r2["Precision"])
		//fmt.Println(r0["Recall"], r1["Recall"], r2["Recall"])
		q0, _ := transmute.CompileCqr2PubMed(q.Query)
		q1, _ := transmute.CompileCqr2PubMed(treeToSimpleQuery(parseTree(buff.String())))
		q2, _ := transmute.CompileCqr2PubMed(m)
		q3, _ := transmute.CompileCqr2PubMed(ent)
		err = ioutil.WriteFile(path.Join(queryOutputDir, "original/", q.Topic), []byte(q0), 0644)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join(queryOutputDir, "simple/", q.Topic), []byte(q1), 0644)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join(queryOutputDir, "entity/", q.Topic), []byte(q2), 0644)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join(queryOutputDir, "cui2vec_expansion/", q.Topic), []byte(q3), 0644)
		if err != nil {
			panic(err)
		}
		/*
				p := protocols[q.Topic]

				title := metaMapCandidates(q.Name)
				fmt.Print(".")
				//objective := metaMapCandidates(p.Objective)
				//fmt.Print(".")
				//indexTests := metaMapCandidates(p.IndexTests)
				//fmt.Print(".")
				//targetConditions := metaMapCandidates(p.TargetConditions)
				//fmt.Print(".")
				//referenceStandards := metaMapCandidates(p.ReferenceStandards)
				//fmt.Print(".")
				//typeOfStudy := metaMapCandidates(p.TypeOfStudy)
				//fmt.Print(".")

				//cuis := append(title, objective...)
				cuis := title
				//cuis := append(append(title, objective...), indexTests...)
				//cuis := append(append(append(append(append(title, objective...), indexTests...), targetConditions...), referenceStandards...), typeOfStudy...)
				var vectors [][]float64
				var concepts []metawrap.MappingCandidate
				for _, cui := range cuis {
					if v, ok := uncompressedEmbeddings.Embeddings[cui.CandidateCUI]; ok {
						vectors = append(vectors, v)
						concepts = append(concepts, cui)
					}
				}
				fmt.Print(".")

				if len(vectors) == 0 {
					continue
				}

				k := 4
				labels, err := kmeans.Kmeans(vectors, k, kmeans.EuclideanDistance, 10)
				if err != nil {
					panic(err)
				}
				fmt.Print(".")

				keywords := make(map[int][]metawrap.MappingCandidate)
				seen := make(map[string]bool)
				for i := 0; i < len(vectors); i++ {
					label := labels[i]
					concept := concepts[i]
					term := concept.CandidatePreferred
					if _, ok := seen[term]; !ok {
						//score, _ := strconv.Atoi(concept.CandidateScore)
						//if math.Abs(float64(score)) >= 100 {
						for _, semType := range concept.SemTypes {
							switch semType {
							case "bird", "clas", "cnce", "enty", "evnt",
								"fish", "fndg", "ftcn", "gora", "grpa", "grup",
								"geoa", "idcn", "inpr", "lang", "orgt", "pros",
								"qlco", "qnco", "resa", "resd", "rnlw", "spco",
								"popg", "lbpr", "tmco", "clna":
								goto skipConcept
							}
						}
						keywords[label] = append(keywords[label], concept)
						//}
					skipConcept:
						seen[term] = true
					}
				}
				fmt.Print(".\n")
				bq := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{})
				atoms := make(map[int][]cqr.CommonQueryRepresentation)
				for k, v := range keywords {
					//fmt.Printf("  + [%d]\n", k)
					for _, x := range v {
						kw := queryReg.ReplaceAllString(x.CandidatePreferred, "")
						f := fields.TitleAbstract
						if mesh.Contains(kw) && mesh.Depth(kw) > 4 {
							f = fields.MeSHTerms
						} else if strings.Contains(kw, " ") {
							kw = fmt.Sprintf(`"%s"`, kw)
						}
						atoms[k] = append(atoms[k], cqr.NewKeyword(kw, f).SetOption("cui", x.CandidateCUI))
						//fmt.Printf("  | %s\n", x)
					}
				}



		*/
	}
}

func evaluateQuery(e stats.EntrezStatisticsSource, q pipeline.Query, size float64, qrels trecresults.QrelsFile) map[string]float64 {
	results, err := e.Execute(q, e.SearchOptions())
	if err != nil {
		panic(err)
	}
	eval.RelevanceGrade = 0
	return eval.Evaluate([]eval.Evaluator{
		eval.PrecisionEvaluator,
		eval.RecallEvaluator,
		eval.F1Measure,
		eval.F05Measure,
		eval.F3Measure,
		eval.NumRel,
		eval.NumRet,
		eval.NumRelRet,
		eval.NewWSSEvaluator(size),
		eval.NewMaximumLikelihoodEvaluator(eval.PrecisionEvaluator),
		eval.NewMaximumLikelihoodEvaluator(eval.RecallEvaluator),
		eval.NewMaximumLikelihoodEvaluator(eval.F1Measure),
		eval.NewMaximumLikelihoodEvaluator(eval.F05Measure),
		eval.NewMaximumLikelihoodEvaluator(eval.F3Measure),
	}, &results, qrels, q.Topic)
}

func mmScoreDistributions(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
	inQuery := make(map[string][]float64)
	notInQuery := make(map[string][]float64)
	for i, q := range queries {
		if _, ok := notFound[q.Topic]; ok {
			continue
		}
		fmt.Printf("+ [%s] (%d/%d)", q.Topic, i, len(queries)-len(notFound))

		var objectives, typeOfStudy, referenceStandards, participants, indexTests, targetConditions, title []metawrap.MappingCandidate

		var wg0 sync.WaitGroup
		wg0.Add(1)
		go func() {
			defer wg0.Done()
			p := protocols[q.Topic]
			objectives = metaMapCandidates(p.Objective)
			fmt.Print(",")
			typeOfStudy = metaMapCandidates(p.TypeOfStudy)
			fmt.Print(",")
			referenceStandards = metaMapCandidates(p.ReferenceStandards)
			fmt.Print(",")
			participants = metaMapCandidates(p.Participants)
			fmt.Print(",")
			indexTests = metaMapCandidates(p.IndexTests)
			fmt.Print(",")
			targetConditions = metaMapCandidates(p.TargetConditions)
			fmt.Print(",")
			title = metaMapCandidates(q.Name)
			fmt.Print(",")
			fmt.Print("*")
		}()

		keywords := analysis.QueryKeywords(q.Query)
		var queryCandidates []metawrap.MappingCandidate
		var wg1 sync.WaitGroup
		for _, kw := range keywords {
			wg1.Add(1)
			go func(k cqr.Keyword) {
				defer wg1.Done()
				queryCandidates = append(queryCandidates, metaMapCandidates(kw.QueryString)...)
				fmt.Print(".")
			}(kw)
		}

		wg1.Wait()
		fmt.Print("*")
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
		fmt.Print("*\n")
	}
	f, err := os.OpenFile(path.Join(dir, "inQueryDistribution.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = json.NewEncoder(f).Encode(inQuery)
	if err != nil {
		panic(err)
	}
	f.Close()
	f, err = os.OpenFile(path.Join(dir, "notInQueryDistribution.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = json.NewEncoder(f).Encode(notInQuery)
	if err != nil {
		panic(err)
	}
	f.Close()
}

func metaMapCandidates(text string) (candidates []metawrap.MappingCandidate) {
	req, err := http.NewRequest("POST", "http://ielab-metamap.uqcloud.net/mm/candidates", bytes.NewBufferString(text))
	if err != nil {
		panic(err)
	}
	resp, err := mmClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.ContentLength == 0 {
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&candidates)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	return
}

func conceptMatchReachability(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
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

	conceptReachabilityMapping := make(map[string][]conceptReachability)
	conceptsNotInTitle := make(map[string]map[string]int)

	for _, q := range queries {
		if _, ok := notFound[q.Topic]; ok {
			continue
		}

		fmt.Printf("+ [%s]%s\n", q.Topic, q.Name)

		var queryConcepts []string
		seen := make(map[string]bool)

		keywords := analysis.QueryKeywords(q.Query)
		for _, keyword := range keywords {
			kw := strings.ToLower(textReg.ReplaceAllString(keyword.QueryString, ""))

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
			candidates := metaMapCandidates(kw)
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
			fmt.Printf(" (%d)\n", len(c))
			cm[kw] = c
		}

		p := protocols[q.Topic]
		objectives := getTextConcepts(p.Objective)
		typeOfStudy := getTextConcepts(p.TypeOfStudy)
		referenceStandards := getTextConcepts(p.ReferenceStandards)
		participants := getTextConcepts(p.Participants)
		indexTests := getTextConcepts(p.IndexTests)
		targetConditions := getTextConcepts(p.TargetConditions)
		title := getTextConcepts(q.Name)

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
			fmt.Printf(" | ? %s\n", k)
			n, ratio, c1, c2 := computeConceptRatio(queryConcepts, v)
			conceptReachabilityMapping[k] = append(conceptReachabilityMapping[k], conceptReachability{
				Overlap:      n,
				OverlapRatio: ratio,
				QueryCount:   c1,
				FieldCount:   c2,
				Topic:        q.Topic,
			})
		}

	}

	f, err := os.OpenFile(path.Join(dir, "conceptReachabilityMapping.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = json.NewEncoder(f).Encode(conceptReachabilityMapping)
	if err != nil {
		panic(err)
	}
	f.Close()

	f, err = os.OpenFile(path.Join(dir, "conceptsNotInTitle.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	err = json.NewEncoder(f).Encode(conceptsNotInTitle)
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

func getTextConcepts(text string) []string {
	candidates := metaMapCandidates(text)
	var c []string
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		if _, ok := seen[candidate.CandidateCUI]; !ok {
			c = append(c, candidate.CandidateCUI)
			seen[candidate.CandidateCUI] = true
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
