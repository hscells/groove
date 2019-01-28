package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/chzyer/readline"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/learning/seed"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"github.com/yizha/go/w2v"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	cui2vecUncompressedPath = "/Users/s4558151/Repositories/cui2vec/cui2vec_pretrained.csv"
	cui2vecPretrainedPath   = "/Users/s4558151/Repositories/cui2vec/testdata/cui2vec_precomputed.bin"
	cuisFreqPath            = "/Users/s4558151/Repositories/cui2vec/ICUI_STR_Frequency.csv"
	qrelsPath               = "/Users/s4558151/Repositories/tar/2018-TAR/Task2/qrel_abs_combined"
	javaClassPath           = "/Users/s4558151/stanford-parser-full-2018-10-17"
	dir                     = "/Users/s4558151/go/src/github.com/hscells/groove/scripts/query_protocol_reachability/test_data/"
	queriesDir              = "/Users/s4558151/Repositories/tar/2018-TAR/Task2/Combined"
	protocolsDir            = "/Users/s4558151/Repositories/tar/2018-TAR/Task1/Training/protocols"
	word2vecLoc             = "/Users/s4558151/Downloads/PubMed-and-PMC-w2v.bin"
	pubDatesFile            = "/Users/s4558151/Repositories/tar/2018-TAR/Task1/Testing/pubdates.txt"

	queryOutputDir   = dir + "queries/"
	queriesBinFile   = dir + "queries.bin"
	protocolsBinFile = dir + "protocols.bin"
	conceptsBinFile  = dir + "concepts.bin"

	LoadQueries                = false
	LoadProtocols              = false
	DoStringMatchReachability  = false
	DoConceptMatchReachability = false
	DoQueryGeneration          = false
	DoQueryDateRestrictions    = true
	DoMMScoreDistributions     = false
)

var textReg = regexp.MustCompile(`[*"]+`)
var queryReg = regexp.MustCompile(`\(.*\)`)
var mmClient = &http.Client{}
var precomputedEmbeddings *cui2vec.PrecomputedEmbeddings
var uncompressedEmbeddings *cui2vec.UncompressedEmbeddings
var pubmedEmbeddings *w2v.Model
var cuiMapping cui2vec.Mapping
var cuiAliases cui2vec.AliasMapping
var ticketGrantingTicket string

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
	//for _, q := range queries {
	//	if _, ok := protocols[q.Topic]; !ok {
	//		fmt.Printf("! topic [%s] not found\n", q.Topic)
	//		notFound[q.Topic] = true
	//	}
	//}
	//fmt.Println(x)

	if DoStringMatchReachability {
		stringMatchReachability(queries, protocols, notFound)
	}

	if DoConceptMatchReachability {
		conceptMatchReachability(queries, protocols, notFound)
	}

	if DoQueryGeneration {
		var err error
		cuiMapping, err = cui2vec.LoadCUIFrequencyMapping(cuisFreqPath)
		if err != nil {
			panic(err)
		}
		cuiAliases, err = cui2vec.LoadCUIAliasMapping(cuisFreqPath)
		if err != nil {
			panic(err)
		}

		fmt.Println("getting umls ticket")
		resp, err := mmClient.Post("https://utslogin.nlm.nih.gov/cas/v1/tickets", "application/x-www-form-urlencoded", bytes.NewBufferString(fmt.Sprintf("username=%s&password=%s", "ielabqut", "ielab@QUT")))
		if err != nil {
			panic(err)
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		re := regexp.MustCompile(`action=".*(?P<Ticket>TGT-.*-cas)"`)
		ticketGrantingTicket = re.FindStringSubmatch(string(b))[1]
		fmt.Println(ticketGrantingTicket)
		//
		//fmt.Println(matchUMLSConcept("C0009044"))

		//fmt.Println("loading w2v model")
		//pubmedEmbeddings, err = w2v.FromReader(f)
		//if err != nil {
		//	panic(err)
		//}

		//f, err = os.OpenFile("w2v.terms", os.O_WRONLY|os.O_CREATE, 0644)
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Println(pubmedEmbeddings.GetVectorByWord("cancer"))
		//fmt.Println(pubmedEmbeddings.GetVectorByWord("neoplasm"))
		//fmt.Println(pubmedEmbeddings.GetVectorByWord("neoplasms"))
		//fmt.Println(pubmedEmbeddings.GetVectorByWord("paracetamol"))
		//fmt.Println(pubmedEmbeddings.GetVectorByWord("hypertension"))
		//for w := range pubmedEmbeddings.Word2id {
		//	_, err = f.WriteString(fmt.Sprintf("%s", w))
		//	if err != nil {
		//		panic(err)
		//	}
		//}

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

	if DoQueryDateRestrictions {
		queryDateRestrictions()
	}

	if DoMMScoreDistributions {
		mmScoreDistributions(queries, protocols, notFound)
	}
}

func queryDateRestrictions() {
	f, err := os.OpenFile(pubDatesFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	type restriction struct {
		topic string
		start time.Time
		end   time.Time
	}
	var restrictions []restriction

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.Split(s.Text(), "\t")
		topic, start, end := line[0], line[1], line[2]
		s, err := time.Parse("20060102", start)
		if err != nil {
			panic(err)
		}
		e, err := time.Parse("20060102", end)
		if err != nil {
			panic(err)
		}
		restrictions = append(restrictions, restriction{
			topic: topic,
			start: s,
			end:   e,
		})
		fmt.Println(s, e)
	}

	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(queryOutputDir)
	if err != nil {
		panic(err)
	}

	for _, dir := range files {
		if !dir.IsDir() {
			continue
		}
		fmt.Println(dir.Name())
		qs := query.NewTransmuteQuerySource(query.MedlineTransmutePipeline)
		queries, err := qs.Load(path.Join(queryOutputDir, dir.Name()))
		if err != nil {
			panic(err)
		}
		for _, q := range queries {
			for _, r := range restrictions {
				if r.topic == q.Topic {
					bq := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
						q.Query,
						cqr.NewKeyword(fmt.Sprintf("%s:%s", r.start.Format("2006/01"), r.end.Format("2006/01")), fields.PublicationDate),
					})

					fmt.Println(q.Topic)
					t, _ := transmute.CompileCqr2Medline(bq)
					fmt.Println(t)
					err := ioutil.WriteFile(path.Join(queryOutputDir, dir.Name(), q.Topic), []byte(t), 0644)
					if err != nil {
						panic(err)
					}
					break
				}
			}
		}
	}
}

func matchUMLSConcept(cui string) (string, error) {
	fmt.Println(ticketGrantingTicket)
	// Request service ticket.
	resp, err := mmClient.Post(fmt.Sprintf("https://utslogin.nlm.nih.gov/cas/v1/tickets/%s", ticketGrantingTicket), "application/x-www-form-urlencoded", bytes.NewBufferString(fmt.Sprintf("service=%s", "http://umlsks.nlm.nih.gov")))
	if err != nil {
		return "", err
	}
	fmt.Println(resp.StatusCode, resp.Status)
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
		return "", err
	}

	ticket := string(b)
	fmt.Println(ticket)
	resp, err = mmClient.Get(fmt.Sprintf("https://uts-ws.nlm.nih.gov/rest/content/current/CUI/%s/atoms/preferred?ticket=%s", cui, ticket))
	if err != nil {
		panic(err)
		return "", err
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	var r map[string]interface{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	fmt.Println(resp.StatusCode, resp.Status)
	s := r["result"].(map[string]interface{})["name"].(string)
	fmt.Println(cui, "->", s)
	return s, nil
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

func cui2vecExpansion(query cqr.CommonQueryRepresentation, kind int) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		bq := cqr.NewBooleanQuery(cqr.OR, nil)
		cui := q.GetOption("cui").(string)
		concepts, err := precomputedEmbeddings.Similar(cui)
		if err != nil {
			panic(err)
		}
		for _, concept := range concepts {
			if kind == 0 {
				//map using UMLS
				//matched = strings.ToLower(c.CandidateMatched)
				fmt.Println("match: NOT ALLOWED")
			} else if kind == 1 {
				fmt.Println(concept.CUI)
				v, err := matchUMLSConcept(concept.CUI)
				if err != nil {
					continue
				}
				fmt.Println("preferred:", v)
				bq.Children = append(bq.Children, cqr.NewKeyword(fmt.Sprintf(`"%s"`, v), fields.TitleAbstract))
			} else if kind == 2 {
				if v, ok := cuiMapping[concept.CUI]; ok {
					matched := strings.ToLower(v)
					if strings.Contains(matched, "year") || strings.ContainsAny(v, "()[].^*+/<>") {
						continue
					}
					if len(v) == 0 {
						continue
					}
					fmt.Println("frequency:", v)
					bq.Children = append(bq.Children, cqr.NewKeyword(fmt.Sprintf(`"%s"`, v), fields.TitleAbstract))
				}
			} else {
				fmt.Println("alias: yeah...")
				return aliasExpansion(q, make(map[string]bool))
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
			bq.Children = append(bq.Children, cui2vecExpansion(child, kind))
		}
		return bq
	}
	return nil
}

func aliasExpansion(query cqr.CommonQueryRepresentation, seen map[string]bool) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		bq := cqr.NewBooleanQuery(cqr.OR, nil)
		cui := q.GetOption("cui").(string)
		seen[q.QueryString] = true
		if aliases, ok := cuiAliases[cui]; ok {
			for _, alias := range aliases {
				if strings.Contains(alias, "year") || strings.ContainsAny(alias, "()[].^*+/") {
					continue
				}
				a := strings.ToLower(alias)
				if _, ok := seen[a]; !ok {
					bq.Children = append(bq.Children, cqr.NewKeyword(fmt.Sprintf(`"%s"`, strings.ToLower(a)), fields.TitleAbstract))
					seen[a] = true
				}
			}
		}
		if len(bq.Children) == 0 || bq.Children == nil {
			return q
		}
		bq.Children = append(bq.Children, q)
		return bq
	case cqr.BooleanQuery:
		bq := cqr.NewBooleanQuery(q.Operator, nil)
		for _, child := range q.Children {
			bq.Children = append(bq.Children, aliasExpansion(child, seen))
		}
		return bq
	}
	return nil
}

func stemQuery(query cqr.CommonQueryRepresentation, d map[string]bool, seen map[string]bool) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		for k := range d {
			if strings.Contains(strings.ToLower(q.QueryString), strings.Replace(strings.ToLower(k), "*", "", -1)) {
				q.QueryString = k
				if _, ok := seen[k]; !ok {
					q.SetOption("truncated", true)
					seen[k] = true
					return q
				} else {
					return nil
				}
			}
		}
		return q
	case cqr.BooleanQuery:
		var c []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			s := stemQuery(child, d, seen)
			if s != nil {
				c = append(c, s)
			}
		}
		q.Children = c
		return q
	default:
		return q
	}
}

func tfidf(tf, df, N float64) float64 {
	return tf * math.Log(N/df)
}

func pseudoRelevanceFeedbackExpansion(query cqr.CommonQueryRepresentation, qrels trecresults.Qrels, kind int) cqr.CommonQueryRepresentation {
	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		panic(err)
	}

	//tq, err := transmute.CompileCqr2PubMed(query)
	//if err != nil {
	//	panic(err)
	//}
	//search:
	//ids, err := e.Search(tq, func(p *entrez.Parameters) {
	//	p.Sort = "relevance"
	//})
	//if err != nil {
	//	fmt.Println(err)
	//	goto search
	//}
	var relevant []int
	//for _, id := range ids {
	//	if len(relevant) == 3 {
	//		break
	//	}
	//	if v, ok := qrels[strconv.Itoa(id)]; ok {
	//		if v.Score > 0 {
	//			relevant = append(relevant, id)
	//		}
	//	}
	//}
	for _, q := range qrels {
		if len(relevant) >= 3 {
			break
		}
		if q.Score > 0 {
			v, err := strconv.Atoi(q.DocId)
			if err != nil {
				panic(err)
			}
			relevant = append(relevant, v)
		}
	}
fetch:
	docs, err := e.Fetch(relevant)
	if err != nil {
		fmt.Println(err)
		goto fetch
	}

	type term struct {
		text  string
		tf    float64
		df    float64
		tfidf float64
	}
	terms := make(map[string]term)
	var last int
	seen := make(map[string]bool)
	for i, doc := range docs {
		if i > last {
			last = i
			seen = make(map[string]bool)
		}
		candidates := metaMapCandidates(fmt.Sprintf("%s. %s", doc.Title, doc.Text))
		for _, candidate := range candidates {
			if _, ok := terms[candidate.CandidateMatched]; !ok {
				terms[candidate.CandidateMatched] = term{
					text: candidate.CandidateMatched,
					tf:   1,
					df:   1,
				}
				seen[candidate.CandidateMatched] = true
			} else if _, ok := seen[candidate.CandidateMatched]; !ok {
				t := terms[candidate.CandidateMatched]
				t.tf++
				t.df++
				terms[candidate.CandidateMatched] = t
				seen[candidate.CandidateMatched] = true
			} else {
				t := terms[candidate.CandidateMatched]
				t.tf++
				terms[candidate.CandidateMatched] = t
			}
		}
	}

	// Compute tf-idf for all the terms.
	termSlice := make([]term, len(terms))
	var i int
	for _, v := range terms {
		termSlice[i] = v
		termSlice[i].tfidf = tfidf(termSlice[i].tf, termSlice[i].df, float64(len(docs)))
		i++
	}

	// Sort the terms in terms of tf-idf.
	sort.Slice(termSlice, func(i, j int) bool {
		return termSlice[i].tfidf > termSlice[j].tfidf
	})

	// Get the top-5 terms.
	seen = make(map[string]bool)
	var expansionTerms []string
	for _, term := range termSlice {
		if len(expansionTerms) == 5 {
			break
		}
		t := strings.ToLower(term.text)
		if _, ok := seen[t]; !ok {
			fmt.Println(t)
			expansionTerms = append(expansionTerms, t)
			seen[t] = true
		}
	}

	// Add the new query terms to the query.
	bq := cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{})
	for _, term := range expansionTerms {
		bq.Children = append(bq.Children, cqr.NewKeyword(term, fields.TitleAbstract))
	}
	return bq
}

//noinspection GoBoolExpressions
func generateQueries(queries []pipeline.Query, protocols protocols, notFound map[string]bool) {
	f, err := os.OpenFile(qrelsPath, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	rels, err := trecresults.QrelsFromReader(f)
	if err != nil {
		panic(err)
	}

	for _, q := range queries {
		switch q.Topic {
		case "CD008782", "CD009593", "CD009925", "CD010386", "CD010632", "CD010705", "CD011975", "CD011984", "CD012216", "CD012599":
			continue
		default:
			q.Name = strings.Replace(q.Name, "®", "", -1)
			q.Name = strings.Replace(q.Name, "’", "", -1)
			q.Name = strings.Replace(q.Name, "'", "", -1)
		}
		//if q.Topic != "CD009519" {
		//	continue
		//}
		fmt.Printf("+ [%s] %s \n", q.Topic, q.Name)
		// Stop if the p does not have a protocol.
		if _, ok := notFound[q.Topic]; ok {
			continue
		}

		type entity struct {
			score int
			text  string
			cui   string
		}

		// Map the terms in the query to "Concepts".
		// This transforms the query from the previous step by replacing terms with concepts
		// and annotating the query with a CUI for the next step.
		var mapQuery func(r cqr.CommonQueryRepresentation, kind int) cqr.CommonQueryRepresentation
		mapQuery = func(r cqr.CommonQueryRepresentation, kind int) (v cqr.CommonQueryRepresentation) {
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
					var matched string
					if kind == 0 {
						matched = strings.ToLower(c.CandidateMatched)
					} else if kind == 1 {
						matched = strings.ToLower(c.CandidatePreferred)
					} else if kind == 2 {
						if v, ok := cuiMapping[c.CandidateCUI]; ok {
							matched = strings.ToLower(v)
							if strings.Contains(matched, "year") || strings.ContainsAny(v, "()[].^*+/<>") {
								continue
							}
						} else {
							matched = strings.ToLower(c.CandidateMatched)
						}
					} else {
						if v, ok := cuiAliases[c.CandidateCUI]; ok {
							for _, x := range v {
								matched = strings.ToLower(x)
								if strings.Contains(matched, "year") || strings.ContainsAny(x, "()[].^*+/<>") {
									continue
								}
							}
						} else {
							matched = strings.ToLower(c.CandidateMatched)
						}
					}
					if _, ok := seen[matched]; !ok {
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
				if len(entities) == 0 {
					return nil
				} else if len(entities) == 1 {
					return cqr.NewKeyword(fmt.Sprintf(`"%s"`, entities[0].text), q.Fields...).SetOption("cui", entities[0].cui)
				}
				b := cqr.NewBooleanQuery(cqr.OR, nil)
				for _, concept := range entities {
					b.Children = append(b.Children, cqr.NewKeyword(fmt.Sprintf(`"%s"`, concept.text), q.Fields...).SetOption("cui", concept.cui))
				}
				return b
			case cqr.BooleanQuery:
				b := cqr.NewBooleanQuery(q.Operator, nil)
				var qs []string
				for _, child := range q.Children {
					switch v := child.(type) {
					case cqr.Keyword:
						qs = append(qs, v.QueryString)
					case cqr.BooleanQuery:
						m := mapQuery(child, kind)
						if m != nil {
							b.Children = append(b.Children, m)
						}
					}
				}

				k := cqr.NewKeyword(fmt.Sprintf(`"%s"`, strings.Join(qs, " ")), fields.TitleAbstract)
				if len(qs) == len(q.Children) {
					//b.Children = append(b.Children, cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{mapQuery(k)}))
					return mapQuery(k, kind)
				} else if len(qs) > 0 {
					v := mapQuery(k, kind)
					if v != nil {
						b.Children = append(b.Children, v)
					}
				}
				return b
			}
			return
		}

		stemDict := make(map[string]bool)
		for _, kw := range analysis.QueryKeywords(q.Query) {
			if v, ok := kw.Options["truncated"]; ok {
				if v.(bool) == true {
					stemDict[kw.QueryString] = true
				}
			}
		}

		//simple := cqr.NewBooleanQuery(cqr.OR, nil)
		//seen := make(map[string]bool)
		//for _, c := range metaMapCandidates(q.Name) {
		//	if _, ok := seen[strings.ToLower(c.CandidateMatched)]; !ok {
		//		simple.Children = append(simple.Children, cqr.NewKeyword(c.CandidateMatched, fields.TitleAbstract))
		//		seen[strings.ToLower(c.CandidateMatched)] = true
		//	}
		//}
		//s1, _ := transmute.CompileCqr2Medline(simple)
		//err = os.MkdirAll(path.Join(queryOutputDir, "simple"), 0777)
		//if err != nil {
		//	panic(err)
		//}
		//err = ioutil.WriteFile(path.Join(queryOutputDir, "simple", q.Topic), []byte(s1), 0644)
		//if err != nil {
		//	panic(err)
		//}

		type fquery struct {
			query  cqr.CommonQueryRepresentation
			output string
		}

		var queries []fquery
		// Entity mapping.
		for i, logicMethod := range []string{"nlp_", "manual_"} {
			outputDir0 := logicMethod
			var p cqr.CommonQueryRepresentation
			var err error
			if i == 0 {
				p, err = nlpQueryLogicComposer(q)
				if err != nil {
					panic(err)
				}
			}

			if i == 1 {
				p, err = manualQueryLogicComposer(q)
				if err != nil {
					panic(err)
				}
			}
			fmt.Sprintln(outputDir0, p, rels)
			//
			for j, mappingMethod := range []string{"match", "preferred", "frequency", "alias"} {
				// Create the 'Match' query.
				outputDir1 := outputDir0 + mappingMethod
				m := mapQuery(p, j)
				queries = append(queries, fquery{query: m, output: outputDir1})

				if j == 0 {
					continue
				}
				for k, expansionMethod := range []string{"_c2v"} {
					// Expand the 'Match' query.
					outputDir2 := outputDir1 + expansionMethod
					var exp cqr.CommonQueryRepresentation
					switch k {
					case 0:
						exp = cui2vecExpansion(m, j)
					}
					queries = append(queries, fquery{query: exp, output: outputDir2})
				}
			}
		}

		var squeries []fquery
		for _, f := range queries {
			squeries = append(squeries, fquery{query: stemQuery(f.query, stemDict, make(map[string]bool)), output: "stem_" + f.output})

			rct20 := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{f.query, seed.SensitivityFilter})
			rct21 := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{f.query, seed.PrecisionSensitivityFilter})

			for l, rct := range []cqr.CommonQueryRepresentation{rct20, rct21} {
				var outputDir3 string
				switch l {
				case 0:
					outputDir3 = "rcts_" + f.output
				case 1:
					outputDir3 = "rctp_" + f.output
				}
				squeries = append(squeries, fquery{query: rct, output: outputDir3})
			}
		}

		queries = append(queries, squeries...)
		for _, s := range queries {
			tq, _ := transmute.CompileCqr2Medline(s.query)
			err = os.MkdirAll(path.Join(queryOutputDir, s.output), 0777)
			if err != nil {
				panic(err)
			}
			err = ioutil.WriteFile(path.Join(queryOutputDir, s.output, q.Topic), []byte(tq), 0644)
			if err != nil {
				panic(err)
			}
		}
	}
}

func manualQueryLogicComposer(q pipeline.Query) (cqr.CommonQueryRepresentation, error) {
	p := cqr.NewBooleanQuery(cqr.AND, nil)

	outputPath := path.Join(dir, "manual", q.Topic)
	if _, err := os.Stat(outputPath); err == nil {
		b, err := ioutil.ReadFile(outputPath)
		if err != nil {
			return nil, err
		}
		s := bufio.NewScanner(bytes.NewBuffer(b))
		for s.Scan() {
			line := s.Text()
			//fmt.Println(line)
			p.Children = append(p.Children, cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{cqr.NewKeyword(line, fields.TitleAbstract)}))
		}
		return p, nil
	}
	var buff string
	//fmt.Println(q.Name)
	l, err := readline.New("> ")
	if err != nil {
		return nil, err
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			return nil, err
		}
		switch line {
		case "q":
			goto exit
		default:
			buff += fmt.Sprintln(line)
			p.Children = append(p.Children, cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{cqr.NewKeyword(line, fields.TitleAbstract)}))
		}
	}
exit:
	err = ioutil.WriteFile(outputPath, []byte(buff), 0644)
	return p, err
}

func nlpQueryLogicComposer(q pipeline.Query) (cqr.CommonQueryRepresentation, error) {
	// Parse title: "Query Logic Composition".
	cmd := exec.Command("bash", "-c", fmt.Sprintf(`echo "%s" | java -cp "%s/*" edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat "penn" %s/englishPCFG.ser.gz -`, q.Name, javaClassPath, javaClassPath))
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	var buff bytes.Buffer
	s := bufio.NewScanner(bufio.NewReader(r))
	for s.Scan() {
		_, err = buff.Write(s.Bytes())
		if err != nil {
			return nil, err
		}
	}

	// The magic part.
	p := simplify(treeToQuery(parseTree(buff.String())))
	return p, nil
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

		//fmt.Printf("+ [%s]%s\n", q.Topic, q.Name)

		var queryConcepts []string
		seen := make(map[string]bool)

		keywords := analysis.QueryKeywords(q.Query)
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
			//fmt.Printf(" (%d)\n", len(c))
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
			//fmt.Printf(" | ? %s\n", k)
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

		//fmt.Printf("+ [%s]%s\n", q.Topic, q.Name)

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

func simplifyOriginal(query cqr.CommonQueryRepresentation) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		if len(q.Fields) == 1 {
			switch q.Fields[0] {
			case fields.MeshHeadings, fields.MeSHMajorTopic, fields.MeSHSubheading, fields.MeSHTerms, fields.FloatingMeshHeadings, fields.MajorFocusMeshHeading:
				return nil
			}
		} else {
			q.Fields = []string{fields.TitleAbstract}
		}
		return q
	case cqr.BooleanQuery:
		var c []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			s := simplifyOriginal(child)
			if s != nil {
				c = append(c, s)
			}
		}
		q.Children = c
		return q
	}
	return query
}

func readAndWriteQueries() []pipeline.Query {
	qs := query.TARTask2QueriesSource{}
	queries, err := qs.Load(queriesDir)
	if err != nil {
		panic(err)
	}
	//for _, q := range queries {
	//	//q1, _ := transmute.CompileCqr2Medline(q.Query)
	//	//err = os.MkdirAll(path.Join(queryOutputDir, "original"), 0777)
	//	//if err != nil {
	//	//	panic(err)
	//	//}
	//	//err = ioutil.WriteFile(path.Join(queryOutputDir, "original", q.Topic), []byte(q1), 0644)
	//	//if err != nil {
	//	//	panic(err)
	//	//}
	//	//q2, _ := transmute.CompileCqr2Medline(simplifyOriginal(q.Query))
	//	//err = os.MkdirAll(path.Join(queryOutputDir, "original_simplified"), 0777)
	//	//if err != nil {
	//	//	panic(err)
	//	//}
	//	//err = ioutil.WriteFile(path.Join(queryOutputDir, "original_simplified", q.Topic), []byte(q2), 0644)
	//	//if err != nil {
	//	//	panic(err)
	//	//}
	//}
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
