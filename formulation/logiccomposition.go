package formulation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	rake "github.com/afjoseph/RAKE.Go"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/guru"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"gopkg.in/olivere/elastic.v7"
	"math/bits"
	"os/exec"
	"sort"
	"strings"
	"unicode"
)

type LogicComposer interface {
	Compose(query pipeline.Query) (cqr.CommonQueryRepresentation, error)
}

// NLPLogicComposer composes queries logically using the stanford English parser.
type NLPLogicComposer struct {
	javaClassPath string
}

func NewNLPLogicComposer(javaClassPath string) *NLPLogicComposer {
	return &NLPLogicComposer{javaClassPath: javaClassPath}
}

//// ManualLogicComposer composes queries with the help of human intervention.
//type ManualLogicComposer struct {
//}
//
//func NewManualLogicComposer() ManualLogicComposer {
//	return ManualLogicComposer{
//	}
//}

type RAKELogicComposer struct {
	semtypes      map[string]guru.SemType
	cuiSemtypes   semTypeMapping
	titles        map[string]string
	seedPMIDs     trecresults.QrelsFile
	metamapURL    string
	elasticClient *elastic.Client
	cui2vecClient *cui2vec.VecClient
}

func NewRAKELogicComposer(semtypes, metamap string, titles map[string]string, seedPMIDs trecresults.QrelsFile, esClient *elastic.Client, vecClient *cui2vec.VecClient) RAKELogicComposer {
	s := guru.LoadSemTypes(guru.SEMTYPES)
	x := make(map[string]guru.SemType)
	for _, v := range s {
		x[v.TUI] = v
	}
	z, err := loadSemTypesMapping(semtypes)
	if err != nil {
		panic(err)
	}
	return RAKELogicComposer{
		semtypes:      x,
		cuiSemtypes:   z,
		titles:        titles,
		seedPMIDs:     seedPMIDs,
		metamapURL:    metamap,
		elasticClient: esClient,
		cui2vecClient: vecClient,
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

func (n NLPLogicComposer) Compose(query pipeline.Query) (cqr.CommonQueryRepresentation, error) {
	text := query.Name
	// Parse title: "Query Logic Composition".
	cmd := exec.Command("bash", "-c", fmt.Sprintf(`echo "%s" | java -cp "%s/*" edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat "penn" %s/englishPCFG.ser.gz -`, text, n.javaClassPath, n.javaClassPath))
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

//func (m ManualLogicComposer) Compose(text string) (cqr.CommonQueryRepresentation, error) {
//	p := cqr.NewBooleanQuery(cqr.AND, nil)
//
//	outputPath := path.Join(m.outputPath, m.topic)
//	if _, err := os.Stat(outputPath); err == nil {
//		b, err := ioutil.ReadFile(outputPath)
//		if err != nil {
//			return nil, err
//		}
//		s := bufio.NewScanner(bytes.NewBuffer(b))
//		for s.Scan() {
//			line := s.Text()
//			//fmt.Println(line)
//			p.Children = append(p.Children, cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{cqr.NewKeyword(line, fields.TitleAbstract)}))
//		}
//		return p, nil
//	}
//	var buff string
//	//fmt.Println(qrels.Name)
//	l, err := readline.New("> ")
//	if err != nil {
//		return nil, err
//	}
//	defer l.Close()
//
//	for {
//		line, err := l.Readline()
//		if err != nil {
//			return nil, err
//		}
//		switch line {
//		case "qrels":
//			goto exit
//		default:
//			buff += fmt.Sprintln(line)
//			p.Children = append(p.Children, cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{cqr.NewKeyword(line, fields.TitleAbstract)}))
//		}
//	}
//exit:
//	err = ioutil.WriteFile(outputPath, []byte(buff), 0644)
//	return p, err
//}

//func quickUMLSTerms(terms []string, client quickumlsrest.Client) (mapping, error) {
//	m := make(mapping)
//	for _, term := range terms {
//		client.Match()
//	}
//}

func elasticUMLSMapTerms(terms []string, client *elastic.Client, st map[string]guru.SemType) (mapping, error) {
	mapping := make(mapping)
	for _, term := range terms {
		retries := 5
	search:
		res, err := client.Search("umls").Query(elastic.NewQueryStringQuery(fmt.Sprintf(`"%s"`, term))).TerminateAfter(1).Do(context.Background())
		if err != nil {
			retries--
			fmt.Println(retries, err)
			if retries < 1 {
				continue
			}
			goto search
		}
		if res.Hits != nil {
			if len(res.Hits.Hits) > 0 {
				b, _ := res.Hits.Hits[0].Source.MarshalJSON()
				body := make(map[string]interface{})
				err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&body)
				if err != nil {
					retries--
					fmt.Println(retries, err)
					if retries < 1 {
						continue
					}
					goto search
				}
				cui := res.Hits.Hits[0].Id
				mapping[term] = mappingPair{
					CUI: cui,
				}
				semtypes := body["semtypes"].([]interface{})
				if len(semtypes) > 0 {
					tui := semtypes[0].(map[string]interface{})["TUI"]
					if s, ok := st[tui.(string)]; ok {
						mapping[term] = mappingPair{
							CUI:  cui,
							Abbr: s.Abbreviation,
						}
					}
					continue
				}
			}
		}
	}
	return mapping, nil
}

func (r RAKELogicComposer) Compose(query pipeline.Query) (cqr.CommonQueryRepresentation, error) {
	text := query.Name
	topic := query.Topic

	seeds := make([]string, len(r.seedPMIDs.Qrels[topic]))
	i := 0
	for pmid := range r.seedPMIDs.Qrels[topic] {
		seeds[i] = pmid
		i++
	}

	titles := make([]string, len(seeds))
	for i, pmid := range seeds {
		titles[i] = r.titles[pmid]
	}
	// The titles are now in order and should not be sorted past this point.

	// BIG assumption: there can be no more than 64 seed studies.
	index := make(map[string]uint64)

	seen := make(map[string]struct{})
	toks, err := tokenise(strings.Join(append(titles, text), " "))
	candidates := make([]rake.Pair, len(toks))
	for i, tok := range toks {
		if len(tok) < 2 {
			continue
		}
		for _, t := range rake.StopWordsSlice {
			if t == string(tok) {
				goto skipTerm
			}
		}
		if _, ok := seen[string(tok)]; !ok {
			candidates[i] = rake.Pair{Key: string(tok)}
			seen[string(tok)] = struct{}{}
		}
	skipTerm:
	}

	//candidates := rake.RunRakeI18N(strings.Join(append(titles, text), " "), append(rake.StopWordsSlice, "versus"))
	fmt.Println("candidates:", len(candidates))
	var terms []string
	for _, candidate := range candidates {
		switch candidate.Key {
		case "the", "group", "groups", "and", "excluding", "community",
			"one", "two", "three", "four", "a", "s", "d",
			"low", "red flags":
			continue
		}
		if len(candidate.Key) < 2 {
			continue
		}
		terms = append(terms, candidate.Key)

		// Populate the index.
		for i, title := range titles {
			// Prevent integer overflows.
			if uint64(i) > (1 << 63) {
				continue
			}
			if strings.Contains(title, candidate.Key) {
				// Set the bit in the byte corresponding to the title.
				index[candidate.Key] |= 1 << uint64(i)
			}
		}
	}

	fmt.Println("terms:", len(terms))

	fmt.Println("umls mapping")

	mapping, err := elasticUMLSMapTerms(terms, r.elasticClient, r.semtypes)
	if err != nil {
		return nil, err
	}

	//fmt.Println("metamap mapping")
	//
	//mapping1, err := metaMapTerms(terms, metawrap.HTTPClient{URL: r.metamapURL})
	//if err != nil {
	//	return nil, err
	//}
	//
	//fmt.Println("combining mappings")
	//
	//for k, v := range mapping1 {
	//	if len(v.CUI) > 0 {
	//		mapping[k] = v
	//	}
	//}

	fmt.Println("mapped terms")

	type mappedTerm struct {
		term string
		cui  string
		vec  []float64
		df   int
	}

	var buckets [][]mappedTerm
	var mappedTerms []mappedTerm
	for t, v := range mapping {
		vec, err := r.cui2vecClient.Vec(v.CUI)
		if err != nil {
			return nil, err
		}
		mappedTerms = append(mappedTerms, mappedTerm{
			term: t,
			cui:  v.CUI,
			vec:  vec,
		})
	}

	fmt.Println("creating buckets")

	for _, term := range mappedTerms {
		if len(term.vec) == 0 {
			continue
		}
		maxSim := 0.3
		bestBucket := -1
		for i, bucket := range buckets {
			for _, bucketTerm := range bucket {
				if len(term.vec) != len(bucketTerm.vec) {
					continue
				}
				sim, err := cui2vec.Cosine(term.vec, bucketTerm.vec)
				if err != nil {
					return nil, err
				}
				//fmt.Println(sim, len(term.vec), term.term, bucketTerm.term)
				if sim > maxSim {
					maxSim = sim
					bestBucket = i
				}
			}
		}
		if bestBucket > -1 {
			buckets[bestBucket] = append(buckets[bestBucket], term)
		} else {
			buckets = append(buckets, []mappedTerm{term})
		}
	}

	fmt.Println("created buckets")
	fmt.Println("performing query reduction")
	var bestCoverage uint64
	for i, bucket := range buckets {

		if len(bucket) < 2 {
			continue
		}

		var maxCoverage uint64

		// Rank each term by DF and compute maximum maxCoverage of terms.
		for j, term := range bucket {
			posting := index[term.term]         // Posting is a uint64 where set bits indicate presence of term in a doc.
			term.df = bits.OnesCount64(posting) // DF is the count of set bits -- this is a constant time operation.
			bucket[j] = term
			maxCoverage |= posting
		}
		sort.Slice(bucket, func(i, j int) bool {
			return bucket[i].df > bucket[j].df
		})

		fmt.Printf("[bucket %d] terms: %d, max coverage: %b\n", i, len(bucket), maxCoverage)

		// Perform the maxCoverage trick using bitwise OR.
		var coverage uint64
		tmp := bucket[:0]
		for _, term := range bucket {
			newCoverage := index[term.term] | coverage
			//fmt.Printf("%d %d %b [%d, %d]\n", j, len(bucket), coverage, bits.OnesCount64(newCoverage), bits.OnesCount64(coverage))
			if bits.OnesCount64(newCoverage) > bits.OnesCount64(coverage) {
				// The term contributed to increasing the coverage, so add it to new temporary slice.
				coverage = newCoverage
				tmp = append(tmp, term)
			}
		}

		if bits.OnesCount64(coverage) > bits.OnesCount64(bestCoverage) {
			bestCoverage = coverage
		}

		fmt.Printf("[bucket %d] terms: %d, coverage: %b\n", i, len(tmp), coverage)

		// Update the bucket.
		buckets[i] = tmp
	}

	fmt.Println("reducing clauses")

	tmp := buckets[:0]
	for _, bucket := range buckets {
		var coverage uint64

		// Rank each term by DF and compute maximum maxCoverage of terms.
		for _, term := range bucket {
			posting := index[term.term] // Posting is a uint64 where set bits indicate presence of term in a doc.
			coverage |= posting
		}

		fmt.Printf("terms: %d, coverage: %b, best coverage: %b\n", len(bucket), coverage, bestCoverage)

		if bits.OnesCount64(bestCoverage)-bits.OnesCount64(coverage) < 5 {
			tmp = append(tmp, bucket)
		}
	}

	fmt.Println("reduced", len(buckets), "clauses to", len(tmp))

	fmt.Println("----------------------------------")
	bq := cqr.NewBooleanQuery(cqr.AND, nil)
	for i, bucket := range tmp {
		kws := make([]cqr.CommonQueryRepresentation, len(bucket))
		for j, term := range bucket {
			fmt.Println(i, term.term)
			kws[j] = cqr.NewKeyword(term.term, fields.TitleAbstract).SetOption(Entity, term.cui)
		}
		bq.Children = append(bq.Children, cqr.NewBooleanQuery(cqr.OR, kws))
	}
	//for t, v := range mapping {
	//	fmt.Println(t)
	//	bq.Children = append(bq.Children, cqr.NewKeyword(t, fields.TitleAbstract).SetOption(Entity, v.CUI))
	//}
	fmt.Println("----------------------------------")

	return bq, nil

	//conditions, treatments, studyTypes, other := classifyQueryTerms(terms, mapping, r.cuiSemtypes)
	//conditionsKeywords, treatmentsKeywords, studyTypesKeywords, otherKeywords := makeKeywords(conditions, treatments, studyTypes, other, mapping)
	//conditionsKeywords, _, _, _ := makeKeywords(conditions, treatments, studyTypes, other, mapping)
	//kw := make([]cqr.CommonQueryRepresentation, len(conditionsKeywords))
	//for i, w := range conditionsKeywords {
	//	kw[i] = w
	//}
	//return constructQuery(conditionsKeywords, treatmentsKeywords, studyTypesKeywords, otherKeywords...), nil
	//return cqr.NewBooleanQuery(cqr.AND, kw), nil
}
