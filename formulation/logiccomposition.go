package formulation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	rake "github.com/afjoseph/RAKE.Go"
	"github.com/hscells/cqr"
	"github.com/hscells/guru"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"gopkg.in/olivere/elastic.v7"
	"os/exec"
	"strings"
	"unicode"
)

type LogicComposer interface {
	Compose(text string) (cqr.CommonQueryRepresentation, error)
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
	metamapURL    string
	elasticClient *elastic.Client
}

func NewRAKELogicComposer(semtypes, metamap string, client *elastic.Client) RAKELogicComposer {
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
		metamapURL:    metamap,
		elasticClient: client,
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

func (n NLPLogicComposer) Compose(text string) (cqr.CommonQueryRepresentation, error) {
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
		res, err := client.Search("umls").Query(elastic.NewQueryStringQuery(term)).TerminateAfter(1).Do(context.Background())
		if err != nil {
			return nil, err
		}
		if res.Hits != nil {
			if len(res.Hits.Hits) > 0 {
				b, _ := res.Hits.Hits[0].Source.MarshalJSON()
				body := make(map[string]interface{})
				err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&body)
				if err != nil {
					return nil, err
				}
				cui := res.Hits.Hits[0].Id
				mapping[term] = mappingPair{
					CUI: cui,
				}
				semtypes := body["semtypes"].([]interface{})
				if len(semtypes) == 1 {
					tui := semtypes[0].(map[string]interface{})["TUI"]
					if s, ok := st[tui.(string)]; ok {
						mapping[term] = mappingPair{
							CUI:  cui,
							Abbr: s.Abbreviation,
						}
					}
				}
			}
		}
	}
	return mapping, nil
}

func (r RAKELogicComposer) Compose(text string) (cqr.CommonQueryRepresentation, error) {

	candidates := rake.RunRake(text)

	terms := make([]string, len(candidates))
	for i, candidate := range candidates {
		terms[i] = candidate.Key
	}

	mapping, err := elasticUMLSMapTerms(terms, r.elasticClient, r.semtypes)
	if err != nil {
		return nil, err
	}
	fmt.Println(mapping)

	conditions, treatments, studyTypes, other := classifyQueryTerms(terms, mapping, r.cuiSemtypes)
	fmt.Println(conditions)
	fmt.Println(treatments)
	fmt.Println(studyTypes)
	fmt.Println(other)
	conditionsKeywords, treatmentsKeywords, studyTypesKeywords, otherKeywords := makeKeywords(conditions, treatments, studyTypes, other...)
	return constructQuery(conditionsKeywords, treatmentsKeywords, studyTypesKeywords, otherKeywords...), nil
}
