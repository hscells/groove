package stats

import (
	"bytes"
	"fmt"
	"github.com/biogo/ncbi"
	"github.com/biogo/ncbi/entrez"
	"github.com/biogo/ncbi/entrez/search"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/guru"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/trecresults"
	"github.com/mailru/easyjson"
	"gopkg.in/jdkato/prose.v2"
	"io/ioutil"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EntrezStatisticsSource struct {
	tool       string
	key        string
	email      string
	parameters map[string]float64
	options    SearchOptions
	// The size of PubMed.
	n float64
}

type term struct {
	count int
	token string
}

type EntrezDocument struct {
	ID           string
	Title        string
	Text         string
	MeSHHeadings []string
}

func formatTerm(term string) string {
	buff := bytes.NewBufferString("")
	for _, c := range term {
		if c == '[' {
			return buff.String()
		}
		buff.WriteRune(c)
	}
	return buff.String()
}

func extractTerms(node search.Node) (terms []term) {
	switch n := node.(type) {
	case *search.Op:
		for _, o := range n.Operands {
			t := extractTerms(o)
			terms = append(terms, t...)
		}
	case *search.Term:
		terms = append(terms, term{count: n.Count, token: formatTerm(n.Term)})
	}
	return
}

func mapTerms(terms []term) map[string]float64 {
	m := make(map[string]float64)
	for _, term := range terms {
		m[term.token] = float64(term.count)
	}
	return m
}

type Search struct {
	Count int `xml:"Count"`
}

func (e EntrezStatisticsSource) Count(term, field string) float64 {
	var s Search
count:
	err := entrez.SearchURL.GetXML(map[string][]string{"field": {field}, "api_key": {e.key}, "term": {term}}, e.tool, e.email, entrez.Limit, &s)
	if err != nil {
		log.Println(err)
		goto count
	}
	return float64(s.Count)
}

func (e EntrezStatisticsSource) SearchStart(n int) func(p *entrez.Parameters) {
	return func(p *entrez.Parameters) {
		p.RetStart = n
	}
}

func (e EntrezStatisticsSource) SearchSize(n int) func(p *entrez.Parameters) {
	return func(p *entrez.Parameters) {
		p.RetMax = n
	}
}

// fillParams adds elements to v based on the "param" tag of p if the value is not the
// zero value for that type.
func fillParams(p *entrez.Parameters, v url.Values) {
	if p == nil {
		return
	}
	pv := reflect.ValueOf(p).Elem()
	n := pv.NumField()
	t := pv.Type()
	for i := 0; i < n; i++ {
		tf := t.Field(i)
		if tf.PkgPath != "" && !tf.Anonymous {
			continue
		}
		tag := tf.Tag.Get("param")
		if tag != "" {
			in := pv.Field(i).Interface()
			switch cv := in.(type) {
			case int:
				if cv != 0 {
					v[tag] = []string{fmt.Sprint(cv)}
				}
			case string:
				if cv != "" {
					v[tag] = []string{cv}
				}
			default:
				panic("cannot reach")
			}
		}
	}
}

type esearch struct {
	EsearchResult esearchresult `json:"esearchresult"`
}

type esearchresult struct {
	RetStart string   `json:"retstart"`
	Count    string   `json:"count"`
	Idlist   []string `json:"idlist"`
}

// Search uses the entrez eutils to get the pmids for a given query.
func (e EntrezStatisticsSource) Search(query string, options ...func(p *entrez.Parameters)) ([]int, error) {
	fmt.Printf("%s", query)
	p := &entrez.Parameters{}
	p.RetMax = e.options.Size
	for _, option := range options {
		option(p)
	}
	if e.options.Size == 0 {
		e.options.Size = 100000
		p.RetMax = e.options.Size
	}
	p.APIKey = e.key
	p.RetMode = "json"

	//entrez.Limit.Wait()
	v := url.Values{}
	v["db"] = []string{"pubmed"}
	v["term"] = []string{query}
	fillParams(p, v)
	fmt.Print(".")
	r, err := entrez.SearchURL.Get(v, e.tool, e.email, entrez.Limit)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	fmt.Print(".")
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var s esearch
	fmt.Print(".")
	err = easyjson.Unmarshal(b, &s)
	if err != nil {
		return nil, err
	}
	fmt.Print(".\n")

	pmids := make([]int, len(s.EsearchResult.Idlist))
	for i, pmid := range s.EsearchResult.Idlist {
		pmids[i], err = strconv.Atoi(pmid)
		if err != nil {
			return nil, err
		}
	}
	retstart, err := strconv.Atoi(s.EsearchResult.RetStart)
	if err != nil {
		return nil, err
	}

	//pmids = s.EsearchResult.Idlist
	log.Printf("%d/%s\n", retstart+len(pmids), s.EsearchResult.Count)
	//log.Println(len(pmids) == e.options.Size, len(pmids), e.options.Size)
	// If the number of pmids equals the execute size, there might be more to come.
	if len(pmids) == e.options.Size {
		l, err := e.Search(query, e.SearchStart(p.RetStart+len(pmids)), e.SearchSize(e.SearchOptions().Size))
		if err != nil {
			return nil, err
		}
		pmids = append(pmids, l...)
	}
	return pmids, nil
}

// Fetch uses the entrez eutils to fetch the pubmed Article given a set of pubmed identifiers.
func (e EntrezStatisticsSource) Fetch(pmids []int, options ...func(p *entrez.Parameters)) ([]EntrezDocument, error) {
	p := &entrez.Parameters{}
	for _, option := range options {
		option(p)
	}
	p.RetMax = e.options.Size
	p.RetMode = "text"
	p.RetType = "medline"
	p.APIKey = e.key

	r, err := entrez.Fetch("pubmed", p, e.tool, e.email, nil, pmids...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	s := guru.UnmarshallMedline(r)

	docs := make([]EntrezDocument, len(s))
	for i, doc := range s {
		docs[i] = EntrezDocument{
			ID:           doc.PMID,
			Title:        doc.TI,
			Text:         doc.AB,
			MeSHHeadings: doc.MH,
		}
	}
	return docs, nil
}

func (e EntrezStatisticsSource) SearchOptions() SearchOptions {
	return e.options
}

func (e EntrezStatisticsSource) Parameters() map[string]float64 {
	return e.parameters
}

func (e EntrezStatisticsSource) TermFrequency(term, field, document string) (float64, error) {
	d, err := strconv.ParseInt(document, 10, 64)
	if err != nil {
		return 0, err
	}
	r, err := entrez.Fetch("pubmed", &entrez.Parameters{RetMode: "xml", APIKey: e.key}, e.tool, e.email, nil, int(d))
	if err != nil {
		return 0, err
	}
	defer r.Close()

	docs := guru.UnmarshallMedline(r)

	if len(docs) == 0 {
		return 0, nil
	}

	var n int
	t := docs[0].TI
	a := docs[0].AB
	n += strings.Count(fmt.Sprintf("%s %s", t, a), term)

	return float64(n), nil
}

func (e EntrezStatisticsSource) TermVector(document string) (TermVector, error) {
	// Get the pmid as an int.
	d, err := strconv.ParseInt(document, 10, 64)
	if err != nil {
		return nil, err
	}

	docs, err := e.Fetch([]int{int(d)})
	if err != nil {
		return nil, err
	}

	// Do not continue if we did not retrieve anything.
	if len(docs) == 0 {
		return nil, nil
	}

	// Extract the title and the Abstract.
	doc, err := prose.NewDocument(strings.ToLower(strings.Join([]string{docs[0].Title, docs[0].Text}, ". ")))
	if err != nil {
		return nil, err
	}

	var terms []string
	for _, tok := range doc.Tokens() {
		switch tok.Tag {
		case "JJ", "JJR", "JJS", "NN", "NNP", "NNPS", "NNS":
			terms = append(terms, tok.Text)
		default:
			continue
		}
	}

	// Create the term vector and populate it with all the statistics.
	termFrequencies := make(map[string]float64)
	for _, term := range terms {
		for _, t := range terms {
			if t == term {
				termFrequencies[t]++
			}
		}
	}

	ch := make(chan TermVectorTerm)
	var vec TermVector
	var wg sync.WaitGroup
	go func() {
		for tv := range ch {
			vec = append(vec, tv)
		}
	}()
	for term, tf := range termFrequencies {
		wg.Add(1)
		//go func(x string, y float64) {
		log.Println(term)
		s := e.Count(term, "tiab")
		ch <- TermVectorTerm{
			DocumentFrequency:  s,
			TotalTermFrequency: s,
			TermFrequency:      tf,
			Field:              "tiab",
			Term:               term,
		}
		wg.Done()

		//}(term, tf)
	}

	wg.Wait()
	close(ch)

	return vec, nil
}

func (e EntrezStatisticsSource) DocumentFrequency(term, field string) (float64, error) {
	s, err := entrez.DoSearch("pubmed", term, &entrez.Parameters{APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return 0, err
	}
	return float64(s.Count), nil
}

func (e EntrezStatisticsSource) TotalTermFrequency(term, field string) (float64, error) {
	pmids, err := e.Search(term, func(p *entrez.Parameters) {
		p.Field = field
	})
	if err != nil {
		return 0, err
	}

	docs, err := e.Fetch(pmids)
	if err != nil {
		return 0, err
	}

	var n int
	for _, doc := range docs {
		t := strings.ToLower(doc.Title)
		a := strings.ToLower(doc.Text)
		n += strings.Count(fmt.Sprintf("%s %s", t, a), term)
	}

	return float64(n), nil
}

func (e EntrezStatisticsSource) InverseDocumentFrequency(term, field string) (float64, error) {
	nt := e.Count(term, field)
	return idf(e.n, nt), nil
}

func (e EntrezStatisticsSource) RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error) {
	// First we need to transform the query into a PubMed query (suitable for entrez)
	d, err := backend.NewCQRQuery(query).String()
	if err != nil {
		return 0, err
	}
	bq, err := transmute.Cqr2Pubmed.Execute(d)
	if err != nil {
		return 0, err
	}
	q, err := bq.String()
	if err != nil {
		return 0, err
	}

	fails := 20
retry:
	s, err := entrez.DoSearch("pubmed", q, &entrez.Parameters{RetType: "xml", APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		if fails > 0 {
			log.Printf("error: %v, retrying %d more times", err, fails)
			fails--
			time.Sleep(5 * time.Second)
			goto retry
		}
		panic(err)
	}
	return float64(s.Count), nil
}

func (e EntrezStatisticsSource) VocabularySize(field string) (float64, error) {
	i, err := entrez.DoInfo("pubmed", e.tool, e.email)
	if err != nil {
		return 0, err
	}
	for _, f := range i.DbInfo.FieldList {
		if f.Name == field {
			return float64(f.TermCount), nil
		}
	}
	return 0, nil
}

func (e EntrezStatisticsSource) Execute(query pipeline.Query, options SearchOptions) (trecresults.ResultList, error) {
	// First we need to transform the query into a PubMed query (suitable for entrez)
	d, err := backend.NewCQRQuery(query.Query).String()
	if err != nil {
		return nil, err
	}
	bq, err := transmute.Cqr2Pubmed.Execute(d)
	if err != nil {
		return nil, err
	}
	q, err := bq.String()
	if err != nil {
		return nil, err
	}

	fails := 20
execute:
	pmids, err := e.Search(q)
	if err != nil {
		if fails > 0 {
			log.Printf("error: %v, retrying %d more times", err, fails)
			fails--
			time.Sleep(5 * time.Second)
			goto execute
		}
		return nil, err
	}

	r := make(trecresults.ResultList, len(pmids))
	for i, pmid := range pmids {
		r[i] = &trecresults.Result{
			DocId:   strconv.Itoa(pmid),
			RunName: e.options.RunName,
			Topic:   query.Topic,
		}
	}

	return r, nil
}

func (e EntrezStatisticsSource) CollectionSize() (float64, error) {
	if e.n > 0 {
		return e.n, nil
	}
	info, err := entrez.DoInfo("pubmed", e.tool, e.email)
	if err != nil {
		return 0, err
	}
	return float64(info.DbInfo.Count), nil
}

// EntrezTool sets the tool name for entrez.
func EntrezTool(tool string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.tool = tool
	}
}

// EntrezTool sets the email for entrez.
func EntrezEmail(email string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.email = email
	}
}

// EntrezTool sets the API key for entrez.
func EntrezAPIKey(key string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.key = key
	}
}

// EntrezOptions sets any additional options for the entrez statistics source.
func EntrezOptions(options SearchOptions) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.options = options
	}
}

// NewEntrezStatisticsSource creates a new entrez statistics source for searching pubmed.
// When an API key is specified, the entrez request limit is raised to 10 per second instead of the default 3.
func NewEntrezStatisticsSource(options ...func(source *EntrezStatisticsSource)) (EntrezStatisticsSource, error) {
	e := &EntrezStatisticsSource{}
	for _, option := range options {
		option(e)
	}

	if len(e.key) > 0 {
		entrez.Limit = ncbi.NewLimiter(time.Second / 10)
	}

	ncbi.SetTimeout(0)

	var err error
	e.n, err = e.CollectionSize()
	if err != nil {
		return EntrezStatisticsSource{}, err
	}

	return *e, nil
}
