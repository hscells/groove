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
	"github.com/jdkato/prose/v2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EntrezStatisticsSource struct {
	Limit      int
	tool       string
	key        string
	email      string
	db         string
	parameters map[string]float64
	rank       bool
	options    SearchOptions
	// The size of PubMed.
	N float64
}

type term struct {
	count int
	token string
}

//type EntrezDocument struct {
//	ID               string
//	Title            string
//	Text             string
//	MeSHHeadings     []string
//	PublicationTypes []string
//}

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

func (e EntrezStatisticsSource) SetDB(db string) EntrezStatisticsSource {
	e.db = db
	return e
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
	//fmt.Printf("%s", query)
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
	if e.rank {
		p.Sort = "rank"
		p.RetMax = e.options.Size
	}

	v := url.Values{}
	v["db"] = []string{e.db}
	v["term"] = []string{query}
	fillParams(p, v)
	fmt.Print(".")
	fails, nfails := 20, 20
search:
	r, err := entrez.SearchURL.Get(v, e.tool, e.email, entrez.Limit)
	if err != nil {
		if fails > 0 {
			log.Printf("search error: %v, retrying %d more times for %s", err, fails, time.Duration(((nfails-fails)*5)*int(time.Second)))
			fails--
			time.Sleep(time.Duration((nfails-fails)*5) * time.Second)
			goto search
		}
		goto search
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
		fmt.Println(string(b))
		return nil, err
	}
	fmt.Print(".")

	pmids := make([]int, len(s.EsearchResult.Idlist))
	for i, pmid := range s.EsearchResult.Idlist {
		pmids[i], err = strconv.Atoi(pmid)
		if err != nil {
			fmt.Println(string(b))
			return nil, err
		}
	}
	retstart, err := strconv.Atoi(s.EsearchResult.RetStart)
	if err != nil {
		fmt.Println(string(b))
		return nil, err
	}

	fmt.Printf("[%d/%s]", retstart+len(pmids), s.EsearchResult.Count)

	// If the number of pmids equals the execute size, there might be more to come.
	if e.rank || (e.Limit > 0 && len(pmids) >= e.Limit) {
		return pmids, nil
	} else if len(pmids) == e.options.Size {
		fails, nfails := 20, 20
	retry:
		l, err := e.Search(query, e.SearchStart(p.RetStart+len(pmids)), e.SearchSize(e.SearchOptions().Size))
		if err != nil {
			if fails > 0 {
				log.Printf("recursive search error: %v, retrying %d more times for %f seconds", err, fails, time.Duration(((nfails-fails)*5)*int(time.Second)).Seconds())
				fails--
				time.Sleep(time.Duration((nfails-fails)*5) * time.Second)
				goto retry
			}
			return nil, err
		}
		pmids = append(pmids, l...)
	}
	return pmids, nil
}

// Summary uses the entrez eutils to obtain summary documents for the ids.
func (e EntrezStatisticsSource) Summary(ids []string, value interface{}, options ...func(p *entrez.Parameters)) error {
	p := &entrez.Parameters{}
	for _, option := range options {
		option(p)
	}
	v := url.Values{}
	v["db"] = []string{e.db}
	v["id"] = []string{strings.Join(ids, ",")}
	fillParams(p, v)
	p.RetMax = e.options.Size
	p.RetMode = "xml"
	p.APIKey = e.key
	return entrez.SummaryURL.GetXML(v, e.tool, e.email, entrez.Limit, value)
}

// Fetch uses the entrez eutils to fetch the pubmed Article given a set of pubmed identifiers.
func (e EntrezStatisticsSource) Fetch(pmids []int, options ...func(p *entrez.Parameters)) ([]guru.MedlineDocument, error) {
	if len(pmids) == 0 {
		return guru.MedlineDocuments{}, nil
	}

	p := &entrez.Parameters{}
	//p.RetMode = "asn.1"
	p.RetMode = "text"
	p.RetType = "medline"
	p.APIKey = e.key
	for _, option := range options {
		option(p)
	}
	fails, nfails := 20, 20

retry:
	r, err := entrez.Fetch(e.db, p, e.tool, e.email, nil, pmids...)
	if err != nil {
		if fails > 0 {
			log.Printf("fetch error: %v, retrying %d more times for %f seconds", err, fails, time.Duration(((nfails-fails)*5)*int(time.Second)).Seconds())
			fails--
			time.Sleep(time.Duration((nfails-fails)*5) * time.Second)
			goto retry
		}
		return nil, err
	}

	//log.Println("reading")
	//ncbi.SetTimeout(1 * time.Hour
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	//d := make(map[string]interface{})
	//s, err := asn1.Unmarshal(b, &d)
	//if err != nil {
	//	return nil, err
	//}
	//
	//fmt.Println(s)
	//log.Println("unmarshalling")
	//s := guru.UnmarshalAbstract(bytes.NewReader(b))
	s := guru.UnmarshalMedline(bytes.NewReader(b))
	//log.Println("done")
	return s, r.Close()
}

func (e EntrezStatisticsSource) Link(pmids []int, linkname string) ([]int, error) {
	link, err := entrez.DoLink(e.db, "pubmed", "neighbor", "", &entrez.Parameters{
		LinkName: linkname,
	}, e.tool, e.email, nil, pmids)
	if err != nil {
		return nil, err
	}

	var links []int
	for _, ls := range link.LinkSets {
		for _, n := range ls.Neighbor {
			for _, l := range n.Link {
				links = append(links, l.Id.Id)
			}
		}
	}

	return links, nil
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
	r, err := entrez.Fetch(e.db, &entrez.Parameters{RetMode: "xml", APIKey: e.key}, e.tool, e.email, nil, int(d))
	if err != nil {
		return 0, err
	}
	defer r.Close()

	docs := guru.UnmarshalMedline(r)

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
	doc, err := prose.NewDocument(strings.ToLower(strings.Join([]string{docs[0].TI, docs[0].AB}, ". ")))
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
	s, err := entrez.DoSearch(e.db, term, &entrez.Parameters{APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return 0, err
	}
	return float64(s.Count), nil
}

func (e EntrezStatisticsSource) TotalTermFrequency(term, _ string) (float64, error) {
	pmids, err := e.Search(fmt.Sprintf("%s[Title/Abstract]", term))
	if err != nil {
		return 0, err
	}

	if len(pmids) == 0 {
		return 0, nil
	}

	docs, err := e.Fetch(pmids)
	if err != nil {
		return 0, err
	}

	var n int
	for _, doc := range docs {
		t := strings.ToLower(doc.TI)
		a := strings.ToLower(doc.AB)
		n += strings.Count(fmt.Sprintf("%s %s", t, a), term)
	}

	return float64(n), nil
}

func (e EntrezStatisticsSource) InverseDocumentFrequency(term, field string) (float64, error) {
	nt := e.Count(term, field)
	return idf(e.N, nt), nil
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
retry:
	entrez.Limit.Wait()
	c := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&rettype=count&term=%s&api_key=%s&tool=%s&email=%s", url.QueryEscape(q), e.key, e.tool, e.email), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		goto retry
	}

	var buff bytes.Buffer
	_, err = buff.ReadFrom(resp.Body)
	if err != nil {
		return 0, err
	}
	re := regexp.MustCompile("<Count>(?P<count>[0-9]+)</Count>")
	matches := re.FindSubmatch(buff.Bytes())
	if len(matches) >= 2 {
		return strconv.ParseFloat(string(bytes.TrimSpace(matches[1])), 32)
	}
	return 0, nil
}

func (e EntrezStatisticsSource) VocabularySize(field string) (float64, error) {
	i, err := entrez.DoInfo(e.db, e.tool, e.email)
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
			log.Printf("search execution error: %v, retrying %d more times for query: %s", err, fails, q)
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

var pmCollectionSize float64

func (e EntrezStatisticsSource) CollectionSize() (float64, error) {
	if e.N > 0 {
		return e.N, nil
	}
	info, err := entrez.DoInfo(e.db, e.tool, e.email)
	if err != nil {
		return 0, err
	}
	if pmCollectionSize == 0 {
		pmCollectionSize = float64(info.DbInfo.Count)
	}
	return pmCollectionSize, nil
}

func (e EntrezStatisticsSource) Translation(term string) ([]string, error) {
	s, err := entrez.DoSearch("pubmed", term, nil, nil, e.tool, e.email)
	if err != io.EOF && err != nil {
		return nil, err
	}
	if s == nil || len(s.TranslationStack) == 0 {
		return nil, nil
	}
	var translations []string
	_, nodes := s.TranslationStack[0].Consume(s.TranslationStack)
	for _, node := range nodes {
		if t, ok := node.(*search.Term); ok {
			translations = append(translations, strings.ReplaceAll(strings.ToLower(t.Term), "[all fields]", ""))
		}
	}
	return translations, nil
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

// EntrezOptions sets any additional options for the entrez statistics source.
func EntrezLimiter(limit time.Duration) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		entrez.Limit = ncbi.NewLimiter(limit)
	}
}

// EntrezDb sets the database to search.
func EntrezDb(db string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.db = db
	}
}

func EntrezRank(rank bool) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.rank = rank
	}
}

// NewEntrezStatisticsSource creates a new entrez statistics source for searching pubmed.
// When an API key is specified, the entrez request Limit is raised to 10 per second instead of the default 3.
func NewEntrezStatisticsSource(options ...func(source *EntrezStatisticsSource)) (EntrezStatisticsSource, error) {
	e := &EntrezStatisticsSource{
		db:   "pubmed",
		rank: false,
	}

	//if len(e.key) > 0 {
	entrez.Limit = ncbi.NewLimiter(time.Second / 9)
	//}

	//ncbi.SetTimeout(0)

	for _, option := range options {
		option(e)
	}

	var err error
	e.N, err = e.CollectionSize()
	if err != nil {
		return EntrezStatisticsSource{}, err
	}

	return *e, nil
}
