package stats

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/biogo/ncbi"
	"github.com/biogo/ncbi/entrez"
	"github.com/biogo/ncbi/entrez/search"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/trecresults"
	"gopkg.in/neurosnap/sentences.v1"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type EntrezStatisticsSource struct {
	tool       string
	key        string
	email      string
	api        string
	parameters map[string]float64
	options    SearchOptions
}

type pubmedArticleSet struct {
	PubmedArticle []pubmedArticle `xml:"PubmedArticle"`
}

type pubmedArticle struct {
	MedlineCitation medlineCitation `xml:"MedlineCitation"`
}

type medlineCitation struct {
	Article article `xml:"Article"`
}

type article struct {
	ArticleTitle string   `xml:"ArticleTitle"`
	Abstract     abstract `xml:"Abstract"`
}

type abstract struct {
	AbstractText string `xml:"AbstractText"`
}

type term struct {
	count int
	token string
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

func (e EntrezStatisticsSource) searchStart(pmid int) func(p *entrez.Parameters) {
	return func(p *entrez.Parameters) {
		p.RetStart = pmid
	}
}

func (e EntrezStatisticsSource) search(query string, options ...func(p *entrez.Parameters)) ([]int, error) {
	var pmids []int
	p := &entrez.Parameters{}
	for _, option := range options {
		option(p)
	}
	p.RetMax = e.options.Size
	p.APIKey = e.key

	s, err := entrez.DoSearch("pubmed", query, p, nil, e.tool, e.email)
	if err != nil {
		return nil, err
	}
	pmids = s.IdList
	log.Println("start", p.RetStart, "got", len(pmids))

	// If the number of pmids equals the execute size, there might be more to come.
	if len(pmids) == e.options.Size {
		l, err := e.search(query, e.searchStart(p.RetStart+len(pmids)))
		if err != nil {
			return nil, err
		}
		pmids = append(pmids, l...)
	}
	return pmids, nil
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

	buff := new(bytes.Buffer)
	buff.ReadFrom(r)

	var p pubmedArticleSet
	xml.Unmarshal(buff.Bytes(), &p)
	if len(p.PubmedArticle) == 0 {
		return 0, nil
	}

	if len(p.PubmedArticle) == 0 {
		return 0, nil
	}

	var n int
	t := p.PubmedArticle[0].MedlineCitation.Article.ArticleTitle
	a := p.PubmedArticle[0].MedlineCitation.Article.Abstract.AbstractText
	n += strings.Count(fmt.Sprintf("%s %s", t, a), term)

	return float64(n), nil
}

func (e EntrezStatisticsSource) TermVector(document string) (TermVector, error) {
	// Get the pmid as an int.
	d, err := strconv.ParseInt(document, 10, 64)
	if err != nil {
		return nil, err
	}

	// Fetch the document for computing the term statistics.
	r, err := entrez.Fetch("pubmed", &entrez.Parameters{RetMode: "xml", APIKey: e.key}, e.tool, e.email, nil, int(d))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	buff := new(bytes.Buffer)
	buff.ReadFrom(r)

	var p pubmedArticleSet
	err = xml.Unmarshal(buff.Bytes(), &p)
	if err != nil {
		return nil, err
	}

	// Do not continue if we did not retrieve anything.
	if len(p.PubmedArticle) == 0 {
		return TermVector{}, nil
	}

	// Extract the title and the abstract.
	t := p.PubmedArticle[0].MedlineCitation.Article.ArticleTitle
	a := p.PubmedArticle[0].MedlineCitation.Article.Abstract.AbstractText

	reg, err := regexp.Compile("[^a-zA-Z0-9 -]+")
	if err != nil {
		return nil, err
	}

	// Format the title and abstract sentences.
	t = reg.ReplaceAllString(strings.ToLower(t), "")
	a = reg.ReplaceAllString(strings.ToLower(a), "")

	// Compute term frequencies within the document.
	tm := make(map[string]int)
	for _, token := range sentences.NewWordTokenizer(sentences.NewPunctStrings()).Tokenize(t, false) {
		tm[token.Tok]++
	}

	am := make(map[string]int)
	for _, token := range sentences.NewWordTokenizer(sentences.NewPunctStrings()).Tokenize(a, false) {
		am[token.Tok]++
	}

	// Create the strings that get submitted to pubmed.
	ts := make([]string, len(tm))
	as := make([]string, len(am))
	var i int
	for term := range tm {
		ts[i] = term
		i++
	}
	i = 0
	for term := range am {
		as[i] = term
		i++
	}

	// Get the document frequencies for each term.
	tf, err := entrez.DoSearch("pubmed", strings.Join(ts, " "), &entrez.Parameters{Field: "title", APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return nil, err
	}
	af, err := entrez.DoSearch("pubmed", strings.Join(as, " "), &entrez.Parameters{Field: "text", APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return nil, err
	}

	ast, err := tf.TranslationStack.AST()
	if err != nil {
		return nil, err
	}
	tt := mapTerms(extractTerms(ast))

	ast, err = af.TranslationStack.AST()
	if err != nil {
		return nil, err
	}
	at := mapTerms(extractTerms(ast))

	// Create the term vector and populate it with all the statistics.
	// TODO: total term frequency, term frequency.
	var tv TermVector
	for term, df := range tt {
		tv = append(tv, TermVectorTerm{
			DocumentFrequency: df,
			Field:             "title",
			Term:              term,
		})
	}
	for term, df := range at {
		tv = append(tv, TermVectorTerm{
			DocumentFrequency: df,
			Field:             "abstract",
			Term:              term,
		})
	}

	return tv, nil
}

func (e EntrezStatisticsSource) DocumentFrequency(term, field string) (float64, error) {
	s, err := entrez.DoSearch("pubmed", term, &entrez.Parameters{APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return 0, err
	}
	return float64(s.Count), nil
}

func (e EntrezStatisticsSource) TotalTermFrequency(term, field string) (float64, error) {
	pmids, err := e.search(term)
	if err != nil {
		return 0, err
	}

	r, err := entrez.Fetch("pubmed", &entrez.Parameters{Field: field, RetType: "xml", APIKey: e.key}, e.tool, e.email, nil, pmids...)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	buff := new(bytes.Buffer)
	buff.ReadFrom(r)

	var p pubmedArticleSet
	xml.Unmarshal(buff.Bytes(), &p)
	if len(p.PubmedArticle) == 0 {
		return 0, nil
	}

	var n int
	for _, article := range p.PubmedArticle {
		t := strings.ToLower(article.MedlineCitation.Article.ArticleTitle)
		a := strings.ToLower(article.MedlineCitation.Article.Abstract.AbstractText)
		n += strings.Count(fmt.Sprintf("%s %s", t, a), term)
	}

	return float64(n), nil
}

func (e EntrezStatisticsSource) InverseDocumentFrequency(term, field string) (float64, error) {
	info, err := entrez.DoInfo("pubmed", e.tool, e.email)
	if err != nil {
		return 0, err
	}
	N := float64(info.DbInfo.Count)

	s, err := entrez.DoSearch("pubmed", term, &entrez.Parameters{Field: field, APIKey: e.key}, nil, e.tool, e.email)
	if err != nil {
		return 0, err
	}
	nt := float64(s.Count)

	return idf(N, nt), nil
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

	pmids, err := e.search(q)
	if err != nil {
		return 0, err
	}

	return float64(len(pmids)), nil
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

func (e EntrezStatisticsSource) Execute(query groove.PipelineQuery, options SearchOptions) (trecresults.ResultList, error) {
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

	pmids, err := e.search(q)
	if err != nil {
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

func EntrezTool(tool string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.tool = tool
	}
}

func EntrezEmail(email string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.email = email
	}
}

func EntrezAPIKey(key string) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.key = key
	}
}

func EntrezOptions(options SearchOptions) func(source *EntrezStatisticsSource) {
	return func(source *EntrezStatisticsSource) {
		source.options = options
	}
}

func NewEntrezStatisticsSource(options ...func(source *EntrezStatisticsSource)) EntrezStatisticsSource {
	e := &EntrezStatisticsSource{}
	for _, option := range options {
		option(e)
	}

	if len(e.key) > 0 {
		entrez.Limit = ncbi.NewLimiter(time.Second / 10)
	}

	ncbi.SetTimeout(time.Minute)

	return *e
}
