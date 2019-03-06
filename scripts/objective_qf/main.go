package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/ghost"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/query"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/meshexp"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"io"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type Terms map[string]int

type MappingPair struct {
	CUI  string
	Abbr string
}

type Mapping map[string]MappingPair

// SemTypeMapping is a mapping of STY->Classification.
type SemTypeMapping map[string]string

type QueryCategory int

type Evaluation struct {
	Development map[string]float64
	Validation  map[string]float64
	Unseen      map[string]float64
}

const (
	None QueryCategory = iota
	Condition
	Treatment
	StudyType
)

var (
	categories = map[string]QueryCategory{
		"Test":      Treatment,
		"Treatment": Treatment,
		"Diagnosis": Condition,
	}
)

func main() {
	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		panic(err)
	}

	topics, err := query.TARTask2QueriesSource{}.Load("/Users/s4558151/Repositories/tar/2018-TAR/Task2/topics")
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile("/Users/s4558151/Repositories/tar/2018-TAR/Task2/qrel_abs_combined", os.O_RDONLY, 0664)
	qrels, err := trecresults.QrelsFromReader(f)
	if err != nil {
		panic(err)
	}

	// Get the population (background) collection.
	population, err := GetPopulationSet(e)
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		fmt.Println(topic.Topic)
		rels := qrels.Qrels[topic.Topic]
		var docs []int
		for _, rel := range rels {
			if rel.Score > 0 {
				v, err := strconv.Atoi(rel.DocId)
				if err != nil {
					panic(err)
				}
				docs = append(docs, v)
			}
		}

		if len(docs) <= 50 {
			continue
		}

		test, err := FetchDocuments(docs, e)
		if err != nil {
			panic(err)
		}

		rand.Seed(1000)

		// Split the 'test' set into dev, val, and unseen.
		dev, val, unseen := split(test)
		fmt.Println(len(dev), len(val), len(unseen))

		// Perform 'term frequency analysis' on the development set.
		devDF, err := TermFrequencyAnalysis(dev)
		if err != nil {
			panic(err)
		}

		// Take terms from dev where the DF > 20% of size of dev.
		cut := CutDevelopmentTerms(devDF, dev)

		// Rank the cut terms in dev by DF.
		terms := RankTerms(cut, dev, topic.Topic)

		// Perform 'term frequency analysis' on the population.
		popDF, err := TermFrequencyAnalysis(population)
		if err != nil {
			panic(err)
		}

		// Identify dev terms which appear in <= 2% of the DF of the population set.
		queryTerms := CutDevelopmentTermsWithPopulation(terms, popDF, topic.Topic)

		mapping, err := MetaMapTerms(queryTerms, metawrap.HTTPClient{URL: "http://ielab-metamap.uqcloud.net"})
		if err != nil {
			panic(err)
		}

		// Load the sem type mapping.
		semTypes, err := loadSemTypesMapping("/Users/s4558151/Papers/sysrev_queries/amia2019_objective/experiments/cui_semantic_types.txt")
		if err != nil {
			panic(err)
		}

		// Classify query terms.
		conditions, treatments, studyTypes := ClassifyQueryTerms(queryTerms, mapping, semTypes)

		// And then filter the query terms.
		conditions, treatments, studyTypes, err = FilterQueryTerms(conditions, treatments, studyTypes, fields.TitleAbstract, makeQrels(dev), e)
		if err != nil {
			panic(err)
		}

		// Create keywords for the proceeding query.
		conditionsKeywords, treatmentsKeywords, studyTypesKeywords := MakeKeywords(conditions, treatments, studyTypes)

		conditionsKeywordsWithMeSH, treatmentsKeywordsWithMeSH, studyTypesKeywordsWithMeSH, err := AddMeSHTerms(conditionsKeywords, treatmentsKeywords, studyTypesKeywords, dev, e, topic.Topic)
		if err != nil {
			panic(err)
		}

		// Create the query from the three categories.
		q := ConstructQuery(conditionsKeywords, treatmentsKeywords, studyTypesKeywords)

		qWithMeSH := ConstructQuery(conditionsKeywordsWithMeSH, treatmentsKeywordsWithMeSH, studyTypesKeywordsWithMeSH)

		fmt.Println("evaluation without mesh terms:")
		qEval, _ := transmute.CompileCqr2PubMed(q)
		f, err := os.OpenFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/queries", topic.Topic+".query"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
		if err != nil {
			panic(err)
		}
		_, err = f.WriteString(qEval)
		if err != nil {
			panic(err)
		}
		f.Close()
		evalWithoutMeSH, err := Evaluate(q, e, dev, val, unseen)
		if err != nil {
			panic(err)
		}
		b, err := json.Marshal(evalWithoutMeSH)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/evaluation", topic.Topic+".evaluation.json"), b, 0664)
		if err != nil {
			panic(err)
		}

		fmt.Println("evaluation with mesh terms:")
		qWithMeshEval, _ := transmute.CompileCqr2PubMed(qWithMeSH)
		f, err = os.OpenFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/queries", topic.Topic+"_mesh.query"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
		if err != nil {
			panic(err)
		}
		_, err = f.WriteString(qWithMeshEval)
		if err != nil {
			panic(err)
		}
		f.Close()
		evalWithMeSH, err := Evaluate(qWithMeSH, e, dev, val, unseen)
		if err != nil {
			panic(err)
		}

		b, err = json.Marshal(evalWithMeSH)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/evaluation", topic.Topic+"_mesh.evaluation.json"), b, 0664)
		if err != nil {
			panic(err)
		}
	}
}

func Evaluate(query cqr.CommonQueryRepresentation, e stats.EntrezStatisticsSource, dev, val, unseen []guru.MedlineDocument) (Evaluation, error) {
	// Execute the query and find the effectiveness.
	filecache := combinator.NewFileQueryCache("/Users/s4558151/filecache")
	pq := pipeline.NewQuery("0", "0", query)
	tree, _, err := combinator.NewLogicalTree(pq, e, filecache)
	if err != nil {
		panic(err)
	}
	results := tree.Documents(filecache).Results(pq, "0")
	eval.RelevanceGrade = 0
	ev := []eval.Evaluator{eval.NumRel, eval.NumRet, eval.NumRelRet, eval.RecallEvaluator, eval.PrecisionEvaluator, eval.F1Measure, eval.F05Measure, eval.F3Measure}
	devEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": makeQrels(dev)}}, "0")
	valEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": makeQrels(val)}}, "0")
	unseenEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": makeQrels(unseen)}}, "0")
	return Evaluation{
		Development: devEval,
		Validation:  valEval,
		Unseen:      unseenEval,
	}, nil
}

// split creates three slices of documents: 2:4 development, 1:4 validation, 1:4 unseen.
func split(docs []guru.MedlineDocument) ([]guru.MedlineDocument, []guru.MedlineDocument, []guru.MedlineDocument) {
	rand.Shuffle(len(docs), func(i, j int) {
		docs[i], docs[j] = docs[j], docs[i]
	})

	devSplit := float64(len(docs)) * 0.5
	valSplit := float64(len(docs)) * 0.25

	var (
		dev    []guru.MedlineDocument
		val    []guru.MedlineDocument
		unseen []guru.MedlineDocument
	)
	for i, j := 0.0, 0; i < float64(len(docs)); i++ {
		if i < devSplit {
			dev = append(dev, docs[j])
		} else if i < devSplit+valSplit {
			val = append(val, docs[j])
		} else {
			unseen = append(unseen, docs[j])
		}
		j++
	}
	return dev, val, unseen
}

func loadSemTypesMapping(filename string) (SemTypeMapping, error) {
	m := make(SemTypeMapping)

	f, err := os.OpenFile(filename, os.O_RDONLY, 0664)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(f)
	r.Comma = '|'
	r.LazyQuotes = true
	for {
		if row, err := r.Read(); err == nil {
			m[row[1]] = row[4]
		} else if err == io.EOF {
			break
		} else {
			return nil, err
		}
	}

	return m, nil
}

func makeQrels(docs []guru.MedlineDocument) trecresults.Qrels {
	qrels := make(trecresults.Qrels)
	for _, doc := range docs {
		qrels[doc.PMID] = &trecresults.Qrel{
			Topic:     "0",
			Iteration: "0",
			DocId:     doc.PMID,
			Score:     1,
		}
	}
	return qrels
}

// FetchDocuments retrieves the references to studies of target PMID(s).
func FetchDocuments(refs []int, e stats.EntrezStatisticsSource) ([]guru.MedlineDocument, error) {
	// Open the document store.
	g, err := ghost.Open("/Users/s4558151/ghost/groove/objective/docs", ghost.NewGobSchema(guru.MedlineDocument{}))
	if err != nil {
		return nil, err
	}

	// Try to retrieve as many docs from disk as possible.
	var (
		fetching   []int
		docs       []guru.MedlineDocument
		references []guru.MedlineDocument
	)

	for _, ref := range refs {
		pmid := strconv.Itoa(ref)
		if g.Contains(pmid) {
			var d guru.MedlineDocument
			err := g.Get(pmid, &d)
			if err != nil {
				return nil, err
			}
			references = append(references, d)
		} else {
			fetching = append(fetching, ref)
		}
	}

	fmt.Println("found", len(references), "fetching", len(fetching))

	if len(fetching) > 0 {
		// Retrieve the documents of the references.
		docs, err = e.Fetch(fetching)
		if err != nil {
			return nil, err
		}

		// Put the docs in the store.
		for _, d := range docs {
			err := g.Put(d.PMID, d)
			if err != nil {
				return nil, err
			}
			fmt.Println("put", d.PMID)
		}
	}
	// Close the document store.
	err = g.Close()
	if err != nil {
		return nil, err
	}

	return append(references, docs...), nil
}

// TermFrequencyAnalysis computes the document frequency for the input documents.
func TermFrequencyAnalysis(docs []guru.MedlineDocument) (Terms, error) {
	t := make(Terms)
	for _, doc := range docs {
		seen := make(map[string]bool)
		o, err := tokenise(strings.ToLower(fmt.Sprintf("%s. %s", doc.TI, doc.AB)), true)
		if err != nil {
			return nil, err
		}
		for _, tok := range o {
			term := string(tok)
			if _, ok := seen[term]; ok {
				continue
			}
			t[term]++
			seen[term] = true
		}
	}
	return t, nil
}

// RankTerms ranks a map of terms by document frequency.
func RankTerms(t Terms, dev []guru.MedlineDocument, topic string) []string {
	type pair struct {
		K string
		V int
		P float64
	}

	var pairs []pair
	for k, v := range t {
		pairs = append(pairs, pair{K: k, V: v, P: float64(v) / float64(len(dev))})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].V > pairs[j].V
	})

	f, err := os.OpenFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/document_frequency", topic+".development.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}

	err = json.NewEncoder(f).Encode(pairs)
	if err != nil {
		panic(err)
	}

	terms := make([]string, len(pairs))
	for i, v := range pairs {
		terms[i] = v.K
	}

	return terms
}

// GetPopulationSet retrieves a set of publications to form a population set.
func GetPopulationSet(e stats.EntrezStatisticsSource) ([]guru.MedlineDocument, error) {
	e.Limit = 10000
	pmids, err := e.Search(`("0000"[Date - Publication] : "2018"[Date - Publication])`, e.SearchSize(10000), )
	if err != nil {
		return nil, err
	}
	e.Limit = 0
	fmt.Println("fetching docs")

	return FetchDocuments(pmids, e)
}

// CutDevelopmentTerms takes terms from the development set where the DF of the term is >= 20%.
func CutDevelopmentTerms(t Terms, dev []guru.MedlineDocument) Terms {
	n := float64(len(dev)) * 0.2 // term must be <= 20% of development DF.
	terms := make(Terms)

	for k, v := range t {
		if float64(v) >= n {
			terms[k] = v
		}
	}

	return terms
}

// CutDevelopmentTermsWithPopulation takes terms where the DF in the population collection is <= 2%.
func CutDevelopmentTermsWithPopulation(dev []string, population Terms, topic string) []string {
	n := float64(len(population)) * 0.02 // Term must be <= 2% of population DF.
	var t []string

	type pair struct {
		K string
		V int
		P float64
	}
	terms := make(Terms)

	for _, term := range dev {
		if df, ok := population[term]; ok {
			if float64(df) <= n {
				terms[term] = df
				t = append(t, term)
			}
		}
	}

	var pairs []pair
	for k, v := range terms {
		pairs = append(pairs, pair{K: k, V: v, P: float64(v) / float64(len(population))})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].V > pairs[j].V
	})

	f, err := os.OpenFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/document_frequency", topic+".population.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}

	err = json.NewEncoder(f).Encode(pairs)
	if err != nil {
		panic(err)
	}

	return t
}

// MetaMapTerms maps terms to CUIs.
func MetaMapTerms(terms []string, client metawrap.HTTPClient) (Mapping, error) {
	// Open the document store.
	g, err := ghost.Open("/Users/s4558151/ghost/groove/objective/metamap", ghost.NewGobSchema(MappingPair{}))
	if err != nil {
		return nil, err
	}

	cuis := make(Mapping)
	for _, term := range terms {
		if g.Contains(term) {
			var p MappingPair
			err := g.Get(term, &p)
			if err != nil {
				return nil, err
			}
			cuis[term] = p
			fmt.Println("get term", term)
		} else {
			candidates, err := client.Candidates(term)
			if err != nil {
				return nil, err
			}
			found := false
			for _, candidate := range candidates {
				if strings.ToLower(candidate.CandidateMatched) == term {
					p := MappingPair{
						CUI:  candidate.CandidateCUI,
						Abbr: candidate.SemTypes[0],
					}
					cuis[term] = p
					err := g.Put(term, p)
					if err != nil {
						return nil, err
					}
					found = true
					fmt.Println("put term", term)
					break
				}
			}

			if !found {
				err := g.Put(term, MappingPair{})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	err = g.Close()
	if err != nil {
		return nil, err
	}

	return cuis, nil
}

// ClassifyQueryTerms automatically classifies query terms based on the
// semantic type of the query.
func ClassifyQueryTerms(terms []string, mapping Mapping, semTypes SemTypeMapping) ([]string, []string, []string) {
	var (
		conditions []string
		treatments []string
		studyTypes []string
	)

	// Loop through all the terms to become query terms.
	for _, term := range terms {
		// All terms _should_ have been meta-mapped
		if p, ok := mapping[term]; ok {
			// The sem type of the term _should_ also be in this sem group.
			if s, ok := guru.MapSemType(p.Abbr); ok {
				group, ok := guru.MapSemGroup(s.TUI)
				if !ok {
					continue
				}

				sty := group.STY

				var category QueryCategory
				// Now, the STY might not be mapped in Bevan's mapping.
				if c, ok := semTypes[sty]; ok {
					category = categories[c]
				} else { // If it was not, we need to manually classify.
					switch group.Identifier {
					case "ACTI", "CHEM", "DEVI", "OBJC", "PROC":
						category = Treatment
					case "ANAT", "DISO", "GENE", "LIVB", "OCCU", "PHEN", "PHYS":
						category = Condition
					case "ORGA":
						category = StudyType
					case "CONC":
						category = None
					}
				}

				// Now that we have decided a category, we can continue
				// adding query terms to the appropriate category.
				switch category {
				case Treatment:
					treatments = append(treatments, term)
				case Condition:
					conditions = append(conditions, term)
				case StudyType:
					studyTypes = append(studyTypes, term)
				}
				// Clearly, if the category is None (default), then we
				// can just continue constructing the query without that term
				// as it is likely rubbish.
			}
		}
	}

	return conditions, treatments, studyTypes
}

type res struct {
	name   string
	relret []string
}

// FilterQueryTerms reduces further query terms by identifying the best combination
// of terms based on how many relevant documents they retrieve from the development set.
func FilterQueryTerms(conditions, treatments, studyTypes []string, field string, development trecresults.Qrels, e stats.EntrezStatisticsSource) ([]string, []string, []string, error) {
	eval.RelevanceGrade = 0
	terms := make([][]string, 3)
	terms[0] = conditions
	terms[1] = treatments
	terms[2] = studyTypes

	results := make([][]res, 3)

	fileCache := combinator.NewFileQueryCache("/Users/s4558151/filecache/")

	// Identify how many relevant documents from the development set each term in each category retrieves.
	for i := 0; i < len(terms); i++ {
		results[i] = make([]res, len(terms[i]))
		for j := 0; j < len(terms[i]); j++ {
			pq := pipeline.NewQuery("0", "0", cqr.NewKeyword(terms[i][j], field))
			tree, _, err := combinator.NewLogicalTree(pq, e, fileCache)
			if err != nil {
				return nil, nil, nil, err
			}
			var relret []string
			for _, result := range tree.Documents(fileCache).Results(pq, "0") {
				if _, ok := development[result.DocId]; ok {
					relret = append(relret, result.DocId)
				}
			}
			results[i][j] = res{
				name:   terms[i][j],
				relret: relret,
			}
		}
	}

	globalDocs := make(map[string]int)
	conditionsDocs := make(map[string]bool)
	treatmentsDocs := make(map[string]bool)
	studyTypesDocs := make(map[string]bool)

	// Identify which terms contribute to the overall recall.
	var idx int
	for i := 0; i < len(results); i++ {
		for j := 0; j < len(results[i]); j++ {
			for _, r := range results[i][j].relret {
				if _, ok := globalDocs[r]; !ok {
					globalDocs[r] = idx
					idx++
				}
				switch i {
				case 0:
					conditionsDocs[r] = true
				case 1:
					treatmentsDocs[r] = true
				case 2:
					studyTypesDocs[r] = true
				}
			}
		}
	}

	// Create three binary vectors indicating whether a relevant doc appears in a category.
	var (
		conditionsVec big.Int
		treatmentsVec big.Int
		studyTypesVec big.Int
	)
	for i := 0; i < len(results); i++ {
		for k, idx := range globalDocs {
			if _, ok := conditionsDocs[k]; ok {
				conditionsVec.SetBit(&conditionsVec, idx, 1)
			}
			if _, ok := treatmentsDocs[k]; ok {
				treatmentsVec.SetBit(&treatmentsVec, idx, 1)
			}
			if _, ok := studyTypesDocs[k]; ok {
				studyTypesVec.SetBit(&studyTypesVec, idx, 1)
			}
		}
	}

	// G represents the maximum coverage of all terms globally (i.e., when AND'ed together).
	G := maximumCoverage(conditionsVec, treatmentsVec, studyTypesVec)

	// Add terms to c, t, and s on the condition that the removal of the term affects G.
	var (
		c []string
		t []string
		s []string
	)
	for i := 0; i < len(results); i++ {
		// Sort the slice to look at high DF terms first.
		sort.Slice(results[i], func(j, k int) bool {
			return len(results[i][j].relret) < len(results[i][k].relret)
		})

		for j := 0; j < len(results[i]); j++ {
			term := results[i][j].name
			v := newVec(globalDocs, results[i], term)
			var v2 big.Int
			// If the term affects G, then we want to keep it!
			if G.Cmp(v2.And(G, v)) != 0 {
				switch i {
				case 0:
					c = append(c, term)
				case 1:
					t = append(t, term)
				case 2:
					s = append(s, term)
				}
			}
		}

		// When no terms affect the query at all, then all
		// terms must be added to the query.
		switch i {
		case 0:
			if len(c) == 0 {
				for j := 0; j < len(results[i]); j++ {
					c = append(c, results[i][j].name)
				}
			}
		case 1:
			if len(t) == 0 {
				for j := 0; j < len(results[i]); j++ {
					t = append(t, results[i][j].name)
				}
			}
		case 2:
			if len(s) == 0 {
				for j := 0; j < len(results[i]); j++ {
					s = append(s, results[i][j].name)
				}
			}
		}

	}

	// Transform the data back into something more appropriate.
	return c, t, s, nil
}
func newVec(m map[string]int, results []res, without string) *big.Int {
	var n big.Int

	seen := make(map[string]bool)
	for _, r := range results {
		if r.name != without {
			for _, doc := range r.relret {
				seen[doc] = true
			}
		}
	}

	for k, idx := range m {
		if _, ok := seen[k]; ok {
			n.SetBit(&n, idx, 1)
		}
	}

	return &n
}

func maximumCoverage(conditionsVec big.Int, treatmentsVec big.Int, studyTypesVec big.Int) *big.Int {
	var (
		v1 big.Int
		G  *big.Int
	)
	if conditionsVec.Int64() == 0 && treatmentsVec.Int64() == 0 && studyTypesVec.Int64() != 0 {
		G = &studyTypesVec
	} else if conditionsVec.Int64() == 0 && treatmentsVec.Int64() != 0 && studyTypesVec.Int64() == 0 {
		G = &treatmentsVec
	} else if conditionsVec.Int64() != 0 && treatmentsVec.Int64() == 0 && studyTypesVec.Int64() == 0 {
		G = &conditionsVec
	} else if conditionsVec.Int64() != 0 && treatmentsVec.Int64() != 0 && studyTypesVec.Int64() == 0 {
		G = v1.And(&conditionsVec, &treatmentsVec)
	} else if conditionsVec.Int64() != 0 && treatmentsVec.Int64() == 0 && studyTypesVec.Int64() != 0 {
		G = v1.And(&conditionsVec, &studyTypesVec)
	} else if conditionsVec.Int64() == 0 && treatmentsVec.Int64() != 0 && studyTypesVec.Int64() != 0 {
		G = v1.And(&treatmentsVec, &studyTypesVec)
	} else {
		v1.And(&conditionsVec, &treatmentsVec)
		G = v1.And(&v1, &studyTypesVec)
	}
	return G
}

func MakeKeywords(conditions, treatments, studyTypes []string) ([]cqr.Keyword, []cqr.Keyword, []cqr.Keyword) {
	terms := make([][]string, 3)
	terms[0] = conditions
	terms[1] = treatments
	terms[2] = studyTypes

	keywords := make([][]cqr.Keyword, 3)

	for i, t := range terms {
		keywords[i] = make([]cqr.Keyword, len(terms[i]))
		for j, term := range t {
			keywords[i][j] = cqr.NewKeyword(term, fields.TitleAbstract)
		}
	}
	return keywords[0], keywords[1], keywords[2]
}

func AddMeSHTerms(conditions, treatments, studyTypes []cqr.Keyword, dev []guru.MedlineDocument, e stats.EntrezStatisticsSource, topic string) ([]cqr.Keyword, []cqr.Keyword, []cqr.Keyword, error) {
	subheadingsFreq := make(map[string]int)
	for _, doc := range dev {
		for _, mh := range doc.MH {
			if strings.Contains(mh, "/") {
				mh = strings.Split(mh, "/")[0]
			}
			mh = strings.Replace(mh, "*", "", -1)
			fmt.Println(mh)
			subheadingsFreq[mh]++
		}
	}

	type pair struct {
		mh   string
		freq int
	}

	var subheadings []pair
	for k, v := range subheadingsFreq {
		subheadings = append(subheadings, pair{
			mh:   k,
			freq: v,
		})
	}

	var topSubheadings []string
	if len(subheadingsFreq) < 20 {
		for _, p := range subheadings {
			topSubheadings = append(topSubheadings, p.mh)
		}
	} else {
		sort.Slice(subheadings, func(i, j int) bool {
			return subheadings[i].freq > subheadings[j].freq
		})
		for i := 0; i < 20; i++ {
			topSubheadings = append(topSubheadings, subheadings[i].mh)
		}
	}

	fmt.Println("top subheadings:")
	fmt.Println(topSubheadings)

	f, err := os.OpenFile(path.Join("/Users/s4558151/go/src/github.com/hscells/groove/scripts/objective_qf/top_subheadings", topic), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}

	err = json.NewEncoder(f).Encode(topSubheadings)
	if err != nil {
		panic(err)
	}

	tree, err := meshexp.Default()
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		c []string
		t []string
		s []string
	)

	for _, mh := range topSubheadings {
		categorised := classifyMeSHCategory(mh, tree)
		for _, category := range categorised {
			switch category {
			case Condition:
				c = append(c, mh)
			case Treatment:
				t = append(t, mh)
			case StudyType:
				s = append(s, mh)
			}
		}
	}

	c, t, s, err = FilterQueryTerms(c, t, s, fields.MeshHeadings, makeQrels(dev), e)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, v := range c {
		conditions = append(conditions, cqr.NewKeyword(v, fields.MeshHeadings))
	}

	for _, v := range t {
		treatments = append(treatments, cqr.NewKeyword(v, fields.MeshHeadings))
	}

	for _, v := range s {
		studyTypes = append(studyTypes, cqr.NewKeyword(v, fields.MeshHeadings))
	}

	return conditions, treatments, studyTypes, nil
}

func classifyMeSHCategory(mh string, tree *meshexp.MeSHTree) []QueryCategory {
	references := tree.Reference(mh)
	fmt.Println(mh)
	seen := make(map[uint8]bool)
	var cats []QueryCategory
	for _, ref := range references {
		c := ref.TreeLocation[0][0]
		fmt.Println("->", string(c), ref.MedicalSubjectHeading)
		switch c {
		case 'A', 'B', 'C', 'F', 'G', 'H', 'M':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, Condition)
			}
		case 'D', 'E':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, Treatment)
			}
		case 'L', 'V', 'Z':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, StudyType)
			}
		case 'I', 'J', 'K', 'N':
			continue
		}
	}
	return cats
}

// ConstructQuery takes three slices and creates a query from them.
func ConstructQuery(conditions, treatments, studyTypes []cqr.Keyword) cqr.CommonQueryRepresentation {
	q := cqr.NewBooleanQuery(cqr.AND, nil)
	conditionsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	treatmentsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	studyTypesClause := cqr.NewBooleanQuery(cqr.OR, nil)

	// Add the conditions clause to the query.
	if len(conditions) > 0 {
		for _, t := range conditions {
			conditionsClause.Children = append(conditionsClause.Children, t)
		}
		q.Children = append(q.Children, conditionsClause.SetOption("category", Condition))
	}

	// Add the treatments clause to the query.
	if len(treatments) > 0 {
		for _, t := range treatments {
			treatmentsClause.Children = append(treatmentsClause.Children, t)
		}
		q.Children = append(q.Children, treatmentsClause.SetOption("category", Treatment))
	}

	// Add the treatments clause to the query.
	if len(studyTypes) > 0 {
		for _, t := range studyTypes {
			studyTypesClause.Children = append(studyTypesClause.Children, t)
		}
		q.Children = append(q.Children, studyTypesClause.SetOption("category", StudyType))
	}

	return q
}
