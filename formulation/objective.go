package formulation

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/ghost"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/preprocess"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/meshexp"
	"github.com/hscells/metawrap"
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

// Splitter splits a test set into development, validation, and unseen.
type Splitter interface {
	Split(docs []guru.MedlineDocument) ([]guru.MedlineDocument, []guru.MedlineDocument, []guru.MedlineDocument)
}

// TermAnalyser records term/phrase statistics about a set of documents.
type TermAnalyser func(docs []guru.MedlineDocument) (TermStatistics, error)

type BackgroundCollection interface {
	Statistic(term string) (float64, error)
	Size() (float64, error)
}

// -----------------------------------------------------------

type TermStatistics map[string]float64

type PopulationSet TermStatistics

func (p PopulationSet) Statistic(term string) (float64, error) {
	return p[term], nil
}

func (p PopulationSet) Size() (float64, error) {
	return float64(len(p)), nil
}

type PubMedSet struct {
	e stats.EntrezStatisticsSource
}

func (p PubMedSet) Statistic(term string) (float64, error) {
doDf:
	df, err := p.e.DocumentFrequency(term, fields.TitleAbstract)
	if err != nil {
		goto doDf
	}
	return df, nil
}

func (p PubMedSet) Size() (float64, error) {
	return p.e.CollectionSize()
}

func NewPubMedSet(e stats.EntrezStatisticsSource) PubMedSet {
	return PubMedSet{
		e: e,
	}
}

type mappingPair struct {
	CUI  string
	Abbr string
}

type mapping map[string]mappingPair

// SemTypeMapping is a mapping of STY->Classification.
type semTypeMapping map[string]string

type queryCategory int

type evaluation struct {
	Development map[string]float64
	Validation  map[string]float64
	Unseen      map[string]float64
}

const (
	none queryCategory = iota
	condition
	treatment
	studyType
)

var (
	categories = map[string]queryCategory{
		"Test":      treatment,
		"treatment": treatment,
		"Diagnosis": condition,
	}
)

// GetPopulationSet retrieves a set of publications to form a population set.
func GetPopulationSet(e stats.EntrezStatisticsSource, analyser TermAnalyser) (BackgroundCollection, error) {
	e.Limit = 10000
	pmids, err := e.Search(`("0000"[Date - Publication] : "2018"[Date - Publication])`, e.SearchSize(10000), )
	if err != nil {
		return nil, err
	}
	e.Limit = 0
	fmt.Println("fetching docs")

	// Perform 'term frequency analysis' on the population.
	docs, err := fetchDocuments(pmids, e)
	if err != nil {
		return nil, err
	}

	pop, err := analyser(docs)
	if err != nil {
		return nil, err
	}

	return PopulationSet(pop), nil
}

// FetchDocuments retrieves the references to studies of target PMID(s).
func fetchDocuments(refs []int, e stats.EntrezStatisticsSource) ([]guru.MedlineDocument, error) {
	// Open the document store.
	d, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	g, err := ghost.Open(path.Join(d, "/ghost/groove/objective/docs"), ghost.NewGobSchema(guru.MedlineDocument{}))
	if err != nil {
		panic(err)
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
				fetching = append(fetching, ref)
			} else {
				references = append(references, d)
			}
		} else {
			fetching = append(fetching, ref)
		}
	}

	fmt.Println("found", len(references), "fetching", len(fetching))

	if len(fetching) > 0 {
		// Retrieve the documents of the references.
		docs, err = e.Fetch(fetching)
		if err != nil {
			panic(err)
		}

		// Put the docs in the store.
		for _, d := range docs {
			err := g.Put(d.PMID, d)
			if err != nil {
				return nil, err
			}
			//fmt.Println("put", d.PMID)
		}
	}
	// Close the document store.
	err = g.Close()
	if err != nil {
		return nil, err
	}

	return append(references, docs...), nil
}

type RandomSplitter int64

// splitTest creates three slices of documents: 2:4 development, 1:4 validation, 1:4 unseen.
func (r RandomSplitter) Split(docs []guru.MedlineDocument) ([]guru.MedlineDocument, []guru.MedlineDocument, []guru.MedlineDocument) {
	rand.Seed(int64(r))
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

// TermFrequencyAnalyser computes the document frequency for the input documents.
func TermFrequencyAnalyser(docs []guru.MedlineDocument) (TermStatistics, error) {
	t := make(TermStatistics)
	for _, doc := range docs {
		seen := make(map[string]bool)
		o, err := tokenise(strings.ToLower(fmt.Sprintf("%s. %s", doc.TI, doc.AB)))
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

func loadSemTypesMapping(filename string) (semTypeMapping, error) {
	m := make(semTypeMapping)

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

// cutDevelopmentTerms takes TermStatistics from the development set where the DF of the term is >= 20%.
func cutDevelopmentTerms(t TermStatistics, dev []guru.MedlineDocument, cut float64) TermStatistics {
	n := float64(len(dev)) * cut // term must be <= 20% of development DF.
	terms := make(TermStatistics)

	for k, v := range t {
		if float64(v) >= n {
			terms[k] = v
		}
	}

	return terms
}

// cutDevelopmentTermsWithPopulation takes TermStatistics where the DF in the population collection is <= 2%.
func cutDevelopmentTermsWithPopulation(dev []string, population BackgroundCollection, topic, folder string, cut float64) []string {
	size, err := population.Size()
	if err != nil {
		panic(err)
	}
	n := size * cut // Term must be <= 2% of population DF.
	var t []string

	type pair struct {
		K string
		V float64
		P float64
	}
	terms := make(TermStatistics)

	for _, term := range dev {
		fmt.Println(" -", term)
		if df, err := population.Statistic(term); err == nil {
			if float64(df) <= n {
				terms[term] = df
				t = append(t, term)
			}
		} else {
			panic(err)
		}
	}

	var pairs []pair
	for k, v := range terms {
		pairs = append(pairs, pair{K: k, V: v, P: float64(v) / size})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].V > pairs[j].V
	})

	f, err := os.OpenFile(path.Join("./document_frequency", folder, topic+".population.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		panic(err)
	}

	err = json.NewEncoder(f).Encode(pairs)
	if err != nil {
		panic(err)
	}

	return t
}

// metaMapTerms maps TermStatistics to CUIs.
func metaMapTerms(terms []string, client metawrap.HTTPClient) (mapping, error) {
	// Open the document store.
	d, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	g, err := ghost.Open(path.Join(d, "/ghost/groove/objective/metamap"), ghost.NewGobSchema(mappingPair{}))
	if err != nil {
		panic(err)
	}
	cuis := make(mapping)
	for _, term := range terms {
		if !g.Contains(term) {
			candidates, err := client.Candidates(term)
			if err != nil {
				return nil, err
			}
			found := false
			for _, candidate := range candidates {
				if strings.ToLower(candidate.CandidateMatched) == term {
					p := mappingPair{
						CUI:  candidate.CandidateCUI,
						Abbr: candidate.SemTypes[0],
					}
					cuis[term] = p
					err := g.Put(term, p)
					if err != nil {
						panic(err)
					}
					found = true
					fmt.Println("put term", term)
					break
				}
			}

			if !found {
				err := g.Put(term, mappingPair{})
				if err != nil {
					panic(err)
				}
			}
		} else {
			var p mappingPair
			err := g.Get(term, &p)
			if err != nil {
				//panic(err)
				candidates, err := client.Candidates(term)
				if err != nil {
					return nil, err
				}
				//found := false
				for _, candidate := range candidates {
					if strings.ToLower(candidate.CandidateMatched) == term {
						p := mappingPair{
							CUI:  candidate.CandidateCUI,
							Abbr: candidate.SemTypes[0],
						}
						cuis[term] = p
						err := g.Put(term, p)
						if err != nil {
							panic(err)
						}
						//found = true
						fmt.Println("put term", term)
						break
					}
				}
			}
			cuis[term] = p
			fmt.Println("get term", term)
		}
	}

	err = g.Close()
	if err != nil {
		return nil, err
	}

	return cuis, nil
}

// classifyQueryTerms automatically classifies query TermStatistics based on the
// semantic type of the query.
func classifyQueryTerms(terms []string, mapping mapping, semTypes semTypeMapping) ([]string, []string, []string) {
	var (
		conditions []string
		treatments []string
		studyTypes []string
	)

	// Loop through all the TermStatistics to become query TermStatistics.
	for _, term := range terms {
		// All TermStatistics _should_ have been meta-mapped
		if p, ok := mapping[term]; ok {
			// The sem type of the term _should_ also be in this sem group.
			if s, ok := guru.MapSemType(p.Abbr); ok {
				group, ok := guru.MapSemGroup(s.TUI)
				if !ok {
					continue
				}

				sty := group.STY

				var category queryCategory
				// Now, the STY might not be mapped in Bevan's mapping.
				if c, ok := semTypes[sty]; ok {
					category = categories[c]
				} else { // If it was not, we need to manually classify.
					switch group.Identifier {
					case "ACTI", "CHEM", "DEVI", "OBJC", "PROC":
						category = treatment
					case "ANAT", "DISO", "GENE", "LIVB", "OCCU", "PHEN", "PHYS":
						category = condition
					case "ORGA", "GEOG":
						category = studyType
					case "CONC":
						category = none
					}
				}

				// Now that we have decided a category, we can continue
				// adding query TermStatistics to the appropriate category.
				switch category {
				case treatment:
					treatments = append(treatments, term)
				case condition:
					conditions = append(conditions, term)
				case studyType:
					studyTypes = append(studyTypes, term)
				}
				// Clearly, if the category is none (default), then we
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

var rejectMeSH = []string{"humans", "animals", "aged", "adult", "male", "female", "adolescent", "child", "child, preschool", "middle aged", "Young Adult", "Infant", "time factors",
	"off", "positive", "negative", "suspicious", "true", "false"}

// FilterQueryTerms reduces further query TermStatistics by identifying the best combination
// of TermStatistics based on how many relevant documents they retrieve from the development set.
func FilterQueryTerms(conditions, treatments, studyTypes []string, field string, development trecresults.Qrels, e stats.EntrezStatisticsSource) ([]string, []string, []string, error) {
	eval.RelevanceGrade = 0
	terms := make([][]string, 3)
	terms[0] = conditions
	terms[1] = treatments
	terms[2] = studyTypes

	results := make([][]res, 3)

	// Open the document store.
	d, err := os.UserCacheDir()
	if err != nil {
		return nil, nil, nil, err
	}
	fileCache := combinator.NewFileQueryCache(path.Join(d, "filecache"))

	// Identify how many relevant documents from the development set each term in each category retrieves.
	for i := 0; i < len(terms); i++ {
		//results[i] = []res
		for j := 0; j < len(terms[i]); j++ {
			skip := false
			for _, v := range rejectMeSH {
				if strings.ToLower(terms[i][j]) == strings.ToLower(v) {
					skip = true
				}
			}
			if skip {
				continue
			}
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
			results[i] = append(results[i], res{
				name:   terms[i][j],
				relret: relret,
			})
		}
	}

	globalDocs := make(map[string]int)
	conditionsDocs := make(map[string]bool)
	treatmentsDocs := make(map[string]bool)
	studyTypesDocs := make(map[string]bool)

	// Identify which TermStatistics contribute to the overall recall.
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

	// G represents the maximum coverage of all TermStatistics globally (i.e., when AND'ed together).
	G := maximumCoverage(conditionsVec, treatmentsVec, studyTypesVec)

	// Add TermStatistics to c, t, and s on the condition that the removal of the term affects G.
	var (
		c []string
		t []string
		s []string
	)
	for i := 0; i < len(results); i++ {
		// Sort the slice to look at high DF TermStatistics first.
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

		// When no TermStatistics affect the query at all, then all
		// TermStatistics must be added to the query.
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

func makeKeywords(conditions, treatments, studyTypes []string) ([]cqr.Keyword, []cqr.Keyword, []cqr.Keyword) {
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

// MakeQrels creates a set of relevance assessments from some medline documents.
func MakeQrels(docs []guru.MedlineDocument, topic string) trecresults.Qrels {
	qrels := make(trecresults.Qrels)
	for _, doc := range docs {
		qrels[doc.PMID] = &trecresults.Qrel{
			Topic:     topic,
			Iteration: "0",
			DocId:     doc.PMID,
			Score:     1,
		}
	}
	return qrels
}

func addMeSHTerms(conditions, treatments, studyTypes []cqr.Keyword, dev []guru.MedlineDocument, e stats.EntrezStatisticsSource, topic string, k int, folder string) ([]cqr.Keyword, []cqr.Keyword, []cqr.Keyword, error) {
	subheadingsFreq := make(map[string]int)
	for _, doc := range dev {
		for _, mh := range doc.MH {
			for _, reject := range rejectMeSH {
				if strings.ToLower(reject) == strings.ToLower(mh) {
					goto skip
				}
			}
			if strings.Contains(mh, "/") {
				mh = strings.Split(mh, "/")[0]
			}
			mh = strings.Replace(mh, "*", "", -1)
			fmt.Println(mh)
			subheadingsFreq[mh]++
		skip:
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
	if len(subheadingsFreq) < k {
		for _, p := range subheadings {
			topSubheadings = append(topSubheadings, p.mh)
		}
	} else {
		sort.Slice(subheadings, func(i, j int) bool {
			return subheadings[i].freq > subheadings[j].freq
		})
		for i := 0; i < k; i++ {
			topSubheadings = append(topSubheadings, subheadings[i].mh)
		}
	}

	fmt.Println("top subheadings:")
	for i, sh := range topSubheadings {
		fmt.Println(i, sh)
	}

	f, err := os.OpenFile(path.Join("./top_subheadings", folder, topic), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
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
			case condition:
				c = append(c, mh)
			case treatment:
				t = append(t, mh)
			case studyType:
				s = append(s, mh)
			}
		}
	}

	c, t, s, err = FilterQueryTerms(c, t, s, fields.MeshHeadings, MakeQrels(dev, topic), e)
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

func classifyMeSHCategory(mh string, tree *meshexp.MeSHTree) []queryCategory {
	references := tree.Reference(mh)
	fmt.Println(mh)
	seen := make(map[uint8]bool)
	var cats []queryCategory
	for _, ref := range references {
		c := ref.TreeLocation[0][0]
		fmt.Println("->", string(c), ref.MedicalSubjectHeading)
		switch c {
		case 'A', 'B', 'C', 'F', 'G', 'H', 'M':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, condition)
			}
		case 'D', 'E':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, treatment)
			}
		case 'L', 'V', 'Z':
			if _, ok := seen[c]; !ok {
				seen[c] = true
				cats = append(cats, studyType)
			}
		case 'I', 'J', 'K', 'N':
			continue
		}
	}
	return cats
}

// ConstructQuery takes three slices and creates a query from them.
func constructQuery(conditions, treatments, studyTypes []cqr.Keyword) cqr.CommonQueryRepresentation {
	q := cqr.NewBooleanQuery(cqr.AND, nil)
	conditionsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	treatmentsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	studyTypesClause := cqr.NewBooleanQuery(cqr.OR, nil)

	// Add the conditions clause to the query.
	if len(conditions) > 0 {
		for _, t := range conditions {
			conditionsClause.Children = append(conditionsClause.Children, t)
		}
		q.Children = append(q.Children, conditionsClause.SetOption("category", condition))
	}

	// Add the treatments clause to the query.
	if len(treatments) > 0 {
		for _, t := range treatments {
			treatmentsClause.Children = append(treatmentsClause.Children, t)
		}
		q.Children = append(q.Children, treatmentsClause.SetOption("category", treatment))
	}

	// Add the treatments clause to the query.
	if len(studyTypes) > 0 {
		for _, t := range studyTypes {
			studyTypesClause.Children = append(studyTypesClause.Children, t)
		}
		q.Children = append(q.Children, studyTypesClause.SetOption("category", studyType))
	}

	return q
}

// evaluate computes evaluation measures for each of the dev, val, and unseen sets.
func evaluate(query cqr.CommonQueryRepresentation, e stats.EntrezStatisticsSource, dev, val, unseen []guru.MedlineDocument, topic string) (evaluation, error) {
	// Execute the query and find the effectiveness.
	//d, err := os.UserCacheDir()
	//if err != nil {
	//	return evaluation{}, err
	//}
	//filecache := combinator.NewFileQueryCache(path.Join(d, "filecache"))
	//pq := pipeline.NewQuery("0", "0", query)
	//tree, _, err := combinator.NewLogicalTree(pq, e, filecache)
	//if err != nil {
	//	panic(err)
	//}
	//results := tree.Documents(filecache).Results(pq, "0")
	results, err := e.Execute(pipeline.NewQuery("", "", query), e.SearchOptions())
	if err != nil {
		return evaluation{}, err
	}
	eval.RelevanceGrade = 0
	ev := []eval.Evaluator{eval.NumRel, eval.NumRet, eval.NumRelRet, eval.Recall, eval.Precision, eval.F1Measure, eval.F05Measure, eval.F3Measure, eval.NNR}
	devEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": MakeQrels(dev, topic)}}, "0")
	valEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": MakeQrels(val, topic)}}, "0")
	unseenEval := eval.Evaluate(ev, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": MakeQrels(unseen, topic)}}, "0")
	return evaluation{
		Development: devEval,
		Validation:  valEval,
		Unseen:      unseenEval,
	}, nil
}

// RankTerms ranks term statistics.
func rankTerms(t TermStatistics, dev []guru.MedlineDocument, topic, folder string) []string {
	type pair struct {
		K string
		V float64
		P float64
	}

	var pairs []pair
	for k, v := range t {
		pairs = append(pairs, pair{K: k, V: v, P: float64(v) / float64(len(dev))})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].V > pairs[j].V
	})

	f, err := os.OpenFile(path.Join("./statistics", folder, topic+".development.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
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

// derive actually performs the objective derivation for the objective method.
func (o ObjectiveFormulator) derive(devDF TermStatistics, dev, val []guru.MedlineDocument, population BackgroundCollection, m eval.Evaluator) (cqr.CommonQueryRepresentation, cqr.CommonQueryRepresentation, error) {
	var (
		bestEval float64
		bestD    float64
		bestP    float64
		bestM    int

		bestConditions []cqr.Keyword
		bestTreatments []cqr.Keyword
		bestStudyTypes []cqr.Keyword

		bestQ         cqr.CommonQueryRepresentation
		bestQWithMesh cqr.CommonQueryRepresentation
	)

	// Load the sem type mapping.
	semTypes, err := loadSemTypesMapping(o.SemTypes)
	if err != nil {
		return nil, nil, err
	}

	fmt.Printf("grid search over %v, %v\n", o.DevK, o.PopK)

	// Grid search over dev and pop values for the best query on validation.
	for _, d := range o.DevK {
		for _, p := range o.PopK {
			fmt.Println(d, p)

			fmt.Println("cutting terms")
			// Take terms from dev.
			cut := cutDevelopmentTerms(devDF, dev, d)

			fmt.Println("ranking terms")
			// Rank the cut terms in dev.
			terms := rankTerms(cut, dev, o.query.Topic, o.Folder)

			fmt.Println("cutting dev terms with population")
			// Identify dev TermStatistics which appear in <= 2% of the DF of the population set.
			queryTerms := cutDevelopmentTermsWithPopulation(terms, population, o.query.Topic, o.Folder, p)

			fmt.Println("mapping terms")
			// Map sem types in TermStatistics.
			mapping, err := metaMapTerms(queryTerms, metawrap.HTTPClient{URL: o.MetaMapURL})
			if err != nil {
				return nil, nil, err
			}

			fmt.Println("classifying terms")
			// Classify query TermStatistics.
			conditions, treatments, studyTypes := classifyQueryTerms(queryTerms, mapping, semTypes)

			fmt.Println("creating keywords")
			// Create keywords for the proceeding query.
			conditionsKeywords, treatmentsKeywords, studyTypesKeywords := makeKeywords(conditions, treatments, studyTypes)

			fmt.Println("filtering keywords")
			// And then filter the query TermStatistics.
			conditions, treatments, studyTypes, err = FilterQueryTerms(conditions, treatments, studyTypes, fields.TitleAbstract, MakeQrels(dev, o.Topic()), o.s)
			if err != nil {
				return nil, nil, err
			}

			fmt.Println("constructing final query")
			// Create the query from the three categories.
			q := constructQuery(conditionsKeywords, treatmentsKeywords, studyTypesKeywords)
			fmt.Println(q)
			q = preprocess.DateRestrictions(o.Pubdates)(q, o.query.Topic)()
			fmt.Println(q)

			fmt.Println("evaluating final query")
			ev, err := evaluate(q, o.s, dev, val, nil, o.Topic())
			if err != nil {
				return nil, nil, err
			}

			if ev.Validation[m.Name()] > bestEval {
				bestEval = ev.Validation[m.Name()]
				bestQ = q
				bestConditions = conditionsKeywords
				bestTreatments = treatmentsKeywords
				bestStudyTypes = studyTypesKeywords
				bestD = d
				bestP = p
			}
		}
	}
	fmt.Println("completed grid search")

	// Grid search parameters of k for the number of mesh keywords to add to a query.
	bestEval = 0.0
	for _, k := range o.MeSHK {
		fmt.Println(k)
		conditionsKeywordsWithMeSH, treatmentsKeywordsWithMeSH, studyTypesKeywordsWithMeSH, err := addMeSHTerms(bestConditions, bestTreatments, bestStudyTypes, dev, o.s, o.query.Topic, k, o.Folder)
		if err != nil {
			return nil, nil, err
		}
		qWithMeSH := constructQuery(conditionsKeywordsWithMeSH, treatmentsKeywordsWithMeSH, studyTypesKeywordsWithMeSH)
		qWithMeSH = preprocess.DateRestrictions(o.Pubdates)(qWithMeSH, o.query.Topic)()

		ev, err := evaluate(qWithMeSH, o.s, dev, val, nil, o.Topic())
		if err != nil {
			return nil, nil, err
		}

		if ev.Validation[m.Name()] > bestEval {
			bestEval = ev.Validation[m.Name()]
			bestQWithMesh = qWithMeSH
			bestM = k
		}
	}

	v := make(map[string]interface{})
	v["d"] = bestD
	v["p"] = bestP
	v["m"] = bestM
	b, err := json.Marshal(v)
	if err != nil {
		return nil, nil, err
	}

	err = ioutil.WriteFile(path.Join("./objective_qf/params", o.Folder, o.query.Topic+"params.json"), b, 0664)
	if err != nil {
		return nil, nil, err
	}

	return bestQ, bestQWithMesh, nil
}
