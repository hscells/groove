package main

import (
	"encoding/csv"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/ghost"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/meshexp"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"io"
	"math/big"
	"math/rand"
	"os"
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

	// Construct the 'test' set.

	//test, err := FetchDocuments([]int{15361993, 19187517, 10715695, 17175704, 11989528, 16483425, 18038150, 12689544, 18160449, 10561760, 18320124, 11773110, 15591666, 16711337, 11003728, 16931409, 15155979, 19635896, 9612775, 14513934, 18240951, 9861398, 18256418, 18297164, 17006296, 9986839, 11693975, 18165483, 12516921, 11952945, 11425156, 10948029, 17360871, 14605156, 10232784, 19399156, 12409392, 16530797, 18711803, 10884866, 12461593, 12596367, 17962153, 10900922, 10928351, 15821800, 12567635, 16553923, 18983278, 19019233, 19399160, 11398664, 16117960, 18981501, 15552593, 9431947, 15728862, 10929047, 11230422, 14708867, 19686072, 16943212, 8940977, 19416497, 18498626, 12791849, 17244413, 11398670, 11262857, 9988333, 9582585, 12387772, 12712759, 19271402, 19478249, 19413207, 18524335, 19682968, 12587607, 18441999, 16603648, 12572958, 18620560, 10339800, 19222821, 12219143, 19362256, 7856824, 10444320, 8944258, 16259289, 16492359, 17034070, 10511977, 2446789, 11251904, 10489737, 19519915, 17135432, 15490787, 9754317, 11388505, 19996417, 15655012, 10696408, 10696409, 12139227, 14516298, 11205592, 17543127, 10699018, 9509174, 14740873, 16282293, 19019399, 14552743, 16192446, 15638301, 11728042, 18383801, 18959777, 19173772, 14655635, 10696410, 18166801, 18793410, 18719728, 19735557, 12454171, 17713633, 16216106, 9227798, 12792694, 11411942, 9017909, 17300631, 12061980, 19836683, 16813240, 17919672, 12078230, 11532200, 10492788, 9769889, 18589463, 17148078, 12950138, 11487366, 11225143, 8980591, 16027061, 18202438, 15767635, 10203469, 12214515, 19482001, 18513771, 11904103, 9231194, 11358000, 8266395, 16124417, 16124416, 8653815, 18298604, 17362594, 17486264, 10072132, 10072131, 16505556, 18951743, 9279979, 19270300, 9764322, 18680960, 11380433, 9444000, 12436172, 18163135, 9929032, 15461206, 12206045, 10444878, 15066340, 7570824, 9886191, 17506881, 18626455, 10463010, 12454146, 11700187, 14510166, 15916076, 12971557, 11500771, 15549388, 9509627, 8846490, 15452770, 15904775, 9797836, 9656392, 15365017, 10492756, 10746389, 16859025, 19930609, 11427610, 11755428, 12932092, 10583325, 11781267, 12174773, 12174772, 10983554, 19527797, 18598344, 16862709, 15007639, 11582872, 17172395, 9125830, 10405377, 8665389, 17556616, 18053347, 14766829, 10999091, 11123823, 9439488, 18031779, 15000726, 17401497, 15255320, 9692145, 11191436, 15217738, 19407111, 12662417, 10546844, 9373632, 9373633, 9373631, 18976561, 12632146, 11388507, 15228812, 10862317, 9236821, 9002372, 16014858, 14996360, 19272157, 9063360, 10734522, 15272742, 18541764, 10497842, 8758063, 11716121, 11716120, 7906328, 12812358, 11293505, 12164291, 16548497, 17259188, 12518844, 10748901, 10748900, 12224568, 1406728}, e)
	//if err != nil {
	//	panic(err)
	//}
	test, err := FetchDocuments([]int{16757701, 21973143, 22684726, 14675319, 12948281, 16156945, 12771924, 22233689, 15352050, 24685404, 11102899, 21051986, 21053103, 15721416, 21903190, 21865084, 10507785, 20007673, 11284951, 21075054, 14667741, 23803065, 15847874, 20884314, 26019212, 20950847, 20719907, 21684207, 19141778, 21685039, 15798842, 20683409, 12365959, 23443346, 11146077, 25793281, 22851828, 19127262, 11008913, 10565688, 10833984, 14504189, 10951346, 20949059, 23621259, 21334200, 21502436, 21479179, 14560561, 20686384, 17051008, 11531293, 16136031, 18843032, 23334437, 20039323, 21060889, 22251922, 12676841, 25169534, 15818610, 11606114, 23136614, 23052314, 15270934, 20941740, 22562132, 21297040, 10645859, 7791438, 19585574, 18805733, 12509400, 23724527, 23075359, 21630255, 20354519, 20197765, 21525231}, e)
	if err != nil {
		panic(err)
	}

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
	fmt.Println(cut)

	// Rank the cut terms in dev by DF.
	terms := RankTerms(cut)

	// Get the population (background) collection.
	population, err := GetPopulationSet(e)
	if err != nil {
		panic(err)
	}

	// Perform 'term frequency analysis' on the population.
	popDF, err := TermFrequencyAnalysis(population)
	if err != nil {
		panic(err)
	}

	// Identify dev terms which appear in <= 2% of the DF of the population set.
	queryTerms := CutDevelopmentTermsWithPopulation(terms, popDF)
	fmt.Println(queryTerms)

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
	fmt.Println(conditions, treatments, studyTypes)

	// And then filter the query terms.
	conditions, treatments, studyTypes, err = FilterQueryTerms(conditions, treatments, studyTypes, makeQrels(dev), e)
	if err != nil {
		panic(err)
	}
	fmt.Println(conditions, treatments, studyTypes)

	// Create keywords for the proceeding query.
	conditionsKeywords, treatmentsKeywords, studyTypesKeywords := MakeKeywords(conditions, treatments, studyTypes)

	// Create the query from the three categories.
	query := ConstructQuery(conditionsKeywords, treatmentsKeywords, studyTypesKeywords)
	fmt.Println(transmute.CompileCqr2PubMed(query))

	// Execute the query and find the effectiveness.
	filecache := combinator.NewFileQueryCache("/Users/s4558151/filecache")
	pq := pipeline.NewQuery("0", "0", query)
	tree, _, err := combinator.NewLogicalTree(pq, e, filecache)
	if err != nil {
		panic(err)
	}
	results := tree.Documents(filecache).Results(pq, "0")
	eval.RelevanceGrade = 0

	valEval := eval.Evaluate([]eval.Evaluator{eval.NumRel, eval.NumRet, eval.NumRelRet, eval.RecallEvaluator}, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": makeQrels(val)}}, "0")
	unseenEval := eval.Evaluate([]eval.Evaluator{eval.NumRel, eval.NumRet, eval.NumRelRet, eval.RecallEvaluator, eval.PrecisionEvaluator, eval.F1Measure, eval.F05Measure, eval.F3Measure}, &results, trecresults.QrelsFile{Qrels: map[string]trecresults.Qrels{"0": makeQrels(unseen)}}, "0")

	fmt.Println(valEval)
	fmt.Println(unseenEval)
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
func RankTerms(t Terms) []string {
	type pair struct {
		k string
		v int
	}

	var pairs []pair
	for k, v := range t {
		pairs = append(pairs, pair{k: k, v: v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].v > pairs[j].v
	})

	terms := make([]string, len(pairs))
	for i, v := range pairs {
		terms[i] = v.k
	}

	return terms
}

// GetPopulationSet retrieves a set of publications to form a population set.
func GetPopulationSet(e stats.EntrezStatisticsSource) ([]guru.MedlineDocument, error) {
	e.Limit = 10000
	pmids, err := e.Search(`("0000"[Date - Publication] : "3000"[Date - Publication])`, e.SearchSize(10000), )
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
func CutDevelopmentTermsWithPopulation(dev []string, population Terms) []string {
	n := float64(len(population)) * 0.02 // Term must be <= 2% of population DF.
	var t []string
	for _, term := range dev {
		if df, ok := population[term]; ok {
			if float64(df) <= n {
				t = append(t, term)
			}
		}
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
func FilterQueryTerms(conditions, treatments, studyTypes []string, development trecresults.Qrels, e stats.EntrezStatisticsSource) ([]string, []string, []string, error) {
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
			pq := pipeline.NewQuery("0", "0", cqr.NewKeyword(terms[i][j], fields.TitleAbstract))
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
			fmt.Println(results[i][j].name)
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
			fmt.Println(term, G.Cmp(v2.And(G, v)))
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
	terms[0] = treatments
	terms[0] = studyTypes

	keywords := make([][]cqr.Keyword, 3)

	for i, t := range terms {
		keywords[i] = make([]cqr.Keyword, len(terms[i]))
		for j, term := range t {
			keywords[i][j] = cqr.NewKeyword(term, fields.TitleAbstract)
		}
	}
	return keywords[0], keywords[1], keywords[2]
}

func AddMeSHTerms(conditions, treatments, studyTypes []cqr.Keyword, dev []guru.MedlineDocument) ([]cqr.Keyword, []cqr.Keyword, []cqr.Keyword, error) {
	subheadingsFreq := make(map[string]int)
	for _, doc := range dev {
		for _, mh := range doc.MH {
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
			return subheadings[i].freq < subheadings[j].freq
		})
		for i := 0; i < 20; i++ {
			topSubheadings = append(topSubheadings, subheadings[i].mh)
		}
	}

	tree, err := meshexp.Default()
	if err != nil {
		return nil, nil, nil, err
	}

	for _, mh := range topSubheadings {
		term := mh
		for {
			p := tree.Parents(term)
			if len(p) == 0 {
				break
			}
		}
	}

}

// ConstructQuery takes three slices and creates a query from them.
func ConstructQuery(conditions, treatments, studyTypes []cqr.Keyword) cqr.CommonQueryRepresentation {
	query := cqr.NewBooleanQuery(cqr.AND, nil)
	conditionsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	treatmentsClause := cqr.NewBooleanQuery(cqr.OR, nil)
	studyTypesClause := cqr.NewBooleanQuery(cqr.OR, nil)

	// Add the conditions clause to the query.
	if len(conditions) > 0 {
		for _, t := range conditions {
			conditionsClause.Children = append(conditionsClause.Children, t)
		}
		query.Children = append(query.Children, conditionsClause.SetOption("category", Condition))
	}

	// Add the treatments clause to the query.
	if len(treatments) > 0 {
		for _, t := range treatments {
			treatmentsClause.Children = append(treatmentsClause.Children, t)
		}
		query.Children = append(query.Children, treatmentsClause.SetOption("category", Treatment))
	}

	// Add the treatments clause to the query.
	if len(studyTypes) > 0 {
		for _, t := range studyTypes {
			studyTypesClause.Children = append(studyTypesClause.Children, t)
		}
		query.Children = append(query.Children, studyTypesClause.SetOption("category", StudyType))
	}

	return query
}
