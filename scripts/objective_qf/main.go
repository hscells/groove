package main

import (
	"encoding/csv"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/ghost"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/hscells/metawrap"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Terms map[string]int

type Mapping map[string]struct {
	CUI     string
	SemType string
}

type SemTypeMapping map[string]string

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
	test, err := FetchDocuments([]int{15361993, 19187517, 10715695, 17175704, 11989528, 16483425, 18038150, 12689544, 18160449, 10561760, 18320124, 11773110, 15591666, 16711337, 11003728, 16931409, 15155979, 19635896, 9612775, 14513934, 18240951, 9861398, 18256418, 18297164, 17006296, 9986839, 11693975, 18165483, 12516921, 11952945, 11425156, 10948029, 17360871, 14605156, 10232784, 19399156, 12409392, 16530797, 18711803, 10884866, 12461593, 12596367, 17962153, 10900922, 10928351, 15821800, 12567635, 16553923, 18983278, 19019233, 19399160, 11398664, 16117960, 18981501, 15552593, 9431947, 15728862, 10929047, 11230422, 14708867, 19686072, 16943212, 8940977, 19416497, 18498626, 12791849, 17244413, 11398670, 11262857, 9988333, 9582585, 12387772, 12712759, 19271402, 19478249, 19413207, 18524335, 19682968, 12587607, 18441999, 16603648, 12572958, 18620560, 10339800, 19222821, 12219143, 19362256, 7856824, 10444320, 8944258, 16259289, 16492359, 17034070, 10511977, 2446789, 11251904, 10489737, 19519915, 17135432, 15490787, 9754317, 11388505, 19996417, 15655012, 10696408, 10696409, 12139227, 14516298, 11205592, 17543127, 10699018, 9509174, 14740873, 16282293, 19019399, 14552743, 16192446, 15638301, 11728042, 18383801, 18959777, 19173772, 14655635, 10696410, 18166801, 18793410, 18719728, 19735557, 12454171, 17713633, 16216106, 9227798, 12792694, 11411942, 9017909, 17300631, 12061980, 19836683, 16813240, 17919672, 12078230, 11532200, 10492788, 9769889, 18589463, 17148078, 12950138, 11487366, 11225143, 8980591, 16027061, 18202438, 15767635, 10203469, 12214515, 19482001, 18513771, 11904103, 9231194, 11358000, 8266395, 16124417, 16124416, 8653815, 18298604, 17362594, 17486264, 10072132, 10072131, 16505556, 18951743, 9279979, 19270300, 9764322, 18680960, 11380433, 9444000, 12436172, 18163135, 9929032, 15461206, 12206045, 10444878, 15066340, 7570824, 9886191, 17506881, 18626455, 10463010, 12454146, 11700187, 14510166, 15916076, 12971557, 11500771, 15549388, 9509627, 8846490, 15452770, 15904775, 9797836, 9656392, 15365017, 10492756, 10746389, 16859025, 19930609, 11427610, 11755428, 12932092, 10583325, 11781267, 12174773, 12174772, 10983554, 19527797, 18598344, 16862709, 15007639, 11582872, 17172395, 9125830, 10405377, 8665389, 17556616, 18053347, 14766829, 10999091, 11123823, 9439488, 18031779, 15000726, 17401497, 15255320, 9692145, 11191436, 15217738, 19407111, 12662417, 10546844, 9373632, 9373633, 9373631, 18976561, 12632146, 11388507, 15228812, 10862317, 9236821, 9002372, 16014858, 14996360, 19272157, 9063360, 10734522, 15272742, 18541764, 10497842, 8758063, 11716121, 11716120, 7906328, 12812358, 11293505, 12164291, 16548497, 17259188, 12518844, 10748901, 10748900, 12224568, 1406728}, e)
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

	// Map the terms to CUIs.
	mapping, err := MetaMapTerms(queryTerms, metawrap.HTTPClient{URL: "http://ielab-metamap.uqcloud.net"})
	if err != nil {
		panic(err)
	}

	// Load the sem type mapping.
	semTypes, err := loadSemTypesMapping("/Users/s4558151/Papers/sysrev_queries/amia2019_objective/experiments/cui_semantic_types.txt")
	if err != nil {
		panic(err)
	}

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
	cuis := make(Mapping)
	for _, term := range terms {
		candidates, err := client.Candidates(term)
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			if candidate.CandidateMatched == term {
				cuis[term] = struct {
					CUI     string
					SemType string
				}{CUI: candidate.CandidateCUI, SemType: candidate.SemTypes[0]}
				break
			}
		}
	}

	return cuis, nil
}

func CreateTextQuery(mapping Mapping, semTypes SemTypeMapping) cqr.CommonQueryRepresentation {
	return nil
}
