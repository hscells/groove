package seed_test

import (
	"github.com/hscells/groove/learning/seed"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute"
	"testing"
)

func TestConstruction(t *testing.T) {

	ss, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("22a11de46af145ce59bb288e0ede66721f09"),
		stats.EntrezEmail("harryscells@gmail.com"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	c := seed.NewQuickUMLSProtocolConstructor(`
To evaluate the diagnostic accuracy of physical tests, applied singly or in combination, for shoulder impingements (subacromial or internal) or local lesions of bursa, rotator cuff or labrum that may accompany impingement, in people whose symptoms and/or history suggest any of these disorders.
We also examined the physical tests according to whether they were intended to:
identify impingement in general (or differentiate it from other causes of shoulder pain, e.g. 'frozen shoulder')
subcategorise impingement as subacromial outlet impingement (impingement under the acromion process) or internal impingement (impingement within the shoulder joint)
diagnose lesions of bursa, tendon or glenoid labrum that may be associated with impingement
form part of a diagnostic package or process and, if so, according to the stages at which they may apply.
Investigation of sources of heterogeneity
We planned to investigate the following potential sources of heterogeneity.
Study population: older general population; young athletic population; other well defined groups e.g. wheelchair users or swimmers (see the Differences between protocol and review)
Stage of clinical care: primary (generally in the community setting), secondary (referral following preliminary screening) or tertiary (referral to a specialist centre)
Study design: cross sectional (or cohort) versus case-control; retrospective versus prospective design
Type of reference test. This will vary according to the target condition and setting, but generally surgery versus non-invasive imaging will be considered (seeTable 3)
Aspects of study conduct, specifically: blinding and reporting of uninterpretable or intermediate results.`,
		`Studies had to include patients with neutropenia or patients whose neutrophils are functionally compromised. We included studies with the following patient groups:
patients with haematological malignancies, receiving haematopoietic stem cell transplants, chemotherapeutics or immunosuppressive drugs;
solid organ transplant recipients and other patients who are receiving immunosuppressive drugs for a prolonged time;
patients with cancer who are receiving chemotherapeutics;
patients with a medical condition compromising the immune system, such as HIV/AIDS and chronic granulomatous disease (CGD, an inherited abnormality of the neutrophils).`,
		`Physical tests used singly or in combination to identify shoulder impingement, such as the painful arc test (Cyriax 1982); to classify shoulder impingements, e.g. Neer’s test (Neer 1977; Neer 1983), the modified relocation test (Hamner 2000), the internal rotation resistance strength test (Zaslav 2001); or to diagnose localised conditions that may accompany impingement, e.g. Yergason’s test (Yergason 1931), the lift off test (Gerber 1991a; Gerber 1996; Hertel 1996a), the crank test (Liu 1996b), the active compression test (O'Brien 1998a) and the biceps load II test (Kim 2001) (see Table 1).
Ideally, articles for inclusion should have described a physical test, or reference a source that did so, in sufficient detail to enable its replication, and clearly indicate what constituted a positive index test result. Those that did not were included only if they provided sufficient information to be of clinical value. Studies reporting the collective diagnostic accuracy of a series of tests were considered, providing each component, and its manner of inclusion, were adequately described. Generic terms such as 'physical examination', as used to denote an unspecified combination of physical tests, led to exclusion unless further details were obtained from authors.`,
		`Subacromial or internal impingement of the shoulder and the localised conditions that may accompany these classifications, namely bursitis, rotator cuff tears, glenoid labrum tears, and inflammation or rupture of the biceps tendon.
Instability may underlie impingement, but tests of instability were only included if they were intended to demonstrate associated impingement pain, as in the modified relocation test (Hamner 2000), as opposed to instability per se. Similarly, tests for ACJ disorders were only included if, like the active compression test (O'Brien 1998a), they had a component intended to reproduce impingement pain.`,
		"http://43.240.96.223:5000", 0.1, ss)
	queries, _ := c.Construct()
	t.Log(len(queries))
	for _, q := range queries {
		t.Log(q.GetOption(seed.ProtocolOption))
		t.Log(transmute.CompileCqr2PubMed(q))
	}

	//f, err := os.OpenFile("/Users/harryscells/Repositories/cui2vec/testdata/cui2vec_precomputed.bin", os.O_RDONLY, os.ModePerm)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//m, err := cui2vec.LoadCUIMapping("/Users/harryscells/Repositories/cui2vec/cuis.csv")
	//if err != nil {
	//	t.Fatal(err)
	//}
	//p, err := cui2vec.NewPrecomputedEmbeddings(f)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//v, err := learning.Variations(learning.NewCandidateQuery(queries[len(queries)-1], "1", nil),
	//	ss,
	//	analysis.NewDiskMeasurementExecutor(diskv.New(diskv.Options{
	//		BasePath:     "statistics_cache",
	//		Transform:    combinator.BlockTransform(16),
	//		CacheSizeMax: 4096 * 1024,
	//		Compression:  diskv.NewGzipCompression(),
	//	})),
	//	[]analysis.Measurement{
	//		analysis.BooleanNonAtomicClauses,
	//		analysis.BooleanAndCount,
	//		analysis.BooleanOrCount,
	//		analysis.BooleanNotCount,
	//		analysis.BooleanFields,
	//		analysis.BooleanFieldsTitle,
	//		analysis.BooleanFieldsAbstract,
	//		analysis.BooleanFieldsMeSH,
	//		analysis.BooleanFieldsOther,
	//		analysis.TermCount,
	//		analysis.BooleanKeywords,
	//		analysis.MeshKeywordCount,
	//		analysis.MeshExplodedCount,
	//		analysis.MeshAvgDepth,
	//		analysis.MeshMaxDepth,
	//		preqpp.RetrievalSize,
	//	},
	//	learning.NewLogicalOperatorTransformer(),
	//	learning.NewFieldRestrictionsTransformer(),
	//	learning.NewMeSHExplosionTransformer(),
	//	learning.NewMeshParentTransformer(),
	//	learning.NewClauseRemovalTransformer(),
	//	learning.NewFieldRestrictionsTransformer(),
	//	learning.Newcui2vecExpansionTransformer(p, m),
	//)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//for _, q := range v {
	//	t.Log(q.Query.GetOption(seed.ProtocolOption))
	//	t.Log(transmute.CompileCqr2Medline(q.Query))
	//}
	//t.Log(len(v))
}
