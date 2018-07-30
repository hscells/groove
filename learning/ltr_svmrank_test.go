package learning_test

import (
	"bytes"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/learning"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/trecresults"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"testing"
)

func TestLTR(t *testing.T) {
	cqrPipeline := pipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})

	rawQuery := `1. objective.tw.
2. assess.tw.
3. accuracy.tw.
4. galactomannan.tw.
5. invasive.tw.
6. aspergillosis.tw.
7. immunocompromised.tw.
8. objectives.tw.
9. aimed.tw.
10. sources.tw.
11. heterogeneity.tw.
12. subgroups.tw.
13. interpretations.tw.
14. criteria.tw.
15. 1 or 2 or 3 or 4 or 5 or 6 or 7 or 8 or 9 or 10 or 11 or 12 or 13 or 14
16. assessed.tw.
17. accuracy.tw.
18. galactomannan.tw.
19. sandwich.tw.
20. elisa.tw.
21. either.tw.
22. retrospective.tw.
23. collection.tw.
24. alone.tw.
25. 16 or 17 or 18 or 19 or 20 or 21 or 22 or 23 or 24
26. include.tw.
27. neutropenia.tw.
28. whose.tw.
29. neutrophils.tw.
30. functionally.tw.
31. included.tw.
32. haematological.tw.
33. malignancies.tw.
34. receiving.tw.
35. haematopoietic.tw.
36. transplants.tw.
37. chemotherapeutics.tw.
38. immunosuppressive.tw.
39. organ.tw.
40. transplant.tw.
41. recipients.tw.
42. prolonged.tw.
43. condition.tw.
44. compromising.tw.
45. granulomatous.tw.
46. cgd.tw.
47. inherited.tw.
48. abnormality.tw.
49. 26 or 27 or 28 or 29 or 30 or 31 or 32 or 33 or 34 or 35 or 36 or 37 or 38 or 39 or 40 or 41 or 42 or 43 or 44 or 45 or 46 or 47 or 48
50. commercially.tw.
51. galactomannan.tw.
52. sandwich.tw.
53. elisa.tw.
54. included.tw.
55. concerning.tw.
56. excluded.tw.
57. addressing.tw.
58. bal.tw.
59. fluids.tw.
60. csf.tw.
61. peritoneal.tw.
62. evaluating.tw.
63. 50 or 51 or 52 or 53 or 54 or 55 or 56 or 57 or 58 or 59 or 60 or 61 or 62
64. condition.tw.
65. invasive.tw.
66. aspergillosis.tw.
67. called.tw.
68. 64 or 65 or 66 or 67
69. standards.tw.
70. define.tw.
71. condition.tw.
72. autopsy.tw.
73. criteria.tw.
74. de.tw.
75. pauw.tw.
76. demonstration.tw.
77. hyphal.tw.
78. invasion.tw.
79. biopsies.tw.
80. aspergillus.tw.
81. gold.tw.
82. specimens.tw.
83. histopathological.tw.
84. rarely.tw.
85. therefore.tw.
86. decided.tw.
87. take.tw.
88. divide.tw.
89. categories.tw.
90. proven.tw.
91. invasive.tw.
92. aspergillosis.tw.
93. probably.tw.
94. possibly.tw.
95. see.tw.
96. table.tw.
97. division.tw.
98. microbiological.tw.
99. shown.tw.
100. match.tw.
101. especially.tw.
102. true.tw.
103. investigating.tw.
104. example.tw.
105. recommended.tw.
106. probable.tw.
107. exclusion.tw.
108. regarded.tw.
109. atypical.tw.
110. likely.tw.
111. accuracy.tw.
112. excluded.tw.
113. explicitly.tw.
114. excluding.tw.
115. clear.tw.
116. 69 or 70 or 71 or 72 or 73 or 74 or 75 or 76 or 77 or 78 or 79 or 80 or 81 or 82 or 83 or 84 or 85 or 86 or 87 or 88 or 89 or 90 or 91 or 92 or 93 or 94 or 95 or 96 or 97 or 98 or 99 or 100 or 101 or 102 or 103 or 104 or 105 or 106 or 107 or 108 or 109 or 110 or 111 or 112 or 113 or 114 or 115
117. 15 and 25 and 49 and 63 and 68 and 116`

	topic := "CD007394"

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadFile("/Users/harryscells/Papers/sysrev_queries/clef2018_tar/qrel")
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchHosts("http://sef-is-017660:8200/"),
		stats.ElasticsearchIndex("med_stem_sim2"),
		stats.ElasticsearchDocumentType("doc"),
		stats.ElasticsearchAnalysedField("stemmed"),
		//stats.ElasticsearchField("_all"),
		stats.ElasticsearchScroll(true),
		stats.ElasticsearchSearchOptions(stats.SearchOptions{Size: 10000, RunName: "test"}))

	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}
	gq := groove.NewPipelineQuery("test", topic, repr.(cqr.CommonQueryRepresentation))

	// Cache for the statistics of the query performance predictors.
	statisticsCache := diskv.New(diskv.Options{
		BasePath:     "../statistics_cache",
		Transform:    combinator.BlockTransform(8),
		CacheSizeMax: 4096 * 1024,
		Compression:  diskv.NewGzipCompression(),
	})

	ltr := learning.NewLTRQueryCandidateSelector("/Users/harryscells/Papers/sysrev_queries/clef2018_tar/clef2018.model")
	qc := learning.NewQueryChain(ltr, ss, analysis.NewDiskMeasurementExecutor(statisticsCache), learning.NewAdjacencyReplacementTransformer(), learning.NewAdjacencyRangeTransformer(), learning.NewMeSHExplosionTransformer(), learning.NewFieldRestrictionsTransformer(), learning.NewLogicalOperatorTransformer())
	tq, err := qc.Execute(gq)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("executing queries")

	results1, err := ss.ExecuteFast(gq, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	d1 := make(combinator.Documents, len(results1))
	for i, r := range results1 {
		d1[i] = combinator.Document(r)
	}

	fmt.Println(tq.PipelineQuery)
	results2, err := ss.ExecuteFast(tq.PipelineQuery, ss.SearchOptions())
	if err != nil {
		t.Fatal(err)
	}
	d2 := make(combinator.Documents, len(results2))
	for i, r := range results2 {
		d2[i] = combinator.Document(r)
	}

	r1 := d1.Results(gq, gq.Name)
	r2 := d2.Results(gq, gq.Name)

	fmt.Println(len(r1), len(r2))

	fmt.Println(repr.(cqr.CommonQueryRepresentation))
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &r1, qrels, topic))
	fmt.Println(tq.PipelineQuery.Query)
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.RecallEvaluator, eval.PrecisionEvaluator, eval.NumRet, eval.NumRel, eval.NumRelRet}, &r2, qrels, topic))

	fmt.Println("chain: ")
	for _, q := range tq.QueryChain {
		fmt.Println(q)
	}
	fmt.Println(tq.PipelineQuery.Query)
}
