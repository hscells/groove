package rank_test

import (
	"fmt"
	"github.com/hscells/groove/combinator"
	"github.com/hscells/groove/eval"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/rank"
	"github.com/hscells/groove/stats"
	"github.com/hscells/merging"
	"github.com/hscells/transmute"
	"github.com/hscells/trecresults"
	"os"
	"path"
	"testing"
)

func TestBoolCOMB(t *testing.T) {
	e, err := stats.NewEntrezStatisticsSource(
		stats.EntrezAPIKey("eb8d49885d85feea8f7188e9c60d29e3d308"),
		stats.EntrezEmail("h.scells@uq.edu.au"),
		stats.EntrezTool("groove"),
		stats.EntrezOptions(stats.SearchOptions{Size: 100000}))
	if err != nil {
		t.Fatal(err)
	}

	//strQuery := `(((cervix OR cervical OR cervico*) AND (cancer OR carcinoma OR neoplas* OR dysplas* OR squamous OR CIN[tw] OR CINII*[tw] OR CIN2*[tw] OR CINIII*[tw] OR CIN3[tw] OR SIL[tw] OR HSIL[tw] OR H-SIL[tw] OR LSIL[tw] OR L-SIL OR ASCUS[tw] OR “ASC R”[tw] OR “ASC US”[tw] OR “ASC H”[tw])) AND (HPV OR “human papillomavirus” OR papillomaviridae OR papillomavirus infections[MeSH Terms] OR “hybrid capture” OR HC2 OR “HC 2” OR HCII OR “HC II” OR DNA OR viral OR virolog*) AND (triage OR management OR followup OR “follow up”))`
	//strQuery := `((Ultrasonography [mh] OR ultrasound [tw] OR ultrasonograph* [tw] OR sonograp*[tw] OR us [sh]) OR (Magnetic Resonance Imaging [mh] OR MR imag*[tw] OR magnetic resonance imag* [tw] OR MRI [tw])) AND (Rotator Cuff [mh] OR rotator cuff* [tw] OR musculotendinous cuff* [tw] OR subscapularis [tw] OR supraspinatus [tw] OR infraspinatus OR teres minor [tw]) AND (Rupture [mh:noexp] OR tear* [tw] OR torn [tw] OR thickness [tw] OR lesion* [tw] OR ruptur* [tw] OR injur* [tw])`
	//strQuery := `(test[tiab] OR assay[tiab] OR antigen[tiab] OR Ag[tiab] OR lateral flow assay*[tiab] OR urine antigen[tiab] OR point of care[tiab]) AND (LAM[tiab] OR "lipoarabinomannan"[Supplementary Concept] OR lipoarabinomannan[tiab]) AND (Tuberculosis[Mesh] OR Mycobacterium tuberculosis[Mesh] OR tuberculosis[tiab] Or TB[tiab]) AND 1940/01/01:2015/02/28[Date - Publication]`
	strQuery := `(1992/01:2012/08[Publication Date] AND ((brain dea*[All Fields] OR brain stem dea*[All Fields] OR coma depasse[All Fields] OR irreversible coma[All Fields]) AND (comput aided tomograph angio*[All Fields] OR CT[All Fields] OR CTA[All Fields] OR CTCA[All Fields] OR comput tomograph*[All Fields] OR comput aided tomograph*[All Fields] OR comput tomograph angio*[All Fields])))`
	q, err := transmute.CompilePubmed2Cqr(strQuery)
	if err != nil {
		t.Fatal(err)
	}

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	cacher := combinator.NewFileQueryCache(path.Join(cacheDir, "groove", "file_cache"))

	pq := pipeline.NewQuery("CD009694", "CD009694", q)
	r, err := rank.BoolCOMB(pq, cacher, &rank.BM25Scorer{K1: 2, B: 0.75}, merging.CombMNZ{Normaliser: merging.MinMaxNorm}, e)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile("/Users/s4558151/Repositories/tar/2018-TAR/Task2/qrel_abs_combined", os.O_RDONLY, 0664)
	if err != nil {
		t.Fatal(err)
	}
	qrels, err := trecresults.QrelsFromReader(f)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("begin evaluation")
	eval.RelevanceGrade = 0
	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.AP, eval.NumRel, eval.NDCG{K: 10}, eval.NDCG{K: 100}, eval.NDCG{}, eval.Precision, eval.PrecisionAtK{K: 10}, eval.PrecisionAtK{K: 100}, eval.RecallAtK{K: 10}, eval.RecallAtK{K: 100}, eval.Recall}, &r, qrels, pq.Topic))
	if len(r) > 5 {
		for _, res := range r[:5] {
			fmt.Println(res)
		}
	}

	g, err := os.OpenFile("fusion.run", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range r {
		_, err := g.WriteString(fmt.Sprintf("%s\n", result.String()))
		if err != nil {
			t.Fatal(err)
		}
	}

	//r1, err := e.Execute(pq, e.SearchOptions())
	//if err != nil {
	//	t.Fatal(err)
	//}

	runner := rank.NewRunner(cacheDir, []string{strQuery}, []string{"ti", "ab", "mh"}, e, &rank.BM25Scorer{K1: 2, B: 0.75})
	sd, err := runner.Run()
	if err != nil {
		t.Fatal(err)
	}

	r1 := make(trecresults.ResultList, len(sd[0].Docs))
	for i, doc := range sd[0].Docs {
		r1[i] = &trecresults.Result{
			Topic:     pq.Topic,
			Iteration: "0",
			DocId:     doc.PMID,
			Rank:      int64(doc.Rank),
			Score:     doc.Score,
		}
	}

	fmt.Println(eval.Evaluate([]eval.Evaluator{eval.NumRel, eval.NDCG{K: 10}, eval.NDCG{K: 100}, eval.NDCG{}, eval.Precision, eval.PrecisionAtK{K: 10}, eval.PrecisionAtK{K: 100}, eval.RecallAtK{K: 10}, eval.RecallAtK{K: 100}, eval.Recall}, &r1, qrels, pq.Topic))
	if len(r1) > 5 {
		for _, res := range r1[:5] {
			fmt.Println(res)
		}
	}
}
