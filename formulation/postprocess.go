package formulation

import (
	"fmt"
	rake "github.com/afjoseph/RAKE.Go"
	"github.com/hscells/cqr"
	"github.com/hscells/cui2vec"
	"github.com/hscells/groove/analysis"
	"github.com/hscells/guru"
	"github.com/hscells/metawrap"
	"github.com/hscells/transmute/fields"
	"strings"
)

// PostProcess applies any post-formatting to a query.
type PostProcess func(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error)

func sumVecs(v1, v2 []float64) []float64 {
	if len(v1) != len(v2) {
		panic("slice lengths are not the same")
	}
	v := make([]float64, len(v1))
	for i := range v1 {
		v[i] = v1[i] + v2[i]
	}
	return v
}

func avgVecs(vs ...[]float64) []float64 {
	if len(vs) < 2 {
		return nil
	}
	v := vs[0]
	for i := 1; i < len(vs); i++ {
		v = sumVecs(v, vs[i])
	}

	N := float64(len(vs))
	for i := range v {
		v[i] = v[i] / N
	}

	return v
}

func RelevanceFeedback(query cqr.CommonQueryRepresentation, docs guru.MedlineDocuments, mm metawrap.HTTPClient) (cqr.CommonQueryRepresentation, error) {

	// Open a connection to vector client.
	client, err := cui2vec.NewVecClient("localhost:8003")
	if err != nil {
		return nil, err
	}

	// Function for embedding a clause by averaging child vectors.
	var embed func(q cqr.CommonQueryRepresentation) []float64
	embed = func(q cqr.CommonQueryRepresentation) []float64 {
		switch x := q.(type) {
		case cqr.Keyword:
			if x.GetOption("entity") == nil {
				return []float64{}
			}
			v, _ := client.Vec(x.GetOption("entity").(string))
			return v
		case cqr.BooleanQuery:
			var vecs [][]float64
			for _, child := range x.Children {
				v := embed(child)
				if len(v) == 0 {
					continue
				}
				vecs = append(vecs, v)
			}
			return avgVecs(vecs...)
		}
		return nil
	}

	// The root node of the query.
	bq := query.(cqr.BooleanQuery)

	// Contains an embedding for each clause.
	clauseVecs := make([][]float64, len(bq.Children))

	// Create the embeddings for each child.
	for i, child := range bq.Children {
		clauseVecs[i] = embed(child)
	}

	keywords := make(map[string]struct{})
	for _, doc := range docs {
		list := rake.RunRake(fmt.Sprintf("%s %s", doc.TI, doc.AB))
		for _, pair := range list {
			if pair.Value > 1 {
				keywords[pair.Key] = struct{}{}
			}
		}
	}

	// Loop over the extracted keywords.
	for keyword := range keywords {

		// Obtain CUIs for a keyword.
		concepts, err := mm.Candidates(keyword)
		if err != nil {
			return nil, err
		}

		// For each of the extracted CUIs.
		for _, concept := range concepts {

			// Obtain an embedding for the CUI.
			embedding, _ := client.Vec(concept.CandidateCUI)

			// Find the clause to add the CUI to using the most similar clause.
			var highestSim float64
			var clause int
			for i := range clauseVecs {
				sim, err := cui2vec.Cosine(clauseVecs[i], embedding)
				if err != nil {
					return nil, err
				}
				if sim > highestSim {
					highestSim = sim
					clause = i
				}
			}

			// Add the CUI into the Boolean query depending on the most similar clause.
			child := bq.Children[clause].(cqr.BooleanQuery)
			kw := cqr.NewKeyword(keyword, fields.TitleAbstract)
			kw.SetOption(Entity, concept.CandidateCUI)
			child.Children = append(child.Children, kw)
			bq.Children[clause] = child
		}
	}

	return bq, nil
}

// Stem uses already stemmed terms from the original query to
// replace terms from the query that requires post-processing.
func Stem(original cqr.CommonQueryRepresentation) PostProcess {
	return func(query cqr.CommonQueryRepresentation) (cqr.CommonQueryRepresentation, error) {
		stemDict := make(map[string]bool)
		for _, kw := range analysis.QueryKeywords(original) {
			if v, ok := kw.Options["truncated"]; ok {
				if v.(bool) == true {
					stemDict[kw.QueryString] = true
				}
			}
		}
		return stemQuery(query, stemDict, make(map[string]bool)), nil
	}
}

func stemQuery(query cqr.CommonQueryRepresentation, d map[string]bool, seen map[string]bool) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		for k := range d {
			if strings.Contains(strings.ToLower(q.QueryString), strings.Replace(strings.ToLower(k), "*", "", -1)) {
				q.QueryString = k
				if _, ok := seen[k]; !ok {
					q.SetOption("truncated", true)
					seen[k] = true
					return q
				} else {
					return nil
				}
			}
		}
		return q
	case cqr.BooleanQuery:
		var c []cqr.CommonQueryRepresentation
		for _, child := range q.Children {
			s := stemQuery(child, d, seen)
			if s != nil {
				c = append(c, s)
			}
		}
		q.Children = c
		return q
	default:
		return q
	}
}

var (
	/*
		#1 randomized controlled trial [pt]
		#2 controlled clinical trial [pt]
		#3 randomized [tiab]
		#4 placebo [tiab]
		#5 drug therapy [sh]
		#6 randomly [tiab]
		#7 trial [tiab]
		#8 groups [tiab]
		#9 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7 OR #8
		#10 animals [mh] NOT humans [mh]
		#11 #9 NOT #10
	*/
	SensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("drug therapy", fields.FloatingMeshHeadings),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.TitleAbstract),
			cqr.NewKeyword("groups", fields.TitleAbstract),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})

	/*
		#1 randomized controlled trial [pt]
		#2 controlled clinical trial [pt]
		#3 randomized [tiab]
		#4 placebo [tiab]
		#5 clinical trials as topic [mesh: noexp]
		#6 randomly [tiab]
		#7 trial [ti]
		#8 #1 OR #2 OR #3 OR #4 OR #5 OR #6 OR #7
		#9 animals [mh] NOT humans [mh]
		#10 #8 NOT #9
	*/
	PrecisionSensitivityFilter = cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
		cqr.NewBooleanQuery(cqr.OR, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("randomized controlled trial", fields.PublicationType),
			cqr.NewKeyword("controlled clinical trial", fields.PublicationType),
			cqr.NewKeyword("randomized", fields.TitleAbstract),
			cqr.NewKeyword("placebo", fields.TitleAbstract),
			cqr.NewKeyword("clinical trials as topic", fields.MeshHeadings).SetOption(cqr.ExplodedString, false),
			cqr.NewKeyword("randomly", fields.TitleAbstract),
			cqr.NewKeyword("trial", fields.Title),
		}),
		cqr.NewBooleanQuery(cqr.NOT, []cqr.CommonQueryRepresentation{
			cqr.NewKeyword("animals", fields.MeshHeadings),
			cqr.NewKeyword("humans", fields.MeshHeadings),
		}),
	})
)
