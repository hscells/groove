package formulation

import (
	"fmt"
	"github.com/bbalet/stopwords"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/hscells/guru"
	"github.com/xtgo/set"
	"gopkg.in/jdkato/prose.v2"
	"math"
	"sort"
	"strings"
)

type DecisionTreeFormulator struct {
	// Topic of the query.
	topic string
	// Terms identified as candidate query terms.
	N        [][]string          // Attributes.
	labels   map[string]bool     // map[pmid] -> YES?NO
	training map[string][]string // map[attribute] -> []pmid

	statistics stats.EntrezStatisticsSource
}

type invertedIndex map[string]map[string]float64

type leaf int

const (
	NA  leaf = 0
	YES      = 1
	NO       = -1
)

type tree struct {
	value leaf

	left      *tree
	candidate string
	right     *tree
}

//func insert(t *tree, candidate string, value bool)

func buildIndex(docs guru.MedlineDocuments, index invertedIndex) error {
	for _, doc := range docs {
		clean := stopwords.CleanString(fmt.Sprintf("%s. %s", doc.TI, doc.AB), "en", false)
		d, err := prose.NewDocument(clean)
		if err != nil {
			return err
		}
		for _, term := range d.Tokens() {
			if _, ok := index[term.Text]; !ok {
				index[term.Text] = make(map[string]float64)
			}
			index[term.Text][doc.PMID]++
		}
	}
	return nil
}

func (i invertedIndex) tf(term string, pmid string) float64 {
	return i[term][pmid]
}

func (i invertedIndex) df(term string, pmid string) float64 {
	return float64(len(i[term]))
}

func Entropy(positive, negative float64) float64 {
	if negative == 0 || positive == 0 {
		return 0
	}
	samples := positive + negative
	return -((positive / samples) * math.Log2(positive/samples)) - ((negative / samples) * math.Log2(negative/samples))
}

func InformationGain(attr string, training map[string][]string, labels map[string]bool) (float64, []string, []string) {
	var (
		numPos, numNeg                 float64
		lhsPos, lhsNeg, rhsPos, rhsNeg float64
		lhs, rhs                       []string
	)

	for _, label := range labels {
		if label {
			numPos++
		} else {
			numNeg++
		}
	}

	for pmidLabel, label := range labels {
		pmidFound := false
		for _, pmidAttr := range training[attr] {
			if pmidLabel == pmidAttr {
				pmidFound = true
				break
			}
		}
		if pmidFound {
			lhs = append(lhs, pmidLabel)
			if label {
				lhsPos++
			} else {
				lhsNeg++
			}
		} else {
			rhs = append(rhs, pmidLabel)
			if label {
				rhsPos++
			} else {
				rhsNeg++
			}
		}
	}

	if numPos == 0 || numNeg == 0 {
		return 0, lhs, rhs
	}

	entropy := Entropy(numPos, numNeg)

	var (
		a, b float64
	)
	a = ((lhsPos + lhsNeg) / (numPos + numNeg)) * Entropy(lhsPos, lhsNeg)
	b = ((rhsPos + rhsNeg) / (numPos + numNeg)) * Entropy(rhsPos, rhsNeg)

	//fmt.Println(attr, numPos, numNeg, entropy)
	//fmt.Println(attr, lhsPos, lhsNeg, (lhsPos+lhsNeg)/(numPos+numNeg), Entropy(lhsPos, lhsNeg))
	//fmt.Println(attr, rhsPos, rhsNeg, (rhsPos+rhsNeg)/(numPos+numNeg), Entropy(rhsPos, rhsNeg))
	//fmt.Println(attr, entropy, a, b)

	return entropy - (a + b), lhs, rhs
}

type _pmids []string

func (p _pmids) Len() int {
	return len(p)
}

func (p _pmids) Less(i, j int) bool {
	return p[i] < p[j]
}

func (p _pmids) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func ID3(training map[string][]string, labels map[string]bool, attrs []string) *tree {
	// Create a root node for the tree.
	node := &tree{}

	var (
		posLabels, negLabels int
	)
	for _, label := range labels {
		if label {
			posLabels++
		} else {
			negLabels++
		}
	}

	// If all examples are positive, Return the single-node tree Root, with label = +.
	if posLabels == len(labels) {
		node.value = YES
		return node
	}

	// If all examples are negative, Return the single-node tree Root, with label = -.
	if negLabels == len(labels) {
		node.value = NO
		return node
	}

	// If number of predicting attributes is empty, then Return the single node tree Root,
	// with label = most common value of the target attribute in the examples.
	if len(attrs) == 0 {
		if posLabels >= negLabels {
			node.value = YES
		} else {
			node.value = NO
		}
		return node
	}

	// bestAttr is the Attribute that best classifies examples.
	var (
		bestAttr string
		lhs, rhs []string
		attrIdx  int
	)
	gain := 0.0
	for i, attr := range attrs[1:] {
		g, l, r := InformationGain(attr, training, labels)
		fmt.Println(attr, g)
		if g > gain {
			bestAttr = attr
			gain = g
			lhs, rhs = l, r
			attrIdx = i
		}
	}

	if gain == 0 {
		node.value = NO
		return node
	}

	// Decision Tree attribute for Root = A.
	node.candidate = bestAttr

	// For each possible value, vi, of A,

	// If Examples(vi) is empty then below this new branch add a leaf node
	// with label = most common target value in the examples.
	if len(lhs) == 0 {
		if posLabels >= negLabels {
			node.left = &tree{
				value: YES,
			}
		} else {
			node.left = &tree{
				value: NO,
			}
		}
	} else { // Else below this new branch add the subtree ID3().
		t := make(map[string][]string)
		for attr, pmids := range training {
			p := append(_pmids(pmids), _pmids(lhs)...)
			s := set.Union(p, len(pmids))
			t[attr] = p[:s]
		}
		// Only retain labels for attribute.
		l := make(map[string]bool)
		for pmidLabel, label := range labels {
			pmidFound := false
			for _, pmidAttr := range lhs {
				if pmidLabel == pmidAttr {
					pmidFound = true
					break
				}
			}
			if !pmidFound {
				l[pmidLabel] = label
			}
		}
		a := make([]string, len(attrs)-1)
		copy(a[:attrIdx], attrs[:attrIdx])
		copy(a[attrIdx:], attrs[attrIdx+1:])
		node.left = ID3(t, l, a)
	}

	if len(rhs) == 0 {
		if posLabels >= negLabels {
			node.right = &tree{
				value: YES,
			}
		} else {
			node.right = &tree{
				value: NO,
			}
		}
	} else {
		t := make(map[string][]string)
		for attr, pmids := range training {
			p := append(_pmids(pmids), _pmids(rhs)...)
			s := set.Union(p, len(pmids))
			t[attr] = p[:s]
		}
		l := make(map[string]bool)
		for pmidLabel, label := range labels {
			pmidFound := false
			for _, pmidAttr := range rhs {
				if pmidLabel == pmidAttr {
					pmidFound = true
					break
				}
			}
			if !pmidFound {
				l[pmidLabel] = label
			}
		}
		a := make([]string, len(attrs)-1)
		copy(a[:attrIdx], attrs[:attrIdx])
		copy(a[attrIdx:], attrs[attrIdx+1:])
		node.right = ID3(t, l, a)
	}

	return node
}

func (t *tree) walk(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(".")
	}
	if t.value == NA {
		fmt.Printf("(%s)\n", t.candidate)
		t.left.walk(n + 1)
		t.right.walk(n + 1)
	} else {
		fmt.Printf("=> [%v]\n", t.value)
	}
}

func (dt DecisionTreeFormulator) Formulate() ([]cqr.CommonQueryRepresentation, []SupplementalData, error) {
	var (
		S []cqr.CommonQueryRepresentation
	)
	for _, can := range dt.N {
		t := ID3(dt.training, dt.labels, can)
		t.walk(0)
	}
	return S, nil, nil
}

func (dt DecisionTreeFormulator) Method() string {
	return "dt"
}

func (dt DecisionTreeFormulator) Topic() string {
	return dt.topic
}

func NewDecisionTreeFormulator(topic string, positive, negative guru.MedlineDocuments) (*DecisionTreeFormulator, error) {
	var (
		err error
		N   [][]string
	)

	docLens := make(map[string]float64)
	for _, doc := range positive {
		clean := stopwords.CleanString(fmt.Sprintf("%s. %s", doc.TI, doc.AB), "en", false)
		d, err := prose.NewDocument(clean)
		if err != nil {
			return nil, err
		}
		docLens[doc.PMID] = float64(len(d.Tokens()))
	}

	// Construct the inverted indexes.
	indexPositive := make(invertedIndex)
	indexNegative := make(invertedIndex)
	err = buildIndex(positive, indexPositive)
	if err != nil {
		return nil, err
	}
	err = buildIndex(negative, indexNegative)
	if err != nil {
		return nil, err
	}

	// Using a language model, construct the sets of attributes, N.
	lm := make(map[string]float64)
	for term, dv := range indexPositive {
		prelTf := 0.0
		prelDl := 0.0
		for pmid, tf := range dv {
			prelTf += tf
			prelDl += docLens[pmid]
		}
		lm[term] = prelTf / prelDl
	}

	type term struct {
		a string  // attribute.
		v float64 // value.
	}

	var terms []term

	for k, v := range lm {
		terms = append(terms, term{
			a: k,
			v: v,
		})
	}

	sort.Slice(terms, func(i, j int) bool {
		return terms[i].v > terms[j].v
	})

	attrs := make([]string, len(terms))
	for i, term := range terms {
		attrs[i] = term.a
	}

	for i := 5; i <= 100; i += 5 {
		if len(attrs) >= i {
			N = append(N, attrs[:i])
		}
	}

	labels := make(map[string]bool)       // map[pmid] -> YES?NO
	training := make(map[string][]string) // map[attribute] -> []pmid

	for _, attr := range attrs {
		for _, doc := range positive {
			clean := stopwords.CleanString(fmt.Sprintf("%s. %s", doc.TI, doc.AB), "en", false)
			if strings.Contains(clean, attr) {
				training[attr] = append(training[attr], doc.PMID)
			}
		}
		for _, doc := range negative {
			clean := stopwords.CleanString(fmt.Sprintf("%s. %s", doc.TI, doc.AB), "en", false)
			if strings.Contains(clean, attr) {
				training[attr] = append(training[attr], doc.PMID)
			}
		}
	}

	for _, doc := range positive {
		labels[doc.PMID] = true
	}
	for _, doc := range negative {
		labels[doc.PMID] = false
	}

	return &DecisionTreeFormulator{
		training: training,
		labels:   labels,
		N:        N,
		topic:    topic,
	}, nil
}
