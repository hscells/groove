// Package combinator contains methods for performing logical operations on queries.
package combinator

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/fields"
	"github.com/hscells/trecresults"
	"github.com/pkg/errors"
	"github.com/xtgo/set"
	"hash/crc64"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// OrOperator combines documents using `OR`.
	OrOperator = orOperator{}
	// AndOperator combines documents using `AND`.
	AndOperator = andOperator{}
	// NotOperator combines documents using `NOT`.
	NotOperator = notOperator{}

	mu sync.Mutex
)

// Operator can combine different nodes of a tree together.
type Operator interface {
	Combine(clauses []LogicalTreeNode, cache QueryCacher) Documents
	String() string
}

// LogicalTree can compute the number of documents retrieved for atomic components.
type LogicalTree struct {
	Root  LogicalTreeNode
	cache QueryCacher
}

// LogicalTreeNode is a node in a logical tree.
type LogicalTreeNode interface {
	Query() cqr.CommonQueryRepresentation
	Documents(cache QueryCacher) Documents
	String() string
}

// Clause is the most basic component of a logical tree.
type Clause struct {
	Hash  uint64
	Query cqr.CommonQueryRepresentation
}

// Combinator is an operator in a query.
type Combinator struct {
	Operator
	Clause
	Clauses []LogicalTreeNode
	N       float64
	R       float64
}

// Atom is the smallest possible component of a query.
type Atom struct {
	Clause
	N float64
	R float64
}

// AdjAtom is a special type of atom for adjacent queries.
type AdjAtom struct {
	Clause
}

// Document is a document that has been retrieved.
type Document uint32

// Documents are a group of retrieved documents.
type Documents []Document

func (d Documents) Len() int {
	return len(d)
}

func (d Documents) Less(i, j int) bool {
	return d[i] < d[j]
}

func (d Documents) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// andOperator is the intersection of documents.
type andOperator struct {
}

// orOperator is the union of documents.
type orOperator struct {
}

// notOperator is the relative compliment of documents.
type notOperator struct {
}

// Results converts the documents from the resulting logical operator tree into eval-compatible trec results.
func (d Documents) Results(query pipeline.Query, run string) trecresults.ResultList {
	r := make(trecresults.ResultList, len(d))
	for i, doc := range d {
		r[i] = &trecresults.Result{
			Topic:     query.Topic,
			Iteration: "Q0",
			DocId:     strconv.Itoa(int(doc)),
			Rank:      int64(i),
			Score:     0,
			RunName:   run,
		}
	}
	return r
}

// Set creates a map from a slice of documents.
func (d Documents) Set() map[Document]struct{} {
	m := make(map[Document]struct{}, len(d))
	for _, doc := range d {
		m[doc] = struct{}{}
	}
	return m
}

func (andOperator) Combine(nodes []LogicalTreeNode, cache QueryCacher) Documents {
	if len(nodes) == 0 {
		return Documents{}
	}
	if len(nodes) == 1 {
		return nodes[0].Documents(cache)
	}

	var wg sync.WaitGroup
	docIDs := make([]Documents, len(nodes))
	for i, node := range nodes {
		wg.Add(1)
		go func(n LogicalTreeNode, j int) {
			defer wg.Done()
			docIDs[j] = n.Documents(cache)
			if !sort.IsSorted(docIDs[j]) {
				sort.Sort(docIDs[j])
			}
		}(node, i)
	}
	wg.Wait()

	// initial set of nodes
	docs := docIDs[0]
	for i := 1; i < len(docIDs); i++ {
		pivot := len(docs)

		docs = append(docs /* next */, docIDs[i]...)

		size := set.Inter(docs, pivot)
		docs = docs[:size]
	}
	return docs
}

func (andOperator) String() string {
	return "and"
}

func (orOperator) Combine(nodes []LogicalTreeNode, cache QueryCacher) Documents {
	if len(nodes) == 0 {
		return Documents{}
	}
	if len(nodes) == 1 {
		if nodes[0] == nil {
			return Documents{}
		}
		return nodes[0].Documents(cache)
	}

	totalDocs := 0
	var docIds []Documents
	var wg sync.WaitGroup
	var mu2 sync.Mutex
	for i, node := range nodes {
		wg.Add(1)
		go func(n LogicalTreeNode, j int) {
			defer wg.Done()
			d := n.Documents(cache)
			if len(d) > 0 {
				mu2.Lock()
				docIds = append(docIds, d)
				totalDocs += len(d)
				mu2.Unlock()
			}
		}(node, i)
	}
	wg.Wait()

	idx := make([]int, len(docIds))
	docs := make(Documents, totalDocs)
	k := 0
	//var docs Documents
	for len(docIds) > 0 {
		j := -1
		minDoc := Document(math.MaxUint32)
		for i := 0; i < len(docIds); i++ {
			ptr := idx[i]
			d := docIds[i]
			if d[ptr] < minDoc {
				minDoc = d[ptr]
				j = i
			}
		}
		docs[k] = minDoc
		k++
		//docs = append(docs, minDoc)
		idx[j]++

		if idx[j] >= len(docIds[j]) {
			// Delete the slice.
			docIds = append(docIds[:j], docIds[j+1:]...)
			idx = append(idx[:j], idx[j+1:]...)
		}
	}

	//sort.Sort(docs)
	size := set.Uniq(docs)
	docs = docs[:size]
	return docs
}

func (orOperator) String() string {
	return "or"
}

func (notOperator) Combine(nodes []LogicalTreeNode, cache QueryCacher) Documents {
	if len(nodes) == 0 {
		return Documents{}
	}
	if len(nodes) == 1 {
		return nodes[0].Documents(cache)
	}

	var a Documents
	b := make([]map[Document]struct{}, len(nodes))

	a = append(a, nodes[0].Documents(cache)...)

	for i := 1; i < len(nodes); i++ {
		b[i] = nodes[i].Documents(cache).Set()
	}

	// Now make b prime, comprising the docs not in a.
	bP := make(map[Document]bool)
	for i := 0; i < len(b); i++ {
		for k := range b[i] {
			bP[k] = true
		}
	}

	// Relative compliment.
	var docs Documents
	for _, doc := range a {
		if !bP[doc] {
			docs = append(docs, doc)
		}
	}

	return docs
}

func (notOperator) String() string {
	return "not"
}

// Query returns the underlying query of the combinator.
func (c Combinator) Query() cqr.CommonQueryRepresentation {
	return c.Clause.Query
}

// Documents returns the documents retrieved by the combinator.
func (c Combinator) Documents(cache QueryCacher) Documents {
	return c.Combine(c.Clauses, cache)
}

// String is the combinator name.
func (c Combinator) String() string {
	return c.Operator.String()
}

// Query returns the underlying query of the atom.
func (a Atom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

// Documents returns the documents retrieved by the atom.
func (a Atom) Documents(cache QueryCacher) Documents {
	docs, err := cache.Get(a.Clause.Query)
	if err == ErrCacheMiss {
		return Documents{}
	}
	if err != nil {
		panic(err)
	}
	return docs
}

// String returns the query string.
func (a Atom) String() string {
	return a.Query().StringPretty()
}

// Query returns the underlying query of the adjacency operator.
func (a AdjAtom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

// Documents returns the documents retrieved by the adjacency operator.
func (a AdjAtom) Documents(cache QueryCacher) Documents {
	docs, err := cache.Get(a.Clause.Query)
	if err == ErrCacheMiss {
		return Documents{}
	}
	if err != nil {
		panic(err)
	}
	return docs
}

// String returns the query string.
func (a AdjAtom) String() string {
	return a.Query().String()
}

// String returns the string representation of the documents.
func (d Document) String() string {
	return fmt.Sprintf("%d", d)
}

// NewAtom creates a new atom.
func NewAtom(keyword cqr.Keyword) Atom {
	return Atom{
		Clause: Clause{
			Hash:  HashCQR(keyword),
			Query: keyword,
		},
	}
}

// NewAdjAtom creates a new adjacent atom.
func NewAdjAtom(query cqr.BooleanQuery) AdjAtom {
	return AdjAtom{
		Clause{
			Hash:  HashCQR(query),
			Query: query,
		},
	}
}

// NewCombinator creates a new combinator.
func NewCombinator(query cqr.BooleanQuery, operator Operator, clauses ...LogicalTreeNode) Combinator {
	return Combinator{
		Operator: operator,
		Clause: Clause{
			Hash:  HashCQR(query),
			Query: query,
		},
		Clauses: clauses,
	}
}

// HashCQR creates a hash of the query.
func HashCQR(representation cqr.CommonQueryRepresentation) uint64 {
	if representation == nil {
		return 0
	}
	return crc64.Checksum([]byte(representation.String()), crc64.MakeTable(crc64.ISO))
	//h := fnv.New64a()
	//h.Write([]byte(representation.String()))
	//return h.Sum64()
}

// constructTree creates a logical tree recursively by descending top down. If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func constructTree(query pipeline.Query, ss stats.StatisticsSource, seen QueryCacher) (LogicalTreeNode, QueryCacher, error) {
	if seen == nil {
		seen = NewMapQueryCache()
	}
	if query.Query == nil {
		return NewCombinator(cqr.NewBooleanQuery(cqr.OR, nil), OrOperator, nil), nil, nil
	}
	switch q := query.Query.(type) {
	case cqr.Keyword:
		// Return a seen clause.
		var docs Documents

		{
			mu.Lock()
			docs, err := seen.Get(q)
			if err == nil && docs != nil {
				mu.Unlock()
				return NewAtom(q), seen, nil
			} else if err != nil && err != ErrCacheMiss {
				mu.Unlock()
				return nil, nil, err
			}
			mu.Unlock()
		}

		ids, err := stats.GetDocumentIDs(query, ss)
		if err != nil {
			return nil, nil, err
		}

		docs = make(Documents, len(ids))
		for i, id := range ids {
			docs[i] = Document(id)
		}

		{
			mu.Lock()
			defer mu.Unlock()
			// Create the new clause add it to the seen list.
			a := NewAtom(q)
			err = seen.Set(a.Query(), docs)
			if err != nil {
				return nil, nil, err
			}
			return a, seen, nil
		}
	case cqr.BooleanQuery:
		var operator Operator
		switch strings.ToLower(q.Operator) {
		case "or":
			operator = OrOperator
		case "and":
			operator = AndOperator
		case "not":
			operator = NotOperator
		default:
			operator = OrOperator
		}

		// We need to create a special case for adjacent clauses.
		if strings.Contains(strings.ToLower(q.Operator), "adj") {
			operator = AndOperator
			//// Return a seen clause.
			//docs, err := seen.Get(q)
			//if err == nil && docs != nil {
			//	return NewAdjAtom(q), seen, nil
			//} else if err != nil && err != ErrCacheMiss {
			//	return nil, nil, err
			//}
			//
			//ids, err := stats.GetDocumentIDs(query, ss)
			//if err != nil {
			//	return nil, nil, err
			//}
			//
			//docs = make(Documents, len(ids))
			//for i, id := range ids {
			//	docs[i] = Document(id)
			//}
			//
			//a := NewAdjAtom(q)
			//err = seen.Set(a.Query(), docs)
			//if err != nil {
			//	return nil, nil, err
			//}
			//return a, seen, nil
		}

		// Otherwise, we can just perform the operation with a typical operator.
		clauses := make([]LogicalTreeNode, len(q.Children))
		var wg sync.WaitGroup
		var once sync.Once
		var errOnce error
		for i, child := range q.Children {
			wg.Add(1)
			go func(idx int, c cqr.CommonQueryRepresentation) {
				defer wg.Done()
				var err error
				clauses[idx], seen, err = constructTree(pipeline.NewQuery(query.Name, query.Topic, c), ss, seen)
				if err != nil {
					once.Do(func() {
						errOnce = err
					})
				}
			}(i, child)
		}
		wg.Wait()
		if errOnce != nil {
			return nil, nil, errOnce
		}
		return NewCombinator(q, operator, clauses...), seen, nil
	}
	return nil, nil, errors.New(fmt.Sprintf("supplied query is not supported: %s", query.Query))
}

func addRelevant(q cqr.CommonQueryRepresentation, relevant Documents) cqr.CommonQueryRepresentation {
	clauses := make([]cqr.CommonQueryRepresentation, len(relevant))
	for i, r := range relevant {
		clauses[i] = cqr.NewKeyword(r.String(), fields.PMID)
	}
	return cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
		q,
		cqr.NewBooleanQuery(cqr.OR, clauses),
	})
}

func constructShallowTree(query pipeline.Query, s stats.StatisticsSource, relevant Documents) (LogicalTreeNode, error) {
	switch q := query.Query.(type) {
	case cqr.Keyword:
		var n, r float64
		var err error
		if len(relevant) > 0 {
			r, err = s.RetrievalSize(addRelevant(q, relevant))
			if err != nil {
				return nil, err
			}
		}
		n, err = s.RetrievalSize(q)
		if err != nil {
			return nil, err
		}
		fmt.Println(" - ", q, n, r)
		a := NewAtom(q)
		a.N = n
		a.R = r
		return a, nil
	case cqr.BooleanQuery:
		var operator Operator
		switch strings.ToLower(q.Operator) {
		case cqr.OR:
			operator = OrOperator
		case cqr.AND:
			operator = AndOperator
		case cqr.NOT:
			operator = NotOperator
		default:
			operator = OrOperator
		}
		clauses := make([]LogicalTreeNode, len(q.Children))
		//var wg sync.WaitGroup
		var err error
		for i, child := range q.Children {
			//wg.Add(1)
			//go func(c cqr.CommonQueryRepresentation, idx int) {
			clauses[i], err = constructShallowTree(pipeline.NewQuery(query.Name, query.Topic, child), s, relevant)
			if err != nil {
				panic(err)
			}
			//wg.Done()
			//}(child, i)
		}
		var r float64
		n, err := s.RetrievalSize(q)
		if err != nil {
			return nil, err
		}
		if len(relevant) > 0 {
			r, err = s.RetrievalSize(addRelevant(q, relevant))
			if err != nil {
				return nil, err
			}
		}
		b := NewCombinator(q, operator, clauses...)
		b.N = n
		b.R = r
		//wg.Wait()
		//fmt.Printf("%v - (%f, %f)\n", q, n, r)
		return b, nil
	}
	return nil, errors.New(fmt.Sprintf("supplied query is not supported: %s", query.Query))
}

// NewLogicalTree creates a new logical tree.  If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func NewLogicalTree(query pipeline.Query, ss stats.StatisticsSource, seen QueryCacher) (LogicalTree, QueryCacher, error) {
	if seen == nil {
		seen = NewMapQueryCache()
	}
	root, seen, err := constructTree(query, ss, seen)
	if err != nil {
		return LogicalTree{}, nil, err
	}
	return LogicalTree{
		Root: root,
	}, seen, nil
}

func NewShallowLogicalTree(query pipeline.Query, s stats.StatisticsSource, relevant Documents) (LogicalTree, error) {
	node, err := constructShallowTree(query, s, relevant)
	return LogicalTree{
		Root: node,
	}, err
}

// Documents returns the documents that the tree (query) would return if executed.
func (root LogicalTree) Documents(cache QueryCacher) Documents {
	return root.Root.Documents(cache)
}

// ToCQR creates a query backwards from a logical tree.
func (root LogicalTree) ToCQR() cqr.CommonQueryRepresentation {
	switch c := root.Root.(type) {
	case Atom:
		return c.Query()
	case AdjAtom:
		return c.Query()
	case Combinator:
		return c.Query()
	}
	return nil
}
