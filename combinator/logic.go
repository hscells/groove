// Package combinator contains methods for performing logical operations on queries.
package combinator

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/groove/stats"
	"github.com/hscells/trecresults"
	"github.com/pkg/errors"
	"github.com/xtgo/set"
	"hash/fnv"
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
	Root LogicalTreeNode
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
}

// Atom is the smallest possible component of a query.
type Atom struct {
	Clause
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
func (d Documents) Results(query groove.PipelineQuery, run string) trecresults.ResultList {
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
		next := docIDs[i]

		docs = append(docs, next...)

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
		return nodes[0].Documents(cache)
	}

	totalDocs := 0
	var docIds []Documents
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(n LogicalTreeNode, j int) {
			defer wg.Done()
			d := n.Documents(cache)
			if len(d) > 0 {
				mu.Lock()
				docIds = append(docIds, d)
				totalDocs += len(d)
				mu.Unlock()
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
		Clause{
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
	h := fnv.New64a()
	h.Write([]byte(representation.String()))
	return h.Sum64()
}

// constructTree creates a logical tree recursively by descending top down. If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func constructTree(query groove.PipelineQuery, ss stats.StatisticsSource, seen QueryCacher) (LogicalTreeNode, QueryCacher, error) {
	if seen == nil {
		seen = NewMapQueryCache()
	}
	switch q := query.Query.(type) {
	case cqr.Keyword:
		// Return a seen clause.
		var docs Documents

		mu.Lock()
		defer mu.Unlock()
		docs, err := seen.Get(q)
		if err == nil && docs != nil {
			return NewAtom(q), seen, nil
		} else if err != nil && err != ErrCacheMiss {
			return nil, nil, err
		}

		ids, err := stats.GetDocumentIDs(query, ss)
		if err != nil {
			return nil, nil, err
		}

		docs = make(Documents, len(ids))
		for i, id := range ids {
			docs[i] = Document(id)
		}

		// Create the new clause add it to the seen list.
		a := NewAtom(q)
		err = seen.Set(a.Query(), docs)
		if err != nil {
			return nil, nil, err
		}
		return a, seen, nil
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
			// Return a seen clause.
			docs, err := seen.Get(q)
			if err == nil && docs != nil {
				return NewAdjAtom(q), seen, nil
			} else if err != nil && err != ErrCacheMiss {
				return nil, nil, err
			}

			ids, err := stats.GetDocumentIDs(query, ss)
			if err != nil {
				return nil, nil, err
			}

			docs = make(Documents, len(ids))
			for i, id := range ids {
				docs[i] = Document(id)
			}

			a := NewAdjAtom(q)
			err = seen.Set(a.Query(), docs)
			if err != nil {
				return nil, nil, err
			}
			return a, seen, nil
		}

		// Otherwise, we can just perform the operation with a typical operator.
		clauses := make([]LogicalTreeNode, len(q.Children))
		for i, child := range q.Children {
			var err error
			clauses[i], seen, err = constructTree(groove.NewPipelineQuery(query.Name, query.Topic, child), ss, seen)
			if err != nil {
				return nil, seen, err
			}
		}
		c := NewCombinator(q, operator, clauses...)
		return c, seen, nil
	}
	return nil, nil, errors.New("supplied query is not supported")
}

// NewLogicalTree creates a new logical tree.  If the operator of the query is unknown
// (i.e. it is not one of `or`, `and`, `not`, or an `adj` operator) the default operator will be `or`.
//
// Note that once one tree has been constructed, the returned map can be used to save processing.
func NewLogicalTree(query groove.PipelineQuery, ss stats.StatisticsSource, seen QueryCacher) (LogicalTree, QueryCacher, error) {
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
