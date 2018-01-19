// Package combinator contains methods for performing logical operations on queries.
package combinator

import (
	"fmt"
	"github.com/hscells/cqr"
	"hash/fnv"
	"github.com/hscells/groove/stats"
	"github.com/hscells/groove"
	"strings"
	"github.com/pkg/errors"
	"github.com/TimothyJones/trecresults"
)

var (
	OrOperator  = orOperator{}
	AndOperator = andOperator{}
	NotOperator = notOperator{}
)

// Operator can combine different nodes of a tree together.
type Operator interface {
	Combine(clauses []LogicalTreeNode) Documents
	String() string
}

// LogicalTree can compute the number of documents retrieved for atomic components.
type LogicalTree struct {
	Root LogicalTreeNode
}

// LogicalTreeNode is a node in a logical tree.
type LogicalTreeNode interface {
	Query() cqr.CommonQueryRepresentation
}

// Clause is the most basic component of a logical tree.
type Clause struct {
	Hash      uint64
	Documents Documents
	Query     cqr.CommonQueryRepresentation
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
type Document struct {
	Score float64
	Id    string
}

// Documents are a group of retrieved documents.
type Documents []Document

// andOperator is the intersection of documents.
type andOperator struct {
}

// orOperator is the union of documents.
type orOperator struct {
}

// notOperator is the relative compliment of documents.
type notOperator struct {
}

func (d Documents) Results(query groove.PipelineQuery, run string) trecresults.ResultList {
	r := make(trecresults.ResultList, len(d))
	for i, doc := range d {
		r[i] = &trecresults.Result{
			Topic:     query.Topic,
			Iteration: "Q0",
			DocId:     doc.Id,
			Rank:      int64(i),
			Score:     doc.Score,
			RunName:   run,
		}
	}
	return r
}

// Set creates a set from a slice of documents.
func (d Documents) Set() map[Document]bool {
	m := make(map[Document]bool)
	for _, doc := range d {
		m[doc] = true
	}
	return m
}

func (andOperator) Combine(nodes []LogicalTreeNode) Documents {
	nodeDocs := make([]map[Document]bool, len(nodes))
	for i, node := range nodes {
		switch n := node.(type) {
		case Atom:
			nodeDocs[i] = n.Documents.Set()
		case AdjAtom:
			nodeDocs[i] = n.Documents.Set()
		case Combinator:
			nodeDocs[i] = n.Documents.Set()
		}
	}

	intersection := make(map[Document]bool)
	for i := 0; i < len(nodeDocs)-1; i++ {
		for k := range nodeDocs[i] {
			if nodeDocs[i+1][k] {
				intersection[k] = true
			}
		}
	}

	docs := make(Documents, len(intersection))
	i := 0
	for doc := range intersection {
		docs[i] = doc
		i++
	}

	return docs
}

func (andOperator) String() string {
	return "and"
}

func (orOperator) Combine(nodes []LogicalTreeNode) Documents {
	union := make(map[Document]bool)
	for _, node := range nodes {
		switch n := node.(type) {
		case Atom:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		case AdjAtom:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		case Combinator:
			for _, doc := range n.Documents {
				union[doc] = true
			}
		}
	}

	var docs Documents
	for k := range union {
		docs = append(docs, k)
	}

	return docs
}

func (orOperator) String() string {
	return "or"
}

func (notOperator) Combine(nodes []LogicalTreeNode) Documents {
	var a Documents
	b := make([]map[Document]bool, len(nodes))

	switch n := nodes[0].(type) {
	case Atom:
		a = append(a, n.Documents...)
	case AdjAtom:
		a = append(a, n.Documents...)
	case Combinator:
		a = append(a, n.Documents...)
	}

	for i := 1; i < len(nodes); i++ {
		switch n := nodes[i].(type) {
		case Atom:
			b[i] = n.Documents.Set()
		case AdjAtom:
			b[i] = n.Documents.Set()
		case Combinator:
			b[i] = n.Documents.Set()
		}
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

func (c Combinator) Query() cqr.CommonQueryRepresentation {
	return c.Clause.Query
}

func (a Atom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

func (a AdjAtom) Query() cqr.CommonQueryRepresentation {
	return a.Clause.Query
}

func (d Document) String() string {
	return fmt.Sprintf("%v\t%v", d.Id, d.Score)
}

// NewAtom creates a new atom.
func NewAtom(keyword cqr.Keyword, docs Documents) Atom {
	return Atom{
		Clause{
			Hash:      HashCQR(keyword),
			Documents: docs,
			Query:     keyword,
		},
	}
}

// NewAdjAtom creates a new adjacent atom.
func NewAdjAtom(query cqr.BooleanQuery, docs Documents) AdjAtom {
	return AdjAtom{
		Clause{
			Hash:      HashCQR(query),
			Documents: docs,
			Query:     query,
		},
	}
}

// NewCombinator creates a new combinator.
func NewCombinator(query cqr.BooleanQuery, operator Operator, clauses ...LogicalTreeNode) Combinator {
	docs := operator.Combine(clauses)
	return Combinator{
		Operator: operator,
		Clause: Clause{
			Hash:      HashCQR(query),
			Documents: docs,
			Query:     query,
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

// constructTree creates a logical tree recursively by descending top down.
func constructTree(query groove.PipelineQuery, ss stats.StatisticsSource, seen map[uint64]LogicalTreeNode) (LogicalTreeNode, map[uint64]LogicalTreeNode, error) {
	switch q := query.Query.(type) {
	case cqr.Keyword:
		// Return a seen atom.
		if atom, ok := seen[HashCQR(query.Query)]; ok {
			return atom, seen, nil
		}

		// Otherwise, get the documents for this atom.
		results, err := ss.Execute(query, ss.SearchOptions())
		if err != nil {
			return nil, seen, err
		}
		// Transform the results into something that can be used by the combinators.
		docs := make(Documents, len(results))
		for i, result := range results {
			docs[i] = Document{
				Id:    result.DocId,
				Score: result.Score,
			}
		}
		// Create the new atom add it to the seen list.
		a := NewAtom(q, docs)
		seen[a.Hash] = a
		return a, seen, nil
	case cqr.BooleanQuery:
		// Return a seen combinator.
		if combinator, ok := seen[HashCQR(query.Query)]; ok {
			return combinator, seen, nil
		}

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
			results, err := ss.Execute(query, ss.SearchOptions())
			if err != nil {
				return nil, seen, err
			}
			docs := make(Documents, len(results))
			for i, result := range results {
				docs[i] = Document{
					Id:    result.DocId,
					Score: result.Score,
				}
			}
			a := NewAdjAtom(q, docs)
			seen[a.Hash] = a
			return a, seen, nil
		} else {
			clauses := make([]LogicalTreeNode, len(q.Children))
			for i, child := range q.Children {
				var err error
				clauses[i], seen, err = constructTree(groove.NewPipelineQuery(query.Name, query.Topic, child), ss, seen)
				if err != nil {
					return nil, seen, err
				}
			}
			c := NewCombinator(q, operator, clauses...)
			seen[c.Hash] = c
			return c, seen, nil
		}
	}
	return nil, nil, errors.New("supplied query is not supported")
}

// NewLogicalTree creates a new logical tree.
func NewLogicalTree(query groove.PipelineQuery, ss stats.StatisticsSource, seen map[uint64]LogicalTreeNode) (LogicalTree, map[uint64]LogicalTreeNode, error) {
	root, seen, err := constructTree(query, ss, seen)
	if err != nil {
		return LogicalTree{}, nil, nil
	}
	return LogicalTree{
		Root: root,
	}, seen, nil
}

func (root LogicalTree) Documents() Documents {
	switch c := root.Root.(type) {
	case Atom:
		return c.Documents
	case AdjAtom:
		return c.Documents
	case Combinator:
		return c.Combine(c.Clauses)
	}
	return nil
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
