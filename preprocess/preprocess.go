// Package preprocess handles preprocessing and transformation of queries.
package preprocess

import (
	"github.com/hscells/cqr"
	"regexp"
	"strings"
)

// QueryProcessor is applied to queries before analysing/measuring.
type QueryProcessor func(text string) string

var (
	alphanum, _ = regexp.Compile("[^a-zA-Z0-9 ]+")
)

// AlphaNum removes all non-alphanumeric characters from a query.
func AlphaNum(text string) string {
	return alphanum.ReplaceAllString(text, "")
}

// Lowercase transforms all capital letters to lowercase.
func Lowercase(text string) string {
	return strings.ToLower(text)
}

// ProcessQuery applies a query processor to a query.
func ProcessQuery(query cqr.CommonQueryRepresentation, processor QueryProcessor) cqr.CommonQueryRepresentation {
	switch q := query.(type) {
	case cqr.Keyword:
		q.QueryString = processor(q.QueryString)
		return q
	case cqr.BooleanQuery:
		for i, child := range q.Children {
			q.Children[i] = ProcessQuery(child, processor)
		}
		return q
	}
	return query
}
