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

func AlphaNum(text string) string {
	return alphanum.ReplaceAllString(text, "")
}

func Lowercase(text string) string {
	return strings.ToLower(text)
}

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
