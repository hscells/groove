package output

import "github.com/hscells/trecresults"

// TrecResults represents the output format for trec results.
type TrecResults struct {
	Path    string
	Results trecresults.ResultList
}
