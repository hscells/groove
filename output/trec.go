package output

import "github.com/TimothyJones/trecresults"

// TrecResults represents the output format for trec results.
type TrecResults struct {
	Path    string
	Results trecresults.ResultList
}
