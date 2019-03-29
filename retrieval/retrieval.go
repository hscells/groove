// package retrieval provides handlers which operate on result lists.
package retrieval

import "github.com/hscells/trecresults"

// ResultsHandler is the interface for operations on result lists.
type ResultsHandler interface {
	Handle(list *trecresults.ResultList) error
}
