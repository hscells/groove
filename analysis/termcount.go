package analysis

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"strings"
	"github.com/pkg/errors"
	"fmt"
	"reflect"
)

// TermCount is a measurement that counts the number of terms in the query.
type TermCount struct{}

// Name returns the name of the measurement.
func (tc TermCount) Name() string {
	return "TermCount"
}

// TermCount counts the total number of terms in a query. If a Keyword has more than one terms, it will split it and
// count each individual term in that query string.
func (tc TermCount) Execute(q cqr.CommonQueryRepresentation, s stats.StatisticsSource) (float64, error) {
	numTerms := 0.0
	switch query := q.(type) {
	case cqr.Keyword:
		numTerms = float64(len(strings.Split(query.QueryString, " ")))
	case cqr.BooleanQuery:
		for _, k := range query.Children {
			nt, err := tc.Execute(k, s)
			if err != nil {
				return 0, err
			}
			numTerms += nt
		}
	default:
		return 0, errors.New(fmt.Sprintf("undefined type %v", reflect.TypeOf(query)))
	}
	return numTerms, nil
}
