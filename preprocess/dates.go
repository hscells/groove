package preprocess

import (
	"bufio"
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/transmute/fields"
	"os"
	"strings"
	"time"
)

// DateRestrictions loads a file in the format:
//
//	CD008122	19400101	20100114
// 	CD008587	19920101	20151130
// 	CD008759	19460101	20160630
// 	CD008892	19660101	20160301
// 	CD009175	19500101	20120626
// 	CD009263	19400101	20120921
// 	CD009694	19920101	20120831
// 	CD010213	19460101	20160719
// 	CD010296	19460101	20160701
// 	CD010502	19800101	20130201
// 	CD010657	19400101	20160331
//
// Where the fist column is the topic of the query, and the other
// two columns are the start and end dates of the restriction.
func DateRestrictions(pubDatesFile, topic string) BooleanTransformation {
	return func(query cqr.CommonQueryRepresentation) Transformation {
		return func() cqr.CommonQueryRepresentation {
			f, err := os.OpenFile(pubDatesFile, os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}

			type restriction struct {
				topic string
				start time.Time
				end   time.Time
			}
			var restrictions []restriction

			s := bufio.NewScanner(f)
			for s.Scan() {
				line := strings.Split(s.Text(), "\t")
				topic, start, end := line[0], line[1], line[2]
				s, err := time.Parse("20060102", start)
				if err != nil {
					panic(err)
				}
				e, err := time.Parse("20060102", end)
				if err != nil {
					panic(err)
				}
				restrictions = append(restrictions, restriction{
					topic: topic,
					start: s,
					end:   e,
				})
			}
			for _, r := range restrictions {
				if r.topic == topic {
					bq := cqr.NewBooleanQuery(cqr.AND, []cqr.CommonQueryRepresentation{
						query,
						cqr.NewKeyword(fmt.Sprintf("%s:%s", r.start.Format("2006/01"), r.end.Format("2006/01")), fields.PublicationDate),
					})
					return bq
				}
			}
			return query
		}
	}
}
