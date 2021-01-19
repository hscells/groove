package formulation

import (
	"github.com/dan-locke/clean-html"
	"github.com/hscells/go-unidecode"
	"strings"
	"unicode"
)

type tokeniseOutput [][]byte

type currType int

const (
	char currType = iota
	num
	space
	other
)

func tokenise(text string) (tokeniseOutput, error) {
	var curr currType
	var tokens [][]byte

	var currWordLen int

	txt := unidecode.Unidecode(strings.ToLower(text))

	portions, err := clean_html.TextPos([]byte(txt))
	if err != nil {
		return nil, err
	}

	for i := range portions.Positions {
		for j, t := range txt[portions.Positions[i][0]:portions.Positions[i][1]] {
			prev := curr

			if unicode.IsSpace(t) {
				curr = space
			} else if unicode.IsNumber(t) {
				curr = num
			} else if unicode.IsLetter(t) {
				curr = char
			} else {
				curr = other
			}

			// Remove this if not doing lower ...
			if curr == char {
				t = unicode.ToLower(t)
			}

			var change bool

			if prev != curr {
				change = true
			} else if curr == other {
				continue
			}

			if change {
				start := portions.Positions[i][0] + j - currWordLen
				if start < 0 {
					start = 0
				}
				if currWordLen != 0 {
					if curr == other || curr == num {
						currWordLen = 0
						continue
					}
					tokens = append(tokens, []byte(txt[start:portions.Positions[i][0]+j]))
					currWordLen = 0
				}
			}

			if portions.Positions[i][0]+j+1 == len(txt) {
				if curr != space {
					if curr == other || curr == num {
						currWordLen = 0
						continue
					}
					tokens = append(tokens, []byte(txt[portions.Positions[i][0]+j-currWordLen:portions.Positions[i][0]+j+1]))
					currWordLen = 0
				}
			}

			if curr != space && curr != other && curr != num {
				currWordLen++
			}
		}
	}

	return tokens, nil
}
