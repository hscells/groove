package rewrite

import (
	"github.com/hscells/cqr"
	"gopkg.in/neurosnap/sentences.v1"
	"github.com/jdkato/prose/tokenize"
	"strings"
)

var (
	StopwordsEn = []string{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
		"it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this",
		"to", "was", "will", "with",

		"e.g.", "i.e.", "we", "than", "after", "met", "vs.",

		"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
		"he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
		"theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
		"was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
		"the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
		"against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up",
		"down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when",
		"where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no",
		"nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
		"should", "now"}
)

func MakeKeywords(text string, stopwords []string, punctuation sentences.PunctStrings) []cqr.Keyword {
	// tokenise the sentence
	tokeniser := tokenize.NewTreebankWordTokenizer()
	tokens := tokeniser.Tokenize(strings.ToLower(text))

	var keywords []cqr.Keyword

	stopMap := make(map[string]struct{})
	seen := make(map[string]struct{})
	for _, word := range stopwords {
		stopMap[word] = struct{}{}
	}

	punkt := sentences.NewWordTokenizer(punctuation)

	for _, token := range tokens {
		if punkt.IsAlpha(sentences.NewToken(token)) {
			if _, ok := seen[token]; !ok {
				if _, ok := stopMap[token]; !ok {
					keywords = append(keywords, cqr.NewKeyword(token, "title", "text"))
				}
				seen[token] = struct{}{}
			}
		}
	}
	return keywords
}
