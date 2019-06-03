package rank

type ScoredDocument struct {
	PMID  string
	Score float64
	Rank  float64
}

type ScoredDocuments struct {
	Docs []ScoredDocument
}
