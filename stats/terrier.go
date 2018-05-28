//+build darwin windows

package stats

import (
	"fmt"
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/jnigi"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/trecresults"
	"github.com/magiconair/properties"
	"log"
	"path/filepath"
	"strconv"
)

// TerrierStatisticsSource is a source of statistics using the terrier information retrieval project;
// http://terrier.org/
type TerrierStatisticsSource struct {
	propertiesPath string
	indexPath      string
	indexPrefix    string

	env          *jnigi.Env
	idx          *jnigi.ObjectRef
	queryManager *jnigi.ObjectRef

	field string

	options    SearchOptions
	parameters map[string]float64
}

// SearchOptions gets the search options for this source.
func (t TerrierStatisticsSource) SearchOptions() SearchOptions {
	return t.options
}

// Parameters gets the parameters for this source.
func (t TerrierStatisticsSource) Parameters() map[string]float64 {
	return t.parameters
}

// TermFrequency does not work.
// TODO implement this.
func (t TerrierStatisticsSource) TermFrequency(term, document string) (float64, error) {
	panic("implement me")
	docID, err := strconv.Atoi(document)
	if err != nil {
		return 0.0, err
	}

	// Grab the lexicon object from the index.
	postingRef, err := t.idx.CallMethod(t.env, "getInvertedIndex", "org/terrier/structures/PostingIndex")
	if err != nil {
		return 0.0, err
	}

	// Cast the lexicon to an appropriate implementation.
	posting := postingRef.(*jnigi.ObjectRef)
	posting = posting.Cast("org/terrier/structures/bit/BitPostingIndex")

	documentIndexRef, err := t.idx.CallMethod(t.env, "getDocumentIndex", "org/terrier/structures/DocumentIndex")
	if err != nil {
		return 0.0, err
	}
	documentIndex := documentIndexRef.(*jnigi.ObjectRef)
	documentEntry, err := documentIndex.CallMethod(t.env, "getDocumentEntry", "org/terrier/structures/DocumentIndexEntry", docID)

	postingsRef, err := posting.CallMethod(t.env, "getPostings", "org/terrier/structures/postings/IterablePosting", documentEntry)
	if err != nil {
		return 0.0, err
	}
	postings := postingsRef.(*jnigi.ObjectRef)

	log.Println(postings)

	return 0.0, err
}

// TermVector does not work.
// TODO implement this.
func (t TerrierStatisticsSource) TermVector(document string) (TermVector, error) {
	panic("Implement me")
}

// DocumentFrequency is the document frequency (the number of documents containing the current term).
func (t TerrierStatisticsSource) DocumentFrequency(term string) (float64, error) {
	nt, err := lexiconEntryForTerm(t.env, t.idx, "term", "getDocumentFrequency")
	if err != nil {
		return 0.0, err
	}
	return nt, nil
}

// TotalTermFrequency is a sum of total term frequencies (the sum of total term frequencies of each term in this field).
func (t TerrierStatisticsSource) TotalTermFrequency(term, field string) (float64, error) {
	tf, err := lexiconEntryForTerm(t.env, t.idx, "term", "getFrequency")
	if err != nil {
		return 0.0, err
	}
	return tf, nil
}

// InverseDocumentFrequency is the ratio of of documents in the collection to the number of documents the term appears
// in, logarithmically smoothed.
func (t TerrierStatisticsSource) InverseDocumentFrequency(term, field string) (float64, error) {
	N, err := t.DocumentFrequency(term)
	if err != nil {
		return 0.0, err
	}

	nt, err := t.TotalTermFrequency(term, field)
	if err != nil {
		return 0.0, err
	}

	return idf(N, nt), nil
}

// RetrievalSize is the minimum number of documents that contains at least one of the query terms.
func (t TerrierStatisticsSource) RetrievalSize(query cqr.CommonQueryRepresentation) (float64, error) {
	resultSet, err := search(t.env, query, SearchOptions{}, t)
	if err != nil {
		return 0.0, err
	}
	resultSize, err := resultSet.CallMethod(t.env, "getResultSize", jnigi.Int)
	if err != nil {
		return 0.0, err
	}

	return float64(resultSize.(int)), nil
}

// VocabularySize is the total number of terms in the vocabulary.
func (t TerrierStatisticsSource) VocabularySize(field string) (float64, error) {
	collStatsRef, err := t.idx.CallMethod(t.env, "getCollectionStatistics", "org/terrier/structures/CollectionStatistics")
	if err != nil {
		return 0.0, err
	}

	collStats := collStatsRef.(*jnigi.ObjectRef)
	vocabRef, err := collStats.CallMethod(t.env, "getNumberOfTokens", jnigi.Long)
	if err != nil {
		return 0.0, err
	}

	return float64(vocabRef.(int64)), nil
}

// Execute issues a query to terrier.
func (t TerrierStatisticsSource) Execute(query groove.PipelineQuery, options SearchOptions) (trecresults.ResultList, error) {
	var (
		scores, docIDs []int64
		N              int
	)
	trecResultSet := trecresults.ResultList{}

	// Grab the result set from terrier.
	resultSet, err := search(t.env, query.Query, options, t)
	if err != nil {
		return trecResultSet, err
	}

	// Get the size of results for later on.
	resultSize, err := resultSet.CallMethod(t.env, "getResultSize", jnigi.Int)
	if err != nil {
		return trecResultSet, err
	}

	// Get the doc ids and scores from terrier.
	N = resultSize.(int)
	docIdsRef, err := resultSet.CallMethod(t.env, "getDocids", jnigi.Int|jnigi.Array)
	if err != nil {
		return trecResultSet, err
	}
	scoresRef, err := resultSet.CallMethod(t.env, "getScores", jnigi.Int|jnigi.Array)
	if err != nil {
		return trecResultSet, err
	}

	// Cast everything to Go.
	docIDs = make([]int64, N)
	for i, docID := range docIdsRef.([]interface{}) {
		docIDs[i] = docID.(int64)
	}

	scores = make([]int64, N)
	for i, score := range scoresRef.([]interface{}) {
		scores[i] = score.(int64)
	}

	// Populate the trec results list.
	trecResultSet = make(trecresults.ResultList, N)
	for i := 0; i < N; i++ {
		trecResultSet[i] = &trecresults.Result{
			Topic:     query.Topic,
			Iteration: "Q0",
			DocId:     strconv.FormatInt(docIDs[i], 10),
			Rank:      int64(i),
			Score:     float64(scores[i]),
			RunName:   options.RunName,
		}
	}

	return trecResultSet, nil
}

// search executes a query on terrier.
func search(env *jnigi.Env, query cqr.CommonQueryRepresentation, options SearchOptions, t TerrierStatisticsSource) (*jnigi.ObjectRef, error) {
	cqrString, err := backend.NewCQRQuery(query).String()
	if err != nil {
		return nil, err
	}
	p := pipeline.NewPipeline(parser.NewCQRParser(), backend.NewTerrierBackend(), pipeline.TransmutePipelineOptions{RequiresLexing: false, FieldMapping: map[string][]string{"default": {t.field}}})
	terrierQuery, err := p.Execute(cqrString)
	if err != nil {
		return nil, err
	}

	// Wrap arguments in Java strings.
	s, err := terrierQuery.String()
	if err != nil {
		return nil, err
	}
	jQuery, err := env.NewObject("java/lang/String", []byte(s))
	if err != nil {
		log.Fatal(err)
	}
	jEndName, err := env.NewObject("java/lang/String", []byte("end"))
	if err != nil {
		log.Fatal(err)
	}
	jEndValue, err := env.NewObject("java/lang/String", []byte(strconv.FormatInt(int64(options.Size), 10)))
	if err != nil {
		log.Fatal(err)
	}
	jQueryID, err := env.NewObject("java/lang/String", []byte(options.RunName))
	srqRef, err := t.queryManager.CallMethod(env, "newSearchRequest", "org/terrier/querying/SearchRequest", jQueryID, jQuery)
	if err != nil {
		return nil, err
	}

	srq := srqRef.(*jnigi.ObjectRef)

	srq.CallMethod(env, "setControl", jnigi.Void, jEndName, jEndValue)
	t.queryManager.CallMethod(env, "runPreProcessing", jnigi.Void, srq)
	t.queryManager.CallMethod(env, "runMatching", jnigi.Void, srq)
	//queryManager.CallMethod(env, "runPostProcess", jnigi.Void, srq)
	t.queryManager.CallMethod(env, "runPostFilters", jnigi.Void, srq)

	req := srq.Cast("org/terrier/querying/Request")
	resultSetRef, err := req.CallMethod(env, "getResultSet", "org/terrier/matching/ResultSet")
	if err != nil {
		return nil, err
	}
	resultSet := resultSetRef.(*jnigi.ObjectRef)
	return resultSet, nil

}

// lexiconEntryForTerm
func lexiconEntryForTerm(env *jnigi.Env, idx *jnigi.ObjectRef, term, statistic string) (float64, error) {
	// Grab the lexicon object from the index.
	lexiconRef, err := idx.CallMethod(env, "getLexicon", "org/terrier/structures/Lexicon")
	if err != nil {
		return 0.0, err
	}

	// Cast the lexicon to an appropriate implementation.
	lexicon := lexiconRef.(*jnigi.ObjectRef)
	lexicon = lexicon.Cast("org/terrier/structures/FSOMapFileLexicon")

	// Wrap the term to analyse.
	jTerm, err := env.NewObject("java/lang/String", []byte(term))
	if err != nil {
		return 0.0, err
	}

	// Get the LexiconEntry object for the term.
	lexEntryRef, err := lexicon.CallMethod(env, "getLexiconEntry", "org/terrier/structures/LexiconEntry", jTerm)
	if err != nil {
		return 0.0, err
	}
	lexEntry := lexEntryRef.(*jnigi.ObjectRef)

	// If the term isn't in the index, we want to return 0 instead of an error.
	if lexEntry.IsNil() {
		return 0.0, nil
	}

	// Otherwise, the term exists and we can get a statistic for it.
	result, err := lexEntry.CallMethod(env, statistic, jnigi.Int)
	if err != nil {
		return 0.0, err
	}

	// And then return that statistic as a Go type.
	return float64(result.(int)), nil
}

// TerrierPropertiesPath sets the properties path field.
func TerrierPropertiesPath(path string) func(*TerrierStatisticsSource) {
	return func(t *TerrierStatisticsSource) {
		t.propertiesPath = path
		return
	}
}

// TerrierField sets the field to use.
func TerrierField(field string) func(*TerrierStatisticsSource) {
	return func(t *TerrierStatisticsSource) {
		t.field = field
		return
	}
}

// TerrierSearchOptions sets the search options for the statistic source.
func TerrierSearchOptions(options SearchOptions) func(*TerrierStatisticsSource) {
	return func(ts *TerrierStatisticsSource) {
		ts.options = options
		return
	}
}

// TerrierParameters sets the search options for the statistic source.
func TerrierParameters(params map[string]float64) func(*TerrierStatisticsSource) {
	return func(ts *TerrierStatisticsSource) {
		ts.parameters = params
		return
	}
}

// NewTerrierStatisticsSource creates a new terrier statistics source.
func NewTerrierStatisticsSource(options ...func(*TerrierStatisticsSource)) *TerrierStatisticsSource {
	t := TerrierStatisticsSource{}

	for _, option := range options {
		option(&t)
	}

	// We need to get some configuration from the terrier properties path.
	p := properties.MustLoadFile(t.propertiesPath, properties.UTF8)
	terrierHome, ok := p.Get("terrier.home")
	if !ok {
		log.Fatal("terrier.home must be specified in terrier.properties")
	}

	if _, ok := p.Get("terrier.index.path"); !ok {
		log.Fatal("terrier.index.path must be specified in terrier.properties")
	}

	if _, ok := p.Get("terrier.index.prefix"); !ok {
		log.Fatal("terrier.index.prefix must be specified in terrier.properties")
	}

	// Point the JVM to the terrier jar and the terrier properties file.
	_, env, err := jnigi.CreateJVM(jnigi.NewJVMInitArgs(false, true, jnigi.DEFAULT_VERSION, []string{fmt.Sprintf("-Djava.class.path=%s", filepath.Join(terrierHome, "/target/terrier-core-4.2-jar-with-dependencies.jar")), fmt.Sprintf("-Dterrier.setup=%s", t.propertiesPath)}))
	if err != nil {
		log.Fatal(err)
	}

	// Get the index.
	idxRef, err := env.CallStaticMethod("org/terrier/structures/Index", "createIndex", "org/terrier/structures/IndexOnDisk")
	if err != nil {
		log.Fatal(err)
	}

	idx := idxRef.(*jnigi.ObjectRef)

	queryManager, err := env.NewObject("org/terrier/querying/Manager")
	if err != nil {
		log.Fatal(err)
	}

	// Our statistics source now has the Java environment and index object as fields.
	t.idx = idx
	t.env = env
	t.queryManager = queryManager
	return &t
}
