package query

import (
	"github.com/hscells/cqr"
	"github.com/hscells/groove"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	"github.com/hscells/transmute/pipeline"
	"io/ioutil"
)

var (
	// MedlineTransmutePipeline is a default pipeline for Medline queries.
	MedlineTransmutePipeline = pipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})
	// PubMedTransmutePipeline is a default pipeline for PubMed queries.
	PubMedTransmutePipeline = pipeline.NewPipeline(
		parser.NewPubMedParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: true,
			},
			RequiresLexing: true,
		})
)

// TransmuteQuerySource is a source for queries.
type TransmuteQuerySource struct {
	pipeline pipeline.TransmutePipeline
	queries  []groove.PipelineQuery
}

// Load takes a directory of queries and parses them using a supplied transmute pipeline.
func (ts TransmuteQuerySource) Load(directory string) ([]groove.PipelineQuery, error) {
	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	// Next, use the transmute pipeline to parse them.
	queries := make([]groove.PipelineQuery, len(files))
	for i, f := range files {
		source, err := ioutil.ReadFile(directory + "/" + f.Name())
		if err != nil {
			return nil, err
		}

		bq, err := ts.pipeline.Execute(string(source))
		if err != nil {
			return nil, err
		}

		topic := f.Name()

		repr, err := bq.Representation()
		if err != nil {
			return nil, err
		}
		queries[i] = groove.NewPipelineQuery(f.Name(), topic, repr.(cqr.CommonQueryRepresentation))
	}

	// Finally, return the parsed queries.
	return queries, nil
}

// NewTransmuteQuerySource creates a new query source from a transmute pipeline.
func NewTransmuteQuerySource(transmutePipeline pipeline.TransmutePipeline) TransmuteQuerySource {
	return TransmuteQuerySource{
		pipeline: transmutePipeline,
	}
}
