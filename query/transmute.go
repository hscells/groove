package query

import (
	"github.com/hscells/cqr"
	gpipeline "github.com/hscells/groove/pipeline"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/parser"
	tpipeline "github.com/hscells/transmute/pipeline"
	"io/ioutil"
	"log"
)

var (
	// MedlineTransmutePipeline is a default pipeline for Medline queries.
	MedlineTransmutePipeline = tpipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		tpipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})
	// PubMedTransmutePipeline is a default pipeline for PubMed queries.
	PubMedTransmutePipeline = tpipeline.NewPipeline(
		parser.NewPubMedParser(),
		backend.NewCQRBackend(),
		tpipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: true,
			},
			AddRedundantParenthesis: true,
			RequiresLexing:          false,
		})
	// PubMedTransmutePipeline is a default pipeline for cqr queries.
	CQRTransmutePipeline = tpipeline.NewPipeline(
		parser.NewCQRParser(),
		backend.NewCQRBackend(),
		tpipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: false,
		})
)

// TransmuteQuerySource is a source for queries.
type TransmuteQuerySource struct {
	pipeline tpipeline.TransmutePipeline
	queries  []gpipeline.Query
}

// Load takes a directory of queries and parses them using a supplied transmute gpipeline.
func (ts TransmuteQuerySource) Load(directory string) ([]gpipeline.Query, error) {
	// First, get a list of files in the directory.
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	// Next, use the transmute gpipeline to parse them.
	queries := make([]gpipeline.Query, len(files))
	for i, f := range files {
		if f.IsDir() {
			continue
		}

		if len(f.Name()) == 0 {
			continue
		}

		source, err := ioutil.ReadFile(directory + "/" + f.Name())
		if err != nil {
			return nil, err
		}

		bq, err := ts.pipeline.Execute(string(source))
		if err != nil {
			log.Printf("transmute error in topic %s\n", f.Name())
			return nil, err
		}

		topic := f.Name()

		repr, err := bq.Representation()
		if err != nil {
			return nil, err
		}
		queries[i] = gpipeline.NewQuery(f.Name(), topic, repr.(cqr.CommonQueryRepresentation))
	}

	// Finally, return the parsed queries.
	return queries, nil
}

// NewTransmuteQuerySource creates a new query source from a transmute gpipeline.
func NewTransmuteQuerySource(transmutePipeline tpipeline.TransmutePipeline) TransmuteQuerySource {
	return TransmuteQuerySource{
		pipeline: transmutePipeline,
	}
}
