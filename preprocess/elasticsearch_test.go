package preprocess

import (
	"testing"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/stats"
	"github.com/hscells/transmute/backend"
	"github.com/hscells/transmute/lexer"
	"github.com/hscells/transmute/pipeline"
	"github.com/hscells/transmute/parser"
)

func TestSetAnalysedField(t *testing.T) {
	cqrPipeline := pipeline.NewPipeline(
		parser.NewMedlineParser(),
		backend.NewCQRBackend(),
		pipeline.TransmutePipelineOptions{
			LexOptions: lexer.LexOptions{
				FormatParenthesis: false,
			},
			RequiresLexing: true,
		})

	rawQuery := `1. ((zentralveno?s* kathet* or venostrom* or venenkathe* or hickman line* or central line* insertion* or pulmonary arter* flotation* or venous or vein* or cannulation or access or catheter* puncture or central venous line* or central venous pressure).mp. or exp Venous Cutdown/ or Central Venous Pressure/ or exp Catheterization Central Venous/)
2. ((ultrasound* or ultrasonic* or Doppler or echography or ultrasonograpgh*).mp. or exp Ultrasonography Doppler Color/ or exp Echocardiography Doppler/ or exp Ultrasonography/ or exp Ultrasonics/)
3. 1 and 2
4. ((("randomized controlled trial" or "controlled clinical trial").pt. or randomized.ab. or placebo.ab. or drug therapy.fs. or randomly.ab. or trial.ab. or groups.ab.) and (humans not animals)).sh.
5. 3 and 4`

	cq, err := cqrPipeline.Execute(rawQuery)
	if err != nil {
		t.Fatal(err)
	}

	ss := stats.NewElasticsearchStatisticsSource(stats.ElasticsearchAnalysedField("stemmed"))
	repr, err := cq.Representation()
	if err != nil {
		t.Fatal(err)
	}
	SetAnalyseField(repr.(cqr.CommonQueryRepresentation), ss)()

}
