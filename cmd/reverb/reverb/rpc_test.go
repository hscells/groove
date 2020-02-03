package reverb_test

import (
	"github.com/hscells/boogie"
	"github.com/hscells/groove/cmd/reverb/reverb"
	"testing"
)

func TestReverb(t *testing.T) {
	dsl := boogie.Pipeline{
		Statistic: boogie.PipelineStatistic{
			Source: "entrez",
		},
		Query: boogie.PipelineQuery{
			Format: "medline",
			Path:   "reverb_queries",
		},
		Measurements: []string{"boolean_clauses", "boolean_keywords"},
		Output: boogie.PipelineOutput{
			Measurements: []boogie.MeasurementOutput{
				{Format: "json", Filename: "test.json"},
			},
		},
	}

	reverb.Execute(dsl, "ielab-sysrev3.uqcloud.net:80", "ielab-sysrev4.uqcloud.net:80")
}
