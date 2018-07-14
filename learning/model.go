package learning

import (
	"io"
	"reflect"
)

// Model is an abstract representation of a machine learning model that can perform a training
// and a testing task. Optionally, the model may also have a validation task.
type Model interface {
	// Train must train a model.
	Train() error
	// Validate must (optionally) validate the model.
	Validate() error
	// Test must test the model to produce some output.
	Test() (interface{}, error)
	// Output must output a learned model (via testing) to a file.
	Output(w io.Writer) error
	// Type specifies the output type of the test method.
	Type() reflect.Type
}

// FeatureGenerator models a way for features to be generated for a machine learning task that
// may be used by a Model.
type FeatureGenerator interface {
	Generate() error
}
