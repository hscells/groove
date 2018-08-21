package learning

// Model is an abstract representation of a machine learning model that can perform a training
// and a testing task. Optionally, the model may also have a validation task.
// Additionally, a model must implement how features for training are generated.
type Model interface {
	// Train must train a model.
	Train() error
	// Validate must (optionally) validate the model.
	Validate() error
	// Test must test the model to produce some output.
	Test() error
	// Generate features for learning.
	Generate() error
}

// FeatureGenerator models a way for features to be generated for a machine learning task that
// may be used by a Model.
type FeatureGenerator interface {
	Generate() error
}
