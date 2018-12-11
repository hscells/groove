package seed

type ClinicalQuestionConstructor struct {
	question string
}

//
//func (c ClinicalQuestionConstructor) Construct() ([]cqr.CommonQueryRepresentation, error) {
//
//}

func NewClinicalQuestionConstructor(question string) ClinicalQuestionConstructor {
	return ClinicalQuestionConstructor{
		question: question,
	}
}
