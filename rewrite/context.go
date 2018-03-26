package rewrite

type TransformationContext struct {
	Depth         float64
	ClauseType    float64
	ChildrenCount float64
}

var (
	booleanClause = 0.0
	keywordClause = 1.0
)

func (t TransformationContext) AddDepth(d float64) TransformationContext {
	t.Depth += d
	return t
}

func (t TransformationContext) SetClauseType(v float64) TransformationContext {
	t.ClauseType = v
	return t
}

func (t TransformationContext) SetChildrenCount(v float64) TransformationContext {
	t.ChildrenCount = v
	return t
}
