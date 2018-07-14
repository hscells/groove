package learning

// TransformationContext is the context under a transformation is applied.
type TransformationContext struct {
	Depth         float64
	ClauseType    float64
	ChildrenCount float64
}

var (
	booleanClause = 0.0
	keywordClause = 1.0
)

// AddDepth increases the depth of the transformation.
func (t TransformationContext) AddDepth(d float64) TransformationContext {
	t.Depth += d
	return t
}

// SetClauseType sets the type of clause (boolean/keyword).
func (t TransformationContext) SetClauseType(v float64) TransformationContext {
	t.ClauseType = v
	return t
}

// SetChildrenCount sets the number of children a clause has.
func (t TransformationContext) SetChildrenCount(v float64) TransformationContext {
	t.ChildrenCount = v
	return t
}
