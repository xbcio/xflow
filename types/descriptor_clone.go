package types

// Clone returns a deep copy of d. Every slice and pointer is copied, and every
// any-typed value (ParamSpec.Default, EnumOption.Value, Condition.Eq/In) is
// deep-copied through cloneParamValue, so a caller that mutates the result
// (or keeps it while the source keeps growing) cannot reach the source. Nil
// slices stay nil and empty non-nil slices stay empty non-nil.
//
// Descriptor() implementations that hand out a descriptor they also retain --
// node.Definition and node.TriggerDefinition -- must return a Clone. A caller
// holding the returned value would otherwise alias the retained slices in both
// directions: its writes reach the definition, and the definition's later
// appends can land in the caller's backing array.
//
// Adding a field to any struct reachable from Descriptor requires extending
// the matching clone function; TestDescriptorCloneCoversEveryField fails until
// its field-count table is updated.
func (d Descriptor) Clone() Descriptor {
	out := d
	out.Credentials = cloneStrings(d.Credentials)
	out.Params = cloneParamSpecs(d.Params)
	out.Inputs = clonePorts(d.Inputs)
	out.Outputs = clonePorts(d.Outputs)
	out.Capabilities = cloneStrings(d.Capabilities)
	out.Groups = cloneSlice(d.Groups, func(g GroupSpec) GroupSpec { return g })
	out.OneOf = cloneSlice(d.OneOf, func(g OneOfGroup) OneOfGroup {
		g.Params = cloneStrings(g.Params)
		return g
	})
	return out
}

// Clone returns a deep copy of p with the same guarantees as Descriptor.Clone.
func (p ParamSpec) Clone() ParamSpec {
	out := p
	out.Default = cloneParamValue(p.Default)
	out.Enum = cloneEnumOptions(p.Enum)
	out.EnumWhen = cloneSlice(p.EnumWhen, func(e ConditionalEnum) ConditionalEnum {
		return ConditionalEnum{When: e.When.clone(), Enum: cloneEnumOptions(e.Enum)}
	})
	if p.Item != nil {
		item := p.Item.Clone()
		out.Item = &item
	}
	out.Fields = cloneParamSpecs(p.Fields)
	if p.Constraints != nil {
		c := p.Constraints.clone()
		out.Constraints = &c
	}
	out.VisibleWhen = cloneConditionPtr(p.VisibleWhen)
	out.RequiredWhen = cloneConditionPtr(p.RequiredWhen)
	return out
}

func (c Constraints) clone() Constraints {
	out := c
	out.Min = clonePtr(c.Min)
	out.Max = clonePtr(c.Max)
	out.MinLength = clonePtr(c.MinLength)
	out.MaxLength = clonePtr(c.MaxLength)
	out.MinItems = clonePtr(c.MinItems)
	out.MaxItems = clonePtr(c.MaxItems)
	return out
}

func (c Condition) clone() Condition {
	out := c
	out.Eq = cloneParamValue(c.Eq)
	out.In = cloneSlice(c.In, cloneParamValue)
	out.Truthy = clonePtr(c.Truthy)
	out.AllOf = cloneSlice(c.AllOf, Condition.clone)
	out.AnyOf = cloneSlice(c.AnyOf, Condition.clone)
	out.Not = cloneConditionPtr(c.Not)
	return out
}

func cloneConditionPtr(c *Condition) *Condition {
	if c == nil {
		return nil
	}
	out := c.clone()
	return &out
}

func cloneParamSpecs(in []ParamSpec) []ParamSpec { return cloneSlice(in, ParamSpec.Clone) }

func cloneEnumOptions(in []EnumOption) []EnumOption {
	return cloneSlice(in, func(o EnumOption) EnumOption {
		o.Value = cloneParamValue(o.Value)
		return o
	})
}

func cloneStrings(in []string) []string { return cloneSlice(in, func(s string) string { return s }) }

func clonePorts(in []PortSpec) []PortSpec {
	return cloneSlice(in, func(p PortSpec) PortSpec { return p })
}

// cloneSlice copies in element-wise through f, keeping nil as nil and empty
// non-nil as empty non-nil.
func cloneSlice[T any](in []T, f func(T) T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, e := range in {
		out[i] = f(e)
	}
	return out
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneParamValue deep-copies the JSON-shaped containers an any-typed schema
// value can hold. Scalars are immutable and returned as-is. Other types are
// returned as-is too: such values are declared by a node author as literals,
// and the JSON-shaped containers below are the only mutable forms the DSL
// produces. Typed-nil containers are returned unchanged (still typed nil).
func cloneParamValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if t == nil {
			return t
		}
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = cloneParamValue(e)
		}
		return out
	case []any:
		return cloneSlice(t, cloneParamValue)
	case []string:
		return cloneStrings(t)
	case []map[string]any:
		return cloneSlice(t, func(m map[string]any) map[string]any {
			out, _ := cloneParamValue(m).(map[string]any)
			return out
		})
	default:
		return v
	}
}
