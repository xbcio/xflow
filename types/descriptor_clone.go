package types

// Clone returns a deep copy of d. Every slice is copied, and ParamSpec.Default
// is deep-copied through cloneParamValue, so a caller that mutates the result
// (or keeps it while the source keeps growing) cannot reach the source.
//
// Descriptor() implementations that hand out a descriptor they also retain --
// node.Definition and node.TriggerDefinition -- must return a Clone. A caller
// holding the returned value would otherwise alias the retained slices in both
// directions: its writes reach the definition, and the definition's later
// appends can land in the caller's backing array.
//
// Adding a field to Descriptor or ParamSpec requires extending this method;
// TestDescriptorCloneCoversEveryField fails until it is.
func (d Descriptor) Clone() Descriptor {
	out := d
	out.Credentials = cloneStrings(d.Credentials)
	out.Inputs = clonePorts(d.Inputs)
	out.Outputs = clonePorts(d.Outputs)
	out.Capabilities = cloneStrings(d.Capabilities)
	if d.Params != nil {
		out.Params = make([]ParamSpec, len(d.Params))
		for i, p := range d.Params {
			out.Params[i] = p.clone()
		}
	}
	return out
}

func (p ParamSpec) clone() ParamSpec {
	out := p
	out.Default = cloneParamValue(p.Default)
	return out
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	return append(make([]string, 0, len(in)), in...)
}

func clonePorts(in []PortSpec) []PortSpec {
	if in == nil {
		return nil
	}
	return append(make([]PortSpec, 0, len(in)), in...)
}

// cloneParamValue deep-copies the JSON-shaped containers a parameter default
// can hold. Scalars are immutable and returned as-is. Other types are returned
// as-is too: a default is declared by a node author as a literal, and the
// JSON-shaped containers below are the only mutable forms the DSL produces.
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
		if t == nil {
			return t
		}
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneParamValue(e)
		}
		return out
	case []string:
		return cloneStrings(t)
	case []map[string]any:
		if t == nil {
			return t
		}
		out := make([]map[string]any, len(t))
		for i, e := range t {
			out[i], _ = cloneParamValue(e).(map[string]any)
		}
		return out
	default:
		return v
	}
}
