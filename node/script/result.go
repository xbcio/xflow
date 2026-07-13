package script

// MapResult normalizes an engine completion value into Output.Data per §5.3:
// object -> passthrough; scalar -> {"result": v}; nil -> {}.
func MapResult(v any) map[string]any {
	switch t := v.(type) {
	case nil:
		return map[string]any{}
	case map[string]any:
		return t
	default:
		return map[string]any{"result": v}
	}
}
