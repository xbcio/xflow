package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/xbcio/xflow/types"
)

// ErrInvalidParams is wrapped by every ApplyParams rejection so callers can
// tell a caller-side input error from an internal failure.
var ErrInvalidParams = errors.New("invalid workflow params")

// Params returns a deep copy of the workflow's top-level params schema.
func (g *Graph) Params() map[string]types.ParamDef {
	if g == nil || len(g.params) == 0 {
		return nil
	}
	out := make(map[string]types.ParamDef, len(g.params))
	for name, def := range g.params {
		def.Default = cloneAny(def.Default)
		def.Validation = cloneStringAnyMap(def.Validation)
		out[name] = def
	}
	return out
}

// ApplyParams enforces the workflow's top-level params schema on an
// invocation's params: it injects declared defaults for omitted params,
// rejects a missing required param, rejects a
// value whose type does not match the declared type, and enforces numeric
// validation.min / validation.max. Undeclared params pass through untouched
// and unknown validation keys are ignored.
//
// A graph without a params schema returns params as given, so such workflows
// behave exactly as before. Otherwise the result is a fresh map; the caller's
// map is never mutated.
func (g *Graph) ApplyParams(params map[string]any) (map[string]any, error) {
	if g == nil || len(g.params) == 0 {
		return params, nil
	}
	out := make(map[string]any, len(params)+len(g.params))
	for k, v := range params {
		out[k] = v
	}
	names := make([]string, 0, len(g.params))
	for name := range g.params {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		def := g.params[name]
		value, present := out[name]
		if !present || value == nil {
			if def.Default != nil {
				value = cloneAny(def.Default)
				out[name] = value
			} else if def.Required {
				return nil, fmt.Errorf("param %q is required: %w", name, ErrInvalidParams)
			} else {
				continue
			}
		}
		if err := checkParamValue(name, def, value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func checkParamValue(name string, def types.ParamDef, value any) error {
	ok := true
	switch def.Type {
	case "string":
		_, ok = value.(string)
	case "boolean":
		_, ok = value.(bool)
	case "number":
		_, ok = paramNumber(value)
	case "object":
		_, ok = value.(map[string]any)
	case "array":
		kind := reflect.ValueOf(value).Kind()
		ok = kind == reflect.Slice || kind == reflect.Array
	}
	if !ok {
		return fmt.Errorf("param %q must be of type %s, got %T: %w", name, def.Type, value, ErrInvalidParams)
	}
	n, isNumber := paramNumber(value)
	if def.Type != "number" || !isNumber {
		return nil
	}
	if limit, has := paramNumber(def.Validation["min"]); has && n < limit {
		return fmt.Errorf("param %q must be >= %v: %w", name, def.Validation["min"], ErrInvalidParams)
	}
	if limit, has := paramNumber(def.Validation["max"]); has && n > limit {
		return fmt.Errorf("param %q must be <= %v: %w", name, def.Validation["max"], ErrInvalidParams)
	}
	return nil
}

// paramNumber reports whether v is a JSON-compatible number in any of the
// forms a decoder or Go caller may produce.
func paramNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case nil:
		return 0, false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	}
	return 0, false
}
