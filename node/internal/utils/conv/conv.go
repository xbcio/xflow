// Package conv provides the parameter conversion helpers shared by builtin
// nodes. These helpers are not part of the public node API.
package conv

import (
	"fmt"
	"time"

	"github.com/spf13/cast"
)

// PositiveInt converts v to a positive int, returning fallback when the value
// is missing, non-numeric, or non-positive.
func PositiveInt(v any, fallback int) int {
	n, err := cast.ToIntE(v)
	if err == nil && n > 0 {
		return n
	}
	return fallback
}

// NonEmptyStringSlice converts v to []string, dropping empty entries. Returns
// nil on failure.
func NonEmptyStringSlice(v any) []string {
	values, err := cast.ToStringSliceE(v)
	if err != nil {
		return nil
	}
	out := values[:0]
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

// PositiveDuration converts v to a strictly positive time.Duration, returning
// an error when v is missing, unparseable, or non-positive.
func PositiveDuration(v any) (time.Duration, error) {
	if v == nil || cast.ToString(v) == "" {
		return 0, fmt.Errorf("duration is required")
	}
	d, err := cast.ToDurationE(v)
	if err != nil {
		return 0, fmt.Errorf("duration is required")
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return d, nil
}

// ToSlice coerces v into []any, copying common slice element types
// ([]string, []int, []float64, []map[string]any). Returns an error for
// unsupported types.
func ToSlice(v any) ([]any, error) {
	switch items := v.(type) {
	case []any:
		return items, nil
	case []string:
		result := make([]any, len(items))
		for i, s := range items {
			result[i] = s
		}
		return result, nil
	case []int:
		result := make([]any, len(items))
		for i, n := range items {
			result[i] = n
		}
		return result, nil
	case []float64:
		result := make([]any, len(items))
		for i, f := range items {
			result[i] = f
		}
		return result, nil
	case []map[string]any:
		result := make([]any, len(items))
		for i, m := range items {
			result[i] = m
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}
