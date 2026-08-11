package transform

import (
	"fmt"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
	"github.com/spf13/cast"
)

func cloneData(input *types.Input) map[string]any {
	if input == nil || input.Data == nil {
		return map[string]any{}
	}
	data := make(map[string]any, len(input.Data))
	for key, value := range input.Data {
		data[key] = value
	}
	return data
}

func itemsFromInput(input *types.Input) ([]any, string, error) {
	if input == nil {
		return nil, "", fmt.Errorf("input is nil")
	}
	itemsExpr := cast.ToString(input.Params["items"])
	if itemsExpr == "" {
		return nil, "", fmt.Errorf("items parameter is required")
	}
	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(itemsExpr, env, false)
	if err != nil {
		return nil, "", err
	}
	items, ok := result.([]any)
	if ok {
		return items, itemsExpr, nil
	}
	return cast.ToSlice(result), itemsExpr, nil
}

func itemFieldValue(item any, field string) any {
	switch typed := item.(type) {
	case map[string]any:
		return typed[field]
	case map[string]string:
		return typed[field]
	default:
		return nil
	}
}

func parseStringMap(value any) (map[string]string, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case map[string]string:
		return typed, nil
	case map[string]any:
		result := make(map[string]string, len(typed))
		for key, value := range typed {
			result[key] = cast.ToString(value)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected object, got %T", value)
	}
}
