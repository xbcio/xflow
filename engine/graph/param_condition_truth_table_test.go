package graph

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// condTruthTablePath is the cross-language condition truth table shared with
// the editor's evaluator (web/packages/composer). Go and TypeScript must agree
// on every case both can express.
var condTruthTablePath = filepath.Join("..", "..", "web", "packages", "composer", "src", "core", "testdata", "cond-truth-table.json")

type condTruthCase struct {
	Name     string          `json:"name"`
	Cond     json.RawMessage `json:"cond"`
	Value    map[string]any  `json:"value"`
	Expected bool            `json:"expected"`
	Error    bool            `json:"error"`
}

// errNotExpressible marks a TypeScript condition that types.Condition cannot
// represent: comparison operators, nested/array-index paths, and ill-typed
// operands (which are evaluation errors in TS and simply unrepresentable in
// Go, whose Condition is typed).
type errNotExpressible struct{ why string }

func (e errNotExpressible) Error() string { return e.why }

// tsCondToGo converts the TS cond syntax ({$state, eq|neq|in|truthy},
// $and/$or/$not, arrays as implicit AND, boolean literals) into the subset of
// types.Condition that expresses the same predicate.
func tsCondToGo(raw any) (types.Condition, error) {
	switch c := raw.(type) {
	case bool:
		if c {
			return types.Condition{}, nil // no clauses: holds
		}
		return types.Condition{Not: &types.Condition{}}, nil // never holds
	case []any:
		all := make([]types.Condition, 0, len(c))
		for _, sub := range c {
			g, err := tsCondToGo(sub)
			if err != nil {
				return types.Condition{}, err
			}
			all = append(all, g)
		}
		return types.Condition{AllOf: all}, nil
	case map[string]any:
		if subs, ok := c["$and"]; ok {
			list, ok := subs.([]any)
			if !ok {
				return types.Condition{}, errNotExpressible{"$and operand is not an array"}
			}
			g, err := tsCondToGo(list)
			if err != nil {
				return types.Condition{}, err
			}
			if g.AllOf == nil {
				g.AllOf = []types.Condition{}
			}
			return g, nil
		}
		if subs, ok := c["$or"]; ok {
			list, ok := subs.([]any)
			if !ok {
				return types.Condition{}, errNotExpressible{"$or operand is not an array"}
			}
			anyOf := make([]types.Condition, 0, len(list)) // non-nil even when empty
			for _, sub := range list {
				g, err := tsCondToGo(sub)
				if err != nil {
					return types.Condition{}, err
				}
				anyOf = append(anyOf, g)
			}
			return types.Condition{AnyOf: anyOf}, nil
		}
		if sub, ok := c["$not"]; ok {
			g, err := tsCondToGo(sub)
			if err != nil {
				return types.Condition{}, err
			}
			return types.Condition{Not: &g}, nil
		}
		ptr, ok := c["$state"].(string)
		if !ok {
			return types.Condition{}, errNotExpressible{"no $state"}
		}
		param, err := singleTokenPointer(ptr)
		if err != nil {
			return types.Condition{}, err
		}
		for _, op := range []string{"gt", "gte", "lt", "lte"} {
			if _, has := c[op]; has {
				return types.Condition{}, errNotExpressible{"comparison operator " + op}
			}
		}
		if v, has := c["eq"]; has {
			return eqCondition(param, v), nil
		}
		if v, has := c["neq"]; has {
			eq := eqCondition(param, v)
			return types.Condition{Not: &eq}, nil
		}
		if v, has := c["in"]; has {
			list, ok := v.([]any)
			if !ok {
				return types.Condition{}, errNotExpressible{"in operand is not an array"}
			}
			if list == nil {
				list = []any{}
			}
			return types.Condition{Param: param, In: list}, nil
		}
		if v, has := c["truthy"]; has {
			b, ok := v.(bool)
			if !ok {
				return types.Condition{}, errNotExpressible{"truthy operand is not a boolean"}
			}
			return types.Condition{Param: param, Truthy: &b}, nil
		}
		return types.Condition{}, errNotExpressible{"no operator"}
	}
	return types.Condition{}, errNotExpressible{"unsupported cond shape"}
}

// eqCondition expresses "param eq v". Condition.Eq == nil means "no Eq
// clause", so eq null is spelled In: [null] -- the same predicate.
func eqCondition(param string, v any) types.Condition {
	if v == nil {
		return types.Condition{Param: param, In: []any{nil}}
	}
	return types.Condition{Param: param, Eq: v}
}

// singleTokenPointer maps "/name" to name (RFC 6901 unescaped). Condition.Param
// names a sibling, so multi-token paths are not expressible.
func singleTokenPointer(ptr string) (string, error) {
	if !strings.HasPrefix(ptr, "/") {
		return "", errNotExpressible{"relative pointer"}
	}
	tok := ptr[1:]
	if strings.Contains(tok, "/") {
		return "", errNotExpressible{"nested path " + ptr}
	}
	return strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~"), nil
}

func TestConditionMatchesSharedTruthTable(t *testing.T) {
	data, err := os.ReadFile(condTruthTablePath)
	if err != nil {
		t.Fatalf("read shared truth table: %v", err)
	}
	var table struct {
		Cases []condTruthCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatalf("decode truth table: %v", err)
	}
	if len(table.Cases) == 0 {
		t.Fatal("truth table has no cases")
	}

	var skipped []string
	checked := 0
	for _, tc := range table.Cases {
		if tc.Error {
			// Evaluation errors come from ill-typed operands and comparison
			// operators, neither of which a types.Condition can carry.
			skipped = append(skipped, tc.Name+" (evaluation error case)")
			continue
		}
		var raw any
		if err := json.Unmarshal(tc.Cond, &raw); err != nil {
			t.Fatalf("%s: decode cond: %v", tc.Name, err)
		}
		cond, err := tsCondToGo(raw)
		if err != nil {
			skipped = append(skipped, tc.Name+" ("+err.Error()+")")
			continue
		}
		checked++
		if got := evalCondition(cond, tc.Value); got != tc.Expected {
			t.Errorf("%s: evalCondition = %v, want %v (cond %s, value %v)", tc.Name, got, tc.Expected, tc.Cond, tc.Value)
		}
	}
	t.Logf("truth table: %d cases checked, %d skipped as not expressible in types.Condition", checked, len(skipped))
	for _, s := range skipped {
		t.Logf("  skipped: %s", s)
	}
	// Guard against the converter silently skipping everything.
	if checked < len(table.Cases)/2 {
		t.Fatalf("only %d of %d truth-table cases were expressible; converter regressed?", checked, len(table.Cases))
	}
}
