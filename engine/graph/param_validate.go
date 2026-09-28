package graph

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	cronlib "github.com/robfig/cron/v3"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// ParamIssue is one finding of ValidateParams. The same shape is logged by the
// SDK, returned by the HTTP control plane as param_issues, and rendered by the
// editor next to the field it names.
//
// Message names the parameter and the rule, never the value: parameter values
// routinely carry credentials.
type ParamIssue struct {
	// Node is the node name; inside a sub-graph body it is "parent/child".
	// ValidateParams leaves it empty -- it only sees one node's params.
	Node string `json:"node"`
	// Path is a JSON Pointer relative to the node, e.g. "/parameters/method",
	// "/parameters/aggregate/dead_letter_topic",
	// "/parameters/rules/0/condition".
	Path string `json:"path"`
	// Code is required | enum | one_of | constraint.<name> | format.<name>.
	Code     string `json:"code"`
	Message  string `json:"message"`
	Severity string `json:"severity"` // ParamIssueError | ParamIssueWarning
}

// ParamIssue severities.
const (
	ParamIssueError   = "error"
	ParamIssueWarning = "warning"
)

// ParamIssue codes. Constraint and format codes are "constraint.<name>" and
// "format.<name>", with names as in the constants below.
const (
	ParamIssueCodeRequired = "required"
	ParamIssueCodeEnum     = "enum"
	ParamIssueCodeOneOf    = "one_of"

	ParamIssueCodeMin         = "constraint.min"
	ParamIssueCodeMax         = "constraint.max"
	ParamIssueCodeMinLength   = "constraint.min_length"
	ParamIssueCodeMaxLength   = "constraint.max_length"
	ParamIssueCodePattern     = "constraint.pattern"
	ParamIssueCodeMinItems    = "constraint.min_items"
	ParamIssueCodeMaxItems    = "constraint.max_items"
	ParamIssueCodeUniqueItems = "constraint.unique_items"

	paramIssueFormatPrefix = "format."
)

// Constraints.Format names the backend enforces. Every other format name
// (url, host-port, code, ...) is an advisory editor hint and is not checked
// here -- the editor must not raise an error-level finding for a format that
// is absent from this list (descriptor-contract design, "Format 的执行范围").
const (
	FormatDuration     = "duration"
	FormatCron         = "cron"
	FormatExpression   = "expression"
	FormatSHA256Digest = "sha256-digest"
	FormatJSON         = "json"
)

// HasParamErrors reports whether any issue has severity error -- the ones
// ParamValidationEnforce rejects.
func HasParamErrors(issues []ParamIssue) bool {
	for _, is := range issues {
		if is.Severity == ParamIssueError {
			return true
		}
	}
	return false
}

// advisoryParamRules lists (node type, path, code) findings that are reported
// with severity warning instead of error, because the Descriptor is stricter
// than the handler that consumes the value -- rejecting them under enforce
// would refuse definitions that run correctly today. Each entry is a known
// Descriptor/handler gap; remove it once the handler (or the Descriptor) is
// changed so both agree.
//
//   - xflow.http method: the handler upper-cases the value and accepts any
//     verb; the Enum lists only GET/POST/PUT/DELETE/PATCH.
//   - xflow.trigger.kafka aggregate.on_overflow / message_schema.on_invalid:
//     the handler matches case-insensitively; the Enum is lower-case.
//   - xflow.wait timeout: the handler ignores a value that does not parse as
//     a duration (cast.ToDurationE error discarded) instead of failing.
//
// A severity downgrade was chosen over a case-insensitive Enum flag because it
// covers all three gaps with one mechanism (http.method accepts values no Enum
// can list) and leaves the Descriptor -- which the editor renders -- unchanged.
var advisoryParamRules = map[string]map[string]map[string]bool{
	"xflow.http": {
		"/parameters/method": {ParamIssueCodeEnum: true},
	},
	"xflow.trigger.kafka": {
		"/parameters/aggregate/on_overflow":     {ParamIssueCodeEnum: true},
		"/parameters/message_schema/on_invalid": {ParamIssueCodeEnum: true},
	},
	"xflow.wait": {
		"/parameters/timeout": {paramIssueFormatPrefix + FormatDuration: true},
	},
}

// ValidateParams checks one node's params against its Descriptor and returns
// every finding, in declaration order. It is pure: params is never modified,
// and no default is written.
//
// Rules (descriptor-contract design §2.2 / §2.4):
//
//   - Required, RequiredWhen: the param must be "set" -- key present, value
//     neither nil nor "".
//   - Enum, EnumWhen: the first EnumWhen whose When holds wins, else Enum.
//     Values compare numerically (a JSON float64 equals an int literal).
//   - Constraints: Min/Max (numbers), MinLength/MaxLength/Pattern (strings,
//     Pattern is RE2 and unanchored unless it anchors itself),
//     MinItems/MaxItems/UniqueItems (arrays). A bound whose value has another
//     type is not applied.
//   - Format: duration (time.ParseDuration), cron (robfig/cron's standard
//     parser, the one xflow.trigger.cron uses), expression (compiles as an
//     expr-lang expression), sha256-digest ("sha256:<64 lowercase hex>"),
//     json (a string value must be valid JSON). Other formats are advisory.
//   - OneOf over top-level params: exactly (the default) | at_most | at_least
//     of the group must be set.
//   - Fields and Item are recursed into for map and array values.
//
// A param whose VisibleWhen is false is hidden: it is exempt from every rule
// above except OneOf, where it still counts (a handler reads any value that is
// present). A value containing a template ({{ }} anywhere, recursively --
// partial templates count) is only checked for being set, since its shape is
// unknown until it is rendered.
//
// There are no type checks: a value whose type differs from ParamSpec.Type is
// not reported (xflow.database's data is declared object but insert_many takes
// an array, and cast-based handlers accept numbers for strings). Bounds and
// formats are applied only to values of the type they are defined for.
func ValidateParams(desc types.Descriptor, params map[string]any) []ParamIssue {
	v := paramValidator{advisory: advisoryParamRules[desc.Type]}
	v.specs(desc.Params, params, "/parameters")
	v.issues = append(v.issues, oneOfIssues(desc, params)...)
	return v.issues
}

// ValidateOneOf applies only the Descriptor.OneOf rules. The SDK re-runs it
// after resolveArtifacts has rewritten Script.File()'s build-time
// __artifact_file_path into artifact_digest, so the final parameter set is
// what the rule is judged on.
func ValidateOneOf(desc types.Descriptor, params map[string]any) []ParamIssue {
	return oneOfIssues(desc, params)
}

type paramValidator struct {
	advisory map[string]map[string]bool
	issues   []ParamIssue
}

func (v *paramValidator) add(path, code, format string, args ...any) {
	severity := ParamIssueError
	if v.advisory[path][code] {
		severity = ParamIssueWarning
	}
	v.issues = append(v.issues, ParamIssue{
		Path:     path,
		Code:     code,
		Message:  fmt.Sprintf(format, args...),
		Severity: severity,
	})
}

// specs validates one level of params: the top level, or the Fields of one
// object value. Conditions are evaluated against values, the siblings.
func (v *paramValidator) specs(specs []types.ParamSpec, values map[string]any, base string) {
	for i := range specs {
		spec := &specs[i]
		if spec.Name == "" {
			continue
		}
		path := base + "/" + escapePointerToken(spec.Name)
		if spec.VisibleWhen != nil && !evalCondition(*spec.VisibleWhen, values) {
			continue
		}
		val, present := values[spec.Name]
		if !isParamSet(val, present) {
			required := spec.Required || (spec.RequiredWhen != nil && evalCondition(*spec.RequiredWhen, values))
			if required {
				v.add(path, ParamIssueCodeRequired, "%s is required", paramLabel(path))
			}
			continue
		}
		if containsTemplate(val) {
			continue
		}
		v.value(spec, val, path, values)
	}
}

// value applies the value-shape rules of spec to one set, template-free value.
func (v *paramValidator) value(spec *types.ParamSpec, val any, path string, siblings map[string]any) {
	v.enum(spec, val, path, siblings)
	v.constraints(spec.Constraints, val, path)
	if len(spec.Fields) > 0 {
		if m, ok := toStringMap(val); ok {
			v.specs(spec.Fields, m, path)
		}
	}
	if spec.Item != nil {
		if items, ok := toAnySlice(val); ok {
			for i, item := range items {
				if !isParamSet(item, true) || containsTemplate(item) {
					continue
				}
				// An array element has no siblings its conditions could name.
				v.value(spec.Item, item, path+"/"+strconv.Itoa(i), nil)
			}
		}
	}
}

func (v *paramValidator) enum(spec *types.ParamSpec, val any, path string, siblings map[string]any) {
	options := spec.Enum
	for _, ce := range spec.EnumWhen {
		if evalCondition(ce.When, siblings) {
			options = ce.Enum
			break
		}
	}
	if len(options) == 0 {
		return
	}
	for _, o := range options {
		if looseEqual(val, o.Value) {
			return
		}
	}
	allowed := make([]string, len(options))
	for i, o := range options {
		allowed[i] = fmt.Sprint(o.Value)
	}
	v.add(path, ParamIssueCodeEnum, "%s must be one of: %s", paramLabel(path), strings.Join(allowed, ", "))
}

func (v *paramValidator) constraints(c *types.Constraints, val any, path string) {
	if c == nil {
		return
	}
	label := paramLabel(path)
	if f, ok := toFloat(val); ok {
		if c.Min != nil && f < *c.Min {
			v.add(path, ParamIssueCodeMin, "%s must be >= %v", label, *c.Min)
		}
		if c.Max != nil && f > *c.Max {
			v.add(path, ParamIssueCodeMax, "%s must be <= %v", label, *c.Max)
		}
	}
	if s, ok := val.(string); ok {
		n := utf8.RuneCountInString(s)
		if c.MinLength != nil && n < *c.MinLength {
			v.add(path, ParamIssueCodeMinLength, "%s must be at least %d characters", label, *c.MinLength)
		}
		if c.MaxLength != nil && n > *c.MaxLength {
			v.add(path, ParamIssueCodeMaxLength, "%s must be at most %d characters", label, *c.MaxLength)
		}
		if c.Pattern != "" {
			if re := compilePattern(c.Pattern); re != nil && !re.MatchString(s) {
				v.add(path, ParamIssueCodePattern, "%s must match pattern %s", label, c.Pattern)
			}
		}
	}
	if items, ok := toAnySlice(val); ok {
		if c.MinItems != nil && len(items) < *c.MinItems {
			v.add(path, ParamIssueCodeMinItems, "%s must have at least %d items", label, *c.MinItems)
		}
		if c.MaxItems != nil && len(items) > *c.MaxItems {
			v.add(path, ParamIssueCodeMaxItems, "%s must have at most %d items", label, *c.MaxItems)
		}
		if c.UniqueItems && !uniqueItems(items) {
			v.add(path, ParamIssueCodeUniqueItems, "%s must not contain duplicate items", label)
		}
	}
	if c.Format != "" {
		if reason := checkFormat(c.Format, val); reason != "" {
			v.add(path, paramIssueFormatPrefix+c.Format, "%s is not a valid %s: %s", label, c.Format, reason)
		}
	}
}

var sha256DigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// checkFormat returns why val violates an enforced format, or "" when it
// conforms, the format is advisory, or val is not a string (json excepted: a
// structured value is JSON by construction). The reason names the rule, never
// the value.
func checkFormat(format string, val any) string {
	s, isString := val.(string)
	switch format {
	case FormatDuration:
		if isString {
			if _, err := time.ParseDuration(s); err != nil {
				return `want a Go duration such as "30s" or "1h30m"`
			}
		}
	case FormatCron:
		if isString {
			if _, err := cronlib.ParseStandard(s); err != nil {
				return `want five cron fields or a descriptor such as "@every 1m"`
			}
		}
	case FormatExpression:
		if isString {
			if err := exprx.CheckExprSyntax(s); err != nil {
				return "expression does not compile"
			}
		}
	case FormatSHA256Digest:
		if isString && !sha256DigestPattern.MatchString(s) {
			return `want "sha256:" followed by 64 lowercase hex digits`
		}
	case FormatJSON:
		if isString && !json.Valid([]byte(s)) {
			return "not valid JSON"
		}
	}
	return ""
}

// oneOfIssues applies Descriptor.OneOf. Every param counts, hidden or not, and
// a template counts as set.
func oneOfIssues(desc types.Descriptor, params map[string]any) []ParamIssue {
	var out []ParamIssue
	for _, g := range desc.OneOf {
		if len(g.Params) == 0 {
			continue
		}
		n := 0
		for _, name := range g.Params {
			val, present := params[name]
			if isParamSet(val, present) {
				n++
			}
		}
		mode := g.Mode
		if mode == "" {
			mode = types.OneOfExactly
		}
		var want string
		switch mode {
		case types.OneOfExactly:
			if n == 1 {
				continue
			}
			want = "exactly one"
		case types.OneOfAtMost:
			if n <= 1 {
				continue
			}
			want = "at most one"
		case types.OneOfAtLeast:
			if n >= 1 {
				continue
			}
			want = "at least one"
		default:
			continue // an unknown mode is a descriptor bug, not the author's
		}
		out = append(out, ParamIssue{
			Path:     "/parameters/" + escapePointerToken(g.Params[0]),
			Code:     ParamIssueCodeOneOf,
			Message:  fmt.Sprintf("%s of %s must be set, %d are", want, strings.Join(g.Params, ", "), n),
			Severity: ParamIssueError,
		})
	}
	return out
}

// evalCondition evaluates a ParamSpec condition against sibling values. The
// semantics are shared with the editor's evaluator and pinned by the
// cross-language truth table web/packages/composer/src/core/testdata/
// cond-truth-table.json (see param_condition_truth_table_test.go):
//
//   - a missing param compares as null;
//   - Eq/In compare numerically (float64 == int), deeply for arrays/objects;
//   - Truthy is JavaScript truthiness;
//   - all non-zero clauses must hold, a Condition with none holds; a non-nil
//     empty AnyOf holds for nothing (JS [].some), an empty AllOf always holds.
func evalCondition(c types.Condition, values map[string]any) bool {
	if c.Eq != nil || c.In != nil || c.Truthy != nil {
		val := normalizeNil(values[c.Param])
		if c.Eq != nil && !looseEqual(val, c.Eq) {
			return false
		}
		if c.In != nil {
			matched := false
			for _, candidate := range c.In {
				if looseEqual(val, candidate) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		if c.Truthy != nil && jsTruthy(val) != *c.Truthy {
			return false
		}
	}
	for _, sub := range c.AllOf {
		if !evalCondition(sub, values) {
			return false
		}
	}
	if c.AnyOf != nil {
		matched := false
		for _, sub := range c.AnyOf {
			if evalCondition(sub, values) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if c.Not != nil && evalCondition(*c.Not, values) {
		return false
	}
	return true
}

// isParamSet is the shared definition of "set": the key is present and the
// value is neither nil (including a typed nil map/slice) nor "".
func isParamSet(val any, present bool) bool {
	if !present {
		return false
	}
	val = normalizeNil(val)
	if val == nil {
		return false
	}
	if s, ok := val.(string); ok && s == "" {
		return false
	}
	return true
}

// containsTemplate reports whether any string leaf of val holds a {{ }}
// segment ("${{ }}" included). Partial templates count: "{{ $config.x }}/api"
// is a template. The segment scan is templateSegments, the same one
// activation-time rendering uses to find a template's expressions.
func containsTemplate(val any) bool {
	switch x := val.(type) {
	case string:
		return len(templateSegments(x)) > 0
	case map[string]any:
		for _, child := range x {
			if containsTemplate(child) {
				return true
			}
		}
		return false
	case []any:
		for _, child := range x {
			if containsTemplate(child) {
				return true
			}
		}
		return false
	}
	if m, ok := toStringMap(val); ok {
		return containsTemplate(m)
	}
	if s, ok := toAnySlice(val); ok {
		return containsTemplate(s)
	}
	return false
}

// jsTruthy is JavaScript truthiness: "", 0, NaN, false, and null are false;
// everything else -- including [], {}, and "0" -- is true.
func jsTruthy(val any) bool {
	val = normalizeNil(val)
	if val == nil {
		return false
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if f, ok := toFloat(val); ok {
		return f != 0 && !math.IsNaN(f)
	}
	if s, ok := val.(string); ok {
		return s != ""
	}
	return true
}

// looseEqual is JSON-value equality with numeric normalization: numbers of
// any Go type compare by value, arrays element-wise, objects key-wise.
// Numbers never equal strings or booleans.
func looseEqual(a, b any) bool {
	a, b = normalizeNil(a), normalizeNil(b)
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	fa, aNum := toFloat(a)
	fb, bNum := toFloat(b)
	if aNum || bNum {
		return aNum && bNum && fa == fb
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	}
	if xs, ok := toAnySlice(a); ok {
		ys, ok := toAnySlice(b)
		if !ok || len(xs) != len(ys) {
			return false
		}
		for i := range xs {
			if !looseEqual(xs[i], ys[i]) {
				return false
			}
		}
		return true
	}
	if xm, ok := toStringMap(a); ok {
		ym, ok := toStringMap(b)
		if !ok || len(xm) != len(ym) {
			return false
		}
		for k, xv := range xm {
			yv, present := ym[k]
			if !present || !looseEqual(xv, yv) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

func uniqueItems(items []any) bool {
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			if looseEqual(items[i], items[j]) {
				return false
			}
		}
	}
	return true
}

// normalizeNil maps a typed nil (nil map, slice, pointer, ...) to untyped nil,
// which is what it encodes to in JSON.
func normalizeNil(val any) any {
	if val == nil {
		return nil
	}
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface, reflect.Func, reflect.Chan:
		if rv.IsNil() {
			return nil
		}
	}
	return val
}

// toFloat converts any Go numeric type (and json.Number) to float64.
func toFloat(val any) (float64, bool) {
	switch x := val.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	}
	return 0, false
}

// toAnySlice views any slice or array value as []any. SDK-built params are not
// JSON-normalized, so a []string or []SwitchRule must be seen as an array too.
func toAnySlice(val any) ([]any, bool) {
	if s, ok := val.([]any); ok {
		return s, true
	}
	if val == nil {
		return nil, false
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// toStringMap views any string-keyed map as map[string]any.
func toStringMap(val any) (map[string]any, bool) {
	if m, ok := val.(map[string]any); ok {
		return m, true
	}
	if val == nil {
		return nil, false
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, true
}

var patternCache sync.Map // pattern string -> *regexp.Regexp (nil when invalid)

// compilePattern compiles a Constraints.Pattern once. An invalid pattern is a
// descriptor bug; it yields nil and the rule is not applied.
func compilePattern(pattern string) *regexp.Regexp {
	if cached, ok := patternCache.Load(pattern); ok {
		re, _ := cached.(*regexp.Regexp)
		return re
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		re = nil
	}
	patternCache.Store(pattern, re)
	return re
}

// escapePointerToken escapes one JSON Pointer reference token (RFC 6901).
func escapePointerToken(s string) string {
	if !strings.ContainsAny(s, "~/") {
		return s
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

// paramLabel renders a pointer path without its "/parameters/" prefix, for
// messages: "aggregate/dead_letter_topic", "rules/0/condition".
func paramLabel(path string) string {
	return strings.TrimPrefix(path, "/parameters/")
}
