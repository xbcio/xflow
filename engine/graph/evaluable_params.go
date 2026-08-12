package graph

// evaluableParams lists, per node type, the parameters whose handler evaluates
// the value itself -- either as an expression (xflow.if's condition, xflow.map's
// expression) or as program text whose own syntax may contain braces
// (xflow.script's code, which never reaches exprx at all: script.go hands it to
// a script engine verbatim).
//
// Its sole consumer is execution/params.go's boundary evaluation layer, which
// derives its exemption set by NEGATING this table: a parameter marked true here
// is one the handler already evaluates, so the boundary must not evaluate it
// again (double-evaluation turns a condition string into a boolean, then
// cast.ToString makes it "true" -- a valid expr that is always truthy, so a
// switch permanently takes its first rule with zero diagnostics).
//
// The registry-coverage test (registry_coverage_test.go) asserts every
// registered node type has an entry, so a new type that forgets to register
// gets caught at test time rather than causing silent double-evaluation.
//
// It is a literal table, not a lookup into node/registry, for the same reason
// transformNodeTypes and bannedBodyMemberTypes are literal: this package must
// not depend on which handlers happen to be linked into the current binary.
// That dependency is real and measured -- the server process sees a POPULATED
// registry today, but only incidentally, because service/control imports
// package node to install an observer. Read from engine/graph itself the same
// registry is empty. A compile-time rule whose verdict changes with the
// binary's import graph is worse than no rule.
//
// The key is (nodeType, paramName), never paramName alone: xflow.trigger.cron
// has a parameter called "expression" that holds a cron spec ("0 */5 * * *"),
// not an expr. A name-only table silently reclassifies it as evaluable.
//
// Task 1's exemption table is this data negated.
var evaluableParams = map[string]map[string]bool{
	"xflow.if": {"condition": true},
	// NOT "rules": the handler evaluates rules[].condition (switch.go:119) but
	// reads rules[].output with cast.ToString and uses it as a port name
	// (switch.go:114). Exempting the whole subtree would let a template in
	// "output" through: not rejected here, not evaluated there, the literal
	// becomes the port name and the switch silently takes its default branch.
	// The condition sub-field is exempted by evaluableSubFields below.
	"xflow.switch": {"expression": true},
	"xflow.map":    {"items": true, "expression": true},
	"xflow.split":  {"items": true},
	// xflow.function's and xflow.script's "code" is the program itself, not a
	// template around one -- function.go:120 evaluates it as an expr, and
	// script.go hands it to a script engine verbatim. Either way "{{" inside it
	// is the program's own syntax. Task 1's exemption table is this data negated.
	"xflow.function":                    {"code": true},
	"xflow.script":                      {"code": true},
	"xflow.transform.set":               {"expressions": true},
	"xflow.transform.filter":            {"items": true, "condition": true},
	"xflow.transform.sort":              {"items": true},
	"xflow.transform.limit":             {"items": true},
	"xflow.transform.aggregate":         {"items": true},
	"xflow.transform.remove_duplicates": {"items": true},
	// The following types evaluate NO parameter themselves -- their handlers
	// use every value verbatim (xflow.http ships it as a header/body,
	// xflow.start ignores params entirely, triggers use them as configuration
	// specs). An empty entry is therefore the strongest one: it tells the
	// boundary to evaluate every parameter of that type, which is what makes a
	// template in xflow.http headers work. The entry must exist all the same --
	// a MISSING type yields an empty exemption set too, but by accident, and
	// the registry-coverage test exists to keep the two apart.
	"xflow.http":              {},
	"xflow.start":             {},
	"xflow.end":               {},
	"xflow.merge":             {},
	"xflow.trigger.cron":      {},
	"xflow.trigger.timer":     {},
	"xflow.trigger.webhook":   {},
	"xflow.trigger.kafka":     {},
	"xflow.trigger.redis_hub": {},
	// Action and group nodes whose handlers never call exprx: database uses
	// params as column/table names, grpc uses them as host/service/method
	// literals, notification sends to/subject/message verbatim, approval and
	// wait use approvers/signal_name as config, supply nodes use resource/content
	// as identifiers, and pick/rename use field lists as mapping keys.
	"xflow.database":         {},
	"xflow.grpc":             {},
	"xflow.notification":     {},
	"xflow.approval":         {},
	"xflow.wait":             {},
	"xflow.supply.external":  {},
	"xflow.supply.static":    {},
	"xflow.transform.pick":   {},
	"xflow.transform.rename": {},
}

// hostSourceParams lists parameters holding source code in a language OTHER than
// expr. The §4.1 "${{ }} must wrap the whole value" form rule does not apply to
// them, because "${{" there is the host language's own syntax, not a template:
// inside a JS template literal `${{a:1}.a}` interpolates an object literal, which
// trips both the text-before-and-after clause and the second-"{{" clause.
//
// This is a strict subset of evaluableParams -- every entry here must also be
// exempt there, which is what makes skipping the compile-time check safe:
// execution/params.go skips exempt parameters entirely, so RenderTemplate never
// sees these values and its rule-1 precondition is not weakened. The
// TestHostSourceParamsAreExemptAtTheBoundary guard pins that subset relation.
//
// The key is (nodeType, paramName), never paramName alone: xflow.function also
// has a parameter called "code", but function.go hands it to EvalExpr
// (executeExpr), so it IS an expr and a malformed template in it is a real error
// the author can fix. Only xflow.script's code is host source -- script.go
// passes it to a script engine verbatim (base64-decoding first for wasm).
var hostSourceParams = map[string]map[string]bool{
	"xflow.script": {"code": true},
}

// evaluableSubFields exempts a path INSIDE a parameter for node types whose
// handler evaluates only part of a structured parameter. Only xflow.switch
// needs it today; the shape generalizes because "the whole parameter is
// evaluable" is the wrong granularity whenever a parameter is an object.
var evaluableSubFields = map[string]map[string][]string{
	// rules is []any of map[string]any; only "condition" of each element is
	// evaluated. "output" and any other key are literal.
	"xflow.switch": {"rules": {"condition"}},
}

// EvaluableParams reports, per node type, which parameters that type's
// handler evaluates. The returned map must be treated as read-only.
//
// Exported for two consumers that must never keep a second copy of this
// data: the boundary evaluation layer, which derives its exemption set by
// negating this table, and the registry-coverage test, which asserts every
// registered node type has an entry here.
//
// The map is returned directly (no deep copy) because both consumers are
// read-only by contract and this is called on hot paths (graph compilation).
// Mutating the returned map is a programming error.
func EvaluableParams() map[string]map[string]bool { return evaluableParams }

// EvaluableSubFields reports, per node type and parameter, which sub-field
// paths inside that parameter the handler evaluates itself. The boundary
// evaluation layer must preserve these sub-fields verbatim (not evaluate them)
// while still evaluating sibling fields in the same parameter.
//
// Exported for the same reason as EvaluableParams: the boundary layer derives
// its sub-field exemption set from this table. There must be no second copy.
// The returned map is read-only by contract.
func EvaluableSubFields() map[string]map[string][]string { return evaluableSubFields }
