package exprx

import (
	"fmt"

	"github.com/expr-lang/expr"
)

// exprFunctions holds the functions registered into every compiled program.
//
// They are registered as expr.Function compile options rather than injected
// into the env map, and the difference is load-bearing. CompileExpr caches
// programs by (code, asBool) alone; a function passed as a compile option is
// baked into the resulting *vm.Program and travels with the cache entry. An
// env-injected function would instead have to be present in every map handed
// to expr.Run, and BuildExprEnv's callers construct a dozen different maps --
// one omission and a cached program compiled with the function would fail at
// Run time against an env that lacks it.
//
// Scope: this list is deliberately short. expr-lang already ships ~140
// builtins, and DSL-SPECIFICATION.md §4.3 documents those. An entry belongs
// here only when the spec advertises it, no builtin covers it, and a workflow
// actually uses it. Functions must stay deterministic and I/O-free -- an
// expression is evaluated at the parameter boundary on every task attempt, so
// a function that reads the clock, the filesystem, or the network would make
// a retried task observe a different parameter value than its first attempt.
//
// Shadowing caveat (pre-existing, not introduced here): BuildExprEnv spreads
// input.Data into the env's top level, so a payload field named after a
// function shadows it -- `sprintf(...)` against data carrying a "sprintf" key
// fails to compile with "string is not callable". Measured to behave
// identically for expr's own builtins (a "len" field shadows len the same
// way), so this is a property of the data spread, not of registration. It
// also interacts with the program cache: which env compiled first decides
// whether the cached program calls the function or indexes the data. Naming a
// payload field after a function is the trigger in every case.
var exprFunctions = []expr.Option{
	// sprintf formats its remaining arguments per a Go format string.
	//
	// expr's builtin string conversions cover concatenation but not width,
	// precision, or padding: "%.2f" on a money amount has no builtin
	// equivalent, which is what docs/dsl-samples/purchase-approval.yaml
	// needs.
	expr.Function("sprintf", func(params ...any) (any, error) {
		if len(params) == 0 {
			return nil, fmt.Errorf("sprintf requires a format string")
		}
		format, ok := params[0].(string)
		if !ok {
			// The error names the ARGUMENT'S TYPE, never its value: a format
			// argument can be a credential expanded from $credentials, and §7
			// puts credentials on an absolute no-log blacklist.
			return nil, fmt.Errorf("sprintf: format argument must be a string, got %T", params[0])
		}
		return fmt.Sprintf(format, params[1:]...), nil
	}),
}
