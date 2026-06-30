// Package js implements the JavaScript language family (goja + qjs runtimes)
// for the xflow.script node. Both runtimes share global injection and result
// mapping so their $helpers exposure and credential model stay identical.
package js

// BuildGlobals returns the globals map to inject. The node layer already
// assembled $input/$credentials/etc; this hook exists so both runtimes consume
// the exact same source and stay consistent.
func BuildGlobals(globals map[string]any) map[string]any {
	if globals == nil {
		return map[string]any{}
	}
	return globals
}
