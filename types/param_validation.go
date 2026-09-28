package types

import "fmt"

// ParamValidationMode selects what a registration path does with the issues
// the ParamSpec validator (engine/graph.ValidateParams) reports. It is shared
// by the SDK (xflow.WithParamValidation) and the HTTP control plane
// (apiserver.Config.ParamValidation), which switch from warn to enforce
// independently.
type ParamValidationMode string

const (
	// ParamValidationOff skips ParamSpec validation entirely. The SDK's
	// long-standing Required check still runs: it predates this switch and
	// is not governed by it.
	ParamValidationOff ParamValidationMode = "off"
	// ParamValidationWarn reports issues (logs, and the HTTP param_issues
	// response field) without rejecting anything. The default.
	ParamValidationWarn ParamValidationMode = "warn"
	// ParamValidationEnforce rejects a definition carrying any issue of
	// severity "error". Issues of severity "warning" are still only reported.
	ParamValidationEnforce ParamValidationMode = "enforce"
)

// DefaultParamValidationMode is the mode a path uses when none is configured.
const DefaultParamValidationMode = ParamValidationWarn

// Valid reports whether m is one of the three defined modes.
func (m ParamValidationMode) Valid() bool {
	switch m {
	case ParamValidationOff, ParamValidationWarn, ParamValidationEnforce:
		return true
	}
	return false
}

// OrDefault returns m, or DefaultParamValidationMode when m is empty.
func (m ParamValidationMode) OrDefault() ParamValidationMode {
	if m == "" {
		return DefaultParamValidationMode
	}
	return m
}

// ParseParamValidationMode parses "off", "warn", or "enforce". The empty
// string parses as DefaultParamValidationMode.
func ParseParamValidationMode(s string) (ParamValidationMode, error) {
	m := ParamValidationMode(s).OrDefault()
	if !m.Valid() {
		return "", fmt.Errorf("invalid param validation mode %q: want off, warn, or enforce", s)
	}
	return m, nil
}
