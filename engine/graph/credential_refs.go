package graph

import (
	"fmt"
	"strings"

	"github.com/xbcio/xflow/types"
)

// credentialRefParams lists, per builtin node type, the parameter that names
// a credential from WorkflowDef.Credentials. engine/graph cannot import node/,
// so the table mirrors the builtin specs: xflow.database "credential",
// xflow.http "authentication", xflow.script "credentials" (a list).
var credentialRefParams = map[string]string{
	"xflow.database": "credential",
	"xflow.http":     "authentication",
	"xflow.script":   "credentials",
}

// validateCredentialRefs rejects a literal credential name that the workflow
// does not declare under credentials (DSL-SPECIFICATION.md: an undeclared
// credential name is a compile error). Expression values (${{ }}) and
// non-string values are left to the runtime checks.
func validateCredentialRefs(def *types.WorkflowDef) error {
	for _, nd := range def.Nodes {
		param, ok := credentialRefParams[nd.Type]
		if !ok {
			continue
		}
		var names []string
		switch v := nd.Parameters[param].(type) {
		case string:
			names = []string{v}
		case []string:
			names = v
		case []any:
			for _, e := range v {
				if s, ok := e.(string); ok {
					names = append(names, s)
				}
			}
		}
		for _, name := range names {
			if name == "" || strings.Contains(name, "${{") {
				continue
			}
			if _, declared := def.Credentials[name]; !declared {
				return fmt.Errorf("node %q: credential %q (parameter %q) is not declared in workflow credentials", nd.Name, name, param)
			}
		}
	}
	return nil
}
