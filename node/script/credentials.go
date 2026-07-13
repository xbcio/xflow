package script

import (
	"fmt"
)

// CredentialResolver resolves a credential value by name. It mirrors
// types.Input.Credential but returns an error so resolution failures can be
// surfaced as config errors. The node layer adapts Input.Credential to this.
type CredentialResolver func(name string) (map[string]any, error)

// ResolveCredentials resolves each declared name into the $credentials map and
// returns the declaration-order first value as the $credential alias.
//
// Security gate: only names in `declared` are resolved and exposed. The error
// path never echoes credential values, only the failing name.
func ResolveCredentials(declared []string, resolver CredentialResolver) (creds map[string]any, first any, err error) {
	creds = make(map[string]any, len(declared))
	for i, name := range declared {
		val, rerr := resolver(name)
		if rerr != nil {
			return nil, nil, fmt.Errorf("script: resolve credential %q: %w", name, rerr)
		}
		if val == nil {
			return nil, nil, fmt.Errorf("script: credential %q not found", name)
		}
		creds[name] = val
		if i == 0 {
			first = val
		}
	}
	return creds, first, nil
}
