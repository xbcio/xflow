package types

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// portConnectionsAlias avoids infinite recursion when falling back to the
// default struct encoding.
type portConnectionsAlias PortConnections

// UnmarshalJSON accepts both the array shorthand and the object form.
//
// The branch is taken on the JSON shape ('[' vs '{') before any semantics are
// read. That ordering matters: a compatibility decision that depends on a field
// value would give that value two meanings, and a later parse error could no
// longer be distinguished from legacy input.
func (p *PortConnections) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return fmt.Errorf("port connections: empty value")
	}

	if trimmed[0] == '[' {
		var targets []Connection
		if err := json.Unmarshal(data, &targets); err != nil {
			return fmt.Errorf("port connections (array form): %w", err)
		}
		p.Type = ConnectionTypeData
		p.Targets = targets
		return nil
	}

	var alias portConnectionsAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return fmt.Errorf("port connections (object form): %w", err)
	}
	*p = PortConnections(alias)
	if p.Type == "" {
		p.Type = ConnectionTypeData
	}
	return nil
}

// MarshalJSON emits the array shorthand for data ports so that pre-existing
// definitions keep a byte-identical runtime hash.
//
// The receiver MUST stay a value receiver. Connections is
// map[string]map[string]PortConnections and map values are not addressable, so
// encoding/json does not reach a pointer method set: a pointer receiver here is
// silently never called, and every stored definition's hash changes.
func (p PortConnections) MarshalJSON() ([]byte, error) {
	// Both the empty string and an explicit "data" mean a data port. omitempty
	// only suppresses the empty string, and the legacy array form decodes to an
	// explicit "data" — checking only for "" would break the round trip.
	if p.Type == "" || p.Type == ConnectionTypeData {
		if p.Targets == nil {
			return []byte("[]"), nil
		}
		return json.Marshal(p.Targets)
	}
	return json.Marshal(portConnectionsAlias(p))
}
