package xflow

import (
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/protocol"
)

// registeredNodeDescriptors is the node/registry view NewRunner reports
// descriptors from. It is a variable only so an in-process test can give a
// runner a type the co-hosted server's registry (the same process-global
// node/registry) does not see.
var registeredNodeDescriptors = registry.Descriptors

// declaredRunnerDescriptors keeps the registered descriptors whose type this
// runner declares as a capability, every registered version of each, so the
// control plane can offer the editor a schema for what this runner executes.
//
// The source is node/registry only: a handler placed solely in a custom
// execution.Registry (WithRunnerNodeRegistry + RegisterGlobal) is not listed
// there and reports nothing. The implicit group capability runnerCapabilities
// appends is not a declared type and is not reported unless declared. It
// returns nil when nothing matches, which reports no descriptors.
func declaredRunnerDescriptors(declared []string, registered []registry.RegisteredDescriptor) []protocol.RunnerDescriptor {
	want := make(map[string]struct{}, len(declared))
	for _, nodeType := range declared {
		if nodeType = strings.TrimSpace(nodeType); nodeType != "" {
			want[nodeType] = struct{}{}
		}
	}
	var out []protocol.RunnerDescriptor
	for _, d := range registered {
		if _, ok := want[d.Type]; !ok {
			continue
		}
		out = append(out, protocol.RunnerDescriptor{
			Type:       d.Type,
			Version:    d.Version,
			Descriptor: d.Descriptor,
		})
	}
	return out
}

// runnerDescriptorsJSON encodes the declared descriptors into the envelope the
// runner forwards on Register. Descriptors are editor metadata, not execution
// critical, so an encode failure (a handler whose Descriptor holds a value
// encoding/json cannot represent) is logged and reports none rather than
// failing runner construction.
func runnerDescriptorsJSON(declared []string, registered []registry.RegisteredDescriptor, logger *slog.Logger) json.RawMessage {
	raw, err := protocol.EncodeRunnerDescriptors(declaredRunnerDescriptors(declared, registered))
	if err != nil {
		logger.Warn("runner node descriptors not reported: encode failed", "err", err)
		return nil
	}
	return raw
}
