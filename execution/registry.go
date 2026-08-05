package execution

import (
	"fmt"
	"strings"
	"sync"

	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// VersionPolicy controls what Registry.Get does when the workflow pins a
// specific handler version but that version is not registered. See
// .claude/specs/handler-version.md.
type VersionPolicy int

const (
	// VersionWarnFallback returns the latest registered handler and emits a
	// warning via the optional Logger. Default policy: avoids breaking
	// rolling-deployment scenarios while still surfacing the drift.
	VersionWarnFallback VersionPolicy = iota
	// VersionStrict returns ErrHandlerVersionMismatch when the pinned version
	// is missing.
	VersionStrict
	// VersionSilentFallback returns the latest registered handler with no
	// logging. Preserves legacy behavior for callers that explicitly opt in.
	VersionSilentFallback
)

// ErrHandlerVersionMismatch is returned by Registry.Get under VersionStrict
// when a workflow pins a handler version that is not registered.
type ErrHandlerVersionMismatch struct {
	NodeType         string
	RequestedVersion int
	LatestAvailable  int // -1 if no handler is registered at all
}

func (e *ErrHandlerVersionMismatch) Error() string {
	if e.LatestAvailable < 0 {
		return fmt.Sprintf("no handler registered for node type %q (requested version %d)", e.NodeType, e.RequestedVersion)
	}
	return fmt.Sprintf("handler %q version %d not registered (latest available: %d)", e.NodeType, e.RequestedVersion, e.LatestAvailable)
}

// VersionLogger is implemented by callers that want to observe WarnFallback
// resolutions. Optional.
type VersionLogger interface {
	Warnf(format string, args ...any)
}

// VersionConfigurator is implemented by handler registries that expose
// version-miss policy and logger configuration. The SDK asserts this interface
// instead of the concrete *Registry so alternative implementations can plug in
// without the SDK depending on a concrete type.
type VersionConfigurator interface {
	SetVersionPolicy(VersionPolicy)
	SetLogger(VersionLogger)
}

// Registry resolves handlers for embedded execution.
//
// Resolution order is:
//  1. execution-scoped direct handler
//  2. node-name direct handler
//  3. explicitly registered node-type handler
//  4. global node registry — exact version first, then policy-controlled
//     fallback to the latest registered version
type Registry struct {
	mu                sync.RWMutex
	executionHandlers map[string]types.ActionHandler
	nodeHandlers      map[string]types.ActionHandler
	globalHandlers    map[string]types.ActionHandler
	policy            VersionPolicy
	logger            VersionLogger
}

// NewRegistry creates a handler registry suitable for embedded runners.
func NewRegistry() *Registry {
	return &Registry{
		executionHandlers: make(map[string]types.ActionHandler),
		nodeHandlers:      make(map[string]types.ActionHandler),
		globalHandlers:    make(map[string]types.ActionHandler),
		policy:            VersionWarnFallback,
	}
}

// SetVersionPolicy updates the registry's miss-handling policy.
func (r *Registry) SetVersionPolicy(p VersionPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policy = p
}

// SetLogger installs a logger for WarnFallback messages. nil is allowed and
// disables the warning surface.
func (r *Registry) SetLogger(l VersionLogger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = l
}

// RegisterExecutionHandler binds a handler to one execution and node name.
func (r *Registry) RegisterExecutionHandler(id types.ExecutionID, nodeName string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executionHandlers[string(id)+"/"+nodeName] = h
}

// DirectHandlerTypePrefix is the synthetic node type a name-scoped handler
// carries. A LocalNode has no portable node type — its handler is bound to the
// node NAME (RegisterNodeHandler) — so the builder stamps its NodeDef.Type as
// DirectHandlerTypePrefix + <node name> to keep the definition self-describing.
//
// The prefix is exported because whoever validates a node type against a
// registry has to know that this one resolves by name instead: a type-only
// lookup for "__direct__/double" finds nothing however many name-scoped
// handlers are registered.
const DirectHandlerTypePrefix = "__direct__/"

// DirectHandlerNodeName returns the node name encoded in a synthetic direct
// handler type, and whether nodeType was one.
func DirectHandlerNodeName(nodeType string) (string, bool) {
	if !strings.HasPrefix(nodeType, DirectHandlerTypePrefix) {
		return "", false
	}
	return strings.TrimPrefix(nodeType, DirectHandlerTypePrefix), true
}

// HasNodeHandler reports whether a handler is registered for this node name.
// It answers the question "can this name-scoped node be dispatched", which
// Get cannot: Get falls through to type lookups and version policy, so a miss
// there does not distinguish "no name-scoped handler" from "no handler at all".
func (r *Registry) HasNodeHandler(nodeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodeHandlers[nodeName]
	return ok
}

// RegisterNodeHandler binds a handler to a node name across executions.
func (r *Registry) RegisterNodeHandler(nodeName string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeHandlers[nodeName] = h
}

// RegisterGlobal binds a handler to a node type across executions.
func (r *Registry) RegisterGlobal(nodeType string, h types.ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globalHandlers[nodeType] = h
}

// Get resolves a handler for the given execution node. Workflow-scoped
// registrations win before consulting the global node registry. When falling
// back to the global registry, the requested version is matched exactly first;
// on miss, behavior follows the registry's VersionPolicy.
func (r *Registry) Get(id types.ExecutionID, nodeName string, nodeType string, version int) (types.ActionHandler, error) {
	r.mu.RLock()
	policy := r.policy
	logger := r.logger
	if h, ok := r.executionHandlers[string(id)+"/"+nodeName]; ok {
		r.mu.RUnlock()
		return h, nil
	}
	if h, ok := r.nodeHandlers[nodeName]; ok {
		r.mu.RUnlock()
		return h, nil
	}
	if h, ok := r.globalHandlers[nodeType]; ok {
		r.mu.RUnlock()
		return h, nil
	}
	r.mu.RUnlock()

	// Exact version match wins.
	if version > 0 {
		if h, ok := nodereg.LookupVersion(nodeType, version); ok {
			return h, nil
		}
	}

	// Pinned version missing — apply the policy.
	if version > 0 {
		switch policy {
		case VersionStrict:
			latest := latestRegisteredVersion(nodeType)
			return nil, &ErrHandlerVersionMismatch{
				NodeType:         nodeType,
				RequestedVersion: version,
				LatestAvailable:  latest,
			}
		case VersionWarnFallback:
			if h, ok := nodereg.Lookup(nodeType); ok {
				if logger != nil {
					logger.Warnf("handler version fallback: node_type=%s node_name=%s requested_version=%d", nodeType, nodeName, version)
				}
				return h, nil
			}
			return nil, &ErrHandlerVersionMismatch{
				NodeType:         nodeType,
				RequestedVersion: version,
				LatestAvailable:  -1,
			}
		case VersionSilentFallback:
			if h, ok := nodereg.Lookup(nodeType); ok {
				return h, nil
			}
		}
	}

	if h, ok := nodereg.Lookup(nodeType); ok {
		return h, nil
	}

	return nil, fmt.Errorf("no handler registered for node type %q", nodeType)
}

// latestRegisteredVersion returns the highest registered version for a node
// type, or -1 if none. Used only to enrich the strict-policy error message.
func latestRegisteredVersion(nodeType string) int {
	versions := nodereg.Versions(nodeType)
	if len(versions) == 0 {
		return -1
	}
	latest := versions[0]
	for _, v := range versions[1:] {
		if v > latest {
			latest = v
		}
	}
	return latest
}
