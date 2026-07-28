package xflow

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// HandlerLocator identifies a single (type, version) pair the registry must
// satisfy.
type HandlerLocator struct {
	Type    string
	Version int
	Kind    types.NodeKind
}

// ErrMissingHandlerVersions is returned by AddWorkflow when the workflow
// references node types/versions that no handler currently satisfies. The
// pre-check is always strict regardless of WithVersionPolicy: at registration
// time all handlers should be known. See
// .claude/specs/handler-version.md.
type ErrMissingHandlerVersions struct {
	Missing []HandlerLocator
}

func (e *ErrMissingHandlerVersions) Error() string {
	parts := make([]string, 0, len(e.Missing))
	for _, m := range e.Missing {
		kind := string(m.Kind)
		if kind == "" {
			kind = string(types.NodeKindAction)
		}
		if m.Version > 0 {
			parts = append(parts, fmt.Sprintf("%s %s@v%d", kind, m.Type, m.Version))
		} else {
			parts = append(parts, fmt.Sprintf("%s %s", kind, m.Type))
		}
	}
	return "handler not registered: " + strings.Join(parts, ", ")
}

func (e *Engine) AddWorkflow(ctx context.Context, wf *WorkflowBuilder) (types.WorkflowID, error) {
	def, err := wf.build()
	if err != nil {
		return "", err
	}

	// Pre-check and compile before mutating global state so failures here
	// leave no side effects.
	if err := preCheckHandlerVersions(def, wf); err != nil {
		return "", err
	}
	g, err := graph.Compile(def)
	if err != nil {
		return "", err
	}
	hash, err := runtimeHash(def)
	if err != nil {
		return "", err
	}
	// Preserve a full-definition audit fingerprint so exported records can
	// trace the exact original payload (including editor metadata).
	audit, err := legacyDefinitionHash(def)
	if err != nil {
		return "", err
	}

	// Serialize registration of global handlers and directHandlerNames map
	// writes to prevent concurrent-map-read-write panics and partial
	// pollution on failure. Both registrations return a rollback that restores
	// the process registry to its prior state so a later failure in this call
	// leaves no side effects behind.
	e.mu.Lock()
	rollbackGlobal, err := e.registerWorkflowHandlersTracked(wf)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	rollbackDirect, err := e.registerDirectHandlersTracked(wf)
	if err != nil {
		rollbackGlobal()
		e.mu.Unlock()
		return "", err
	}
	e.mu.Unlock()

	// rollbackHandlers undoes both handler registrations under the lock.
	rollbackHandlers := func() {
		e.mu.Lock()
		rollbackDirect()
		rollbackGlobal()
		e.mu.Unlock()
	}

	// created tracks whether this call persisted a brand-new record (vs.
	// matching an existing/idempotent one), so a later reconcile failure only
	// removes records we ourselves created.
	created := true
	rec, err := e.workflowRegistry.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:              workflowKey(def),
		Namespace:        def.Namespace,
		Name:             def.Name,
		Version:          def.Version,
		DefinitionHash:   hash,
		AuditFingerprint: audit,
		Definition:       def,
		Graph:            g,
	})
	if err != nil {
		// Legacy-hash compatibility: when a workflow was first registered
		// before commit 3ef36d9 (or before F0-A3 tightened the runtime hash),
		// the stored DefinitionHash is in a format that will never equal the
		// freshly-computed runtime-sha256:v1: hash, so the registry rejects
		// the re-registration as a conflict. Recompute the runtime hash from
		// the stored Definition and, if it matches, atomically upgrade the
		// record's DefinitionHash so future registrations are idempotent.
		if !errors.Is(err, backend.ErrWorkflowConflict) {
			rollbackHandlers()
			return "", err
		}
		existing, lookupErr := e.workflowRegistry.GetWorkflowByKey(ctx, workflowKey(def))
		if lookupErr != nil {
			// Preserve the original conflict error if the lookup fails —
			// the caller's contract is "conflict", not "lookup failed".
			rollbackHandlers()
			return "", err
		}
		effective, needsUpgrade, reconcileErr := reconcileDefinitionHash(existing.DefinitionHash, existing.Definition)
		if reconcileErr != nil {
			rollbackHandlers()
			return "", reconcileErr
		}
		if effective != hash {
			// Real semantic conflict: the stored definition produces a
			// different runtime hash than the new one. Surface the original
			// ErrWorkflowConflict.
			rollbackHandlers()
			return "", err
		}
		if needsUpgrade {
			if upgradeErr := e.workflowRegistry.UpdateDefinitionHash(ctx, existing.ID, existing.DefinitionHash, hash); upgradeErr != nil {
				// CAS mismatch means another registrar concurrently
				// upgraded (or replaced) the record. Re-fetch and re-check
				// once: if it now matches the new hash, treat as idempotent;
				// otherwise surface the original conflict.
				reloaded, reloadErr := e.workflowRegistry.GetWorkflowByKey(ctx, workflowKey(def))
				if reloadErr == nil && reloaded.DefinitionHash == hash {
					existing = reloaded
				} else {
					rollbackHandlers()
					return "", err
				}
			} else {
				existing.DefinitionHash = hash
			}
		}
		// Matched an already-registered record; we did not create it.
		created = false
		rec = existing
	}
	if e.triggerRuntime != nil {
		if err := e.triggerRuntime.ReconcileWorkflow(ctx, rec); err != nil {
			// Undo the half-commit: close any subscriptions this reconcile may
			// have activated, remove the record if we created it, and roll back
			// handler registrations so AddWorkflow leaves no residue.
			_ = e.triggerRuntime.RemoveWorkflow(ctx, rec.ID)
			if created {
				_ = e.workflowRegistry.RemoveWorkflow(ctx, rec.ID)
			}
			rollbackHandlers()
			return "", err
		}
	}
	return rec.ID, nil
}

// preCheckHandlerVersions verifies every NodeDef's (type, version) is
// resolvable. Workflow-bundled handlers (collected by the builder) are paired
// with their NodeDef.Version by construction, so they pass automatically.
// Built-in xflow.* types (xflow.if, xflow.merge, etc.) registered at package
// init time are also considered satisfied. Anything else must resolve through
// the global node registry's versioned lookups. Direct handlers
// (__direct__/<node-name>) are workflow-local and always satisfied.
func preCheckHandlerVersions(def *types.WorkflowDef, wf *WorkflowBuilder) error {
	if def == nil {
		return nil
	}
	bundledActions := wf.workflowHandlers()
	bundledTriggers := wf.workflowTriggerHandlers()

	var missing []HandlerLocator
	for _, nd := range def.Nodes {
		if strings.HasPrefix(nd.Type, "__direct__/") {
			continue // local-only direct handler, name-scoped
		}
		switch nd.Kind {
		case types.NodeKindTrigger:
			if _, ok := bundledTriggers[nd.Type]; ok {
				continue
			}
			if nd.Version > 0 {
				if _, ok := registry.LookupTriggerVersion(nd.Type, nd.Version); ok {
					continue
				}
			} else if _, ok := registry.LookupTrigger(nd.Type); ok {
				continue
			}
			missing = append(missing, HandlerLocator{Type: nd.Type, Version: nd.Version, Kind: types.NodeKindTrigger})
		default:
			if _, ok := bundledActions[nd.Type]; ok {
				continue
			}
			if nd.Version > 0 {
				if _, ok := registry.LookupVersion(nd.Type, nd.Version); ok {
					continue
				}
			} else if _, ok := registry.Lookup(nd.Type); ok {
				continue
			}
			missing = append(missing, HandlerLocator{Type: nd.Type, Version: nd.Version, Kind: types.NodeKindAction})
		}
	}
	if len(missing) > 0 {
		return &ErrMissingHandlerVersions{Missing: missing}
	}
	return nil
}

func (e *Engine) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	if e.triggerRuntime != nil {
		if err := e.triggerRuntime.RemoveWorkflow(ctx, workflowID); err != nil {
			return err
		}
	}
	return e.workflowRegistry.RemoveWorkflow(ctx, workflowID)
}
