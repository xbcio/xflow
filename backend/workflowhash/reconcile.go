package workflowhash

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

// Reconcile returns the effective runtime hash to compare against a new
// registration, given the currently stored DefinitionHash and the stored
// Definition.
//
// A runtime-sha256:v2: hash is current: it is returned as-is with
// needsUpgrade=false.
//
// A runtime-sha256:v1: hash may be stale -- the v1 algorithm ignored node
// Timeout and Output -- so it is recomputed from storedDef, with needsUpgrade
// reporting whether the recomputed hash differs from the stored one. A v1 hash
// with a nil storedDef cannot be recomputed and is returned as-is.
//
// Any other format (bare "sha256:", "sha256:audit:v1:", or an unrecognized
// prefix) is recomputed from storedDef and returned with needsUpgrade=true.
// Callers should then persist the recomputed hash via the registry's
// UpdateDefinitionHash to atomically upgrade the record.
//
// For a legacy (non-runtime) hash with a nil storedDef an error is returned.
// This guards against registries that store the hash without the definition.
func Reconcile(storedHash string, storedDef *types.WorkflowDef) (effectiveHash string, needsUpgrade bool, err error) {
	if strings.HasPrefix(storedHash, RuntimePrefixV2) {
		return storedHash, false, nil
	}
	if strings.HasPrefix(storedHash, RuntimePrefixV1) {
		if storedDef == nil {
			return storedHash, false, nil
		}
		recomputed, err := Runtime(storedDef)
		if err != nil {
			return "", false, fmt.Errorf("reconcile definition hash: %w", err)
		}
		return recomputed, recomputed != storedHash, nil
	}
	if storedDef == nil {
		return "", false, fmt.Errorf("reconcile definition hash: stored definition is nil for legacy hash %q", storedHash)
	}
	recomputed, err := Runtime(storedDef)
	if err != nil {
		return "", false, fmt.Errorf("reconcile definition hash: %w", err)
	}
	return recomputed, true, nil
}

// Registry is the subset of backend.WorkflowRegistry that ReconcileAdd uses.
type Registry interface {
	AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error)
	GetWorkflowByKey(ctx context.Context, key string) (backend.WorkflowRecord, error)
	UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error
}

// ReconcileAdd registers rec, whose DefinitionHash must be the Runtime hash of
// rec.Definition, and reconciles the stored hash formats the registry's plain
// string compare cannot:
//
//   - After a hash hit on a stored v1 hash, the hash is recomputed from the
//     stored definition. A v1 hash may predate node Timeout/Output joining the
//     runtime hash, in which case it also matches a definition that differs
//     from the stored one only in those fields. On a mismatch the stale hash
//     is corrected (best effort) and backend.ErrWorkflowConflict is returned.
//   - On backend.ErrWorkflowConflict, the stored record is fetched by key and
//     its effective hash recomputed with Reconcile. A match is an idempotent
//     registration: a legacy stored hash is CAS-upgraded to rec's hash, and a
//     lost CAS is re-fetched once.
//
// It returns the stored record and whether this call may have created it.
// created is false only when the registration matched an existing record
// through reconciliation; a plain registry success reports true, because the
// registry does not distinguish a fresh write from an idempotent hash match.
func ReconcileAdd(ctx context.Context, reg Registry, rec backend.WorkflowRecord) (stored backend.WorkflowRecord, created bool, err error) {
	hash := rec.DefinitionHash
	got, err := reg.AddWorkflow(ctx, rec)
	if err == nil {
		if strings.HasPrefix(got.DefinitionHash, RuntimePrefixV1) && got.Definition != nil {
			// A record this call just created recomputes to the same hash.
			recomputed, hashErr := Runtime(got.Definition)
			if hashErr != nil {
				return backend.WorkflowRecord{}, false, hashErr
			}
			if recomputed != hash {
				// Correct the stale hash so the stored definition itself
				// re-registers idempotently. Best effort: a lost CAS means
				// another registrar already rewrote the record.
				_ = reg.UpdateDefinitionHash(ctx, got.ID, got.DefinitionHash, recomputed)
				return backend.WorkflowRecord{}, false, backend.ErrWorkflowConflict
			}
		}
		return got, true, nil
	}
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		return backend.WorkflowRecord{}, false, err
	}
	existing, lookupErr := reg.GetWorkflowByKey(ctx, rec.Key)
	if lookupErr != nil {
		// Preserve the original conflict error if the lookup fails -- the
		// caller's contract is "conflict", not "lookup failed".
		return backend.WorkflowRecord{}, false, err
	}
	effective, needsUpgrade, reconcileErr := Reconcile(existing.DefinitionHash, existing.Definition)
	if reconcileErr != nil {
		return backend.WorkflowRecord{}, false, reconcileErr
	}
	if effective != hash {
		// Real semantic conflict: the stored definition produces a different
		// runtime hash than the new one.
		return backend.WorkflowRecord{}, false, err
	}
	if needsUpgrade {
		if upgradeErr := reg.UpdateDefinitionHash(ctx, existing.ID, existing.DefinitionHash, hash); upgradeErr != nil {
			// CAS mismatch means another registrar concurrently upgraded (or
			// replaced) the record. Re-fetch and re-check once: if it now
			// matches the new hash, treat as idempotent; otherwise surface
			// the original conflict.
			reloaded, reloadErr := reg.GetWorkflowByKey(ctx, rec.Key)
			if reloadErr != nil || reloaded.DefinitionHash != hash {
				return backend.WorkflowRecord{}, false, err
			}
			existing = reloaded
		} else {
			existing.DefinitionHash = hash
		}
	}
	return existing, false, nil
}
