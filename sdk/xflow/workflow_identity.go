package xflow

import (
	"fmt"
	"strings"

	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/types"
)

// workflowKey returns the human-meaningful identity used by registries.
// It intentionally uses Namespace+Name+Version — WorkflowDef.ID is an instance
// identifier, not a runtime identity key.
func workflowKey(def *types.WorkflowDef) string {
	return fmt.Sprintf("%s/%s@%s", def.Namespace, def.Name, def.Version)
}

// Hash format prefixes, re-declared from workflowhash so this package's pins
// keep their names. See workflowhash for what each one means.
const (
	runtimeHashPrefix   = workflowhash.RuntimePrefixV1
	runtimeHashPrefixV2 = workflowhash.RuntimePrefixV2
)

// runtimeHashPayload is the payload workflowhash.Runtime marshals, aliased so
// this package's encoding pins can inspect it.
type runtimeHashPayload = workflowhash.RuntimePayload

// runtimeHash is the registry conflict-detection hash. It delegates to
// workflowhash.Runtime, the single implementation every registration path
// shares.
func runtimeHash(def *types.WorkflowDef) (string, error) {
	return workflowhash.Runtime(def)
}

// legacyDefinitionHash is the audit fingerprint. It delegates to
// workflowhash.Audit and must not be used for conflict detection.
func legacyDefinitionHash(def *types.WorkflowDef) (string, error) {
	return workflowhash.Audit(def)
}

// reconcileDefinitionHash returns the effective runtime hash to compare
// against a new registration, given the currently stored DefinitionHash and
// the stored Definition.
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
func reconcileDefinitionHash(storedHash string, storedDef *types.WorkflowDef) (effectiveHash string, needsUpgrade bool, err error) {
	if strings.HasPrefix(storedHash, runtimeHashPrefixV2) {
		return storedHash, false, nil
	}
	if strings.HasPrefix(storedHash, runtimeHashPrefix) {
		if storedDef == nil {
			return storedHash, false, nil
		}
		recomputed, err := runtimeHash(storedDef)
		if err != nil {
			return "", false, fmt.Errorf("reconcile definition hash: %w", err)
		}
		return recomputed, recomputed != storedHash, nil
	}
	if storedDef == nil {
		return "", false, fmt.Errorf("reconcile definition hash: stored definition is nil for legacy hash %q", storedHash)
	}
	recomputed, err := runtimeHash(storedDef)
	if err != nil {
		return "", false, fmt.Errorf("reconcile definition hash: %w", err)
	}
	return recomputed, true, nil
}
