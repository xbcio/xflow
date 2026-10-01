package xflow

import (
	"fmt"

	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/node"
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

// hashParamSpecs is the ParamSpec lookup every SDK registration hashes with.
// It is the builtin-only table, never the live registry, so an omitted builtin
// param hashes like its Default and the hash matches what the HTTP path
// computes for the same workflow. On an SDK build canonicalization is the
// identity (TestCanonicalIsIdentityOnSDKBuilds): the builder already wrote
// those Defaults.
var hashParamSpecs workflowhash.ParamSpecLookup = node.BuiltinParamSpecs

// runtimeHash is the registry conflict-detection hash. It delegates to
// workflowhash.Runtime, the single implementation every registration path
// shares.
func runtimeHash(def *types.WorkflowDef) (string, error) {
	return workflowhash.Runtime(def, hashParamSpecs)
}

// legacyDefinitionHash is the audit fingerprint. It delegates to
// workflowhash.Audit and must not be used for conflict detection.
func legacyDefinitionHash(def *types.WorkflowDef) (string, error) {
	return workflowhash.Audit(def)
}

// reconcileDefinitionHash returns the effective runtime hash of a stored
// record. It delegates to workflowhash.Reconcile; see there for the per-format
// rules.
func reconcileDefinitionHash(storedHash string, storedDef *types.WorkflowDef) (effectiveHash string, needsUpgrade bool, err error) {
	return workflowhash.Reconcile(storedHash, storedDef, hashParamSpecs)
}
