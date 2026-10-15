package distributed

import (
	"fmt"

	"github.com/xbcio/xflow/namespace"
)

// LegacyLeaderLockKey is the leader-lock key used by every deployment through
// v0.0.47 and by every v0.0.48 process that owns the default namespace (or no
// configured set). It is exported so embedders and tests can derive the key
// for their own owned set instead of hard-coding the literal.
const LegacyLeaderLockKey = "xflow:leader:control-plane"

// LeaderLockKey derives the control-plane leader-lock key for an owned
// namespace set. The mapping is the rolling-upgrade invariant of the
// deployment-isolation work:
//
//	nil / empty / contains Default -> LegacyLeaderLockKey
//	                                   The default deployment keeps byte-identical
//	                                   key names, so v0.0.47 and v0.0.48 pods
//	                                   exclude each other on the same lock
//	                                   during a rolling upgrade instead of
//	                                   both believing they lead.
//	exactly one non-default ns x    -> LegacyLeaderLockKey + ":ns:" + x
//	                                   A dedicated deployment (e.g. a local dev
//	                                   process sharing the Redis) campaigns a
//	                                   key no legacy pod ever touches.
//	ambiguous set                   -> error (same rule as
//	                                   namespace.DeploymentSuffix; rejected at
//	                                   construction)
//
// The ":ns:<ns>" suffix sits outside the "xflow:ns:<ns>:" SCAN prefix on
// purpose: namespace.Validate rejects ':' and the glob metacharacters, so the
// suffix cannot corrupt the key schema, and leader keys are not part of any
// namespace-scoped SCAN family.
func LeaderLockKey(owned []namespace.Namespace) (string, error) {
	suffix, err := namespace.DeploymentSuffix(owned)
	if err != nil {
		return "", err
	}
	if suffix == "" {
		return LegacyLeaderLockKey, nil
	}
	return fmt.Sprintf("%s:ns:%s", LegacyLeaderLockKey, suffix), nil
}
