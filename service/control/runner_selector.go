package control

import (
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

const (
	DefaultRunnerLiveTTL = 30 * time.Second
	// DefaultSelectorFallback is the grace period a "default"-mode activation
	// waits before falling back to any capable runner when no label-matching
	// runner is available. 30s ≈ 3 reconcile ticks — long enough for transient
	// overloads to resolve, short enough to avoid prolonged starvation.
	// A zero FallbackGrace on RunnerSelector means "use this default" (not
	// "immediate fallback") — explicit zero is treated as unset.
	DefaultSelectorFallback = 30 * time.Second
)

// RunnerSelector decides which runner can serve a given assignment based on
// liveness, labels, capabilities, policy, and namespace. It is a pure decision
// function shared by memory and Redis directories.
type RunnerSelector struct {
	LiveTTL       time.Duration
	FallbackGrace time.Duration
}

// DefaultRunnerSelector returns a selector with production defaults.
func DefaultRunnerSelector() RunnerSelector {
	return RunnerSelector{
		LiveTTL:       DefaultRunnerLiveTTL,
		FallbackGrace: DefaultSelectorFallback,
	}
}

// IsLive reports whether a runner's last heartbeat is within the live TTL
// relative to the given now time. A runner that has never heartbeated (zero
// LastHeartbeat) is considered dead.
func (s RunnerSelector) IsLive(snap RunnerSnapshot, now time.Time) bool {
	if snap.LastHeartbeat.IsZero() {
		return false
	}
	ttl := s.LiveTTL
	if ttl <= 0 {
		ttl = DefaultRunnerLiveTTL
	}
	return now.Sub(snap.LastHeartbeat) <= ttl
}

// CanAssign is the label/capability/policy eligibility check: the runner must
// be live, have matching capabilities, an authorizing policy, and satisfy the
// required label selector.
//
// It does NOT check namespace membership — it has no parameter carrying the
// target namespace. Namespace eligibility is enforced at two other points:
// RunnerPolicy.AllowsNamespace at registration (which decides what a runner may
// declare) and canServeNamespace at dispatch (ClaimForRunner) and at entry
// activation placement (chooseRunner).
func (s RunnerSelector) CanAssign(snap RunnerSnapshot, policy RunnerPolicy, routing engine.TaskRouting, labels map[string]string, now time.Time) bool {
	if !s.IsLive(snap, now) {
		return false
	}
	if !canRunRouting(snap.Capabilities, routing) {
		return false
	}
	if !policy.Allows(routing.NodeType) {
		return false
	}
	if !MatchLabels(snap.Labels, labels) {
		return false
	}
	return true
}

// MatchLabels returns true if the runner's labels satisfy all required selector
// labels. A nil or empty required set matches any runner (default placement).
func MatchLabels(runnerLabels, required map[string]string) bool {
	for k, v := range required {
		if runnerLabels[k] != v {
			return false
		}
	}
	return true
}

// MatchCapabilities checks if a runner's capabilities cover the routing
// requirements. This extends canRunRouting to also check Features when present
// on the routing requirements.
func MatchCapabilities(capabilities []protocol.Capability, routing engine.TaskRouting) bool {
	if !canRunRouting(capabilities, routing) {
		return false
	}
	if len(routing.Requirements) == 0 {
		return true
	}
	for _, req := range routing.Requirements {
		if !hasCapabilityForRequirement(capabilities, req) {
			return false
		}
	}
	return true
}

func hasCapabilityForRequirement(capabilities []protocol.Capability, req engine.CapabilityRequirement) bool {
	for _, cap := range capabilities {
		if cap.NodeType != req.NodeType {
			continue
		}
		if req.NodeVersion != 0 && cap.NodeVersion != 0 && cap.NodeVersion != req.NodeVersion {
			continue
		}
		if req.Feature != "" && !containsString(cap.Features, req.Feature) {
			continue
		}
		return true
	}
	return false
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
