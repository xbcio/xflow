package control

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// Activation delivery timings.
const (
	// activationDirectiveQueueTTL bounds how long an undelivered directive
	// survives. A queue key is scoped to one runner session, so a directive
	// that was never drained before its session ended is unreachable by
	// construction; the TTL is what eventually reclaims that orphan rather
	// than any session-eviction scan (deliberately not implemented: the queue
	// is written and drained on the heartbeat path, and a scan per session
	// change would cost more than the retained bytes).
	activationDirectiveQueueTTL = 30 * time.Minute
	// activationHostedReportTTL is how long a runner's hosted-activation
	// report stays fresh. It must comfortably exceed the heartbeat interval
	// (5s nominal, observed ~17-21s) so one dropped heartbeat does not read as
	// "runner reports nothing", which would trigger a redelivery pass.
	activationHostedReportTTL = 120 * time.Second
)

// Field prefixes of the per-session directive hash. The suffix is the
// activation identity (see activationDirectiveKey): a: and d: of the same
// activation are the two halves the drain arbitrates between.
const (
	activationDirectiveFieldActivate   = "a:"
	activationDirectiveFieldDeactivate = "d:"
)

// ActivationDeliveryDirectory is an optional RunnerDirectory capability that
// makes activation directives survive leader changes: any control pod can
// deliver a directive to the runner on its next heartbeat, instead of the
// directive living in the enqueuing leader's memory until that one process
// hands it to the one heartbeat it happens to serve.
//
// It also carries the reverse direction: the runner's own report of what it
// currently hosts, which the reconciler uses to notice a directive that never
// arrived (the runner heartbeats happily while hosting nothing) and to notice
// a runner still hosting something the ledger says it no longer owns.
//
// Both implementations (MemoryRunnerDirectory, RedisRunnerDirectory) must obey
// the same semantics: enqueue is deduplicating per activation identity,
// drain is drain-once, cross-type arbitration happens after the drain (see
// resolveActivationDirectiveConflicts), and a report is fresh only within
// activationHostedReportTTL.
type ActivationDeliveryDirectory interface {
	// EnqueueActivationDirective appends (or overwrites, by activation
	// identity) a pending Activate for the runner's current session.
	EnqueueActivationDirective(ctx context.Context, runnerID, sessionID string, d protocol.ActivateDirective) error
	// EnqueueDeactivationDirective appends (or overwrites, by activation
	// identity) a pending Deactivate for the runner's current session.
	EnqueueDeactivationDirective(ctx context.Context, runnerID, sessionID string, d protocol.DeactivateDirective) error
	// ActivationDirectives returns and removes (drain-once) all pending
	// directives for the runner's current session. Directives enqueued for
	// other sessions are unreachable by construction and expire by TTL.
	ActivationDirectives(ctx context.Context, runnerID, sessionID string) ([]protocol.ActivateDirective, []protocol.DeactivateDirective, error)
	// RecordHostedActivations stores the runner's most recent heartbeat report.
	RecordHostedActivations(ctx context.Context, runnerID, sessionID string, items []protocol.ActivationInventoryItem) error
	// HostedActivations returns the most recent report if fresh (ok=false when
	// absent or stale). The report carries the session that sent it; callers
	// compare it against the session they expect before trusting the items.
	HostedActivations(ctx context.Context, runnerID string) (HostedActivationsReport, bool, error)
}

// HostedActivationsReport is one heartbeat's self-report of the activations a
// runner currently hosts. SessionID is the session that sent the report — a
// report from a replaced session must never be read as the current one's
// (it may describe subscriptions the new session no longer has).
type HostedActivationsReport struct {
	SessionID string
	Items     []protocol.ActivationInventoryItem
}

// activationDirectiveKey renders the activation identity shared by
// ActivateDirective and DeactivateDirective into the queue's dedup key. The
// components and their order are the same identity the reconciler's
// EntryActivationKey uses (namespace, workflow, version, entry unit, replica);
// only the rendering differs, because the queue needs a single hash field
// name. Same activation identity => same field => a re-enqueue overwrites its
// predecessor, which is a feature: the newest directive for an activation is
// the only one worth delivering.
func activationDirectiveKey(namespace, workflowID, workflowVersion, entryUnitID string, replicaIndex uint32) string {
	return fmt.Sprintf("%s/%s/%s/%s/%d", namespace, workflowID, workflowVersion, entryUnitID, replicaIndex)
}

func activationDirectiveKeyOfActivate(d protocol.ActivateDirective) string {
	return activationDirectiveKey(d.Namespace, d.WorkflowID, d.WorkflowVersion, d.EntryUnitID, d.ReplicaIndex)
}

func activationDirectiveKeyOfDeactivate(d protocol.DeactivateDirective) string {
	return activationDirectiveKey(d.Namespace, d.WorkflowID, d.WorkflowVersion, d.EntryUnitID, d.ReplicaIndex)
}

// resolveActivationDirectiveConflicts decodes the drained hash fields and
// arbitrates the one case the key cannot express: both an Activate and a
// Deactivate pending for the same activation identity.
//
// The rule is "highest generation wins; on a tie the deactivate wins". It is
// sound because both directive types are enqueued serially by the one leader
// that owns reconciliation, and a generation only ever moves forward — so
// when the two disagree, the higher generation is the newer intent. A tie can
// only mean the fence (which is what produces a Deactivate) and the assignment
// that produced the Activate refer to the same generation, i.e. the stop is
// the later decision and the runner must not (re)start hosting it. The runner
// itself enforces the same ordering on delivery: activateLocked upgrades to a
// higher generation and deactivateLocked refuses to clean up anything newer
// than the stop (see service/runner/activation_tracker.go).
//
// Unknown field shapes and undecodable payloads are dropped rather than
// failing the drain: a single damaged field must not strand every other
// pending directive for the runner. The drained key is gone either way, and a
// lost directive is exactly what the hosted-report reconciliation recovers.
func resolveActivationDirectiveConflicts(entries map[string]string) ([]protocol.ActivateDirective, []protocol.DeactivateDirective, error) {
	type pending struct {
		activate   *protocol.ActivateDirective
		deactivate *protocol.DeactivateDirective
	}
	byKey := make(map[string]*pending, len(entries))
	keys := make([]string, 0, len(entries))
	for field, payload := range entries {
		var suffix string
		var entry *pending
		switch {
		case strings.HasPrefix(field, activationDirectiveFieldActivate):
			var d protocol.ActivateDirective
			if err := json.Unmarshal([]byte(payload), &d); err != nil {
				continue
			}
			suffix = strings.TrimPrefix(field, activationDirectiveFieldActivate)
			if entry = byKey[suffix]; entry == nil {
				entry = &pending{}
				byKey[suffix] = entry
				keys = append(keys, suffix)
			}
			entry.activate = &d
		case strings.HasPrefix(field, activationDirectiveFieldDeactivate):
			var d protocol.DeactivateDirective
			if err := json.Unmarshal([]byte(payload), &d); err != nil {
				continue
			}
			suffix = strings.TrimPrefix(field, activationDirectiveFieldDeactivate)
			if entry = byKey[suffix]; entry == nil {
				entry = &pending{}
				byKey[suffix] = entry
				keys = append(keys, suffix)
			}
			entry.deactivate = &d
		}
	}
	sort.Strings(keys)
	activates := make([]protocol.ActivateDirective, 0, len(keys))
	deactivates := make([]protocol.DeactivateDirective, 0, len(keys))
	for _, key := range keys {
		entry := byKey[key]
		switch {
		case entry.activate != nil && entry.deactivate != nil:
			if entry.deactivate.Generation >= entry.activate.Generation {
				deactivates = append(deactivates, *entry.deactivate)
			} else {
				activates = append(activates, *entry.activate)
			}
		case entry.activate != nil:
			activates = append(activates, *entry.activate)
		case entry.deactivate != nil:
			deactivates = append(deactivates, *entry.deactivate)
		}
	}
	return activates, deactivates, nil
}
