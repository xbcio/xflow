package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// RunnerDescriptorRejected* name why control dropped runner-reported
// descriptors at registration. They are a closed set and the only values
// RunnerDescriptorObserver.OnRunnerDescriptorRejected takes, so they are safe
// as metric label values. The first four drop the whole envelope; the rest
// drop one entry.
const (
	RunnerDescriptorRejectedEnvelopeTooLarge   = "envelope_too_large"
	RunnerDescriptorRejectedTooManyEntries     = "too_many_entries"
	RunnerDescriptorRejectedUnknownSchema      = "unknown_schema"
	RunnerDescriptorRejectedMalformedEnvelope  = "malformed_envelope"
	RunnerDescriptorRejectedUndeclaredType     = "undeclared_type"
	RunnerDescriptorRejectedTypeNotGranted     = "type_not_granted"
	RunnerDescriptorRejectedTypeMismatch       = "type_mismatch"
	RunnerDescriptorRejectedInvalidVersion     = "invalid_version"
	RunnerDescriptorRejectedDuplicate          = "duplicate_entry"
	RunnerDescriptorRejectedDescriptorTooLarge = "descriptor_too_large"
	RunnerDescriptorRejectedMalformed          = "malformed_descriptor"
)

// RunnerDescriptorObserver receives one event per dropped envelope or entry.
// Implementations must be non-blocking and must not use runner identifiers as
// metric labels.
type RunnerDescriptorObserver interface {
	OnRunnerDescriptorRejected(ctx context.Context, reason string)
}

// RunnerNodeDescriptor is one validated runner-reported descriptor as the
// directory stores it. JSON is the canonical encoding of the decoded
// types.Descriptor and Hash its hex sha256, so two runners reporting the same
// descriptor store identical bytes regardless of how their envelopes were
// formatted.
type RunnerNodeDescriptor struct {
	Type    string
	Version int
	Hash    string
	JSON    json.RawMessage
}

// RunnerDescriptorRecord is one live runner session's accepted descriptors
// and the attribution the aggregator needs. Namespaces is the session's
// effective namespace set; PoolID and PoolName are empty for a runner without
// a pool.
type RunnerDescriptorRecord struct {
	RunnerID     string
	PoolID       string
	PoolName     string
	Namespaces   []namespace.Namespace
	RegisteredAt time.Time
	Descriptors  []RunnerNodeDescriptor
}

// RunnerDescriptorDirectory is an optional directory capability, following
// the RunnerControlDirectory pattern so external directories keep compiling.
// LiveRunnerDescriptors returns every session that reported at least one
// descriptor and is live at now under DefaultRunnerLiveTTL — the same rule
// ListLiveRunners' consumers apply. A registration replaces the session's
// set (nil clears it) and RemoveRunner deletes it. Descriptors are never part
// of RunnerSnapshot, so the management JSON stays small.
type RunnerDescriptorDirectory interface {
	LiveRunnerDescriptors(ctx context.Context, now time.Time) ([]RunnerDescriptorRecord, error)
}

// runnerDescriptorsLive is the liveness rule shared by every directory's
// LiveRunnerDescriptors.
func runnerDescriptorsLive(lastHeartbeat, now time.Time) bool {
	return DefaultRunnerSelector().IsLive(RunnerSnapshot{LastHeartbeat: lastHeartbeat}, now)
}

// sortRunnerDescriptorRecords orders records by runner ID so every directory
// returns the same deterministic sequence.
func sortRunnerDescriptorRecords(records []RunnerDescriptorRecord) {
	sort.Slice(records, func(i, j int) bool { return records[i].RunnerID < records[j].RunnerID })
}

// runnerDescriptorRejection is one dropped envelope (Type empty) or entry.
type runnerDescriptorRejection struct {
	Reason  string
	Type    string
	Version int
	Err     error
}

// validateRunnerDescriptors decodes a runner's descriptor envelope and keeps
// only the entries control is willing to serve: the type is one the runner
// declared as a capability and the policy grants, the inner descriptor names
// the same type, the version is positive, the entry fits the size limit, and
// it is the first entry for its (type, version). Everything else is reported
// as a rejection rather than an error: descriptors are editor metadata and
// never fail a registration. The result is sorted by type then version, and
// is nil when nothing survives.
func validateRunnerDescriptors(raw json.RawMessage, capabilities []protocol.Capability, policy RunnerPolicy) ([]RunnerNodeDescriptor, []runnerDescriptorRejection) {
	env, err := protocol.DecodeRunnerDescriptorEnvelope(raw)
	if err != nil {
		return nil, []runnerDescriptorRejection{{Reason: envelopeRejectionReason(err), Err: err}}
	}
	if len(env.Descriptors) == 0 {
		return nil, nil
	}
	declared := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		declared[strings.TrimSpace(capability.NodeType)] = struct{}{}
	}
	type key struct {
		nodeType string
		version  int
	}
	seen := make(map[key]struct{}, len(env.Descriptors))
	var (
		kept     []RunnerNodeDescriptor
		rejected []runnerDescriptorRejection
	)
	for _, entry := range env.Descriptors {
		reject := func(reason string, err error) {
			rejected = append(rejected, runnerDescriptorRejection{Reason: reason, Type: entry.Type, Version: entry.Version, Err: err})
		}
		if _, ok := declared[entry.Type]; !ok || entry.Type == "" {
			reject(RunnerDescriptorRejectedUndeclaredType, nil)
			continue
		}
		if !policy.Allows(entry.Type) {
			reject(RunnerDescriptorRejectedTypeNotGranted, nil)
			continue
		}
		if entry.Version < 1 {
			reject(RunnerDescriptorRejectedInvalidVersion, nil)
			continue
		}
		descriptor, err := protocol.DecodeRunnerDescriptor(entry.Descriptor)
		if err != nil {
			reason := RunnerDescriptorRejectedMalformed
			if errors.Is(err, protocol.ErrRunnerDescriptorTooLarge) {
				reason = RunnerDescriptorRejectedDescriptorTooLarge
			}
			reject(reason, err)
			continue
		}
		if descriptor.Type != entry.Type {
			reject(RunnerDescriptorRejectedTypeMismatch, nil)
			continue
		}
		k := key{nodeType: entry.Type, version: entry.Version}
		if _, dup := seen[k]; dup {
			reject(RunnerDescriptorRejectedDuplicate, nil)
			continue
		}
		canonical, err := json.Marshal(descriptor)
		if err != nil {
			reject(RunnerDescriptorRejectedMalformed, err)
			continue
		}
		if len(canonical) > protocol.MaxRunnerDescriptorBytes {
			// Re-encoding can only grow a descriptor through number or escape
			// normalisation, but the stored copy is what the limit protects.
			reject(RunnerDescriptorRejectedDescriptorTooLarge, nil)
			continue
		}
		seen[k] = struct{}{}
		sum := sha256.Sum256(canonical)
		kept = append(kept, RunnerNodeDescriptor{
			Type:    entry.Type,
			Version: entry.Version,
			Hash:    hex.EncodeToString(sum[:]),
			JSON:    canonical,
		})
	}
	sortRunnerNodeDescriptors(kept)
	return kept, rejected
}

func envelopeRejectionReason(err error) string {
	switch {
	case errors.Is(err, protocol.ErrRunnerDescriptorsTooLarge):
		return RunnerDescriptorRejectedEnvelopeTooLarge
	case errors.Is(err, protocol.ErrRunnerDescriptorsTooMany):
		return RunnerDescriptorRejectedTooManyEntries
	case errors.Is(err, protocol.ErrRunnerDescriptorsUnknownSchema):
		return RunnerDescriptorRejectedUnknownSchema
	default:
		return RunnerDescriptorRejectedMalformedEnvelope
	}
}

func sortRunnerNodeDescriptors(descriptors []RunnerNodeDescriptor) {
	sort.Slice(descriptors, func(i, j int) bool {
		if descriptors[i].Type != descriptors[j].Type {
			return descriptors[i].Type < descriptors[j].Type
		}
		return descriptors[i].Version < descriptors[j].Version
	})
}

func cloneRunnerNodeDescriptors(descriptors []RunnerNodeDescriptor) []RunnerNodeDescriptor {
	if len(descriptors) == 0 {
		return nil
	}
	out := make([]RunnerNodeDescriptor, len(descriptors))
	for i, d := range descriptors {
		out[i] = d
		out[i].JSON = append(json.RawMessage(nil), d.JSON...)
	}
	return out
}

// acceptRunnerDescriptors validates a register request's descriptors and
// reports every drop as a warning log and a metric. It never fails.
func (c *Core) acceptRunnerDescriptors(ctx context.Context, req protocol.RegisterRunnerRequest, policy RunnerPolicy) []RunnerNodeDescriptor {
	kept, rejected := validateRunnerDescriptors(req.DescriptorsJSON, req.Capabilities, policy)
	for _, r := range rejected {
		if c.logger != nil {
			args := []any{"runner", req.RunnerID, "reason", r.Reason}
			if r.Type != "" {
				args = append(args, "node_type", r.Type, "node_version", r.Version)
			}
			if r.Err != nil {
				args = append(args, "err", r.Err)
			}
			c.logger.Warn("runner_descriptor_rejected", args...)
		}
		c.observeRunnerDescriptorRejected(ctx, r.Reason)
	}
	return kept
}

// observeRunnerDescriptorRejected reports one drop. Nil-safe, and a metrics
// adapter can never fail a registration.
func (c *Core) observeRunnerDescriptorRejected(ctx context.Context, reason string) {
	if c == nil || c.runnerDescriptorObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	c.runnerDescriptorObserver.OnRunnerDescriptorRejected(ctx, reason)
}
