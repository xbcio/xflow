package engine

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/types"
)

// ErrorSource categorizes where a committed error originated.
type ErrorSource string

const (
	ErrorSourceSystem       ErrorSource = "system"
	ErrorSourceBusiness     ErrorSource = "business"
	ErrorSourceErrorPort    ErrorSource = "error_port"
	ErrorSourceUnclassified ErrorSource = "unclassified"
)

// RuntimeEvidenceEventType is commit | advance | retry.
type RuntimeEvidenceEventType string

const (
	RuntimeEvidenceCommit  RuntimeEvidenceEventType = "commit"
	RuntimeEvidenceAdvance RuntimeEvidenceEventType = "advance"
	RuntimeEvidenceRetry   RuntimeEvidenceEventType = "retry"
)

// RuntimeEvidenceEvent is a read-only observation of a production mutation
// boundary. It carries no error full text, credential, namespace payload, or
// handler output. It is additive evidence only; it never changes commit
// control flow or return values.
type RuntimeEvidenceEvent struct {
	Version      int
	EventID      string
	Type         RuntimeEvidenceEventType
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeIdx      int
	ActivationID int
	Attempt      int

	CommitOutcome CommitOutcome
	Applied       bool
	OutboxIDs     []string

	ErrorSource ErrorSource
	Classified  bool
	ErrorKind   types.ErrorKind
	Retryable   *bool
	Permanent   *bool
	ErrorCode   string

	NodeStatus types.NodeStatus
	RoutePort  string
}

// EffectiveClassification is the production-derived classification bound to a
// commit receipt, recovered at the retry-decision/commit boundary rather than
// from a fixture name.
//
// Details is the producer's structured diagnostic (which of the three browser
// host-denial sources rejected a URL, which phase timed out). It is carried on
// to the persisted node snapshot so a consumer of the read API can see it.
// Before that plumbing existed this map survived the runner→server wire
// (service/protocol.MarshalTaskResult) and was then dropped here — this struct
// was the last place the *types.ClassifiedError still existed. It is bounded on
// ingest by boundedErrorDetails, because it is runner-supplied.
type EffectiveClassification struct {
	Source     ErrorSource
	Classified bool
	Kind       types.ErrorKind
	Retryable  *bool
	Permanent  *bool
	Code       string
	Details    map[string]any
}

// Limits for the structured error detail that reaches the read API.
//
// Details is filled in by the NODE, not by the engine: whatever a runner puts
// in ClassifiedError.Details would otherwise be reproduced verbatim on an
// operator-facing surface. The engine therefore treats it as attacker-
// influenced and projects it down to a flat, bounded, scalar-only map instead
// of persisting and serving it as-is.
//
// The producers today are two, and they are not equivalent:
//
//   - browser.host_denied / browser.timeout emit {source, rejected_url} and
//     {phase} — short, already sanitized, exactly what a consumer wants.
//   - the http 4xx/5xx constructors (node/internal/action/http.go) emit the
//     response envelope, including the response BODY. That body is up to
//     maxResponseBytes and is attacker-chosen in the sense that the remote
//     server picks it.
//
// The http case is why the bounds are not merely tidiness. It is also why they
// are proportionate rather than alarming: that same envelope is the http node's
// ordinary OUTPUT on the success path, where it is already served on this API,
// so an error detail carries nothing a caller could not already read from a
// node that succeeded — and a node whose policy marks its output private has
// its detail withheld entirely (see inspectNode). What the bounds add is a
// ceiling: at most maxErrorDetailKeys keys of at most maxErrorDetailValueSize
// bytes, whatever a node decides to attach.
//
// Scalar-only is the load-bearing rule. A nested map or slice is the shape a
// payload dump takes — and it is what makes the http case small in practice,
// because a JSON response body decodes to a nested value and is dropped
// outright. An over-long string is truncated rather than dropped so the
// diagnostic prefix survives; for a sanitized rejected URL that prefix is
// scheme+host+path, and truncation cannot reintroduce the query string that
// sanitizeBrowserRejectedURL already removed producer-side.
const (
	maxErrorDetailKeys      = 16
	maxErrorDetailKeyBytes  = 64
	maxErrorDetailValueSize = 512
)

// boundedErrorDetails projects a runner-supplied Details map onto the flat,
// size-bounded shape the read API is allowed to serve. Keys are sorted before
// the key cap is applied because Go map iteration order is random: without
// sorting, WHICH 16 keys survived could differ between two reads of the same
// immutable failure.
//
// It returns nil for an empty result so the omitempty JSON tag keeps a
// detail-free error byte-identical to what it was before this field existed.
func boundedErrorDetails(details map[string]any) map[string]any {
	if len(details) == 0 {
		return nil
	}
	keys := make([]string, 0, len(details))
	for k := range details {
		if k != "" && len(k) <= maxErrorDetailKeyBytes {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > maxErrorDetailKeys {
		keys = keys[:maxErrorDetailKeys]
	}
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		switch v := details[k].(type) {
		case string:
			if len(v) > maxErrorDetailValueSize {
				v = v[:maxErrorDetailValueSize]
			}
			out[k] = v
		case bool, nil, int, int32, int64, uint, uint32, uint64, float32, float64:
			out[k] = v
		default:
			// Nested, structured, or otherwise non-scalar: dropped rather
			// than re-encoded. See the limit comment above.
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// RuntimeEvidenceBuffer is a bounded, non-blocking, read-only evidence sink.
// engine is the only producer and never closes the channel; the owner stops
// producers, cancels the collector, drains, then reads Dropped(). A buffer
// must not be reused across topologies.
type RuntimeEvidenceBuffer struct {
	events  chan RuntimeEvidenceEvent
	dropped atomic.Uint64
}

// NewRuntimeEvidenceBuffer creates a buffer with the given capacity. capacity
// <= 0 panics: a zero-capacity/nil-channel buffer is forbidden.
func NewRuntimeEvidenceBuffer(capacity int) *RuntimeEvidenceBuffer {
	if capacity <= 0 {
		panic("RuntimeEvidenceBuffer capacity must be > 0")
	}
	return &RuntimeEvidenceBuffer{events: make(chan RuntimeEvidenceEvent, capacity)}
}

// Events returns the receive-only end of the evidence channel.
func (b *RuntimeEvidenceBuffer) Events() <-chan RuntimeEvidenceEvent {
	if b == nil {
		return nil
	}
	return b.events
}

// Dropped returns the count of events dropped because the buffer was full.
func (b *RuntimeEvidenceBuffer) Dropped() uint64 {
	if b == nil {
		return 0
	}
	return b.dropped.Load()
}

// EvidenceBuffer returns the wired evidence buffer for test/verification access.
func (e *Engine) EvidenceBuffer() *RuntimeEvidenceBuffer { return e.evidenceBuffer }

// publishRuntimeEvidence sends a non-blocking event. A nil buffer or a full
// channel never blocks or panics; a full channel increments dropped.
func publishRuntimeEvidence(b *RuntimeEvidenceBuffer, event RuntimeEvidenceEvent) {
	if b == nil || b.events == nil {
		return
	}
	select {
	case b.events <- event:
	default:
		b.dropped.Add(1)
	}
}

// newRuntimeEventID returns a fresh crypto/rand UUIDv4. Per spec §4.3.1 the
// EventID MUST be a cross-process UUID and MUST NOT be assembled from
// exec/node/attempt — those values live in separate struct fields on the
// emitted event, so the generator takes no parameters.
func newRuntimeEventID() string {
	return uuid.New().String()
}

// publishAdvanceReceipt publishes a read-only advance evidence event after the
// authoritative AdvanceNode mutation returned. Non-blocking; never changes the
// advance control flow or return values.
func (e *Engine) publishAdvanceReceipt(ctx context.Context, task *Task, result AdvanceNodeResult) {
	if e.evidenceBuffer == nil {
		return
	}
	publishRuntimeEvidence(e.evidenceBuffer, RuntimeEvidenceEvent{
		Version:      1,
		EventID:      newRuntimeEventID(),
		Type:         RuntimeEvidenceAdvance,
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		Attempt:      0,
		Applied:      result.Applied,
		OutboxIDs:    result.OutboxIDs,
	})
}

// publishRetryReceipt publishes a read-only retry evidence event after the
// engine decided to schedule a retry. Non-blocking; never changes retry control
// flow or return values. It carries no error full text, credentials, or namespace
// payload.
func (e *Engine) publishRetryReceipt(ctx context.Context, task *Task, attempt int) {
	if e.evidenceBuffer == nil {
		return
	}
	publishRuntimeEvidence(e.evidenceBuffer, RuntimeEvidenceEvent{
		Version:      1,
		EventID:      newRuntimeEventID(),
		Type:         RuntimeEvidenceRetry,
		ExecutionID:  task.ExecutionID,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		Attempt:      attempt,
	})
}

// buildEffectiveClassification derives the production classification bound to a
// commit. source distinguishes ordinary system error from explicit error-port
// output; it cannot be inferred from systemErr's type alone.
func buildEffectiveClassification(systemErr error, businessErr *types.Error, errorPort bool) EffectiveClassification {
	switch {
	case errorPort:
		return EffectiveClassification{Source: ErrorSourceErrorPort, Classified: false}
	case businessErr != nil:
		return EffectiveClassification{Source: ErrorSourceBusiness, Classified: false}
	case systemErr == nil:
		return EffectiveClassification{Source: ErrorSourceUnclassified, Classified: false}
	}
	var ce *types.ClassifiedError
	if errors.As(systemErr, &ce) {
		r := ce.Retryable
		p := ce.Permanent
		return EffectiveClassification{
			Source: ErrorSourceSystem, Classified: true,
			Kind: ce.Kind, Retryable: &r, Permanent: &p, Code: ce.Code,
			Details: boundedErrorDetails(ce.Details),
		}
	}
	// wrapped permanent but no ClassifiedError
	if types.IsPermanent(systemErr) {
		f := false
		tr := true
		return EffectiveClassification{Source: ErrorSourceSystem, Classified: true, Kind: types.ErrorKindPermanent, Retryable: &f, Permanent: &tr}
	}
	return EffectiveClassification{Source: ErrorSourceUnclassified, Classified: false}
}
