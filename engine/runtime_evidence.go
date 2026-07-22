package engine

import (
	"context"
	"sync/atomic"

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
// boundary. It carries no error full text, credential, tenant payload, or
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
type EffectiveClassification struct {
	Source     ErrorSource
	Classified bool
	Kind       types.ErrorKind
	Retryable  *bool
	Permanent  *bool
	Code       string
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

var _ = context.Background
