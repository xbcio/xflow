package engine

import (
	"context"
	"errors"
	"fmt"
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

var runtimeEventSeq uint64

func newRuntimeEventID(ctx context.Context, execID types.ExecutionID, node string, attempt int) string {
	n := atomic.AddUint64(&runtimeEventSeq, 1)
	return fmt.Sprintf("rev-%d-%s-%s-%d", n, execID, node, attempt)
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
		EventID:      newRuntimeEventID(ctx, task.ExecutionID, task.NodeName, 0),
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
		return EffectiveClassification{Source: ErrorSourceSystem, Classified: true, Kind: ce.Kind, Retryable: &r, Permanent: &p, Code: ce.Code}
	}
	// wrapped permanent but no ClassifiedError
	if types.IsPermanent(systemErr) {
		f := false
		tr := true
		return EffectiveClassification{Source: ErrorSourceSystem, Classified: true, Kind: types.ErrorKindPermanent, Retryable: &f, Permanent: &tr}
	}
	return EffectiveClassification{Source: ErrorSourceUnclassified, Classified: false}
}
