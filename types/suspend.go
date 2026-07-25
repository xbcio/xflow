package types

import (
	"context"
	"time"
)

// SuspendMode describes how a suspending node waits for resumption.
type SuspendMode int

const (
	// ModeSignal waits for one named external signal.
	ModeSignal SuspendMode = iota
	// ModeTimer fires after a fixed duration.
	ModeTimer
	// ModeMultiSignal waits for one or more signals (quorum-based).
	ModeMultiSignal
)

// SuspendingHandler is an optional interface implemented by action handlers
// that need to pause execution and resume later.
type SuspendingHandler interface {
	ActionHandler
	// PrepareSuspend returns the suspension specification for this execution.
	PrepareSuspend(ctx context.Context, input *Input) (*SuspendSpec, error)
	// OnResume is called when the awaited signal (or timeout) fires.
	OnResume(ctx context.Context, input *Input, signal *SignalPayload) (*Output, error)
}

// SuspendSpec describes what the node is waiting for.
type SuspendSpec struct {
	Mode    SuspendMode
	Signals []string      // signal names to listen for
	Quorum  int           // number of signals required; 0 means all
	Timer   time.Duration // fixed wait duration (ModeTimer)
	Timeout time.Duration // maximum wait before routing to timeout port; 0 = no timeout
}

// SignalTrigger identifies what caused a suspended node to resume.
type SignalTrigger int

const (
	// SignalReceived means an external signal was delivered.
	SignalReceived SignalTrigger = iota
	// TimeoutFired means the node's timeout elapsed before a signal arrived.
	TimeoutFired
	// TimerFired means the fixed timer duration elapsed (ModeTimer).
	TimerFired
)

// SignalPayload carries the data delivered to a resuming node.
//
// All is tagged omitempty so a nil All is dropped from the serialized JSON.
// This is load-bearing for the durable multi-signal resume path: the outbox
// entry body is marshaled by Go with All=nil (see DeliverSignalWithOutbox),
// then the deliverSignalWithOutboxLua script splices the collected signal set
// in as "All" once quorum is reached. Without omitempty the marshaled body
// would already carry "All":null, and the splice would produce a duplicate
// "All" key whose trailing null wins on decode — silently dropping every
// collected signal. All consumers treat nil and absent identically.
type SignalPayload struct {
	Triggered SignalTrigger
	Name      string                    // name of the signal that triggered resumption
	Data      map[string]any            // payload of the triggering signal
	All       map[string]map[string]any `json:"All,omitempty"` // all collected signal payloads (multi-signal mode)
}
