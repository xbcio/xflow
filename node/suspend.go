package node

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

// SuspendingHandler is an optional interface implemented by node handlers that
// need to pause execution and resume later (e.g. approval gates, timers).
//
// The engine calls PrepareSuspend first. If a signal was already delivered
// (race-free via SuspendOrConsume), OnResume is called immediately. Otherwise
// the node is parked and OnResume is called when the signal arrives.
type SuspendingHandler interface {
	TaskHandler
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
type SignalPayload struct {
	Triggered SignalTrigger
	Name      string                     // name of the signal that triggered resumption
	Data      map[string]any             // payload of the triggering signal
	All       map[string]map[string]any  // all collected signal payloads (multi-signal mode)
}
