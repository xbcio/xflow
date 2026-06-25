package node

import "github.com/xbcio/xflow/types"

type SuspendMode = types.SuspendMode
type SuspendingHandler = types.SuspendingHandler
type SuspendSpec = types.SuspendSpec
type SignalTrigger = types.SignalTrigger
type SignalPayload = types.SignalPayload

const (
	ModeSignal      = types.ModeSignal
	ModeTimer       = types.ModeTimer
	ModeMultiSignal = types.ModeMultiSignal
	SignalReceived  = types.SignalReceived
	TimeoutFired    = types.TimeoutFired
	TimerFired      = types.TimerFired
)
