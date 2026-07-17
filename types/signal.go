package types

// SignalStatus represents the lifecycle state of a workflow signal.
type SignalStatus string

const (
	// SignalStatusActive means the signal is pending consumption.
	SignalStatusActive SignalStatus = "active"
	// SignalStatusConsumed means the signal has been consumed by a workflow.
	SignalStatusConsumed SignalStatus = "consumed"
	// SignalStatusRevoked means the signal has been revoked before consumption.
	SignalStatusRevoked SignalStatus = "revoked"
)
