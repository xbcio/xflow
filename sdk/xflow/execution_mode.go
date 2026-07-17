package xflow

import (
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
)

// ExecutionMode controls the runtime state-retention contract used by NewLocal and NewCluster.
type ExecutionMode string

const (
	// ExecutionModeDefault preserves the current full-state behavior.
	ExecutionModeDefault ExecutionMode = "default"
	// ExecutionModeTransient keeps only temporary runtime state and disables long-lived signal/suspend workflows.
	ExecutionModeTransient ExecutionMode = "transient"
)

const (
	defaultTransientTTL           = 10 * time.Minute
	defaultTransientCompletionTTL = 30 * time.Second
)

var (
	// ErrTransientInspectionUnavailable reports that Inspect is unavailable in transient mode.
	ErrTransientInspectionUnavailable = errors.New("xflow: inspect is unavailable in transient execution mode")
	// ErrTransientSignalsUnsupported reports that signals are unavailable in transient mode.
	ErrTransientSignalsUnsupported = errors.New("xflow: signals are unsupported in transient execution mode")
	// ErrTransientSuspendUnsupported reports that suspend nodes are unsupported in transient mode.
	// It aliases engine.ErrSuspendUnsupported so the cluster (distributed) transient path
	// surfaces the same sentinel the in-process path used to return.
	ErrTransientSuspendUnsupported = engine.ErrSuspendUnsupported
	// ErrTransientRequiresCluster reports that transient execution mode needs a
	// cluster/Redis backend. NewLocal does not support transient mode; use NewCluster.
	ErrTransientRequiresCluster = errors.New("xflow: transient execution mode requires a cluster/Redis backend, use NewCluster")
)

// WithExecutionMode selects the runtime state-retention contract.
func WithExecutionMode(mode ExecutionMode) Option {
	return func(c *engineConfig) {
		c.executionMode = mode
		c.executionModeSet = true
	}
}

// WithTransientTTL sets the active transient runtime-state TTL.
//
// It must exceed the maximum end-to-end wall-clock duration of any single
// execution. Transient mode slides only the execution-scoped structural Redis
// keys on each mutation; per-node and in-degree keys rely on the EX TTL set at
// write/creation and are not continuously re-slid, so an execution running
// longer than this TTL can lose a key mid-run and stall. Size it as a safety
// ceiling above the slowest expected run, not as a business timeout. Only valid
// with ExecutionModeTransient (cluster mode).
func WithTransientTTL(ttl time.Duration) Option {
	return func(c *engineConfig) {
		c.transientTTL = ttl
		c.transientTTLSet = true
	}
}

// WithTransientCompletionTTL sets the final-result TTL after transient execution completion.
func WithTransientCompletionTTL(ttl time.Duration) Option {
	return func(c *engineConfig) {
		c.transientCompletionTTL = ttl
		c.transientCompletionTTLSet = true
	}
}

func validateExecutionModeConfig(c *engineConfig) error {
	if !c.executionModeSet {
		c.executionMode = ExecutionModeDefault
	}
	switch c.executionMode {
	case ExecutionModeDefault:
		if c.transientTTLSet {
			return errors.New("xflow: WithTransientTTL requires ExecutionModeTransient")
		}
		if c.transientCompletionTTLSet {
			return errors.New("xflow: WithTransientCompletionTTL requires ExecutionModeTransient")
		}
	case ExecutionModeTransient:
		if !c.transientTTLSet {
			c.transientTTL = defaultTransientTTL
		}
		if !c.transientCompletionTTLSet {
			c.transientCompletionTTL = defaultTransientCompletionTTL
		}
		if c.transientTTL <= 0 {
			return errors.New("xflow: transient TTL must be positive")
		}
		if c.transientCompletionTTL <= 0 {
			return errors.New("xflow: transient completion TTL must be positive")
		}
	default:
		return fmt.Errorf("xflow: unknown execution mode %q", c.executionMode)
	}
	return nil
}
