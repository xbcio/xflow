package xflow

import (
	"errors"
	"fmt"
	"time"
)

type ExecutionMode string

const (
	ExecutionModeDefault   ExecutionMode = "default"
	ExecutionModeTransient ExecutionMode = "transient"
)

const (
	defaultTransientTTL           = 10 * time.Minute
	defaultTransientCompletionTTL = 30 * time.Second
)

var (
	ErrTransientInspectionUnavailable = errors.New("xflow: inspect is unavailable in transient execution mode")
	ErrTransientSignalsUnsupported    = errors.New("xflow: signals are unsupported in transient execution mode")
)

func WithExecutionMode(mode ExecutionMode) Option {
	return func(c *engineConfig) {
		c.executionMode = mode
		c.executionModeSet = true
	}
}

func WithTransientTTL(ttl time.Duration) Option {
	return func(c *engineConfig) {
		c.transientTTL = ttl
		c.transientTTLSet = true
	}
}

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
