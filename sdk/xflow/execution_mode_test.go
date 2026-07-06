package xflow

import (
	"strings"
	"testing"
	"time"
)

func TestExecutionModeDefaultsToDefault(t *testing.T) {
	cfg := &engineConfig{}
	if err := validateExecutionModeConfig(cfg); err != nil {
		t.Fatalf("validateExecutionModeConfig() error = %v", err)
	}
	if cfg.executionMode != ExecutionModeDefault {
		t.Fatalf("executionMode = %q, want %q", cfg.executionMode, ExecutionModeDefault)
	}
}

func TestExecutionModeRejectsUnknownMode(t *testing.T) {
	cfg := &engineConfig{}
	WithExecutionMode(ExecutionMode("fast"))(cfg)
	err := validateExecutionModeConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown execution mode") {
		t.Fatalf("error = %v, want unknown execution mode", err)
	}
}

func TestTransientTTLOptionsRequireTransientMode(t *testing.T) {
	cfg := &engineConfig{}
	WithTransientTTL(time.Minute)(cfg)
	err := validateExecutionModeConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "WithTransientTTL requires ExecutionModeTransient") {
		t.Fatalf("error = %v, want transient TTL mode error", err)
	}
}

func TestTransientModeRejectsNonPositiveTTL(t *testing.T) {
	cfg := &engineConfig{}
	WithExecutionMode(ExecutionModeTransient)(cfg)
	WithTransientTTL(0)(cfg)
	err := validateExecutionModeConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "transient TTL must be positive") {
		t.Fatalf("error = %v, want positive TTL error", err)
	}
}

func TestNewLocalRejectsTransientTTLOptionInDefaultMode(t *testing.T) {
	_, err := NewLocal(WithTransientTTL(time.Minute))
	if err == nil || !strings.Contains(err.Error(), "WithTransientTTL requires ExecutionModeTransient") {
		t.Fatalf("error = %v, want transient TTL mode error", err)
	}
}

func TestNewLocalRejectsUnknownExecutionMode(t *testing.T) {
	_, err := NewLocal(WithExecutionMode(ExecutionMode("fast")))
	if err == nil || !strings.Contains(err.Error(), "unknown execution mode") {
		t.Fatalf("error = %v, want unknown execution mode", err)
	}
}

func TestNewClusterRejectsTransientTTLOptionInDefaultModeBeforeRedisValidation(t *testing.T) {
	_, err := NewCluster(ClusterConfig{}, WithTransientTTL(time.Minute))
	if err == nil || !strings.Contains(err.Error(), "WithTransientTTL requires ExecutionModeTransient") {
		t.Fatalf("error = %v, want transient TTL mode error before Redis validation", err)
	}
}

func TestNewClusterRejectsUnknownExecutionModeBeforeRedisValidation(t *testing.T) {
	_, err := NewCluster(ClusterConfig{}, WithExecutionMode(ExecutionMode("fast")))
	if err == nil || !strings.Contains(err.Error(), "unknown execution mode") {
		t.Fatalf("error = %v, want unknown execution mode before Redis validation", err)
	}
}
