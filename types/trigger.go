package types

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cast"
)

type TriggerEvent struct {
	ID      string            `json:"id,omitempty"`
	Kind    string            `json:"kind,omitempty"`
	Source  string            `json:"source,omitempty"`
	Time    time.Time         `json:"time,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Data    map[string]any    `json:"data,omitempty"`
	Raw     []byte            `json:"raw,omitempty"`
}

type TriggerHandler interface {
	Handler
	Activate(ctx context.Context, input *TriggerActivateInput) (TriggerSubscription, error)
}

type TriggerSubscription interface {
	Close(ctx context.Context) error
}

type CloseFunc func(context.Context) error

func (f CloseFunc) Close(ctx context.Context) error { return f(ctx) }

type TriggerActivateInput struct {
	WorkflowID WorkflowID
	NodeName   string
	Params     map[string]any
	Runtime    TriggerRuntime
	// Supplies holds decoded supply content for this trigger's declared
	// dependencies (via DependsOn edges). Keyed by supply node name. Each
	// value is the JSON-decoded supply content (map[string]any). nil when no
	// supplies are declared or none have content yet.
	Supplies map[string]any
}

func (i *TriggerActivateInput) Emit(ctx context.Context, event *TriggerEvent) (ExecutionID, error) {
	return i.Runtime.Emit(ctx, i.WorkflowID, i.NodeName, event)
}

func (i *TriggerActivateInput) GetString(name string) string {
	return cast.ToString(i.Params[name])
}

// GetSupply returns the decoded supply content for the named supply node, or
// nil if not present. The supply node name is the one declared via DependsOn.
func (i *TriggerActivateInput) GetSupply(name string) map[string]any {
	if i.Supplies == nil {
		return nil
	}
	v, ok := i.Supplies[name]
	if !ok {
		return nil
	}
	m, _ := v.(map[string]any)
	return m
}

func (i *TriggerActivateInput) Webhooks() (WebhookRuntime, error) {
	provider, ok := i.Runtime.(interface{ Webhooks() WebhookRuntime })
	if !ok {
		return nil, fmt.Errorf("webhook runtime is not available")
	}
	rt := provider.Webhooks()
	if rt == nil {
		return nil, fmt.Errorf("webhook runtime is not available")
	}
	return rt, nil
}

type TriggerRuntime interface {
	Emit(ctx context.Context, workflowID WorkflowID, nodeName string, event *TriggerEvent) (ExecutionID, error)
	Dedup(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TryLock(ctx context.Context, key string, ttl time.Duration) (TriggerLock, bool, error)
	State(ctx context.Context, scope string) TriggerState
}

type TriggerLock interface {
	Release(ctx context.Context) error
}

type RenewableTriggerLock interface {
	TriggerLock
	Renew(ctx context.Context, ttl time.Duration) (bool, error)
}

type TriggerState interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// EntrySeedRuntime is an optional capability of TriggerRuntime that supports
// entry-unit seed (single node or group node) atomic admission. Kafka trigger
// nodes check for this via type assertion when operating in seed mode. If
// absent, the trigger falls back to the standard Emit path.
type EntrySeedRuntime interface {
	// SeedExecutionFromEntry atomically admits an entry-unit result to the
	// control plane. Only after a successful (accepted/duplicate) response may
	// the caller commit the Kafka offset.
	SeedExecutionFromEntry(ctx context.Context, req EntrySeedRequest) (EntrySeedResponse, error)
}

// EntrySeedRequest is the caller-facing admission request. It wraps
// the engine-level types with the fields a Kafka trigger naturally has. The
// EntrySeedRuntime implementation maps these to the engine request.
type EntrySeedRequest struct {
	AdmissionKey    string
	WorkflowID      WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	Outcome         string // "success" or "failed"
	Exits           []BoundaryExit
	Error           string
}

// BoundaryExit is one boundary output from the entry-unit execution.
type BoundaryExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// EntrySeedResponse is the control-plane response.
type EntrySeedResponse struct {
	Accepted    bool
	Duplicate   bool
	Conflict    bool
	ExecutionID ExecutionID
}
