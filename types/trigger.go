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
}

func (i *TriggerActivateInput) Emit(ctx context.Context, event *TriggerEvent) (ExecutionID, error) {
	return i.Runtime.Emit(ctx, i.WorkflowID, i.NodeName, event)
}

func (i *TriggerActivateInput) GetString(name string) string {
	return cast.ToString(i.Params[name])
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

// TriggerGroupRuntime is an optional capability of TriggerRuntime that supports
// trigger-group atomic admission. Kafka trigger nodes check for this via type
// assertion when operating in trigger-group mode. If absent, the trigger falls
// back to the standard Emit path.
type TriggerGroupRuntime interface {
	// SeedTriggeredGroupResult atomically admits a trigger-group result to the
	// control plane. Only after a successful (accepted/duplicate) response may
	// the caller commit the Kafka offset.
	SeedTriggeredGroupResult(ctx context.Context, req TriggerGroupAdmissionRequest) (TriggerGroupAdmissionResponse, error)
}

// TriggerGroupAdmissionRequest is the caller-facing admission request. It wraps
// the engine-level types with the fields a Kafka trigger naturally has. The
// TriggerGroupRuntime implementation maps these to the engine request.
type TriggerGroupAdmissionRequest struct {
	AdmissionKey    string
	WorkflowID      WorkflowID
	WorkflowVersion string
	GroupID         string
	Outcome         string // "success" or "failed"
	Exits           []TriggerGroupExit
	Error           string
}

// TriggerGroupExit is one boundary output from the group execution.
type TriggerGroupExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// TriggerGroupAdmissionResponse is the control-plane response.
type TriggerGroupAdmissionResponse struct {
	Accepted    bool
	Duplicate   bool
	Conflict    bool
	ExecutionID ExecutionID
}
