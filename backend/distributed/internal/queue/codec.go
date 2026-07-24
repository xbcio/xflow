package queue

import (
	"encoding/json"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
)

// queuedTask carries engine.Task plus queue-internal runtime metadata.
// engine.Task hides AutoDepth/ActivationID from public JSON (json:"-") so
// runner-facing protocols do not expose scheduler internals, but a Transport
// must still carry them between enqueue and consume. They are re-published here
// under underscore-prefixed keys that never collide with the public schema.
type queuedTask struct {
	engine.Task

	TenantID     tenant.TenantID `json:"_tenant_id,omitempty"`
	AutoDepth    int             `json:"_auto_depth,omitempty"`
	ActivationID int             `json:"_activation_id,omitempty"`
}

// Marshal encodes a task into a transport payload, preserving the scheduler
// metadata that engine.Task keeps out of its public JSON contract.
func Marshal(t *engine.Task) ([]byte, error) {
	if t == nil {
		return json.Marshal((*queuedTask)(nil))
	}
	return json.Marshal(queuedTask{
		Task:         *t,
		AutoDepth:    t.AutoDepth,
		ActivationID: t.ActivationID,
	})
}

// MarshalWithTenant encodes a task plus tenant into a transport payload.
func MarshalWithTenant(t *engine.Task, tenantID tenant.TenantID) ([]byte, error) {
	if t == nil {
		return json.Marshal((*queuedTask)(nil))
	}
	return json.Marshal(queuedTask{
		Task:         *t,
		TenantID:     tenantID,
		AutoDepth:    t.AutoDepth,
		ActivationID: t.ActivationID,
	})
}

// Unmarshal decodes a transport payload back into an engine.Task, restoring the
// scheduler metadata carried alongside the public fields. The tenant is returned
// separately when present.
func Unmarshal(data []byte) (*engine.Task, tenant.TenantID, error) {
	var qt queuedTask
	if err := json.Unmarshal(data, &qt); err != nil {
		return nil, tenant.DefaultTenant, err
	}
	task := qt.Task
	task.AutoDepth = qt.AutoDepth
	task.ActivationID = qt.ActivationID
	tenantID := qt.TenantID
	if tenantID == "" {
		tenantID = tenant.DefaultTenant
	}
	return &task, tenantID, nil
}
