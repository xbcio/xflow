package queue

import (
	"encoding/json"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// queuedTask carries engine.Task plus queue-internal runtime metadata.
// engine.Task hides AutoDepth/ActivationID from public JSON (json:"-") so
// runner-facing protocols do not expose scheduler internals, but a Transport
// must still carry them between enqueue and consume. They are re-published here
// under underscore-prefixed keys that never collide with the public schema.
type queuedTask struct {
	engine.Task

	Namespace    namespace.Namespace `json:"_namespace,omitempty"`
	AutoDepth    int                 `json:"_auto_depth,omitempty"`
	ActivationID int                 `json:"_activation_id,omitempty"`
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

// MarshalWithNamespace encodes a task plus namespace into a transport payload.
func MarshalWithNamespace(t *engine.Task, namespaceID namespace.Namespace) ([]byte, error) {
	if t == nil {
		return json.Marshal((*queuedTask)(nil))
	}
	return json.Marshal(queuedTask{
		Task:         *t,
		Namespace:    namespaceID,
		AutoDepth:    t.AutoDepth,
		ActivationID: t.ActivationID,
	})
}

// Unmarshal decodes a transport payload back into an engine.Task, restoring the
// scheduler metadata carried alongside the public fields. The namespace is returned
// separately when present.
func Unmarshal(data []byte) (*engine.Task, namespace.Namespace, error) {
	var qt queuedTask
	if err := json.Unmarshal(data, &qt); err != nil {
		return nil, namespace.Default, err
	}
	task := qt.Task
	task.AutoDepth = qt.AutoDepth
	task.ActivationID = qt.ActivationID
	namespaceID := qt.Namespace
	if namespaceID == "" {
		namespaceID = namespace.Default
	}
	return &task, namespaceID, nil
}
