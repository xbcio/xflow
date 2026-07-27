package queue

import (
	"encoding/json"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// queuedTask carries engine.Task plus queue-internal runtime metadata.
// engine.Task hides AutoDepth/ActivationID/UnitIdx from public JSON (json:"-")
// so runner-facing protocols do not expose scheduler internals, but a
// Transport must still carry them between enqueue and consume. They are
// re-published here under underscore-prefixed keys that never collide with
// the public schema. UnitIdx uses omitempty plus an explicit "has" flag so a
// genuinely absent field (old payload) can be distinguished from a present
// zero value: Unmarshal sets Task.UnitIdx to engine.UnitIdxUnknown when
// _unit_idx is absent instead of silently defaulting to 0.
type queuedTask struct {
	engine.Task

	Namespace    namespace.Namespace `json:"_namespace,omitempty"`
	AutoDepth    int                 `json:"_auto_depth,omitempty"`
	ActivationID int                 `json:"_activation_id,omitempty"`
	UnitIdx      *int                `json:"_unit_idx,omitempty"`
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
		UnitIdx:      unitIdxPtr(t.UnitIdx),
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
		UnitIdx:      unitIdxPtr(t.UnitIdx),
	})
}

// unitIdxPtr omits the wire field entirely when the task's UnitIdx is already
// the "unknown" sentinel, so a task that was never assigned a real unit index
// (e.g. built by test/legacy code paths that predate this field) round-trips
// as absent rather than as a literal -1 value that a future reader might
// mistake for a real index.
func unitIdxPtr(unitIdx int) *int {
	if unitIdx == engine.UnitIdxUnknown {
		return nil
	}
	v := unitIdx
	return &v
}

// Unmarshal decodes a transport payload back into an engine.Task, restoring the
// scheduler metadata carried alongside the public fields. The namespace is returned
// separately when present. Task.UnitIdx is set to engine.UnitIdxUnknown when the
// wire payload has no _unit_idx field; callers that need a real unit index for
// pre-group payloads must resolve it via an authoritative Graph (see the
// graph-aware resolver wired in backend/providers/distributed/backend.go
// bindHandler), not by defaulting it here.
func Unmarshal(data []byte) (*engine.Task, namespace.Namespace, error) {
	var qt queuedTask
	if err := json.Unmarshal(data, &qt); err != nil {
		return nil, namespace.Default, err
	}
	task := qt.Task
	task.AutoDepth = qt.AutoDepth
	task.ActivationID = qt.ActivationID
	if qt.UnitIdx != nil {
		task.UnitIdx = *qt.UnitIdx
	} else {
		task.UnitIdx = engine.UnitIdxUnknown
	}
	namespaceID := qt.Namespace
	if namespaceID == "" {
		namespaceID = namespace.Default
	}
	return &task, namespaceID, nil
}
