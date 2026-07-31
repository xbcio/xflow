// Package supply holds the in-process distribution skeleton for supply content:
// a snapshot cache plus a per-name consumer registry.
//
// The whole package is deliberately invisible to the execution lifecycle. Apply
// creates no execution, dispatches no task, and touches neither the remaining
// counter nor unit in-degree. It only mutates process-local state for the nodes
// that declared a dependency edge on the supply.
//
// Content is opaque bytes and is NOT a secret — but it is also never logged,
// audited, or exported as a metric label. Log the name and a hash prefix only.
package supply

import (
	"context"
	"time"
)

// Snapshot is one version of a supply's content as seen by this process.
type Snapshot struct {
	Name string
	// Content is the opaque snapshot. Treat as read-only; Registry.Get returns
	// a copy so a consumer cannot corrupt the cache.
	Content []byte
	// Hash is the read-side comparison token (store.ContentHash of Content).
	// Consumers rebuild only when it changes.
	Hash string
	// Revision is the server-side monotonic version (SupplyResource.Revision).
	// It is globally comparable across runners, which is what makes a tagged
	// record's config_generation traceable.
	Revision  uint64
	FetchedAt time.Time
}

// Consumer is notified when a supply's content changes. It is implemented by
// node-type authors (wasm ScriptNode is the first), not by DSL users.
//
// Returning an error means the content was rejected; the consumer must keep its
// last-good state. The registry still records the new snapshot so nodes that
// read through $supplies see the current value.
type Consumer interface {
	OnSupplyChanged(ctx context.Context, snap Snapshot) error
}
