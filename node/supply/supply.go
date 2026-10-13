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

	"github.com/xbcio/xflow/namespace"
)

// Key is a supply's identity inside one process: the namespace it belongs to
// plus its name. Supply content lives in a namespace-scoped resource
// (store.SupplyResource carries Namespace), so two workflows in different
// namespaces may each declare a supply node of the same name and they are two
// different supplies with two different contents. Every process-local cache,
// consumer registration, readiness verdict and hint is keyed by this pair; a
// name alone is not an identity.
type Key struct {
	Namespace namespace.Namespace
	Name      string
}

// KeyOf builds the identity key for a supply node in a namespace. An empty
// namespace resolves to namespace.Default — the same single-namespace fallback
// namespace.FromContext applies — so a caller that read the namespace off a
// context cannot key a supply under "" while every other namespace-scoped
// lookup in the process uses "default".
func KeyOf(ns namespace.Namespace, name string) Key {
	if ns == "" {
		ns = namespace.Default
	}
	return Key{Namespace: ns, Name: name}
}

// Snapshot is one version of a supply's content as seen by this process.
type Snapshot struct {
	// Namespace is the namespace this content belongs to. Together with Name it
	// is the snapshot's identity, which is why Apply takes the key from here
	// rather than accepting a second, separately-passed key that could disagree.
	Namespace namespace.Namespace
	Name      string
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

// Key returns the identity this snapshot is stored under.
func (s Snapshot) Key() Key { return KeyOf(s.Namespace, s.Name) }

// Metadata keys added to a JSON-object supply when it is exposed to expressions.
// The $ prefix keeps them out of the content's own key space.
const (
	SupplyRevisionKey = "$revision"
	SupplyHashKey     = "$hash"
)

// Consumer is notified when a supply's content changes. It is implemented by
// node-type authors (wasm ScriptNode is the first), not by DSL users.
//
// Returning an error means the content was rejected; the consumer must keep its
// last-good state. The registry still records the new snapshot so nodes that
// read through $supplies see the current value. A rejection makes the supply
// not-ready (see Registry.IsReady), which is what the activation gate reads.
//
// An implementation may be any type — the registry tracks registrations by
// sequence number, never by comparing Consumer values, so a struct carrying a
// slice, map, or func field is safe.
type Consumer interface {
	OnSupplyChanged(ctx context.Context, snap Snapshot) error
}
