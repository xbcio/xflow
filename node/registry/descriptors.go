package registry

import (
	"sort"

	"github.com/xbcio/xflow/types"
)

// RegisteredDescriptor is one registered (type, version) and the Descriptor its
// handler reports. The registry stores handlers, not Descriptors, and
// Descriptor carries no version of its own -- this pairing is where the version
// lives.
type RegisteredDescriptor struct {
	Type       string
	Version    int
	Descriptor types.Descriptor
}

// Descriptors lists every registered (type, version) in the global registry,
// action and trigger tables merged. See (*registry).descriptors.
func Descriptors() []RegisteredDescriptor { return globalRegistry.descriptors() }

// descriptors merges the action and trigger version tables into one entry per
// (type, version), sorted by type then version, covering every registered
// version rather than only the latest.
//
// A handler implementing both interfaces is written into both tables by
// Register/RegisterTrigger; it appears once. When the two tables hold
// DIFFERENT handlers for the same (type, version) -- separate Register and
// RegisterTrigger calls -- the action table's handler wins, deterministically.
//
// Each Descriptor is Clone()d, so a caller may mutate the result freely even
// when a custom handler returns a shared value. Handlers' Descriptor methods
// are called after the read lock is released, so a handler that consults the
// registry from Descriptor() cannot deadlock against a pending writer.
func (r *registry) descriptors() []RegisteredDescriptor {
	type key struct {
		typ     string
		version int
	}
	type describer interface{ Descriptor() types.Descriptor }

	r.mu.RLock()
	handlers := make(map[key]describer)
	for typ, versions := range r.triggerVer {
		for v, h := range versions {
			handlers[key{typ, v}] = h
		}
	}
	for typ, versions := range r.versioned {
		for v, h := range versions {
			handlers[key{typ, v}] = h // action table wins on a clash
		}
	}
	r.mu.RUnlock()

	out := make([]RegisteredDescriptor, 0, len(handlers))
	for k, h := range handlers {
		out = append(out, RegisteredDescriptor{
			Type:       k.typ,
			Version:    k.version,
			Descriptor: h.Descriptor().Clone(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return out[i].Version < out[j].Version
	})
	return out
}
