package xflow

import (
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// WithServerEntryNamespaces is the embedded host's only way to declare the
// namespaces its control plane reconciles. It must reach apiserver.Config's
// EntryNamespaces — a break on this hop leaves the control plane reconciling
// {namespace.Default} only, and every workflow the host registered in its own
// namespace silently never gets a runner.
func TestWithServerEntryNamespacesReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerEntryNamespaces([]namespace.Namespace{"local-dev"})(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if len(cfg.EntryNamespaces) != 1 || cfg.EntryNamespaces[0] != "local-dev" {
		t.Fatalf("EntryNamespaces = %v, want [local-dev]", cfg.EntryNamespaces)
	}
}

// Unset must leave the field at its zero value: nil is the "single-namespace
// default" signal the control layer's fallbacks key on (len == 0 →
// {namespace.Default}), and an unset embedder must see byte-identical
// behavior to a build without this option.
func TestServerEntryNamespacesUnsetStaysNil(t *testing.T) {
	cfg := buildServerAPIConfig(ServerConfig{}, &serverConfig{})
	if cfg.EntryNamespaces != nil {
		t.Fatalf("EntryNamespaces = %v, want nil when no option was set", cfg.EntryNamespaces)
	}
}

// The option copies its argument (same defensive shape as
// WithServerAssignmentQueueLanes): the caller's slice stays its own, so a
// later mutation there cannot silently change what the server reconciles.
func TestWithServerEntryNamespacesCopiesItsArgument(t *testing.T) {
	ns := []namespace.Namespace{"local-a"}
	sc := &serverConfig{}
	WithServerEntryNamespaces(ns)(sc)

	ns[0] = "mutated"
	if sc.entryNamespaces[0] != "local-a" {
		t.Fatalf("entryNamespaces[0] = %q after the caller mutated its slice; the option must copy", sc.entryNamespaces[0])
	}
}
