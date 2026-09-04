package runner

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// countingObserver records every consumer-count report the supply registry
// emits. It observes through Registry.SetObserver -- the registry's OWN
// production seam (the metrics adapter uses it) -- rather than a hook added
// for this test: a probe that installs its own fake into the code under test
// proves nothing about whether the real path runs.
type countingObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func (o *countingObserver) OnConsumerCount(_ context.Context, name string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts = map[string]int{}
	}
	o.counts[name] = n
}

func (o *countingObserver) get(name string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[name]
}

// observeDefaultRegistry installs obs on supply.Default for the duration of the
// test and removes it afterwards. Production registers into supply.Default (the
// forwarding layer in node/internal/code/script/warmup.go hardcodes it), so a
// test that watches its own supply.NewRegistry() would observe nothing and pass
// for the wrong reason. Install/remove must be symmetric: supply.Default is a
// process-wide singleton shared with every other test in this binary.
func observeDefaultRegistry(t *testing.T) *countingObserver {
	t.Helper()
	obs := &countingObserver{}
	supply.Default.SetObserver(obs)
	t.Cleanup(func() { supply.Default.SetObserver(nil) })
	return obs
}

// testSupplyName derives a supply name unique to the calling test.
// supply.Default is process-wide and UnregisterConsumer never drops the applied
// Snapshot, so a shared name leaves one test's content resident for the next --
// a contamination that has already broken tests in this repo once.
func testSupplyName(t *testing.T) string {
	t.Helper()
	return "rules-" + strings.NewReplacer("/", "-", " ", "_").Replace(t.Name())
}

var (
	seamGuestWasm  []byte
	seamGuestBuild sync.Once
)

// wasmGuestBytes returns a reactorseam guest module, unique per call. The build
// happens once per test binary (~0.35s); the per-call uniqueness comes from an
// appended WASM custom section (id 0) with random content, which changes the
// module's content hash -- and therefore its identity in the process-wide
// reactor host's engine map -- without altering guest behaviour, since any
// conformant loader skips custom sections. Without it, tests would share one
// engine and one registration key.
func wasmGuestBytes(t *testing.T) []byte {
	t.Helper()
	seamGuestBuild.Do(func() {
		dir, err := os.MkdirTemp("", "runnerwasmtest")
		if err != nil {
			t.Fatalf("mkdir temp: %v", err)
		}
		// See node/internal/code/script/wasm_supply_seam_test.go for the full
		// reasoning: the bytes land in seamGuestWasm, so the dir dies with this
		// closure. defer rather than t.Cleanup because sync.Once outlives the t
		// that happened to enter it first.
		defer func() { _ = os.RemoveAll(dir) }()
		out := filepath.Join(dir, "reactorseam.wasm")
		cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out,
			"../../node/internal/code/script/wasm/testdata/reactorseam/main.go")
		cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build reactorseam guest: %s", b)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("read reactorseam guest: %v", err)
		}
		seamGuestWasm = b
	})
	if seamGuestWasm == nil {
		t.Fatal("reactorseam guest build failed in an earlier test")
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return appendWasmCustomSection(seamGuestWasm, "xflow-test-nonce", nonce)
}

func appendWasmCustomSection(mod []byte, name string, payload []byte) []byte {
	content := appendULEB128(nil, uint64(len(name)))
	content = append(content, name...)
	content = append(content, payload...)

	out := append([]byte(nil), mod...)
	out = append(out, 0x00) // custom section id
	out = appendULEB128(out, uint64(len(content)))
	return append(out, content...)
}

func appendULEB128(b []byte, v uint64) []byte {
	for {
		c := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			c |= 0x80
		}
		b = append(b, c)
		if v == 0 {
			return b
		}
	}
}

// wasmActivationFixture is one test's isolated wasm module + supply: a freshly
// built guest, its artifact digest, a resolver that serves its bytes, and a
// per-test supply name whose content is already applied (mirroring what
// SupplyGate.Admit does before Activate runs).
type wasmActivationFixture struct {
	raw        []byte
	digest     string
	supplyName string
	revision   uint64
	resolver   func(ctx context.Context, digest string) ([]byte, error)
}

func newWasmActivationFixture(t *testing.T) *wasmActivationFixture {
	t.Helper()
	raw := wasmGuestBytes(t)
	f := &wasmActivationFixture{
		raw: raw,
		// store.ContentHash is what the control plane records as artifact_digest,
		// and the wasm host derives its module key from the same sha256 -- the
		// two must stay aligned or registration targets a module that does not
		// exist.
		digest:     store.ContentHash(raw),
		supplyName: testSupplyName(t),
		revision:   77,
	}
	f.resolver = func(_ context.Context, digest string) ([]byte, error) {
		if digest != f.digest {
			t.Errorf("resolver called with digest %q, want %q", digest, f.digest)
		}
		return f.raw, nil
	}
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      f.supplyName,
		Content:   []byte(`{"rules":[{"name":"from-supply"}]}`),
		Hash:      "h1",
		Revision:  f.revision,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply supply: %v", err)
	}
	return f
}

func (f *wasmActivationFixture) binding() engine.SupplyConsumerBinding {
	return engine.SupplyConsumerBinding{ModuleDigest: f.digest, SupplyNode: f.supplyName}
}

// wiringTestActivationOwner is the owner identity h.Activate registers wasm
// supply consumers under for every ActivateDirective in this file: WorkflowID
// "wf-1", EntryUnitID "trig", and zero-value Namespace/WorkflowVersion/
// ReplicaIndex (mirrors activationID.supplyConsumerOwner in
// activation_tracker.go). This file is `package runner`, so it can reach
// activationID directly rather than reconstructing the owner string by hand.
// cleanupBindings must release the SAME owner Activate registered under, or
// the release is a no-op against a set that never held that owner and the
// registration leaks into every later test in this binary.
var wiringTestActivationOwner = activationID{WorkflowID: "wf-1", EntryUnitID: "trig"}.supplyConsumerOwner()

// cleanupBindings unregisters bindings at test end. supply.Default outlives the
// test, so a registration left behind would keep rebuilding a dead module's pool
// on every later Apply in this binary.
func cleanupBindings(t *testing.T, bindings ...engine.SupplyConsumerBinding) {
	t.Helper()
	t.Cleanup(func() {
		for _, b := range bindings {
			node.UnregisterWasmSupplyConsumerByDigest(b.ModuleDigest, b.SupplyNode, wiringTestActivationOwner)
		}
	})
}

// runModule executes the module through the real ScriptNode path and returns the
// content revision that configured the pool. It is the end-behaviour probe this
// whole wiring exists for: 0 means the legacy globals path (no supply), a
// transient "no active pool" error means source-driven but never configured, and
// the supply's revision means the content actually reached the guest.
//
// It runs with the inline code string rather than the digest because the wasm
// host derives the same module key from the same bytes either way -- so this
// lands on the very engine the digest registration seeded.
func runModule(t *testing.T, raw []byte) (uint64, error) {
	t.Helper()
	out, err := (&node.ScriptNode{}).Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"code":     base64.StdEncoding.EncodeToString(raw),
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		Data: map[string]any{"payload": "x"},
	})
	if err != nil {
		return 0, err
	}
	// A source-driven module with no configured pool refuses traffic via the
	// error PORT, not a returned error (ScriptNode maps the node's transient
	// error onto out.Port). Both shapes mean the same thing here -- the module
	// is not serving -- so normalize them into one return value.
	if out.Port == "error" {
		return 0, fmt.Errorf("guest returned the error port: %#v", out.Data)
	}
	gen, _ := out.Data["config_generation"].(uint64)
	return gen, nil
}

// TestActivationCompilesModuleBeforeRegisteringConsumer is the deadlock gate.
//
// Registering a supply consumer for a module that has never been compiled is
// worse than not registering at all. The wasm host's notify handler returns nil
// when the engine is absent, the supply registry records that as "content
// accepted", and its re-notify condition then never fires again -- there is no
// waiting queue, redelivery depends entirely on someone calling Register again.
// Meanwhile registration marks the module source-driven, and a source-driven
// module with no configured pool REFUSES every message. The module would be
// permanently stuck: retries only re-enter Execute, which never triggers
// delivery.
//
// Compiling at activation time is what closes that window. This test asserts the
// end state that proves it: after Activate, the module runs and reports the
// SUPPLY's revision as the content that configured it.
func TestActivationCompilesModuleBeforeRegisteringConsumer(t *testing.T) {
	f := newWasmActivationFixture(t)
	cleanupBindings(t, f.binding())

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.binding()},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}

	gen, err := runModule(t, f.raw)
	if err != nil {
		t.Fatalf("module refuses traffic after activation (%v) -- the consumer was "+
			"registered against an uncompiled module, so the supply registry recorded "+
			"the content as accepted and will never redeliver it", err)
	}
	if gen != f.revision {
		t.Fatalf("config_generation = %d, want %d (the supply revision); %d means the "+
			"module fell back to the legacy globals path and evaluated against no rules",
			gen, f.revision, gen)
	}
}

// TestActivationRegistersWasmSupplyConsumer is the regression that replaces the
// old probe: a directive carrying a binding must leave exactly one consumer
// registered against that supply, so a later content change reaches the module.
func TestActivationRegistersWasmSupplyConsumer(t *testing.T) {
	f := newWasmActivationFixture(t)
	cleanupBindings(t, f.binding())
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.binding()},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if fh.gotInput == nil {
		t.Fatal("the trigger handler was never activated; this test would be " +
			"asserting the consumer count for the wrong reason")
	}
	if got := obs.get(f.supplyName); got != 1 {
		t.Fatalf("consumers for %q = %d, want 1 -- without a registration "+
			"OnSupplyChanged can never fire and the module evaluates against no rules",
			f.supplyName, got)
	}
}

// TestActivationWithoutArtifactResolverFailsClosed: a runner with no resolver
// cannot compile the module, so it cannot register the consumer either.
// Activating anyway would put a module into service that passes every record
// through untagged with no diagnostic -- exactly the failure this wiring exists
// to remove.
func TestActivationWithoutArtifactResolverFailsClosed(t *testing.T) {
	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}}) // no resolver

	err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		SupplyConsumers: []engine.SupplyConsumerBinding{{
			ModuleDigest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			SupplyNode:   "rules",
		}},
	})
	if err == nil {
		t.Fatal("expected fail-closed error when a binding is present but no artifact resolver is configured")
	}
	if fh.gotInput != nil {
		t.Fatal("the trigger subscription must not start when consumer registration cannot be completed")
	}
}

// TestGenerationUpgradeKeepsIdenticalBindingRegistered is the highest-value case
// in this file. A generation upgrade re-sends the SAME bindings, and a
// registration key is derived from (module digest, supply node) alone -- so the
// intuitive "unregister everything from the old activation, then register
// everything from the new one" removes the registration just made. The symptom
// would be that supply hot-reload silently stops working, with no diagnostic.
func TestGenerationUpgradeKeepsIdenticalBindingRegistered(t *testing.T) {
	f := newWasmActivationFixture(t)
	cleanupBindings(t, f.binding())
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.binding()},
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("first activate: %v", err)
	}
	d.Generation = 2
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("second activate: %v", err)
	}

	if got := obs.get(f.supplyName); got != 1 {
		t.Fatalf("consumers for %q = %d after a generation upgrade with identical "+
			"bindings, want 1 -- the upgrade unregistered the registration it had "+
			"just made", f.supplyName, got)
	}
	// The registration must still be live, not merely counted: a content change
	// after the upgrade has to reach the module.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      f.supplyName,
		Content:   []byte(`{"rules":[{"name":"after-upgrade"}]}`),
		Hash:      "h2",
		Revision:  f.revision + 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply updated supply: %v", err)
	}
	gen, err := runModule(t, f.raw)
	if err != nil {
		t.Fatalf("module refuses traffic after the upgrade: %v", err)
	}
	if gen != f.revision+1 {
		t.Fatalf("config_generation = %d, want %d -- the post-upgrade content never "+
			"reached the module, so hot reload is silently dead", gen, f.revision+1)
	}
}

// TestGenerationUpgradeSwapsChangedBinding: when the new activation binds the
// same module to a DIFFERENT supply, the old pairing must be dropped and the
// new one registered. This is the other half of the diff -- a handler that
// never unregisters would leave the module rebuilding its pool from a supply
// the workflow no longer references.
func TestGenerationUpgradeSwapsChangedBinding(t *testing.T) {
	f := newWasmActivationFixture(t)
	oldBinding := f.binding()
	newBinding := engine.SupplyConsumerBinding{ModuleDigest: f.digest, SupplyNode: f.supplyName + "-v2"}
	cleanupBindings(t, oldBinding, newBinding)
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		SupplyConsumers: []engine.SupplyConsumerBinding{oldBinding},
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("first activate: %v", err)
	}
	d.Generation = 2
	d.SupplyConsumers = []engine.SupplyConsumerBinding{newBinding}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("second activate: %v", err)
	}

	if got := obs.get(oldBinding.SupplyNode); got != 0 {
		t.Fatalf("consumers for the dropped supply %q = %d, want 0", oldBinding.SupplyNode, got)
	}
	if got := obs.get(newBinding.SupplyNode); got != 1 {
		t.Fatalf("consumers for the new supply %q = %d, want 1", newBinding.SupplyNode, got)
	}
}

// TestDeactivateUnregistersSupplyConsumers: a workflow that leaves this runner
// must stop rebuilding its module's pool on every content change.
// DeactivateDirective carries only the activation identity, so the bindings have
// to be remembered from Activate -- if they are not, this count stays at 1.
func TestDeactivateUnregistersSupplyConsumers(t *testing.T) {
	f := newWasmActivationFixture(t)
	cleanupBindings(t, f.binding())
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		SupplyConsumers: []engine.SupplyConsumerBinding{f.binding()},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := obs.get(f.supplyName); got != 1 {
		t.Fatalf("precondition: consumers = %d, want 1", got)
	}

	if err := h.Deactivate(protocol.DeactivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", Generation: 1,
	}); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if got := obs.get(f.supplyName); got != 0 {
		t.Fatalf("consumers for %q = %d after Deactivate, want 0", f.supplyName, got)
	}
	if !fh.sub.closed {
		t.Error("the trigger subscription must still be closed on Deactivate")
	}
}

// TestActivationWithoutSupplyConsumersRegistersNothing is the backward-compat
// case: a directive from a control plane that does not send SupplyConsumers (or
// for a workflow with no wasm consumer) must activate exactly as before and
// register nothing -- including when no artifact resolver is configured at all.
func TestActivationWithoutSupplyConsumersRegistersNothing(t *testing.T) {
	supplyName := testSupplyName(t)
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}})

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies: []engine.SupplyRequirement{{Node: supplyName}},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if fh.gotInput == nil {
		t.Fatal("the trigger handler was never activated")
	}
	if got := obs.get(supplyName); got != 0 {
		t.Fatalf("consumers for %q = %d, want 0 for a directive carrying no bindings", supplyName, got)
	}
}
