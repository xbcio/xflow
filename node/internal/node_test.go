package internal

// This file is the first test file ever added to node/internal (the
// top-level package, not its subpackages). Before this, every line in
// base.go, node.go and base_trigger.go could be changed arbitrarily and
// `go test ./node/internal/` would still print "ok" -- there were no test
// files, so there was nothing to run.
//
// Package choice: this file uses `package internal` (white-box), not
// `internal_test`, specifically because two of the guards under test
// (newBuilder's and newTriggerBuilder's own nil/empty-type checks) live on
// unexported functions that are, today, only ever called with
// already-validated arguments by Definition.New/TriggerDefinition.New. The
// only way to exercise those guards directly -- as opposed to exercising
// Define/DefineTrigger's own, separate guards -- is to call the unexported
// functions themselves.

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// expectPanic runs fn and fails the test unless it panics with a message
// containing want. Asserting on the message (not just "panicked") matters
// here: newBuilder(nil, ...) and newBuilder(emptyType, ...) both panic, but
// removing one guard while leaving the other would still produce *a* panic
// (a nil interface method call panics too) -- just with a different,
// unintended message. Matching the exact guard message is what tells the
// two guards apart.
func expectPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected a panic containing %q, got none", want)
		}
		msg, ok := r.(string)
		if !ok {
			if err, ok := r.(error); ok {
				msg = err.Error()
			} else {
				t.Fatalf("expected a panic containing %q, got non-string/error panic value: %#v", want, r)
			}
		}
		if !strings.Contains(msg, want) {
			t.Fatalf("panic message = %q, want it to contain %q", msg, want)
		}
	}()
	fn()
}

// fakeActionHandler is a minimal types.ActionHandler used only to drive
// newBuilder's own guards with a non-nil handler whose Descriptor().Type is
// empty -- the one shape Define() itself can never produce (Define already
// panics on an empty nodeType before newBuilder is ever called).
type fakeActionHandler struct {
	desc types.Descriptor
}

func (f *fakeActionHandler) Descriptor() types.Descriptor { return f.desc }
func (f *fakeActionHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

// fakeTriggerHandler is the trigger-side counterpart of fakeActionHandler,
// used to drive newTriggerBuilder's own guards directly.
type fakeTriggerHandler struct {
	desc types.Descriptor
}

func (f *fakeTriggerHandler) Descriptor() types.Descriptor { return f.desc }
func (f *fakeTriggerHandler) Activate(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return nil, nil
}

// fakeSubscription is a comparable stand-in for types.TriggerSubscription:
// unlike a func-based implementation (e.g. types.CloseFunc), a pointer type
// can be compared with != to confirm a value came back unmodified through
// triggerRef.TriggerHandler().Activate.
type fakeSubscription struct{ id int }

func (f *fakeSubscription) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------
// 1. BaseNode.NodeVersion (base.go:13-17)
// ---------------------------------------------------------------------------

// TestBaseNode_NodeVersion_DefaultsToOne pins the `if b.version == 0` branch.
//
// Verified by grep across the whole module (`grep -rn '\.version' node/`):
// there is no setter for BaseNode.version anywhere, and no concrete node
// type in this repo (action/, flow/, transform/, code/, group/, trigger/*)
// ever assigns it. So the zero value below is not a contrived corner case --
// it is the version every real node handler in the system reports today via
// the `interface{ NodeVersion() int }` assertions in node/registry and
// sdk/xflow/builder.go. Flipping `== 0` to `!= 0` would make every node
// report version 0 instead of the documented default of 1.
//
// This intentionally does NOT test the `return b.version` (non-zero) arm.
// version is an unexported field with no setter, so no code outside this
// package -- and no code inside it either, since only base.go touches the
// field -- can ever produce a non-zero BaseNode.version. A test that pokes
// version to a non-zero value would only be exercising white-box field
// assignment I added for the test, not any reachable behavior. The task
// asked me to be honest rather than fake teeth here, so I'm leaving that arm
// untested: the one test below already fails on the described mutation
// (`== 0` -> `!= 0`) because it flips the only value BaseNode.version can
// ever actually hold.
func TestBaseNode_NodeVersion_DefaultsToOne(t *testing.T) {
	var b BaseNode
	if got := b.NodeVersion(); got != 1 {
		t.Fatalf("NodeVersion() on a zero-value BaseNode = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// 2. ExecuteTriggerEntry (node.go:186-197)
// ---------------------------------------------------------------------------

// TestExecuteTriggerEntry_PreservesInputData guards against dropping
// upstream $input data. Every trigger node's Execute (via BaseTrigger, or a
// custom trigger's own Execute) funnels through here, so this is the one
// place that decides whether $input survives from the trigger's activation
// payload into the node's main output port.
func TestExecuteTriggerEntry_PreservesInputData(t *testing.T) {
	in := &types.Input{Data: map[string]any{"a": 1, "b": "two"}}

	out, err := ExecuteTriggerEntry(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("Port = %q, want %q", out.Port, "main")
	}
	if out.Data["a"] != 1 || out.Data["b"] != "two" {
		t.Fatalf("upstream input data was dropped, got Data = %#v", out.Data)
	}
}

// TestExecuteTriggerEntry_DefaultsTriggerEventWhenAbsent guards the "no
// trigger key yet -> synthesize an empty *types.TriggerEvent" fallback.
// Downstream expressions read $input.trigger unconditionally; dropping this
// fallback would turn every such reference into a nil-map access instead of
// a defined (if empty) event.
func TestExecuteTriggerEntry_DefaultsTriggerEventWhenAbsent(t *testing.T) {
	in := &types.Input{Data: map[string]any{"x": "y"}}

	out, err := ExecuteTriggerEntry(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ev, ok := out.Data["trigger"].(*types.TriggerEvent)
	if !ok || ev == nil {
		t.Fatalf("missing default trigger event fallback, Data[\"trigger\"] = %#v", out.Data["trigger"])
	}
	if !reflect.DeepEqual(*ev, types.TriggerEvent{}) {
		t.Fatalf("default trigger event is not zero-valued: %#v", *ev)
	}
}

// TestExecuteTriggerEntry_PreservesExistingTriggerEvent guards the other
// side of the same fallback: when the input already carries a "trigger"
// key (e.g. re-entry, or a caller that pre-populated it), it must be passed
// through untouched rather than clobbered by the default.
func TestExecuteTriggerEntry_PreservesExistingTriggerEvent(t *testing.T) {
	custom := &types.TriggerEvent{ID: "evt-1"}
	in := &types.Input{Data: map[string]any{"trigger": custom}}

	out, err := ExecuteTriggerEntry(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["trigger"] != custom {
		t.Fatalf("existing trigger event was overwritten, got %#v, want the original %#v", out.Data["trigger"], custom)
	}
}

// TestExecuteTriggerEntry_NilInputIsSafe guards the nil-input path used by
// TriggerDefinition.Execute callers that may not always supply an *Input.
func TestExecuteTriggerEntry_NilInputIsSafe(t *testing.T) {
	out, err := ExecuteTriggerEntry(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatalf("ExecuteTriggerEntry(nil) returned a nil output")
	}
	if out.Port != "main" {
		t.Fatalf("Port = %q, want %q", out.Port, "main")
	}
	if _, ok := out.Data["trigger"]; !ok {
		t.Fatalf("nil input did not get the default trigger event fallback")
	}
}

// TestExecuteTriggerEntry_DoesNotAliasInputData pins the copy loop, as
// distinct from the three tests above which only read the *returned* map and
// therefore stay green if the loop is replaced by `data := input.Data`.
//
// That aliasing is not benign. ExecuteTriggerEntry unconditionally writes a
// "trigger" key into `data` when one is absent, so an aliased map means every
// trigger node's Execute silently mutates the caller's own $input on the way
// out. Asserting on the *input* map after the call is the only way to tell a
// copy from an alias.
func TestExecuteTriggerEntry_DoesNotAliasInputData(t *testing.T) {
	src := map[string]any{"x": "y"}
	in := &types.Input{Data: src}

	out, err := ExecuteTriggerEntry(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := src["trigger"]; ok {
		t.Fatalf("ExecuteTriggerEntry wrote its default trigger event back into the caller's input map: %#v", src)
	}
	// Writing through the returned map must not reach the caller's map either.
	out.Data["injected"] = true
	if _, ok := src["injected"]; ok {
		t.Fatalf("the returned Data map aliases input.Data: %#v", src)
	}
}

// ---------------------------------------------------------------------------
// 3. nodeRef.OnError (node.go:60-63)
// ---------------------------------------------------------------------------

// TestNodeRef_OnErrorSetsStrategy guards against OnError being turned into a
// no-op that just returns the receiver. A no-op would make every workflow
// author's `.OnError(xflow.OnErrorContinue)` call silently do nothing, with
// the node falling back to the stop-on-error default instead.
func TestNodeRef_OnErrorSetsStrategy(t *testing.T) {
	def := Define("xflow.test.noderef_onerror", func(context.Context, *types.Input) (*types.Output, error) {
		return &types.Output{}, nil
	})
	b := def.New(map[string]any{"p": 1})

	if got := b.OnErrorStrategy(); got != "" {
		t.Fatalf("OnErrorStrategy() before OnError = %q, want empty", got)
	}

	b2 := b.OnError(types.OnErrorContinue)
	if got := b2.OnErrorStrategy(); got != types.OnErrorContinue {
		t.Fatalf("OnErrorStrategy() after OnError(Continue) = %q, want %q", got, types.OnErrorContinue)
	}
	// OnError mutates the receiver in place (and returns it for chaining) --
	// it must not detach a copy. Observing the mutation through the original
	// reference is what would catch a no-op that returns `r` unchanged but
	// never assigns r.onError.
	if got := b.OnErrorStrategy(); got != types.OnErrorContinue {
		t.Fatalf("original builder OnErrorStrategy() = %q after OnError, want %q", got, types.OnErrorContinue)
	}
}

// ---------------------------------------------------------------------------
// 4. newBuilder's panic guards (node.go:67-76)
// ---------------------------------------------------------------------------

// TestNewBuilder_PanicsOnNilHandler exercises newBuilder directly (not via
// Define, which has its own, separate nil check on `execute` before
// newBuilder is ever reached) with a literal nil handler.
func TestNewBuilder_PanicsOnNilHandler(t *testing.T) {
	expectPanic(t, "handler must not be nil", func() {
		newBuilder(nil, nil)
	})
}

// TestNewBuilder_PanicsOnEmptyDescriptorType exercises newBuilder's second
// guard with a handler that is non-nil but reports an empty Descriptor().Type
// -- a shape Define() itself can never produce, since Define already
// panics on an empty nodeType before constructing the Definition that
// becomes the handler.
func TestNewBuilder_PanicsOnEmptyDescriptorType(t *testing.T) {
	h := &fakeActionHandler{desc: types.Descriptor{Type: ""}}
	expectPanic(t, "empty Descriptor().Type", func() {
		newBuilder(h, nil)
	})
}

// ---------------------------------------------------------------------------
// 5. DefineTrigger's panic guards and produced Descriptor (node.go:199-213)
// ---------------------------------------------------------------------------

func TestDefineTrigger_PanicsOnEmptyNodeType(t *testing.T) {
	expectPanic(t, "nodeType must not be empty", func() {
		DefineTrigger("", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			return nil, nil
		})
	})
}

func TestDefineTrigger_PanicsOnNilActivate(t *testing.T) {
	expectPanic(t, "activate must not be nil", func() {
		DefineTrigger("xflow.test.define_trigger_nil_activate", nil)
	})
}

// TestDefineTrigger_DescriptorKindAndOutputs guards the Kind and Outputs
// DefineTrigger bakes into every trigger's Descriptor. Kind drives compiler
// and registry trigger/action routing; Outputs is what lets a graph wire a
// downstream node to this trigger's "main" port at all -- an empty Outputs
// would make every trigger node unwireable.
func TestDefineTrigger_DescriptorKindAndOutputs(t *testing.T) {
	td := DefineTrigger("xflow.test.define_trigger_descriptor", func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		return nil, nil
	})
	d := td.Descriptor()

	if d.Kind != types.NodeKindTrigger {
		t.Fatalf("Descriptor().Kind = %q, want %q", d.Kind, types.NodeKindTrigger)
	}
	if len(d.Outputs) != 1 || d.Outputs[0].Name != "main" || d.Outputs[0].DisplayName != "Main" {
		t.Fatalf("Descriptor().Outputs = %#v, want [{main Main}]", d.Outputs)
	}
}

// ---------------------------------------------------------------------------
// 6. triggerRef and newTriggerBuilder (node.go:78-104)
// ---------------------------------------------------------------------------

// TestNewTriggerBuilder_PanicsOnNilHandler and
// TestNewTriggerBuilder_PanicsOnEmptyDescriptorType are the trigger-side
// mirror of the newBuilder guard tests above, for the same reason: these
// guards are unreachable via DefineTrigger's own call path (which already
// rejects a nil activate func / empty nodeType earlier), so only a direct
// call proves they still exist.
func TestNewTriggerBuilder_PanicsOnNilHandler(t *testing.T) {
	expectPanic(t, "handler must not be nil", func() {
		newTriggerBuilder(nil, nil)
	})
}

func TestNewTriggerBuilder_PanicsOnEmptyDescriptorType(t *testing.T) {
	h := &fakeTriggerHandler{desc: types.Descriptor{Type: ""}}
	expectPanic(t, "empty Descriptor().Type", func() {
		newTriggerBuilder(h, nil)
	})
}

// TestTriggerRef_BuilderContract is triggerRef's only coverage in this repo.
// It goes through the exported DefineTrigger/.New path (the same path every
// real trigger node factory in node/trigger/* uses) and then exercises every
// triggerRef method: NodeType, RawParams, OnError/OnErrorStrategy, and --
// via the exported TriggerHandlerCarrier assertion -- TriggerHandler(),
// which is what lets the SDK's embedded runtime activate a trigger without
// going through the registry.
func TestTriggerRef_BuilderContract(t *testing.T) {
	activated := false
	var gotInput *types.TriggerActivateInput
	sub := &fakeSubscription{id: 7}
	activateErr := errors.New("boom")

	td := DefineTrigger("xflow.test.triggerref", func(_ context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
		activated = true
		gotInput = in
		return sub, activateErr
	})

	b := td.New(map[string]any{"k": "v"})

	if got := b.NodeType(); got != "xflow.test.triggerref" {
		t.Fatalf("NodeType() = %q, want %q", got, "xflow.test.triggerref")
	}
	params, ok := b.RawParams().(map[string]any)
	if !ok || params["k"] != "v" {
		t.Fatalf("RawParams() = %#v, want map[string]any{\"k\":\"v\"}", b.RawParams())
	}
	if got := b.OnErrorStrategy(); got != "" {
		t.Fatalf("OnErrorStrategy() before OnError = %q, want empty", got)
	}

	b2 := b.OnError(types.OnErrorStop)
	if got := b2.OnErrorStrategy(); got != types.OnErrorStop {
		t.Fatalf("OnErrorStrategy() after OnError(Stop) = %q, want %q", got, types.OnErrorStop)
	}
	if got := b.OnErrorStrategy(); got != types.OnErrorStop {
		t.Fatalf("original builder OnErrorStrategy() = %q after OnError, want %q", got, types.OnErrorStop)
	}

	carrier, ok := b.(TriggerHandlerCarrier)
	if !ok {
		t.Fatalf("builder returned by TriggerDefinition.New does not implement TriggerHandlerCarrier")
	}
	handler := carrier.TriggerHandler()
	if handler == nil {
		t.Fatalf("TriggerHandler() returned nil")
	}

	gotSub, gotErr := handler.Activate(context.Background(), &types.TriggerActivateInput{NodeName: "n1"})
	if !activated {
		t.Fatalf("handler.Activate did not invoke the underlying activate func")
	}
	if gotInput == nil || gotInput.NodeName != "n1" {
		t.Fatalf("handler.Activate did not forward its input, got %#v", gotInput)
	}
	if gotSub != sub {
		t.Fatalf("handler.Activate did not forward its returned subscription")
	}
	if !errors.Is(gotErr, activateErr) {
		t.Fatalf("handler.Activate did not forward its returned error, got %v", gotErr)
	}
}

// ---------------------------------------------------------------------------
// 7. BaseTrigger.Execute (base_trigger.go:25-27)
// ---------------------------------------------------------------------------

// TestBaseTrigger_ExecuteDelegatesToTriggerEntry.
//
// Nothing inside node/internal/ (this package) itself references
// BaseTrigger -- it's only embedded by the five trigger node types in
// node/trigger/* (webhook, redishub, timer, kafka, cron; confirmed by grep).
// But those five are real production node types, and BaseTrigger.Execute is
// their entire Execute implementation: it is not dead code, it's shared
// code that happens to live one package away from all of its callers. Its
// documented contract (base_trigger.go's own comment) is "delegate to the
// common trigger entry wrapper so every trigger node behaves identically
// when invoked as an action" -- so this test pins that delegation directly
// against ExecuteTriggerEntry as the reference implementation, using the
// same package access those five trigger packages don't have.
func TestBaseTrigger_ExecuteDelegatesToTriggerEntry(t *testing.T) {
	var bt BaseTrigger
	in := &types.Input{Data: map[string]any{"k": "v"}}

	got, err := bt.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("BaseTrigger.Execute returned a nil output")
	}

	want, wantErr := ExecuteTriggerEntry(in)
	if wantErr != nil {
		t.Fatalf("reference ExecuteTriggerEntry call failed: %v", wantErr)
	}
	if got.Port != want.Port {
		t.Fatalf("Port = %q, want %q", got.Port, want.Port)
	}
	if got.Data["k"] != want.Data["k"] {
		t.Fatalf("Data[\"k\"] = %#v, want %#v", got.Data["k"], want.Data["k"])
	}
	if _, ok := got.Data["trigger"]; !ok {
		t.Fatalf("BaseTrigger.Execute did not apply the default trigger event fallback")
	}
}

// ---------------------------------------------------------------------------
// 8. nodeRef / Definition builder contract (node.go:57-65, 135-175)
// ---------------------------------------------------------------------------

// TestNodeRef_BuilderContract is the action-side mirror of
// TestTriggerRef_BuilderContract above. The trigger half of this package had
// its whole builder contract pinned; the action half only had OnError. That
// left nodeRef.NodeType/RawParams/Handler/Descriptor and Definition.Execute's
// forwarding with no assertion anywhere, and it left every Definition
// descriptor builder (DisplayName/Param/Input/Output/Credential) free to
// silently drop what it was handed -- which for Credential means a custom
// node's declared credential dependency never reaching the compiler, and so
// never being resolved at run time.
//
// Define's own two panic guards are deliberately NOT tested here:
// node/definition_test.go already covers both (TestDefinePanicsOnEmptyType,
// TestDefinePanicsOnNilExecute), and deleting either guard turns those red.
func TestNodeRef_BuilderContract(t *testing.T) {
	var gotInput *types.Input
	executed := false
	executeErr := errors.New("boom")

	def := Define("xflow.test.noderef_contract", func(_ context.Context, in *types.Input) (*types.Output, error) {
		executed = true
		gotInput = in
		return &types.Output{Port: "alt"}, executeErr
	}).
		DisplayName("NodeRef Contract").
		Param(types.ParamSpec{Name: "p", Type: types.ParamString, Required: true}).
		Input("main").
		Output("main").
		Output("alt").
		Credential("cred-a")

	b := def.New(map[string]any{"p": "v"})

	if got := b.NodeType(); got != "xflow.test.noderef_contract" {
		t.Fatalf("NodeType() = %q, want %q", got, "xflow.test.noderef_contract")
	}
	params, ok := b.RawParams().(map[string]any)
	if !ok || params["p"] != "v" {
		t.Fatalf("RawParams() = %#v, want map[string]any{\"p\":\"v\"}", b.RawParams())
	}

	// Descriptor() must come from the handler, carrying everything the
	// Definition builders appended -- not be re-synthesized from nodeType.
	// types.Builder itself does not declare Descriptor(); the compiler and
	// registry reach it through exactly this kind of assertion, so the test
	// does the same rather than reading def.Descriptor() directly, which
	// would bypass nodeRef's delegation entirely.
	desc, ok := b.(interface{ Descriptor() types.Descriptor })
	if !ok {
		t.Fatalf("builder returned by Definition.New does not expose Descriptor()")
	}
	d := desc.Descriptor()
	if d.Type != "xflow.test.noderef_contract" || d.Kind != types.NodeKindAction {
		t.Fatalf("Descriptor() Type/Kind = %q/%q, want %q/%q", d.Type, d.Kind, "xflow.test.noderef_contract", types.NodeKindAction)
	}
	if d.DisplayName != "NodeRef Contract" {
		t.Fatalf("Descriptor().DisplayName = %q, want %q", d.DisplayName, "NodeRef Contract")
	}
	if len(d.Params) != 1 || d.Params[0].Name != "p" || !d.Params[0].Required {
		t.Fatalf("Descriptor().Params = %#v, want one required param named p", d.Params)
	}
	if len(d.Inputs) != 1 || d.Inputs[0].Name != "main" {
		t.Fatalf("Descriptor().Inputs = %#v, want [{main}]", d.Inputs)
	}
	// Two Output() calls in order: appending, not overwriting, is the contract.
	if len(d.Outputs) != 2 || d.Outputs[0].Name != "main" || d.Outputs[1].Name != "alt" {
		t.Fatalf("Descriptor().Outputs = %#v, want [{main} {alt}]", d.Outputs)
	}
	if len(d.Credentials) != 1 || d.Credentials[0] != "cred-a" {
		t.Fatalf("Descriptor().Credentials = %#v, want [cred-a]", d.Credentials)
	}

	// Handler() is what lets the SDK run a custom node in-process without the
	// registry; it must hand back the Definition itself, and that Definition's
	// Execute must forward ctx/input and both return values verbatim.
	carrier, ok := b.(HandlerCarrier)
	if !ok {
		t.Fatalf("builder returned by Definition.New does not implement HandlerCarrier")
	}
	handler := carrier.Handler()
	if handler != types.ActionHandler(def) {
		t.Fatalf("Handler() = %#v, want the Definition that produced the builder", handler)
	}

	in := &types.Input{Data: map[string]any{"k": "v"}}
	out, err := handler.Execute(context.Background(), in)
	if !executed {
		t.Fatalf("Definition.Execute did not invoke the underlying execute func")
	}
	if gotInput != in {
		t.Fatalf("Definition.Execute did not forward its input, got %#v", gotInput)
	}
	if out == nil || out.Port != "alt" {
		t.Fatalf("Definition.Execute did not forward its returned output, got %#v", out)
	}
	if !errors.Is(err, executeErr) {
		t.Fatalf("Definition.Execute did not forward its returned error, got %v", err)
	}
}
