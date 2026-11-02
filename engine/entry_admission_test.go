package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// fakeEntryAdmissionState is an EntryAdmissionStore test double whose
// SeedExecutionFromEntry response/error is fixed per test case, so tests can
// drive every outcome classification (accepted/duplicate/conflict/error)
// deterministically without a real backend.
type fakeEntryAdmissionState struct {
	*fakeState
	resp    SeedExecutionFromEntryResponse
	err     error
	calls   int
	lastReq SeedExecutionFromEntryRequest
	mutate  func(SeedExecutionFromEntryRequest)
}

func (f *fakeEntryAdmissionState) SeedExecutionFromEntry(_ context.Context, req SeedExecutionFromEntryRequest) (SeedExecutionFromEntryResponse, error) {
	f.calls++
	f.lastReq = req
	if f.mutate != nil {
		f.mutate(f.lastReq)
	}
	return f.resp, f.err
}

var _ EntryAdmissionStore = (*fakeEntryAdmissionState)(nil)

// fakeLimitedEntryAdmissionState additionally applies a workflow's concurrency
// policy and records the admission it was handed, so a test can assert that the
// engine judged a seed by the definition it was seeded with — the limit a
// re-published version declares applies to the seeds that resolve to it and to
// no others, which is only true if the engine reads the limit off the seed's own
// compiled graph.
type fakeLimitedEntryAdmissionState struct {
	*fakeEntryAdmissionState
	limitedCalls int
	admissions   []ConcurrencyAdmission
}

func (f *fakeLimitedEntryAdmissionState) SeedExecutionFromEntryWithConcurrency(ctx context.Context, req SeedExecutionFromEntryRequest, admission ConcurrencyAdmission) (SeedExecutionFromEntryResponse, error) {
	f.limitedCalls++
	f.admissions = append(f.admissions, admission)
	return f.fakeEntryAdmissionState.SeedExecutionFromEntry(ctx, req)
}

var _ ConcurrencyLimitedEntryAdmissionStore = (*fakeLimitedEntryAdmissionState)(nil)

// limitedEntryAdmissionEngine builds an engine over the limiter-capable fake.
func limitedEntryAdmissionEngine(t *testing.T, resp SeedExecutionFromEntryResponse, err error) (*Engine, *fakeLimitedEntryAdmissionState) {
	t.Helper()
	state := &fakeLimitedEntryAdmissionState{
		fakeEntryAdmissionState: &fakeEntryAdmissionState{fakeState: newFakeState(), resp: resp, err: err},
	}
	return New(state, &fakeQueue{}), state
}

// entryAdmissionConcurrencyGraph compiles the single-root trigger group a
// Kafka-style seed carries, with the workflow-level concurrency setting under
// test attached to it.
func entryAdmissionConcurrencyGraph(t *testing.T, settings *types.ConcurrencySettings) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "entry-admission-limited",
		Settings: &types.WorkflowSettings{Concurrency: settings},
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("compile limited entry-admission graph: %v", err)
	}
	return g
}

// findNonGroupUnit locates a UnitNode-kind unit's index in g — the negative
// control counterpart to findGroupUnit (group_observer_test.go), used to drive
// the entry-admission discriminator down its "not a group" branch.
func findNonGroupUnit(t *testing.T, g *graph.Graph) int {
	t.Helper()
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) != graph.UnitGroup {
			return i
		}
	}
	t.Fatal("fixture regressed: no non-group unit found")
	return -1
}

// newEntryAdmissionEngine builds an engine over fakeEntryAdmissionState (so
// e.state implements EntryAdmissionStore) with the given fixed
// response/error and options applied.
func newEntryAdmissionEngine(t *testing.T, resp SeedExecutionFromEntryResponse, err error, opts ...Option) *Engine {
	t.Helper()
	state := &fakeEntryAdmissionState{fakeState: newFakeState(), resp: resp, err: err}
	return New(state, &fakeQueue{}, opts...)
}

func buildEntryAdmissionOutputPolicyGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "entry-admission-output-policy",
		Nodes: []types.NodeDef{
			{Name: "public", Type: "test.action", Kind: types.NodeKindAction},
			{
				Name:   "private",
				Type:   "test.action",
				Kind:   types.NodeKindAction,
				Output: &types.NodeOutputPolicy{Private: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("compile output-policy graph: %v", err)
	}
	return g
}

// graphWithInvalidNodeIndex builds a graph snapshot whose index claims a node
// exists at an out-of-bounds position. Graph.UnmarshalJSON intentionally
// retains the persisted index, so this exercises the admission bounds guard
// without reaching into graph's unexported fields.
func graphWithInvalidNodeIndex(t *testing.T, source *graph.Graph, name string) *graph.Graph {
	t.Helper()
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("marshal source graph: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode source graph wire form: %v", err)
	}
	var index map[string]int
	if err := json.Unmarshal(wire["index"], &index); err != nil {
		t.Fatalf("decode graph index: %v", err)
	}
	index[name] = source.NodeCount()
	wire["index"], err = json.Marshal(index)
	if err != nil {
		t.Fatalf("encode corrupt graph index: %v", err)
	}
	data, err = json.Marshal(wire)
	if err != nil {
		t.Fatalf("encode corrupt graph: %v", err)
	}
	var corrupted graph.Graph
	if err := json.Unmarshal(data, &corrupted); err != nil {
		t.Fatalf("decode corrupt graph: %v", err)
	}
	return &corrupted
}

func TestSeedExecutionFromEntry_DerivesExitPrivacyFromTrustedGraph(t *testing.T) {
	policyGraph := buildEntryAdmissionOutputPolicyGraph(t)
	invalidIndexGraph := graphWithInvalidNodeIndex(t, policyGraph, "public")

	cases := []struct {
		name          string
		graph         *graph.Graph
		nodeName      string
		inboundMarker bool
		wantPrivate   bool
	}{
		{
			name:          "public graph node overrides inbound private marker",
			graph:         policyGraph,
			nodeName:      "public",
			inboundMarker: true,
			wantPrivate:   false,
		},
		{
			name:          "private graph node overrides inbound public marker",
			graph:         policyGraph,
			nodeName:      "private",
			inboundMarker: false,
			wantPrivate:   true,
		},
		{
			name:          "nil graph fails closed",
			graph:         nil,
			nodeName:      "public",
			inboundMarker: false,
			wantPrivate:   true,
		},
		{
			name:          "unknown node fails closed",
			graph:         policyGraph,
			nodeName:      "not-in-graph",
			inboundMarker: false,
			wantPrivate:   true,
		},
		{
			name:          "invalid graph index fails closed",
			graph:         invalidIndexGraph,
			nodeName:      "public",
			inboundMarker: false,
			wantPrivate:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &fakeEntryAdmissionState{
				fakeState: newFakeState(),
				resp:      SeedExecutionFromEntryResponse{State: AdmissionStateAccepted},
			}
			eng := New(state, &fakeQueue{})
			req := SeedExecutionFromEntryRequest{
				Graph: tc.graph,
				Exits: []BoundaryExit{{
					NodeName:      tc.nodeName,
					Port:          "main",
					Data:          map[string]any{"secret": "value"},
					PrivateOutput: tc.inboundMarker,
				}},
			}

			if _, err := eng.SeedExecutionFromEntry(context.Background(), req); err != nil {
				t.Fatalf("SeedExecutionFromEntry() error = %v", err)
			}
			if state.calls != 1 {
				t.Fatalf("store calls = %d, want 1", state.calls)
			}
			if got := state.lastReq.Exits[0].PrivateOutput; got != tc.wantPrivate {
				t.Fatalf("store exit PrivateOutput = %t, want %t", got, tc.wantPrivate)
			}
			if got := req.Exits[0].PrivateOutput; got != tc.inboundMarker {
				t.Fatalf("caller exit PrivateOutput mutated to %t, want original %t", got, tc.inboundMarker)
			}
			if &state.lastReq.Exits[0] == &req.Exits[0] {
				t.Fatal("store received caller's exit slice instead of an admission-owned clone")
			}
		})
	}
}

func TestSeedExecutionFromEntry_DeepClonesExitData(t *testing.T) {
	callerNested := map[string]any{"value": "original"}
	callerListItem := map[string]any{"value": "original"}
	callerList := []any{callerListItem}
	callerStrings := []string{"original"}
	callerTypedMap := map[string][]string{"values": {"original"}}
	callerData := map[string]any{
		"nested":    callerNested,
		"list":      callerList,
		"strings":   callerStrings,
		"typed_map": callerTypedMap,
	}

	state := &fakeEntryAdmissionState{
		fakeState: newFakeState(),
		resp:      SeedExecutionFromEntryResponse{State: AdmissionStateAccepted},
		mutate: func(admission SeedExecutionFromEntryRequest) {
			data := admission.Exits[0].Data
			data["store_only"] = "store mutation"
			data["nested"].(map[string]any)["value"] = "store mutation"
			data["list"].([]any)[0].(map[string]any)["value"] = "store mutation"
			data["strings"].([]string)[0] = "store mutation"
			data["typed_map"].(map[string][]string)["values"][0] = "store mutation"
		},
	}
	eng := New(state, &fakeQueue{})
	req := SeedExecutionFromEntryRequest{
		Graph: buildEntryAdmissionOutputPolicyGraph(t),
		Exits: []BoundaryExit{{
			NodeName: "public",
			Port:     "main",
			Data:     callerData,
		}},
	}

	if _, err := eng.SeedExecutionFromEntry(context.Background(), req); err != nil {
		t.Fatalf("SeedExecutionFromEntry() error = %v", err)
	}

	if _, ok := callerData["store_only"]; ok {
		t.Fatal("store mutation leaked into caller's top-level Data map")
	}
	if got := callerNested["value"]; got != "original" {
		t.Fatalf("caller nested map = %q, want original", got)
	}
	if got := callerListItem["value"]; got != "original" {
		t.Fatalf("caller nested list map = %q, want original", got)
	}
	if got := callerStrings[0]; got != "original" {
		t.Fatalf("caller nested string slice = %q, want original", got)
	}
	if got := callerTypedMap["values"][0]; got != "original" {
		t.Fatalf("caller typed nested container = %q, want original", got)
	}

	// The isolation is bidirectional: a caller retrying with a mutated request
	// cannot retrospectively alter the backend's admitted input either.
	callerData["caller_only"] = "caller mutation"
	callerNested["value"] = "caller mutation"
	callerListItem["value"] = "caller mutation"
	callerStrings[0] = "caller mutation"
	callerTypedMap["values"][0] = "caller mutation"

	stored := state.lastReq.Exits[0].Data
	if _, ok := stored["caller_only"]; ok {
		t.Fatal("caller mutation leaked into store-owned top-level Data map")
	}
	if got := stored["nested"].(map[string]any)["value"]; got != "store mutation" {
		t.Fatalf("store nested map = %q, want store mutation", got)
	}
	if got := stored["list"].([]any)[0].(map[string]any)["value"]; got != "store mutation" {
		t.Fatalf("store nested list map = %q, want store mutation", got)
	}
	if got := stored["strings"].([]string)[0]; got != "store mutation" {
		t.Fatalf("store nested string slice = %q, want store mutation", got)
	}
	if got := stored["typed_map"].(map[string][]string)["values"][0]; got != "store mutation" {
		t.Fatalf("store typed nested container = %q, want store mutation", got)
	}
}

func TestComputeResultHash_IgnoresPrivateOutputMarker(t *testing.T) {
	exits := []BoundaryExit{{
		NodeName:      "private",
		Port:          "main",
		Data:          map[string]any{"credential": "redacted-at-projection"},
		PrivateOutput: false,
	}}
	withPrivateMarker := append([]BoundaryExit(nil), exits...)
	withPrivateMarker[0].PrivateOutput = true

	withoutMarkerHash := ComputeResultHash(GroupOutcomeSuccess, exits)
	withMarkerHash := ComputeResultHash(GroupOutcomeSuccess, withPrivateMarker)
	if withMarkerHash != withoutMarkerHash {
		t.Fatalf("ComputeResultHash changed with PrivateOutput marker: %q != %q", withMarkerHash, withoutMarkerHash)
	}
}

// TestSeedExecutionFromEntry_GroupAdmission_Accepted is the positive control:
// a GROUP entry unit admitted as "accepted" must produce exactly one
// admission-counter observation AND exactly one duration-histogram
// observation from the same call — the reason OnGroupAdmission fans out one
// call into two series rather than being split into two observer methods.
func TestSeedExecutionFromEntry_GroupAdmission_Accepted(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{
		State:       AdmissionStateAccepted,
		ExecutionID: "exec-admission-accepted",
	}, nil, WithGroupObserver(obs))

	resp, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        g,
		EntryUnitIdx: groupUnitIdx,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != AdmissionStateAccepted {
		t.Fatalf("resp.State = %v, want accepted", resp.State)
	}

	if len(obs.admissionOutcomes) != 1 || obs.admissionOutcomes[0] != "accepted" {
		t.Fatalf("admissionOutcomes = %v, want exactly [accepted]", obs.admissionOutcomes)
	}
	if len(obs.admissionDurations) != 1 {
		t.Fatalf("admissionDurations len = %d, want exactly 1", len(obs.admissionDurations))
	}
	if obs.admissionDurations[0] < 0 {
		t.Fatalf("admissionDuration = %v, want >= 0", obs.admissionDurations[0])
	}
}

// TestSeedExecutionFromEntry_GroupAdmission_OutcomeClassification pins the
// literal outcome strings for the three remaining help-declared values:
// duplicate (same key+hash, idempotent retry), conflict (same key, different
// hash), and error (backend/transport failure). Each case asserts an exact
// count of 1 for both the outcome and the duration observation.
func TestSeedExecutionFromEntry_GroupAdmission_OutcomeClassification(t *testing.T) {
	cases := []struct {
		name string
		resp SeedExecutionFromEntryResponse
		err  error
		want string
	}{
		{
			name: "duplicate",
			resp: SeedExecutionFromEntryResponse{State: AdmissionStateAccepted, Duplicate: true, ExecutionID: "exec-dup"},
			want: "duplicate",
		},
		{
			name: "conflict",
			resp: SeedExecutionFromEntryResponse{State: AdmissionStateConflict, ExecutionID: "exec-conflict"},
			want: "conflict",
		},
		{
			name: "error",
			resp: SeedExecutionFromEntryResponse{},
			err:  errors.New("backend unavailable"),
			want: "error",
		},
		{
			// A refusal is neither an accepted admission nor an error: it created
			// no execution, and an operator counting admissions must not see it as
			// one. Both refusal states collapse to the same label because they name
			// different waits, not different outcomes for the metric.
			name: "over limit",
			resp: SeedExecutionFromEntryResponse{State: AdmissionStateOverLimit},
			want: "refused",
		},
		{
			name: "queue full",
			resp: SeedExecutionFromEntryResponse{State: AdmissionStateQueueFull},
			want: "refused",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := buildSingleGroupGraph(t)
			groupUnitIdx := findGroupUnit(t, g)

			obs := &recordingGroupObserver{}
			eng := newEntryAdmissionEngine(t, tc.resp, tc.err, WithGroupObserver(obs))

			_, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
				Graph:        g,
				EntryUnitIdx: groupUnitIdx,
			})
			if (err != nil) != (tc.err != nil) {
				t.Fatalf("err = %v, want err!=nil = %v", err, tc.err != nil)
			}

			if len(obs.admissionOutcomes) != 1 || obs.admissionOutcomes[0] != tc.want {
				t.Fatalf("admissionOutcomes = %v, want exactly [%s]", obs.admissionOutcomes, tc.want)
			}
			if len(obs.admissionDurations) != 1 {
				t.Fatalf("admissionDurations len = %d, want exactly 1", len(obs.admissionDurations))
			}
		})
	}
}

// TestSeedExecutionFromEntry_NonGroupEntryDoesNotObserve is negative control
// A: an entry unit that Graph.UnitKindAt resolves to UnitNode (not
// UnitGroup) must not produce any xflow_group_admission_total or
// xflow_group_admission_duration_seconds observation — this metric only
// counts GROUP entry units, never the non-group ones that also flow through
// SeedExecutionFromEntry.
func TestSeedExecutionFromEntry_NonGroupEntryDoesNotObserve(t *testing.T) {
	g := buildSingleGroupGraph(t)
	nonGroupIdx := findNonGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{State: AdmissionStateAccepted}, nil, WithGroupObserver(obs))

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        g,
		EntryUnitIdx: nonGroupIdx,
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}

	if len(obs.admissionOutcomes) != 0 {
		t.Fatalf("admissionOutcomes = %v, want exactly none (non-group entry unit)", obs.admissionOutcomes)
	}
	if len(obs.admissionDurations) != 0 {
		t.Fatalf("admissionDurations = %v, want exactly none (non-group entry unit)", obs.admissionDurations)
	}
}

// TestSeedExecutionFromEntry_NilGraphDoesNotObserve is negative control B:
// req.Graph == nil means the discriminator cannot be evaluated at all, so it
// must not fire — not as group, not as non-group. This is the nil-sentinel
// half of the guard described on GroupObserver.OnGroupAdmission's doc comment.
func TestSeedExecutionFromEntry_NilGraphDoesNotObserve(t *testing.T) {
	obs := &recordingGroupObserver{}
	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{State: AdmissionStateAccepted}, nil, WithGroupObserver(obs))

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        nil,
		EntryUnitIdx: 0,
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}

	if len(obs.admissionOutcomes) != 0 {
		t.Fatalf("admissionOutcomes = %v, want exactly none (nil Graph)", obs.admissionOutcomes)
	}
}

// TestSeedExecutionFromEntry_OutOfRangeEntryUnitIdxDoesNotPanicOrObserve is
// negative control C: an EntryUnitIdx outside [0, Graph.UnitCount()) must not
// observe the metric, AND — this is the actual tooth of the bounds sentinel —
// must not panic. Graph.UnitKindAt indexes g.units directly with no bounds
// check of its own, so without the sentinel this call panics.
func TestSeedExecutionFromEntry_OutOfRangeEntryUnitIdxDoesNotPanicOrObserve(t *testing.T) {
	g := buildSingleGroupGraph(t)

	obs := &recordingGroupObserver{}
	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{State: AdmissionStateAccepted}, nil, WithGroupObserver(obs))

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        g,
		EntryUnitIdx: g.UnitCount(), // one past the end
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}

	if len(obs.admissionOutcomes) != 0 {
		t.Fatalf("admissionOutcomes = %v, want exactly none (out-of-range EntryUnitIdx)", obs.admissionOutcomes)
	}
}

// TestSeedExecutionFromEntry_CapabilityMissingDoesNotObserve is negative
// control D: a backend that does not implement EntryAdmissionStore takes the
// ErrEntryAdmissionNotSupported early return, which is a static capability
// fact rather than an admission attempt, and must not be counted at all —
// not even as outcome="error".
func TestSeedExecutionFromEntry_CapabilityMissingDoesNotObserve(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingGroupObserver{}
	eng := New(newFakeState(), &fakeQueue{}, WithGroupObserver(obs)) // plain fakeState: not an EntryAdmissionStore

	_, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        g,
		EntryUnitIdx: groupUnitIdx,
	})
	if !errors.Is(err, ErrEntryAdmissionNotSupported) {
		t.Fatalf("err = %v, want ErrEntryAdmissionNotSupported", err)
	}

	if len(obs.admissionOutcomes) != 0 {
		t.Fatalf("admissionOutcomes = %v, want exactly none (capability missing)", obs.admissionOutcomes)
	}
}

// TestSeedExecutionFromEntry_NilObserverDoesNotPanic drives the GROUP admission
// path with no GroupObserver configured (matching group_observer_test.go's
// TestGroupObserver_NilObserverDoesNotPanic style): notifyGroupAdmission must
// no-op silently rather than panic on a nil interface.
func TestSeedExecutionFromEntry_NilObserverDoesNotPanic(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	eng := newEntryAdmissionEngine(t, SeedExecutionFromEntryResponse{State: AdmissionStateAccepted}, nil) // no WithGroupObserver

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		Graph:        g,
		EntryUnitIdx: groupUnitIdx,
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
}

// TestSeedExecutionFromEntry_AppliesTheGraphsConcurrencyPolicy is the wiring
// test for decision 5's accept half: a seed whose graph declares a limit is
// admitted through the concurrency capability, with the admission built from
// that very graph. It is the case a Kafka trigger group depends on — the seed
// arrives with no Submit/Invoke behind it, so nothing else would have consulted
// the limit at all.
func TestSeedExecutionFromEntry_AppliesTheGraphsConcurrencyPolicy(t *testing.T) {
	settings := &types.ConcurrencySettings{
		Limit:        2,
		Mode:         types.ConcurrencyModeQueue,
		MaxQueued:    3,
		QueueTimeout: 45 * time.Second,
	}
	g := entryAdmissionConcurrencyGraph(t, settings)

	eng, state := limitedEntryAdmissionEngine(t,
		SeedExecutionFromEntryResponse{State: AdmissionStateAccepted, ExecutionID: "exec-limited"}, nil)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")

	resp, err := eng.SeedExecutionFromEntry(ctx, SeedExecutionFromEntryRequest{
		AdmissionKey: "k-limited",
		Graph:        g,
		EntryUnitIdx: findGroupUnit(t, g),
		Outcome:      GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != AdmissionStateAccepted {
		t.Fatalf("resp.State = %q, want accepted", resp.State)
	}

	if state.limitedCalls != 1 {
		t.Fatalf("concurrency-limited admissions = %d, want exactly 1: a seed whose graph declares a limit must be judged by it", state.limitedCalls)
	}
	want := ConcurrencyAdmission{
		Namespace:    "tenant-a",
		Workflow:     g.Name(),
		Limit:        2,
		Mode:         types.ConcurrencyModeQueue,
		MaxQueued:    3,
		QueueTimeout: 45 * time.Second,
	}
	if got := state.admissions[0]; got != want {
		t.Fatalf("admission = %+v, want %+v", got, want)
	}
}

// TestSeedExecutionFromEntry_WithoutALimitUsesThePlainAdmission is the negative
// control for the test above: a graph that declares no limit must not reach the
// concurrency capability at all. The plain admission is cheaper, and routing
// every seed through a slot decision would make an unlimited workflow contend
// for slot state it has no use for.
func TestSeedExecutionFromEntry_WithoutALimitUsesThePlainAdmission(t *testing.T) {
	g := entryAdmissionConcurrencyGraph(t, nil)

	eng, state := limitedEntryAdmissionEngine(t,
		SeedExecutionFromEntryResponse{State: AdmissionStateAccepted, ExecutionID: "exec-plain"}, nil)

	if _, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		AdmissionKey: "k-plain",
		Graph:        g,
		EntryUnitIdx: findGroupUnit(t, g),
		Outcome:      GroupOutcomeSuccess,
	}); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}

	if state.limitedCalls != 0 {
		t.Fatalf("concurrency-limited admissions = %d, want 0: the graph declares no limit", state.limitedCalls)
	}
	if state.calls != 1 {
		t.Fatalf("plain admissions = %d, want exactly 1", state.calls)
	}
}

// TestSeedExecutionFromEntry_ConcurrencyCapabilityMissingFailsClosed pins
// decision 2 on the seed path: a store that can seed entries but cannot enforce
// a limit must refuse a seed whose graph declares one, rather than admitting it
// unlimited. Silently running unbounded is the failure the limit exists to
// prevent, and a trigger group's traffic is exactly what it was declared to
// bound.
func TestSeedExecutionFromEntry_ConcurrencyCapabilityMissingFailsClosed(t *testing.T) {
	g := entryAdmissionConcurrencyGraph(t, &types.ConcurrencySettings{Limit: 1})

	// fakeEntryAdmissionState implements EntryAdmissionStore only.
	state := &fakeEntryAdmissionState{fakeState: newFakeState(), resp: SeedExecutionFromEntryResponse{State: AdmissionStateAccepted}}
	eng := New(state, &fakeQueue{})

	_, err := eng.SeedExecutionFromEntry(context.Background(), SeedExecutionFromEntryRequest{
		AdmissionKey: "k-unsupported",
		Graph:        g,
		EntryUnitIdx: findGroupUnit(t, g),
		Outcome:      GroupOutcomeSuccess,
	})
	if !errors.Is(err, ErrConcurrencyUnsupported) {
		t.Fatalf("err = %v, want ErrConcurrencyUnsupported", err)
	}
	if state.calls != 0 {
		t.Fatalf("plain admissions = %d, want 0: failing closed must not fall back to an unlimited admission", state.calls)
	}
}

// TestSeedAdmissionRefusal pins the mapping both backends share: the two
// retryable refusals travel as response states, not as errors, and anything else
// is passed through untouched. A refusal reported as an error would be retried
// by the caller as a failed call rather than held until a slot frees, and one
// reported as "accepted" would have the offset committed for an execution that
// does not exist.
func TestSeedAdmissionRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want AdmissionState
	}{
		{name: "limit", err: ErrConcurrencyLimit, want: AdmissionStateOverLimit},
		{name: "queue full", err: ErrConcurrencyQueueFull, want: AdmissionStateQueueFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := SeedAdmissionRefusal(tc.err)
			if err != nil {
				t.Fatalf("SeedAdmissionRefusal(%v) error = %v, want the refusal reported as a state", tc.err, err)
			}
			if resp.State != tc.want {
				t.Fatalf("state = %q, want %q", resp.State, tc.want)
			}
			if resp.ExecutionID != "" {
				t.Fatalf("refusal named execution %q, want none: nothing was persisted for it", resp.ExecutionID)
			}
		})
	}

	t.Run("unrelated error passes through", func(t *testing.T) {
		want := errors.New("backend unavailable")
		resp, err := SeedAdmissionRefusal(want)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v, want the original error", err)
		}
		if resp.State != "" {
			t.Fatalf("state = %q, want empty", resp.State)
		}
	})
}

// referenceResultHash is the pre-streaming implementation of ComputeResultHash.
// It pins the byte stream the hash has always been computed over: a hash that
// drifts makes a duplicate admission look like a conflict across a rolling
// control plane deploy.
func referenceResultHash(outcome GroupOutcome, exits []BoundaryExit) ResultHash {
	h := sha256.New()
	h.Write([]byte(outcome))
	sorted := make([]BoundaryExit, len(exits))
	copy(sorted, exits)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].NodeName != sorted[j].NodeName {
			return sorted[i].NodeName < sorted[j].NodeName
		}
		return sorted[i].Port < sorted[j].Port
	})
	for _, ex := range sorted {
		fmt.Fprintf(h, "%s/%s/", ex.NodeName, ex.Port)
		keys := make([]string, 0, len(ex.Data))
		for k := range ex.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "%s=%v,", k, ex.Data[k])
		}
	}
	return ResultHash(hex.EncodeToString(h.Sum(nil)))
}

func randomHashValue(r *rand.Rand, depth int) any {
	n := 7
	if depth >= 3 {
		n = 4
	}
	switch r.Intn(n) {
	case 0:
		return nil
	case 1:
		return []string{"", "a b", "x=y,", "中文", "map[k:v]", "<nil>", "\n\t\"q\""}[r.Intn(7)]
	case 2:
		return r.Intn(2) == 0
	case 3:
		return []float64{0, 1, -1, 3.14, 1e21, 1e-7, 123456789, 1.5e300, math.Inf(1), math.NaN()}[r.Intn(10)]
	case 4:
		out := make([]any, r.Intn(4))
		for i := range out {
			out[i] = randomHashValue(r, depth+1)
		}
		return out
	case 5:
		out := make(map[string]any, 4)
		for i := r.Intn(4); i > 0; i-- {
			out[[]string{"a", "B", "k1", "k10", "k2", "", "z z", "é"}[r.Intn(8)]] = randomHashValue(r, depth+1)
		}
		return out
	default:
		return []any(nil)
	}
}

func TestComputeResultHash_MatchesReferenceByteStream(t *testing.T) {
	t.Run("random json-shaped data", func(t *testing.T) {
		r := rand.New(rand.NewSource(20261029))
		for i := 0; i < 3000; i++ {
			exits := make([]BoundaryExit, 1+r.Intn(3))
			for j := range exits {
				data := map[string]any{}
				for k := r.Intn(5); k > 0; k-- {
					data[fmt.Sprintf("key%d", r.Intn(8))] = randomHashValue(r, 0)
				}
				exits[j] = BoundaryExit{NodeName: fmt.Sprintf("n%d", r.Intn(3)), Port: fmt.Sprintf("p%d", r.Intn(2)), Data: data}
			}
			outcome := []GroupOutcome{GroupOutcomeSuccess, GroupOutcomeFailed}[r.Intn(2)]
			if got, want := ComputeResultHash(outcome, exits), referenceResultHash(outcome, exits); got != want {
				t.Fatalf("iteration %d: hash drifted: got %s want %s (exits=%#v)", i, got, want, exits)
			}
		}
	})

	t.Run("non json values fall back to fmt", func(t *testing.T) {
		type named struct{ A int }
		exits := []BoundaryExit{{
			NodeName: "n", Port: "p",
			Data: map[string]any{
				"int":      7,
				"json_num": json.Number("12.50"),
				"bytes":    []byte("xy"),
				"strs":     []string{"a", "b"},
				"typed":    map[string][]string{"v": {"a"}},
				"struct":   named{A: 1},
				"ptr":      &named{A: 2},
				"mixed":    []any{1, "s", map[string]any{"k": int64(3)}},
				"err":      errors.New("boom"),
			},
		}}
		if got, want := ComputeResultHash(GroupOutcomeSuccess, exits), referenceResultHash(GroupOutcomeSuccess, exits); got != want {
			t.Fatalf("hash drifted: got %s want %s", got, want)
		}
	})

	t.Run("empty and nil exits", func(t *testing.T) {
		for _, exits := range [][]BoundaryExit{nil, {}, {{NodeName: "n", Port: "p"}}, {{NodeName: "n", Port: "p", Data: map[string]any{}}}} {
			if got, want := ComputeResultHash(GroupOutcomeFailed, exits), referenceResultHash(GroupOutcomeFailed, exits); got != want {
				t.Fatalf("hash drifted for %#v: got %s want %s", exits, got, want)
			}
		}
	})

	t.Run("exit order does not matter", func(t *testing.T) {
		a := BoundaryExit{NodeName: "a", Port: "p", Data: map[string]any{"x": "1"}}
		b := BoundaryExit{NodeName: "b", Port: "p", Data: map[string]any{"y": []any{"2"}}}
		if ComputeResultHash(GroupOutcomeSuccess, []BoundaryExit{a, b}) != ComputeResultHash(GroupOutcomeSuccess, []BoundaryExit{b, a}) {
			t.Fatal("hash depends on exit order")
		}
	})
}

func BenchmarkComputeResultHash(b *testing.B) {
	body := strings.Repeat("x", 90_000)
	items := make([]any, 100)
	for i := range items {
		items[i] = map[string]any{"req": body[:400], "resp": body[:500], "n": float64(i), "ok": true, "tags": []any{"a", "b"}}
	}
	exits := []BoundaryExit{{NodeName: "collect", Port: "main", Data: map[string]any{"results": items, "raw": body}}}
	b.Run("streaming", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ComputeResultHash(GroupOutcomeSuccess, exits)
		}
	})
	b.Run("reference", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = referenceResultHash(GroupOutcomeSuccess, exits)
		}
	})
}
