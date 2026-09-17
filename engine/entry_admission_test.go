package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
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
