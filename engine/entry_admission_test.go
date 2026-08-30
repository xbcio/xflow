package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
)

// fakeEntryAdmissionState is an EntryAdmissionStore test double whose
// SeedExecutionFromEntry response/error is fixed per test case, so tests can
// drive every outcome classification (accepted/duplicate/conflict/error)
// deterministically without a real backend.
type fakeEntryAdmissionState struct {
	*fakeState
	resp SeedExecutionFromEntryResponse
	err  error
}

func (f *fakeEntryAdmissionState) SeedExecutionFromEntry(_ context.Context, _ SeedExecutionFromEntryRequest) (SeedExecutionFromEntryResponse, error) {
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
