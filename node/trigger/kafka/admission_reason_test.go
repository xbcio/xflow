package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestBatchAdmissionErrorsAreDistinguishable pins that the four ways a batch
// admission can fail are separable in the metric, not only in the log.
//
// The blind spot this closes: a scenario-A run reported 599 of 634 admissions
// in state="error" and the counter could say nothing more. Those 599 could have
// been any mix of four situations that call for four different responses:
//
//	execute_group   the group never ran — this runner cannot execute this
//	                workflow at all (package resolve/compile). Another runner
//	                might. Fix the deployment.
//	group_outcome   the group RAN and a member failed or the deadline blew.
//	                Fix the rule, or the downstream it waits on.
//	seed_transport  the group ran and SUCCEEDED, then the admission round trip
//	                failed. That completed work is discarded and the whole batch
//	                is redelivered and re-executed. Per occurrence this is the
//	                most expensive of the four, and the flat counter hid it
//	                inside the same number as the other three.
//	unknown_state   the control plane answered with none of
//	                accepted/duplicate/conflict — a protocol disagreement no
//	                other signal reports.
//
// The cause text does reach the log, and that is how a human diagnoses ONE
// incident. But free text supports no rate, no ratio, and no alert: you cannot
// page on "seed_transport is 3% of admissions", graph it, or answer whether the
// mix changed after a deploy. That is what the label buys.
//
// The assertion is on state+reason PAIRS. Asserting on state alone is precisely
// the defect being fixed, so a test that did it would go green against the very
// code this replaced.
func TestBatchAdmissionErrorsAreDistinguishable(t *testing.T) {
	msgs := []Message{
		{Topic: "adm", Partition: 3, Offset: 10},
		{Topic: "adm", Partition: 3, Offset: 11},
	}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}

	for _, tc := range []struct {
		name string
		rt   *mockGroupExecRuntime
		want string
		// commit records the offset-commit decision, so this test also guards
		// that adding a label did not move a single one of them.
		commit bool
	}{
		{
			name: "group never ran",
			rt: &mockGroupExecRuntime{
				execErr: errors.New("resolve package: not found"),
			},
			want:   "error/execute_group",
			commit: false,
		},
		{
			name: "group ran and a member failed",
			rt: &mockGroupExecRuntime{
				execResult: types.GroupExecResult{Outcome: "failed", Error: "wasm trap"},
			},
			want:   "error/group_outcome",
			commit: false,
		},
		{
			name: "group ran and the deadline blew",
			rt: &mockGroupExecRuntime{
				execResult: types.GroupExecResult{Outcome: "timeout", Error: "deadline exceeded"},
			},
			want:   "error/group_outcome",
			commit: false,
		},
		{
			// Deterministic failures COMMIT: they skip the batch. Same reason
			// label, different state — which is why both labels are needed to
			// tell the two apart.
			name: "group ran and failed permanently",
			rt: &mockGroupExecRuntime{
				execResult: types.GroupExecResult{
					Outcome: "failed", Error: "rule compile error", Deterministic: true,
				},
			},
			want:   "deterministic_skip/group_outcome",
			commit: true,
		},
		{
			name: "work completed, then the admission round trip failed",
			rt: &mockGroupExecRuntime{
				mockEntrySeedRuntime: mockEntrySeedRuntime{
					err: errors.New("post admission: connection refused"),
				},
				execResult: types.GroupExecResult{Outcome: "success"},
			},
			want:   "error/seed_transport",
			commit: false,
		},
		{
			name: "control plane answered with no state set",
			rt: &mockGroupExecRuntime{
				mockEntrySeedRuntime: mockEntrySeedRuntime{
					response: types.EntrySeedResponse{},
				},
				execResult: types.GroupExecResult{Outcome: "success"},
			},
			want:   "error/unknown_state",
			commit: false,
		},
		{
			name: "success",
			rt: &mockGroupExecRuntime{
				mockEntrySeedRuntime: mockEntrySeedRuntime{
					response: types.EntrySeedResponse{Accepted: true},
				},
				execResult: types.GroupExecResult{Outcome: "success"},
			},
			want:   "accepted/none",
			commit: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &recordingBatchObserver{}
			SetObserver(o)
			defer SetObserver(nil)

			gotCommit := seedEntryBatchViaGroupExec(context.Background(), in, tc.rt, msgs)

			pairs := o.admissionPairs()
			if len(pairs) != 1 {
				t.Fatalf("admissions = %v, want exactly one", pairs)
			}
			if pairs[0] != tc.want {
				t.Errorf("admission = %q, want %q. The state alone cannot say which "+
					"failure this was, which is why the reason label exists.",
					pairs[0], tc.want)
			}
			if gotCommit != tc.commit {
				t.Errorf("commit = %v, want %v — the label change must not move a "+
					"single offset-commit decision", gotCommit, tc.commit)
			}
		})
	}
}

// TestBatchAdmissionReasonsAreDistinct is the discrimination guard for the test
// above. That test asserts each case maps to its expected pair, but a mapping
// that collapsed several causes onto one label could still be made to pass by
// editing the expectations to match. This one asserts the property that makes
// the label worth its cardinality: the three error causes reach the metric as
// three DIFFERENT values.
//
// Concretely, it fails if someone reports execute_group and seed_transport
// under a shared "error" reason — which is the pre-fix behaviour wearing a new
// label, and the outcome the expectations above cannot rule out on their own.
func TestBatchAdmissionReasonsAreDistinct(t *testing.T) {
	msgs := []Message{{Topic: "adm", Partition: 0, Offset: 1}}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}

	runtimes := map[string]*mockGroupExecRuntime{
		"group never ran": {
			execErr: errors.New("resolve package"),
		},
		"member failed": {
			execResult: types.GroupExecResult{Outcome: "failed"},
		},
		"admission failed after the work was done": {
			mockEntrySeedRuntime: mockEntrySeedRuntime{err: errors.New("connection refused")},
			execResult:           types.GroupExecResult{Outcome: "success"},
		},
		"control plane set no state": {
			mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{}},
			execResult:           types.GroupExecResult{Outcome: "success"},
		},
	}

	seen := make(map[string]string, len(runtimes))
	for name, rt := range runtimes {
		o := &recordingBatchObserver{}
		SetObserver(o)
		seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs)
		got := o.admissionPairs()
		SetObserver(nil)

		if len(got) != 1 {
			t.Fatalf("%s: admissions = %v, want exactly one", name, got)
		}
		if prev, dup := seen[got[0]]; dup {
			t.Errorf("%q and %q both report %q. These are different failures "+
				"needing different responses — %q means the deployment is broken "+
				"for this workflow, while an admission failure after a successful "+
				"run means completed work is being discarded and re-executed. "+
				"One label for both is the blind spot this label was added to close.",
				prev, name, got[0], prev)
			continue
		}
		seen[got[0]] = name
	}

	if len(seen) != len(runtimes) {
		t.Errorf("%d distinct admission labels for %d distinct failures: %v",
			len(seen), len(runtimes), seen)
	}
}

// TestRawBatchAdmissionUnknownStateIsObserved covers the non-group path's
// unknown-state branch, which previously reported NOTHING.
//
// The old switch had only three cases. A control-plane response with none of
// accepted/duplicate/conflict set fell through it: the batch was correctly
// withheld from commit, but no counter moved and no line was logged. That
// partition stops making progress and every admission signal reads as idle —
// indistinguishable from a topic with no traffic, which is the same shape of
// invisible failure OnMessageDiscarded exists to prevent.
func TestRawBatchAdmissionUnknownStateIsObserved(t *testing.T) {
	o := &recordingBatchObserver{}
	SetObserver(o)
	defer SetObserver(nil)

	rt := &mockEntrySeedRuntime{response: types.EntrySeedResponse{}}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}
	msgs := []Message{{Topic: "adm", Partition: 0, Offset: 7}}

	if seedEntryBatchMessages(context.Background(), in, rt, msgs) {
		t.Fatal("an unknown admission state must not commit the offset")
	}

	pairs := o.admissionPairs()
	if len(pairs) != 1 || pairs[0] != "error/unknown_state" {
		t.Errorf("admissions = %v, want exactly [error/unknown_state]. Without an "+
			"observation here the partition stalls while every admission metric "+
			"reads as an idle topic.", pairs)
	}
}
