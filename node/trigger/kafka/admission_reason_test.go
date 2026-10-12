package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

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
//	                accepted/duplicate/conflict/refused — a protocol
//	                disagreement no other signal reports.
//
// A concurrency refusal is the one batch outcome that is not a failure at all
// and still withholds the offset: the workflow's own policy declined the seed,
// nothing was persisted, and the batch is retried after a backoff. It is its own
// state (refused), with the reason narrowing WHICH wait it is — concurrency_limit
// for a slot, queue_full for the queue-mode backlog — because an operator needs
// to tell a throttle from an outage and then tell the two throttles apart.
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
			// Deterministic failures remain distinguishable, but they withhold the
			// offset just like every other unsuccessful group execution.
			name: "group ran and failed permanently",
			rt: &mockGroupExecRuntime{
				execResult: types.GroupExecResult{
					Outcome: "failed", Error: "rule compile error", Deterministic: true,
				},
			},
			want:   "deterministic_error/group_outcome",
			commit: false,
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
			// The workflow is at its concurrency limit in REJECT mode. Not an
			// error: the control plane is healthy and did what the definition
			// asked. Not a commit either — nothing was persisted, so the
			// completed group work stays ours and the batch must come back.
			name: "workflow is at its concurrency limit",
			rt: &mockGroupExecRuntime{
				mockEntrySeedRuntime: mockEntrySeedRuntime{
					response: types.EntrySeedResponse{Refused: true},
				},
				execResult: types.GroupExecResult{Outcome: "success"},
			},
			want:   "refused/concurrency_limit",
			commit: false,
		},
		{
			// The same withhold, a different wait: the queue-mode backlog is at
			// max_queued. Separated from the case above because "wait for a slot"
			// and "wait for the queue to drain" are not the same instruction to an
			// operator.
			name: "workflow concurrency queue is full",
			rt: &mockGroupExecRuntime{
				mockEntrySeedRuntime: mockEntrySeedRuntime{
					response: types.EntrySeedResponse{Refused: true, QueueFull: true},
				},
				execResult: types.GroupExecResult{Outcome: "success"},
			},
			want:   "refused/queue_full",
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

			gotCommit := seedEntryBatchViaGroupExec(context.Background(), in, tc.rt, msgs, false)

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
		"workflow refused at its limit": {
			mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{Refused: true}},
			execResult:           types.GroupExecResult{Outcome: "success"},
		},
		"workflow queue is full": {
			mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{Refused: true, QueueFull: true}},
			execResult:           types.GroupExecResult{Outcome: "success"},
		},
	}

	seen := make(map[string]string, len(runtimes))
	for name, rt := range runtimes {
		o := &recordingBatchObserver{}
		SetObserver(o)
		seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, false)
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

// TestRawBatchAdmissionRefusalWithholdsCommit is the runner-level half of the
// concurrency-limit contract: a kafka-style batch seed against a workflow that
// refuses it must NOT commit the offset.
//
// Committing here is the one failure that cannot be recovered from — the batch
// carries the only copy of the entry-unit result, and the refusal means nothing
// was persisted server-side, so the messages would be gone with no execution
// behind them. The refusal is not an error and the control plane is healthy,
// which is exactly why the decision cannot be inferred from whether the call
// returned an error: it has to come from the response's refused flag.
func TestRawBatchAdmissionRefusalWithholdsCommit(t *testing.T) {
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf", Params: map[string]any{}}

	for _, tc := range []struct {
		name string
		resp types.EntrySeedResponse
		want string
	}{
		{"at the limit", types.EntrySeedResponse{Refused: true}, "refused/concurrency_limit"},
		{"queue full", types.EntrySeedResponse{Refused: true, QueueFull: true}, "refused/queue_full"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &recordingBatchObserver{}
			SetObserver(o)
			defer SetObserver(nil)

			rt := &mockEntrySeedRuntime{response: tc.resp}
			msgs := []Message{{Topic: "adm", Partition: 0, Offset: 7}}

			if seedEntryBatchMessages(context.Background(), in, rt, msgs) {
				t.Fatal("a refused admission must not commit the offset — the batch " +
					"is the only copy of a result the control plane never persisted")
			}
			pairs := o.admissionPairs()
			if len(pairs) != 1 || pairs[0] != tc.want {
				t.Errorf("admissions = %v, want exactly [%s]", pairs, tc.want)
			}
		})
	}
}

// TestPerMessageSeedAdmissionRefusalWithholdsCommit covers the same contract on
// the per-message entry-seed path, which classifies through the shared
// classifier but commits itself. A refusal there has to withhold the offset too,
// and the two paths disagreeing about a response state is precisely the drift
// the shared classifier exists to prevent.
func TestPerMessageSeedAdmissionRefusalWithholdsCommit(t *testing.T) {
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 400, Value: []byte("over-limit")}}
	recorder := &commitRecordingConsumer{inner: newScriptedConsumer(msgs)}
	admitter := &mockEntrySeedRuntime{response: types.EntrySeedResponse{Refused: true}}
	rt := &entrySeedTestRuntime{
		admitter: admitter,
		dedup:    func(context.Context, string, time.Duration) (bool, error) { return true, nil },
	}
	in := &types.TriggerActivateInput{
		WorkflowID: "wf1",
		NodeName:   "trigger",
		Params:     map[string]any{"entry_unit_id": "g1", "workflow_version": "v1"},
		Runtime:    rt,
	}

	if seedEntryBatch(context.Background(), in, recorder, msgs[0]) {
		t.Fatal("seedEntryBatch reported handled for a refused admission")
	}
	if commits := recorder.getCommits(); len(commits) != 0 {
		t.Fatalf("commit count = %d, want 0 (a refused seed persists nothing)", len(commits))
	}
}
