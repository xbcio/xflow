package seedadmit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestFireKey pins the admission-key format. The server deduplicates on it, so
// the format is not a style choice: every replica of a trigger must derive the
// same key for the same fire, and no two fires may collide.
func TestFireKey(t *testing.T) {
	got := FireKey("wf-1", "cron-in", KindCron, "2026-10-12T00:01:00Z")
	want := "trigger:wf-1:cron-in:cron:2026-10-12T00:01:00Z"
	if got != want {
		t.Fatalf("FireKey() = %q, want %q", got, want)
	}
}

// TestEnabled covers the two conditions that must both hold. The runtime
// capability is checked first because params alone are not authority: an
// activation carrying entry_seed against the embedded engine's runtime has no
// control plane to admit to, and taking the seed path there would silently stop
// the trigger from firing.
func TestEnabled(t *testing.T) {
	seeded := func(params map[string]any, rt types.TriggerRuntime) *types.TriggerActivateInput {
		return &types.TriggerActivateInput{WorkflowID: "wf", NodeName: "n", Params: params, Runtime: rt}
	}
	tests := []struct {
		name string
		in   *types.TriggerActivateInput
		want bool
	}{
		{"nil input", nil, false},
		{"entry_seed with an entry-seed runtime", seeded(map[string]any{"entry_seed": true}, triggertest.NewFakeEntrySeedRuntime()), true},
		{"entry_unit_id with an entry-seed runtime", seeded(map[string]any{"entry_unit_id": "e"}, triggertest.NewFakeEntrySeedRuntime()), true},
		{"entry_seed with a legacy runtime", seeded(map[string]any{"entry_seed": true}, triggertest.NewFakeRuntime()), false},
		{"entry_unit_id with a legacy runtime", seeded(map[string]any{"entry_unit_id": "e"}, triggertest.NewFakeRuntime()), false},
		{"neither param", seeded(map[string]any{}, triggertest.NewFakeEntrySeedRuntime()), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Enabled(tt.in); got != tt.want {
				t.Fatalf("Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEntryUnitID covers the fallback a single-node entry unit relies on: its
// entry-unit ID IS its node name (spec §11.5), so a runner that stamped no
// entry_unit_id still admits against the right unit.
func TestEntryUnitID(t *testing.T) {
	withParam := &types.TriggerActivateInput{NodeName: "node", Params: map[string]any{"entry_unit_id": "group"}}
	if got := EntryUnitID(withParam); got != "group" {
		t.Fatalf("EntryUnitID() = %q, want group", got)
	}
	without := &types.TriggerActivateInput{NodeName: "node", Params: map[string]any{}}
	if got := EntryUnitID(without); got != "node" {
		t.Fatalf("EntryUnitID() = %q, want the node name", got)
	}
}

// TestExits pins the boundary exit shape: the port the trigger node declares,
// and the "trigger" key the embedded engine passes to engine.Invoke, so a
// downstream node reads $input.trigger the same way in both deployment modes.
func TestExits(t *testing.T) {
	in := &types.TriggerActivateInput{WorkflowID: "wf", NodeName: "entry"}
	event := &types.TriggerEvent{
		ID:     "e1",
		Kind:   "cron",
		Source: "entry",
		Time:   time.Date(2026, 10, 12, 0, 1, 0, 0, time.UTC),
		Data:   map[string]any{"scheduled_time": "2026-10-12T00:01:00Z"},
	}
	exits := Exits(in, event)
	if len(exits) != 1 {
		t.Fatalf("Exits() returned %d exits, want 1", len(exits))
	}
	ex := exits[0]
	if ex.NodeName != "entry" {
		t.Fatalf("exit node = %q, want the entry unit", ex.NodeName)
	}
	if ex.Port != "main" {
		t.Fatalf("exit port = %q, want main (the port the trigger nodes declare)", ex.Port)
	}
	trigger, ok := ex.Data["trigger"].(map[string]any)
	if !ok {
		t.Fatalf("exit data has no trigger map: %#v", ex.Data)
	}
	if trigger["id"] != "e1" || trigger["kind"] != "cron" {
		t.Fatalf("trigger payload = %#v, want the event's id and kind", trigger)
	}
	if _, ok := trigger["data"].(map[string]any); !ok {
		t.Fatalf("trigger payload has no data map: %#v", trigger)
	}
}

// TestAdmit covers the response table one row at a time. The rows are not
// interchangeable: Admitted and Refused both end the fire without a retry,
// while a transport failure retries within the bound and is then reportable as
// dropped.
func TestAdmit(t *testing.T) {
	tests := []struct {
		name         string
		respond      func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error)
		want         Result
		wantAttempts int
	}{
		{
			name: "accepted",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{}, nil
			},
			want:         Admitted,
			wantAttempts: 1,
		},
		{
			name: "duplicate",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Accepted: true, Duplicate: true, ExecutionID: "exec-1"}, nil
			},
			want:         Admitted,
			wantAttempts: 1,
		},
		{
			name: "conflict",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Conflict: true, ExecutionID: "exec-1"}, nil
			},
			want:         Admitted,
			wantAttempts: 1,
		},
		{
			name: "refused",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Refused: true, RetryAfter: time.Second}, nil
			},
			want:         Refused,
			wantAttempts: 1,
		},
		{
			name: "queue full",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Refused: true, QueueFull: true}, nil
			},
			want:         Refused,
			wantAttempts: 1,
		},
		{
			name: "transport failure past the bound",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{}, errors.New("connection reset")
			},
			want:         Dropped,
			wantAttempts: 4, // the first attempt plus three retries
		},
		{
			name: "transport failure then accepted",
			respond: func(_ types.EntrySeedRequest, attempt int) (types.EntrySeedResponse, error) {
				if attempt == 1 {
					return types.EntrySeedResponse{}, errors.New("connection reset")
				}
				return types.EntrySeedResponse{}, nil
			},
			want:         Admitted,
			wantAttempts: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := triggertest.NewFakeEntrySeedRuntime()
			rt.SetResponder(tt.respond)
			got, err := Admit(context.Background(), rt, types.EntrySeedRequest{AdmissionKey: "key"},
				Retry{MaxRetries: 3, Delay: time.Millisecond})
			if got != tt.want {
				t.Fatalf("Admit() = %v (err %v), want %v", got, err, tt.want)
			}
			if tt.want == Dropped && err == nil {
				t.Fatal("Admit() reported Dropped with a nil error; the caller has nothing to log")
			}
			if tt.wantAttempts > 0 {
				if attempts := rt.SeedCount(); attempts != tt.wantAttempts {
					t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
				}
			}
		})
	}
}

// zeroStateRuntime answers every admission with a response that sets none of
// the handled states — the protocol disagreement the classifier must not
// mistake for an admission, and which the fake used above cannot produce
// because it always sets one.
type zeroStateRuntime struct{ calls int }

func (r *zeroStateRuntime) SeedExecutionFromEntry(context.Context, types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	r.calls++
	return types.EntrySeedResponse{}, nil
}

// TestAdmitUnclassifiedResponse proves the direction of the failure: a response
// that sets no state is Dropped, and it is not retried. Treating it as admitted
// would report a tick as delivered when nothing was persisted; asking again
// cannot repair a protocol disagreement.
func TestAdmitUnclassifiedResponse(t *testing.T) {
	rt := &zeroStateRuntime{}
	got, err := Admit(context.Background(), rt, types.EntrySeedRequest{AdmissionKey: "key"},
		Retry{MaxRetries: 3, Delay: time.Millisecond})
	if got != Dropped {
		t.Fatalf("Admit() = %v, want Dropped", got)
	}
	if err == nil {
		t.Fatal("Admit() reported Dropped with a nil error")
	}
	if rt.calls != 1 {
		t.Fatalf("attempts = %d, want 1 (an unclassified response is not retried)", rt.calls)
	}
}

// TestAdmitRetriesCarryTheSameKey asserts the property that makes a retry safe:
// it re-sends the same request, so the control plane answers the second attempt
// with Duplicate rather than admitting a second execution.
func TestAdmitRetriesCarryTheSameKey(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	var keys []string
	rt.SetResponder(func(req types.EntrySeedRequest, attempt int) (types.EntrySeedResponse, error) {
		keys = append(keys, req.AdmissionKey)
		if attempt < 2 {
			return types.EntrySeedResponse{}, errors.New("boom")
		}
		return types.EntrySeedResponse{}, nil
	})
	if got, err := Admit(context.Background(), rt, types.EntrySeedRequest{AdmissionKey: "stable"},
		Retry{MaxRetries: 3, Delay: time.Millisecond}); got != Admitted {
		t.Fatalf("Admit() = %v (err %v), want Admitted", got, err)
	}
	if len(keys) != 2 || keys[0] != "stable" || keys[1] != "stable" {
		t.Fatalf("attempt keys = %v, want two identical keys", keys)
	}
}

// TestAdmitStopsRetryingWhenTheWindowCloses asserts the second half of the
// retry bound: the caller's context is the tick's window, and a closed window
// ends the loop even when the attempt budget is unspent.
func TestAdmitStopsRetryingWhenTheWindowCloses(t *testing.T) {
	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{}, errors.New("boom")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	got, err := Admit(ctx, rt, types.EntrySeedRequest{AdmissionKey: "key"},
		Retry{MaxRetries: 10, Delay: 100 * time.Millisecond})
	if got != Dropped || err == nil {
		t.Fatalf("Admit() = (%v, %v), want Dropped with an error", got, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Admit() took %v; the context deadline must bound the retries", elapsed)
	}
	if attempts := rt.SeedCount(); attempts >= 11 {
		t.Fatalf("attempts = %d; the context deadline must cut the retry budget short", attempts)
	}
}
