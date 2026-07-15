package engine

import (
	"context"
	"testing"
)

type commitOutcomeRecorder struct {
	outcomes []CommitOutcome
}

func (r *commitOutcomeRecorder) OnCommitOutcome(_ context.Context, outcome CommitOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func TestCommitTaskResultWithOutcomeNotifiesCommitObserver(t *testing.T) {
	recorder := &commitOutcomeRecorder{}
	eng := New(newFakeState(), &fakeQueue{}, WithCommitObserver(recorder))

	outcome, err := eng.CommitTaskResultWithOutcome(context.Background(), nil, TaskResult{})
	if err != ErrInvalidLeaseToken {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v, want ErrInvalidLeaseToken", err)
	}
	if outcome != CommitOutcomeStaleToken {
		t.Fatalf("CommitTaskResultWithOutcome() outcome = %q, want %q", outcome, CommitOutcomeStaleToken)
	}
	if len(recorder.outcomes) != 1 || recorder.outcomes[0] != CommitOutcomeStaleToken {
		t.Fatalf("commit observer outcomes = %v, want [%q]", recorder.outcomes, CommitOutcomeStaleToken)
	}
}
