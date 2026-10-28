package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
)

// kindObserver is an observer that also implements queue.ProduceErrorObserver.
// It keeps the reported kinds in call order.
type kindObserver struct {
	recordingObserver
	kmu   sync.Mutex
	kinds []string
}

func (o *kindObserver) OnProduceError(kind string) {
	o.kmu.Lock()
	defer o.kmu.Unlock()
	o.kinds = append(o.kinds, kind)
}

func (o *kindObserver) reported() []string {
	o.kmu.Lock()
	defer o.kmu.Unlock()
	return append([]string(nil), o.kinds...)
}

var _ queue.ProduceErrorObserver = (*kindObserver)(nil)

func newKindTransport(t *testing.T) (*Transport, *fakeWriter, *kindObserver) {
	t.Helper()
	w := &fakeWriter{}
	obs := &kindObserver{}
	tr, err := New(Config{Brokers: []string{"broker:9092"}}, "", withWriter(w), WithObserver(obs))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr, w, obs
}

// Each produce path reports one kind per failure. The same retryable broker
// error is "retryable" on Enqueue, which has no outbox to retry from, and
// "queue_full" on the two paths that turn it into backpressure.
//
// Mutation: dropping the OnProduceError call in observe, or returning a fixed
// kind from produceKind, turns this red.
func TestProduceErrorKinds(t *testing.T) {
	retryable := kafkago.LeaderNotAvailable
	permanent := kafkago.MessageSizeTooLarge

	tests := []struct {
		name string
		call func(tr *Transport) error
		werr error
		want string
	}{
		{"enqueue retryable", func(tr *Transport) error { return tr.Enqueue(context.Background(), sampleTask()) }, retryable, ProduceKindRetryable},
		{"enqueue permanent", func(tr *Transport) error { return tr.Enqueue(context.Background(), sampleTask()) }, permanent, ProduceKindPermanent},
		{"enqueue caller", func(tr *Transport) error { return tr.Enqueue(canceledCtx(), sampleTask()) }, nil, ProduceKindCaller},
		{"enqueue nil task", func(tr *Transport) error { return tr.Enqueue(context.Background(), nil) }, nil, ProduceKindOther},
		{"try retryable", func(tr *Transport) error { return tr.TryEnqueue(context.Background(), sampleTask()) }, retryable, ProduceKindQueueFull},
		{"try permanent", func(tr *Transport) error { return tr.TryEnqueue(context.Background(), sampleTask()) }, permanent, ProduceKindPermanent},
		{"try caller", func(tr *Transport) error { return tr.TryEnqueue(expiredCtx(), sampleTask()) }, nil, ProduceKindCaller},
		{"try nil task", func(tr *Transport) error { return tr.TryEnqueue(context.Background(), nil) }, nil, ProduceKindOther},
		{"delayed retryable", func(tr *Transport) error {
			return tr.EnqueueDelayed(context.Background(), sampleTask(), time.Minute)
		}, retryable, ProduceKindQueueFull},
		{"delayed permanent", func(tr *Transport) error {
			return tr.EnqueueDelayed(context.Background(), sampleTask(), time.Minute)
		}, permanent, ProduceKindPermanent},
		{"delayed caller", func(tr *Transport) error {
			return tr.EnqueueDelayed(canceledCtx(), sampleTask(), time.Minute)
		}, nil, ProduceKindCaller},
		{"delayed nil task", func(tr *Transport) error {
			return tr.EnqueueDelayed(context.Background(), nil, time.Minute)
		}, nil, ProduceKindOther},
		{"delayed too long", func(tr *Transport) error {
			return tr.EnqueueDelayed(context.Background(), sampleTask(), 24*time.Hour)
		}, nil, ProduceKindOther},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, w, obs := newKindTransport(t)
			w.err = tc.werr
			if err := tc.call(tr); err == nil {
				t.Fatal("the call succeeded, want a failure")
			}
			if got := obs.reported(); !slices.Equal(got, []string{tc.want}) {
				t.Errorf("reported kinds = %v, want exactly [%s]", got, tc.want)
			}
			if calls := obs.recorded(); len(calls) != 1 || calls[0].err == nil {
				t.Errorf("OnEnqueue calls = %+v, want one failed call alongside the kind", calls)
			}
		})
	}
}

// A success reports no kind, so a healthy transport leaves the counter
// unregistered. OnEnqueue still sees the success.
func TestProduceErrorKindsSuccessReportsNothing(t *testing.T) {
	tr, _, obs := newKindTransport(t)
	ctx := context.Background()

	if err := tr.Enqueue(ctx, sampleTask()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := tr.TryEnqueue(ctx, batchTask()); err != nil {
		t.Fatalf("TryEnqueue: %v", err)
	}
	if err := tr.EnqueueDelayed(ctx, sampleTask(), time.Minute); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	if err := tr.EnqueueDelayed(ctx, sampleTask(), 0); err != nil {
		t.Fatalf("EnqueueDelayed(0): %v", err)
	}

	if got := obs.reported(); len(got) != 0 {
		t.Errorf("reported kinds after four successes = %v, want none", got)
	}
	if n := len(obs.recorded()); n != 4 {
		t.Errorf("OnEnqueue calls = %d, want 4", n)
	}
}

// The kind is reported next to the error, never inside it: OnEnqueue still gets
// the same error the caller gets, so existing observers keep working unchanged.
func TestProduceErrorKindsLeaveTheErrorUnwrapped(t *testing.T) {
	tr, w, obs := newKindTransport(t)
	w.err = kafkago.MessageSizeTooLarge

	got := tr.TryEnqueue(context.Background(), sampleTask())
	calls := obs.recorded()
	if got != kafkago.MessageSizeTooLarge || len(calls) != 1 || calls[0].err != got {
		t.Fatalf("caller error = %v, OnEnqueue calls = %+v, want both exactly %v", got, calls, kafkago.MessageSizeTooLarge)
	}
}

// An observer without OnProduceError (Asynq-era observers, the plain recording
// one) keeps working: the extra hook is optional.
func TestProduceErrorKindsOptionalForPlainObservers(t *testing.T) {
	tr, w, obs := newTestTransport(t, "")
	w.err = errors.New("connection reset by peer")

	if err := tr.Enqueue(context.Background(), sampleTask()); err == nil {
		t.Fatal("expected the injected error")
	}
	if calls := obs.recorded(); len(calls) != 1 || calls[0].err == nil {
		t.Errorf("OnEnqueue calls = %+v, want one failed call", calls)
	}
}

// Every kind the transport can report is listed, once, and a failure never
// reports one outside the list. The adapter in observability/metrics keeps its
// own copy of the vocabulary; the distributed package pins the two together.
func TestProduceKindsListsTheWholeVocabulary(t *testing.T) {
	kinds := ProduceKinds()
	for _, want := range []string{ProduceKindQueueFull, ProduceKindRetryable, ProduceKindPermanent, ProduceKindCaller, ProduceKindOther} {
		if !slices.Contains(kinds, want) {
			t.Errorf("ProduceKinds() = %v, missing %q", kinds, want)
		}
	}
	seen := map[string]bool{}
	for _, k := range kinds {
		if seen[k] {
			t.Errorf("ProduceKinds() lists %q twice", k)
		}
		seen[k] = true
	}
	for _, err := range []error{nil, kafkago.LeaderNotAvailable, kafkago.MessageSizeTooLarge, context.Canceled, errors.New("x")} {
		if k := produceKind(context.Background(), err); !seen[k] {
			t.Errorf("produceKind(%v) = %q, not in ProduceKinds()", err, k)
		}
	}
}
