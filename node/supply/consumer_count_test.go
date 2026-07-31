package supply

import (
	"context"
	"testing"
)

// recordingConsumerCountObserver captures every OnConsumerCount call so a test
// can assert both the count and that the notification fires on both register
// and unregister — a gauge that only increments would drift upward forever
// across activations/deactivations.
type recordingConsumerCountObserver struct {
	calls []consumerCountCall
}

type consumerCountCall struct {
	name string
	n    int
}

func (r *recordingConsumerCountObserver) OnConsumerCount(_ context.Context, name string, n int) {
	r.calls = append(r.calls, consumerCountCall{name, n})
}

// Registering consumers must report the running count for that supply name
// after each registration.
func TestRegisterConsumerNotifiesConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer("rules", "node/a", &recordingConsumer{})
	r.RegisterConsumer("rules", "node/b", &recordingConsumer{})

	if len(rec.calls) != 2 {
		t.Fatalf("consumer-count notifications = %#v, want 2", rec.calls)
	}
	if rec.calls[0] != (consumerCountCall{"rules", 1}) {
		t.Fatalf("first notification = %#v, want {rules 1}", rec.calls[0])
	}
	if rec.calls[1] != (consumerCountCall{"rules", 2}) {
		t.Fatalf("second notification = %#v, want {rules 2}", rec.calls[1])
	}
}

// Unregistering must report the DECREASED count — this is what keeps the
// gauge from drifting upward forever across repeated activate/deactivate
// cycles.
func TestUnregisterConsumerNotifiesDecreasedConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer("rules", "node/a", &recordingConsumer{})
	r.RegisterConsumer("rules", "node/b", &recordingConsumer{})
	r.UnregisterConsumer("rules", "node/a")

	if len(rec.calls) != 3 {
		t.Fatalf("consumer-count notifications = %#v, want 3", rec.calls)
	}
	if rec.calls[2] != (consumerCountCall{"rules", 1}) {
		t.Fatalf("notification after unregister = %#v, want {rules 1}", rec.calls[2])
	}
}

// Re-registering the same key must not double-count: it replaces, so the
// count after must be the SAME as before, not incremented.
func TestReplacingSameKeyDoesNotIncrementConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer("rules", "node/a", &recordingConsumer{})
	r.RegisterConsumer("rules", "node/a", &recordingConsumer{}) // replace, same key

	if len(rec.calls) != 2 {
		t.Fatalf("consumer-count notifications = %#v, want 2", rec.calls)
	}
	if rec.calls[1] != (consumerCountCall{"rules", 1}) {
		t.Fatalf("notification after replace = %#v, want {rules 1} (not 2)", rec.calls[1])
	}
}

// A nil observer must not panic — most Registry callers in this codebase never
// install one.
func TestConsumerRegistrationWithNoObserverDoesNotPanic(t *testing.T) {
	r := NewRegistry()
	r.RegisterConsumer("rules", "node/a", &recordingConsumer{})
	r.UnregisterConsumer("rules", "node/a")
}
