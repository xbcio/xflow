package supply

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// recordingConsumerCountObserver captures every OnConsumerCount call so a test
// can assert the namespace, the name, and the count — and that the notification
// fires on both register and unregister, since a gauge that only increments
// would drift upward forever across activations/deactivations.
type recordingConsumerCountObserver struct {
	calls []consumerCountCall
}

type consumerCountCall struct {
	ns   namespace.Namespace
	name string
	n    int
}

func (r *recordingConsumerCountObserver) OnConsumerCount(_ context.Context, ns namespace.Namespace, name string, n int) {
	r.calls = append(r.calls, consumerCountCall{ns, name, n})
}

// Registering consumers must report the running count for that supply key
// after each registration.
func TestRegisterConsumerNotifiesConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer(key("rules"), "node/a", &recordingConsumer{})
	r.RegisterConsumer(key("rules"), "node/b", &recordingConsumer{})

	if len(rec.calls) != 2 {
		t.Fatalf("consumer-count notifications = %#v, want 2", rec.calls)
	}
	if rec.calls[0] != (consumerCountCall{namespace.Default, "rules", 1}) {
		t.Fatalf("first notification = %#v, want {default rules 1}", rec.calls[0])
	}
	if rec.calls[1] != (consumerCountCall{namespace.Default, "rules", 2}) {
		t.Fatalf("second notification = %#v, want {default rules 2}", rec.calls[1])
	}
}

// The observer must be told WHICH namespace the supply belongs to: the same
// node name in two namespaces is two supplies, and a metric labelled by name
// alone would report one series for both.
func TestConsumerCountObserverReportsNamespace(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer(KeyOf("tenant-a", "rules"), "node/a", &recordingConsumer{})
	r.RegisterConsumer(KeyOf("tenant-b", "rules"), "node/b", &recordingConsumer{})

	if len(rec.calls) != 2 {
		t.Fatalf("consumer-count notifications = %#v, want 2", rec.calls)
	}
	if rec.calls[0] != (consumerCountCall{"tenant-a", "rules", 1}) {
		t.Fatalf("first notification = %#v, want {tenant-a rules 1}", rec.calls[0])
	}
	if rec.calls[1] != (consumerCountCall{"tenant-b", "rules", 1}) {
		t.Fatalf("second notification = %#v, want {tenant-b rules 1} (a separate supply, not 2)", rec.calls[1])
	}
}

// Unregistering must report the DECREASED count — this is what keeps the
// gauge from drifting upward forever across repeated activate/deactivate
// cycles.
func TestUnregisterConsumerNotifiesDecreasedConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer(key("rules"), "node/a", &recordingConsumer{})
	r.RegisterConsumer(key("rules"), "node/b", &recordingConsumer{})
	r.UnregisterConsumer(key("rules"), "node/a")

	if len(rec.calls) != 3 {
		t.Fatalf("consumer-count notifications = %#v, want 3", rec.calls)
	}
	if rec.calls[2] != (consumerCountCall{namespace.Default, "rules", 1}) {
		t.Fatalf("notification after unregister = %#v, want {default rules 1}", rec.calls[2])
	}
}

// Re-registering the same key must not double-count: it replaces, so the
// count after must be the SAME as before, not incremented.
func TestReplacingSameKeyDoesNotIncrementConsumerCount(t *testing.T) {
	r := NewRegistry()
	rec := &recordingConsumerCountObserver{}
	r.SetObserver(rec)

	r.RegisterConsumer(key("rules"), "node/a", &recordingConsumer{})
	r.RegisterConsumer(key("rules"), "node/a", &recordingConsumer{}) // replace, same key

	if len(rec.calls) != 2 {
		t.Fatalf("consumer-count notifications = %#v, want 2", rec.calls)
	}
	if rec.calls[1] != (consumerCountCall{namespace.Default, "rules", 1}) {
		t.Fatalf("notification after replace = %#v, want {default rules 1} (not 2)", rec.calls[1])
	}
}

// A nil observer must not panic — most Registry callers in this codebase never
// install one.
func TestConsumerRegistrationWithNoObserverDoesNotPanic(t *testing.T) {
	r := NewRegistry()
	r.RegisterConsumer(key("rules"), "node/a", &recordingConsumer{})
	r.UnregisterConsumer(key("rules"), "node/a")
}
