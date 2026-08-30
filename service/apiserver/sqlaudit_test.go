package apiserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// failingAuditAppender always errors, so a SQLAuditSink over it must surface
// the failure (mutations fail-closed on this).
type failingAuditAppender struct{}

func (failingAuditAppender) AppendAudit(context.Context, *store.AuditRecord) error {
	return errors.New("db down")
}

func TestSQLAuditSinkPersistsEvent(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ev := AuditEvent{
		RequestID:  "req-1",
		Principal:  "alice",
		Namespace:  "namespace-a",
		Operation:  OpWorkflowCreate,
		WorkflowID: "wf-1",
		Decision:   DecisionAllow,
		Outcome:    "admitted",
		TraceID:    "trace-1",
		Timestamp:  time.Now(),
	}
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	r := records[0]
	if r.Principal != "alice" || r.Operation != OpWorkflowCreate || r.Outcome != "admitted" {
		t.Fatalf("audit record = %+v, want alice/workflow.create/admitted", r)
	}
	if r.ID == 0 {
		t.Fatal("audit record ID not assigned by appender")
	}
}

func TestSQLAuditSinkFailsClosedWhenAppenderErrors(t *testing.T) {
	sink := NewSQLAuditSink(failingAuditAppender{})
	err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowCreate})
	if err == nil {
		t.Fatal("Append succeeded with failing appender, want error (fail-closed)")
	}
}

func TestSQLAuditSinkNilAppenderIsUnavailable(t *testing.T) {
	sink := NewSQLAuditSink(nil)
	err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowCreate})
	if !errors.Is(err, ErrAuditUnavailable) {
		t.Fatalf("err = %v, want ErrAuditUnavailable", err)
	}
}

func TestSQLAuditSinkStampsTimestampWhenZero(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)
	if err := sink.Append(context.Background(), AuditEvent{Operation: OpWorkflowRead}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	r := db.AuditRecords()[0]
	if r.Timestamp.IsZero() {
		t.Fatal("audit record timestamp not stamped when caller left it zero")
	}
}

func TestSQLAuditSinkPrefersNamespaceFromContext(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-ctx"))
	ev := AuditEvent{
		Principal: "alice",
		Namespace: "namespace-event",
		Operation: OpWorkflowCreate,
		Decision:  DecisionAllow,
		Outcome:   "admitted",
	}
	if err := sink.Append(ctx, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	if got := records[0].Namespace; got != "namespace-ctx" {
		t.Fatalf("audit Namespace = %q, want namespace-ctx (from context)", got)
	}
}

// auditEventFieldsNotPersisted is the explicit, reasoned whitelist of
// AuditEvent fields that are intentionally NOT expected to have a same-named,
// same-value counterpart on store.AuditRecord. It must stay empty unless a
// real such field is found — do not add an entry to make a fuzzy test pass;
// widening this list is the "silently loosen the sieve" failure mode the
// brief calls out. As of this test, every AuditEvent field round-trips
// through SQLAuditSink.Append under its own name, so there is nothing to list.
var auditEventFieldsNotPersisted = map[string]string{}

// fillDistinctNonZero walks ev's exported fields by reflection and assigns
// each a distinct, non-zero value based on its Kind. It then asserts every
// field ended up non-zero, so a future AuditEvent field of a Kind this
// helper doesn't know how to fill fails loudly here instead of silently
// staying at its zero value (which would make the round-trip check below
// compare zero-to-zero and pass even though nothing was proven).
func fillDistinctNonZero(t *testing.T, ev *AuditEvent) {
	t.Helper()
	v := reflect.ValueOf(ev).Elem()
	typ := v.Type()
	timeType := reflect.TypeOf(time.Time{})
	for i := 0; i < typ.NumField(); i++ {
		f := v.Field(i)
		name := typ.Field(i).Name
		switch {
		case f.Type() == timeType:
			f.Set(reflect.ValueOf(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(time.Duration(i+1) * time.Second)))
		case f.Kind() == reflect.String:
			f.SetString(fmt.Sprintf("%s-nonzero-%d", name, i+1))
		case f.Kind() >= reflect.Uint && f.Kind() <= reflect.Uintptr:
			f.SetUint(uint64(i + 1))
		case f.Kind() >= reflect.Int && f.Kind() <= reflect.Int64:
			f.SetInt(int64(i + 1))
		default:
			t.Fatalf("fillDistinctNonZero: field %s has unhandled kind %s; add a case for it", name, f.Kind())
		}
	}
	for i := 0; i < typ.NumField(); i++ {
		if v.Field(i).IsZero() {
			t.Fatalf("fillDistinctNonZero: field %s is still zero; the round-trip check below can't tell zero-to-zero from a working transmit", typ.Field(i).Name)
		}
	}
}

// TestSQLAuditSinkTransmitsAllEventFields is deliberately exhaustive rather
// than "assert Revision persists": SQLAuditSink.Append builds store.AuditRecord
// by hand-copying each AuditEvent field, and it once silently dropped Revision
// (grep for "Revision" inside Append found zero hits before the fix). A test
// that only checks the fields a human remembered to list would not catch the
// next field that gets forgotten the same way — the human who forgets to wire
// a new field into Append is the same human who would forget to add a line
// for it here. So this test enumerates AuditEvent's fields via reflect at run
// time and looks up the same-named field on store.AuditRecord, instead of a
// hand-maintained list: a new field on AuditEvent is in scope automatically,
// with no second place to remember to update.
//
// Do not use InMemoryAuditSink here — it copies the struct by value and would
// pass even with every field dropped.
func TestSQLAuditSinkTransmitsAllEventFields(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	var ev AuditEvent
	fillDistinctNonZero(t, &ev)

	// context.Background() carries no namespace, so the existing
	// context-fallback semantics (covered by the two tests above) apply and
	// ev.Namespace is expected to persist unchanged — this test must not
	// fight that behavior, only prove every field round-trips. Timestamp is
	// non-zero (fillDistinctNonZero guarantees it), so it also does not hit
	// the zero-stamping path covered by its own test.
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}
	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	got := *records[0]

	evVal := reflect.ValueOf(ev)
	evType := evVal.Type()
	gotVal := reflect.ValueOf(got)
	gotType := gotVal.Type()

	for i := 0; i < evType.NumField(); i++ {
		fieldName := evType.Field(i).Name
		if reason, skip := auditEventFieldsNotPersisted[fieldName]; skip {
			t.Logf("field %s intentionally not checked: %s", fieldName, reason)
			continue
		}
		gotField, ok := gotType.FieldByName(fieldName)
		if !ok {
			t.Errorf("AuditEvent field %s has no same-named field on store.AuditRecord; wire it into SQLAuditSink.Append and store.AuditRecord, or if it must never persist, add it to auditEventFieldsNotPersisted with a reason", fieldName)
			continue
		}
		wantField := evVal.Field(i)
		gotFieldVal := gotVal.FieldByIndex(gotField.Index)

		want := wantField.Interface()
		gotV := gotFieldVal.Interface()
		if wantField.Type() != gotFieldVal.Type() {
			// AuditEvent.Decision and store.AuditRecord.Decision are a known
			// case: same underlying Kind (string), different named types.
			// Handle any such convertible pair generically rather than
			// special-casing Decision by name.
			switch {
			case wantField.Type().ConvertibleTo(gotFieldVal.Type()):
				want = wantField.Convert(gotFieldVal.Type()).Interface()
			case gotFieldVal.Type().ConvertibleTo(wantField.Type()):
				gotV = gotFieldVal.Convert(wantField.Type()).Interface()
			default:
				t.Errorf("field %s: AuditEvent type %s and store.AuditRecord type %s are not convertible", fieldName, wantField.Type(), gotFieldVal.Type())
				continue
			}
		}
		if !reflect.DeepEqual(want, gotV) {
			t.Errorf("field %s not transmitted by SQLAuditSink.Append: got %v, want %v", fieldName, gotV, want)
		}
	}
}

func TestSQLAuditSinkFallsBackToEventNamespace(t *testing.T) {
	db := memstore.New()
	sink := NewSQLAuditSink(db)

	ev := AuditEvent{
		Principal: "alice",
		Namespace: "namespace-event",
		Operation: OpWorkflowCreate,
		Decision:  DecisionAllow,
		Outcome:   "admitted",
	}
	if err := sink.Append(context.Background(), ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	records := db.AuditRecords()
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(records))
	}
	if got := records[0].Namespace; got != "namespace-event" {
		t.Fatalf("audit Namespace = %q, want namespace-event (fallback)", got)
	}
}
