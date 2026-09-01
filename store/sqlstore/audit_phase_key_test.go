package sqlstore

import (
	"encoding/hex"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm/schema"

	"github.com/xbcio/xflow/store"
)

func TestToDBAuditPhaseKeyEligibility(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		requestID string
		phase     string
		wantKey   bool
	}{
		{name: "outcome", namespace: "tenant-a", requestID: "request-1", phase: store.AuditPhaseOutcome, wantKey: true},
		{name: "admission", namespace: "tenant-a", requestID: "request-1", phase: store.AuditPhaseAdmission},
		{name: "receipt", namespace: "tenant-a", requestID: "request-1", phase: store.AuditPhaseReceipt},
		{name: "empty phase", namespace: "tenant-a", requestID: "request-1"},
		{name: "empty namespace", requestID: "request-1", phase: store.AuditPhaseOutcome},
		{name: "empty request id", namespace: "tenant-a", phase: store.AuditPhaseOutcome},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toDBAudit(&store.AuditRecord{
				Namespace: tt.namespace,
				RequestID: tt.requestID,
				Phase:     tt.phase,
			}).PhaseKey
			if (got != nil) != tt.wantKey {
				t.Fatalf("PhaseKey = %v, want present=%v", got, tt.wantKey)
			}
		})
	}
}

func TestToDBAuditPhaseKeyIsStableAndIdentityScoped(t *testing.T) {
	record := &store.AuditRecord{
		Namespace: "tenant-a",
		RequestID: "request-42",
		Phase:     store.AuditPhaseOutcome,
	}

	first := toDBAudit(record).PhaseKey
	second := toDBAudit(record).PhaseKey
	if first == nil || second == nil {
		t.Fatal("outcome PhaseKey is nil")
	}
	if *first != *second {
		t.Fatalf("PhaseKey is not stable: %q != %q", *first, *second)
	}
	const want = "7b431f4c80fdcea970f87bd50def8befa039418253653afd59a92ca1eb47a530"
	if *first != want {
		t.Fatalf("PhaseKey = %q, want stable digest %q", *first, want)
	}
	if len(*first) != 64 {
		t.Fatalf("PhaseKey length = %d, want 64", len(*first))
	}
	if _, err := hex.DecodeString(*first); err != nil {
		t.Fatalf("PhaseKey is not hexadecimal: %q: %v", *first, err)
	}

	for _, other := range []*store.AuditRecord{
		{Namespace: "tenant-b", RequestID: record.RequestID, Phase: store.AuditPhaseOutcome},
		{Namespace: record.Namespace, RequestID: "request-43", Phase: store.AuditPhaseOutcome},
	} {
		key := toDBAudit(other).PhaseKey
		if key == nil {
			t.Fatal("comparison outcome PhaseKey is nil")
		}
		if *key == *first {
			t.Fatalf("distinct identity produced the same PhaseKey %q", *key)
		}
	}
}

func TestToDBAuditPhaseKeyAvoidsDelimiterCollision(t *testing.T) {
	left := toDBAudit(&store.AuditRecord{
		Namespace: "a|b",
		RequestID: "c",
		Phase:     store.AuditPhaseOutcome,
	}).PhaseKey
	right := toDBAudit(&store.AuditRecord{
		Namespace: "a",
		RequestID: "b|c",
		Phase:     store.AuditPhaseOutcome,
	}).PhaseKey

	if left == nil || right == nil {
		t.Fatal("outcome PhaseKey is nil")
	}
	if *left == *right {
		t.Fatalf("delimiter-ambiguous identities collided at %q", *left)
	}
}

func TestToDBAuditPhaseKeyHandlesLongAndArbitraryBytes(t *testing.T) {
	record := &store.AuditRecord{
		Namespace: strings.Repeat("命名空间🙂|", 512) + string([]byte{0x00, 0xff}),
		RequestID: strings.Repeat("请求/ß|", 512) + string([]byte{0xfe, 0x00}),
		Phase:     store.AuditPhaseOutcome,
	}

	first := toDBAudit(record).PhaseKey
	second := toDBAudit(record).PhaseKey
	if first == nil || second == nil {
		t.Fatal("outcome PhaseKey is nil")
	}
	if *first != *second {
		t.Fatalf("PhaseKey is not stable for long/arbitrary-byte input: %q != %q", *first, *second)
	}
	if len(*first) != 64 {
		t.Fatalf("PhaseKey length = %d, want fixed length 64", len(*first))
	}
	if _, err := hex.DecodeString(*first); err != nil {
		t.Fatalf("PhaseKey is not safe hexadecimal: %q: %v", *first, err)
	}
}

func TestDBAuditEventPhaseIndexes(t *testing.T) {
	modelType := reflect.TypeOf(dbAuditEvent{})
	phaseKeyField, ok := modelType.FieldByName("PhaseKey")
	if !ok {
		t.Fatal("dbAuditEvent.PhaseKey is missing")
	}
	if phaseKeyField.Type != reflect.TypeOf((*string)(nil)) {
		t.Fatalf("dbAuditEvent.PhaseKey type = %v, want *string", phaseKeyField.Type)
	}

	parsed, err := schema.Parse(&dbAuditEvent{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatalf("parse dbAuditEvent schema: %v", err)
	}
	phaseKey := parsed.LookUpField("PhaseKey")
	if phaseKey == nil {
		t.Fatal("parsed PhaseKey field is missing")
	}
	if phaseKey.NotNull {
		t.Fatal("PhaseKey is unexpectedly NOT NULL")
	}

	unique := parsed.LookIndex("uk_phase_key")
	if unique == nil {
		t.Fatal("uk_phase_key index is missing")
	}
	if unique.Class != "UNIQUE" {
		t.Fatalf("uk_phase_key class = %q, want UNIQUE", unique.Class)
	}
	if len(unique.Fields) != 1 || unique.Fields[0].DBName != "phase_key" {
		t.Fatalf("uk_phase_key fields = %v, want [phase_key]", indexDBNames(unique))
	}

	composite := parsed.LookIndex("idx_namespace_request_phase")
	if composite == nil {
		t.Fatal("idx_namespace_request_phase index is missing")
	}
	wantNames := []string{"namespace", "request_id", "phase"}
	wantPriorities := []int{1, 2, 3}
	if len(composite.Fields) != len(wantNames) {
		t.Fatalf("idx_namespace_request_phase fields = %v, want %v", indexDBNames(composite), wantNames)
	}
	for i, field := range composite.Fields {
		if field.DBName != wantNames[i] || field.Priority != wantPriorities[i] {
			t.Fatalf("idx_namespace_request_phase field %d = (%q, priority %d), want (%q, priority %d)",
				i, field.DBName, field.Priority, wantNames[i], wantPriorities[i])
		}
	}
}

func TestFromDBAuditDoesNotExposePhaseKey(t *testing.T) {
	phaseKey := "internal-only"
	dbRecord := &dbAuditEvent{
		ID:        12,
		Namespace: "tenant-a",
		RequestID: "request-1",
		Phase:     store.AuditPhaseOutcome,
		PhaseKey:  &phaseKey,
	}
	record := fromDBAudit(dbRecord)

	if reflect.ValueOf(record).Elem().FieldByName("PhaseKey").IsValid() {
		t.Fatal("fromDBAudit exposed the internal PhaseKey")
	}
	want := &store.AuditRecord{
		ID:        12,
		SeqID:     12,
		Namespace: "tenant-a",
		RequestID: "request-1",
		Phase:     store.AuditPhaseOutcome,
	}
	if !reflect.DeepEqual(record, want) {
		t.Fatalf("fromDBAudit() = %#v, want %#v", record, want)
	}
}

func indexDBNames(index *schema.Index) []string {
	names := make([]string, len(index.Fields))
	for i, field := range index.Fields {
		names[i] = field.DBName
	}
	return names
}
