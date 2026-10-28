package kafka

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

func sampleTask() *engine.Task {
	return &engine.Task{
		ExecutionID:     "exec-1",
		NodeName:        "step-a",
		NodeIdx:         3,
		Type:            engine.TaskTypeNodeExec,
		AutoDepth:       2,
		ActivationID:    7,
		UnitIdx:         5,
		DeliverableAt:   time.UnixMilli(1_700_000_000_123).UTC(),
		IntentCreatedAt: time.UnixMilli(1_699_999_999_000).UTC(),
		Generation:      4,
	}
}

func TestEncodeDecodeTask_RoundTrip(t *testing.T) {
	task := sampleTask()
	msg, err := encodeTask("xflow.task.default", task, namespace.Namespace("tenant-a"))
	if err != nil {
		t.Fatalf("encodeTask: %v", err)
	}
	if msg.Topic != "xflow.task.default" {
		t.Errorf("Topic = %q, want xflow.task.default", msg.Topic)
	}
	if string(msg.Key) != "exec-1" {
		t.Errorf("Key = %q, want exec-1", msg.Key)
	}

	got, ns, err := decodeTask(msg)
	if err != nil {
		t.Fatalf("decodeTask: %v", err)
	}
	if ns != "tenant-a" {
		t.Errorf("namespace = %q, want tenant-a", ns)
	}
	if got.ExecutionID != task.ExecutionID || got.NodeName != task.NodeName ||
		got.NodeIdx != task.NodeIdx || got.Type != task.Type {
		t.Errorf("public fields changed: got %+v, want %+v", got, task)
	}
	if got.AutoDepth != 2 || got.ActivationID != 7 || got.UnitIdx != 5 || got.Generation != 4 {
		t.Errorf("scheduler metadata lost: AutoDepth=%d ActivationID=%d UnitIdx=%d Generation=%d",
			got.AutoDepth, got.ActivationID, got.UnitIdx, got.Generation)
	}
	if !got.DeliverableAt.Equal(task.DeliverableAt) {
		t.Errorf("DeliverableAt = %v, want %v", got.DeliverableAt, task.DeliverableAt)
	}
	if !got.IntentCreatedAt.Equal(task.IntentCreatedAt) {
		t.Errorf("IntentCreatedAt = %v, want %v", got.IntentCreatedAt, task.IntentCreatedAt)
	}
}

func TestEncodeTask_UnknownUnitIdxStaysUnknown(t *testing.T) {
	task := sampleTask()
	task.UnitIdx = engine.UnitIdxUnknown
	msg, err := encodeTask("t", task, namespace.Default)
	if err != nil {
		t.Fatalf("encodeTask: %v", err)
	}
	got, _, err := decodeTask(msg)
	if err != nil {
		t.Fatalf("decodeTask: %v", err)
	}
	if got.UnitIdx != engine.UnitIdxUnknown {
		t.Errorf("UnitIdx = %d, want engine.UnitIdxUnknown", got.UnitIdx)
	}
}

func TestEncodeTask_DefaultNamespace(t *testing.T) {
	msg, err := encodeTask("t", sampleTask(), namespace.Default)
	if err != nil {
		t.Fatalf("encodeTask: %v", err)
	}
	_, ns, err := decodeTask(msg)
	if err != nil {
		t.Fatalf("decodeTask: %v", err)
	}
	if ns != namespace.Default {
		t.Errorf("namespace = %q, want %q", ns, namespace.Default)
	}
}

func TestEncodeTask_NilTask(t *testing.T) {
	msg, err := encodeTask("t", nil, namespace.Default)
	if err == nil {
		t.Fatal("encodeTask(nil) returned no error")
	}
	if msg.Topic != "" || msg.Value != nil {
		t.Errorf("encodeTask(nil) returned a populated message: %+v", msg)
	}
}

func TestDecodeTask_Garbage(t *testing.T) {
	if _, _, err := decodeTask(kafkago.Message{Value: []byte("not json")}); err == nil {
		t.Fatal("decodeTask accepted a non-JSON value")
	}
}

func TestMessageKey(t *testing.T) {
	cases := []struct {
		name string
		task *engine.Task
		want string
	}{
		{"nil", nil, ""},
		{"no execution id", &engine.Task{NodeName: "n", Type: engine.TaskTypeNodeExec}, ""},
		{"ordinary", &engine.Task{ExecutionID: "exec-1", NodeName: "n", Type: engine.TaskTypeNodeExec}, "exec-1"},
		{"resume", &engine.Task{ExecutionID: "exec-1", NodeName: "n", Type: engine.TaskTypeNodeResume}, "exec-1"},
		{"batch", &engine.Task{ExecutionID: "exec-1", NodeName: "map/_batch/2", Type: engine.TaskTypeNodeBatch}, "exec-1/map/_batch/2"},
	}
	for _, c := range cases {
		got := messageKey(c.task)
		if c.want == "" {
			if got != nil {
				t.Errorf("%s: key = %q, want nil", c.name, got)
			}
			continue
		}
		if string(got) != c.want {
			t.Errorf("%s: key = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestMessageKey_BatchesOfOneMapDiffer(t *testing.T) {
	a := messageKey(&engine.Task{ExecutionID: "e", NodeName: "map/_batch/0", Type: engine.TaskTypeNodeBatch})
	b := messageKey(&engine.Task{ExecutionID: "e", NodeName: "map/_batch/1", Type: engine.TaskTypeNodeBatch})
	if bytes.Equal(a, b) {
		t.Fatalf("batches of one map share key %q and would pin one partition", a)
	}
}

func TestHeaderValue(t *testing.T) {
	msg := kafkago.Message{Headers: []kafkago.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "a", Value: []byte("3")},
		{Key: "empty", Value: nil},
	}}
	if v, ok := headerValue(msg, "a"); !ok || v != "1" {
		t.Errorf("headerValue(a) = %q, %v; want the first value 1", v, ok)
	}
	if v, ok := headerValue(msg, "empty"); !ok || v != "" {
		t.Errorf("headerValue(empty) = %q, %v; want empty, true", v, ok)
	}
	if _, ok := headerValue(msg, "missing"); ok {
		t.Error("headerValue(missing) reported present")
	}
	if _, ok := headerValue(kafkago.Message{}, "a"); ok {
		t.Error("headerValue on a message without headers reported present")
	}
}

func TestSetHeader_AddsAndReplaces(t *testing.T) {
	got := setHeader(nil, "a", "1")
	if len(got) != 1 || got[0].Key != "a" || string(got[0].Value) != "1" {
		t.Fatalf("setHeader(nil) = %+v", got)
	}

	got = setHeader(got, "b", "2")
	got = setHeader(got, "a", "9")
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (replace must not duplicate): %+v", len(got), got)
	}
	if got[0].Key != "a" || string(got[0].Value) != "9" || got[1].Key != "b" || string(got[1].Value) != "2" {
		t.Errorf("headers = %+v, want a=9 then b=2", got)
	}
}

func TestSetHeader_CollapsesDuplicates(t *testing.T) {
	in := []kafkago.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "a", Value: []byte("3")},
	}
	got := setHeader(in, "a", "9")
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	if v, _ := headerValue(kafkago.Message{Headers: got}, "a"); v != "9" {
		t.Errorf("a = %q, want 9", v)
	}
}

func TestSetHeader_DoesNotMutateArgument(t *testing.T) {
	// Spare capacity is the case an append-based implementation would write
	// through into the caller's backing array.
	backing := make([]kafkago.Header, 2, 8)
	backing[0] = kafkago.Header{Key: "a", Value: []byte("1")}
	backing[1] = kafkago.Header{Key: "b", Value: []byte("2")}
	in := backing[:2]

	added := setHeader(in, "c", "3")
	replaced := setHeader(in, "a", "9")

	if len(in) != 2 || string(in[0].Value) != "1" || string(in[1].Value) != "2" {
		t.Errorf("argument changed: %+v", in)
	}
	if extra := backing[:3][2]; extra.Key != "" || extra.Value != nil {
		t.Errorf("spare capacity of the argument was written: %+v", extra)
	}
	if len(added) != 3 || len(replaced) != 2 {
		t.Errorf("results wrong: added=%+v replaced=%+v", added, replaced)
	}
}

func TestAttemptOf(t *testing.T) {
	cases := []struct {
		name    string
		headers []kafkago.Header
		want    int
	}{
		{"missing", nil, 0},
		{"zero", []kafkago.Header{{Key: hdrAttempt, Value: []byte("0")}}, 0},
		{"positive", []kafkago.Header{{Key: hdrAttempt, Value: []byte("4")}}, 4},
		{"negative", []kafkago.Header{{Key: hdrAttempt, Value: []byte("-3")}}, 0},
		{"garbled", []kafkago.Header{{Key: hdrAttempt, Value: []byte("abc")}}, 0},
		{"empty", []kafkago.Header{{Key: hdrAttempt, Value: nil}}, 0},
		{"fractional", []kafkago.Header{{Key: hdrAttempt, Value: []byte("1.5")}}, 0},
		{"unrelated header", []kafkago.Header{{Key: "other", Value: []byte("9")}}, 0},
	}
	for _, c := range cases {
		if got := attemptOf(kafkago.Message{Headers: c.headers}); got != c.want {
			t.Errorf("%s: attemptOf = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestNotBeforeOf(t *testing.T) {
	const ms int64 = 1_700_000_000_456
	msg := kafkago.Message{Headers: []kafkago.Header{{Key: hdrNotBefore, Value: []byte("1700000000456")}}}
	got, ok := notBeforeOf(msg)
	if !ok || got.UnixMilli() != ms {
		t.Errorf("notBeforeOf = %v, %v; want %d ms, true", got, ok, ms)
	}

	for name, headers := range map[string][]kafkago.Header{
		"missing":  nil,
		"garbled":  {{Key: hdrNotBefore, Value: []byte("soon")}},
		"empty":    {{Key: hdrNotBefore, Value: nil}},
		"zero":     {{Key: hdrNotBefore, Value: []byte("0")}},
		"negative": {{Key: hdrNotBefore, Value: []byte("-5")}},
		"float":    {{Key: hdrNotBefore, Value: []byte("1.5e12")}},
	} {
		if got, ok := notBeforeOf(kafkago.Message{Headers: headers}); ok || !got.IsZero() {
			t.Errorf("%s: notBeforeOf = %v, %v; want zero, false", name, got, ok)
		}
	}
}

func TestTruncateUTF8(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"shorter", "abc", 10, "abc"},
		{"exact", "abc", 3, "abc"},
		{"ascii cut", "abcdef", 4, "abcd"},
		{"empty", "", 4, ""},
		{"zero max", "abc", 0, ""},
		{"negative max", "abc", -1, ""},
		// "é" is 2 bytes, "世" 3 bytes, "😀" 4 bytes.
		{"two byte kept whole", "aé", 3, "aé"},
		{"two byte cut back", "aéb", 2, "a"},
		{"three byte cut back one", "世界", 5, "世"},
		{"three byte cut back two", "世界", 4, "世"},
		{"four byte cut back", "a😀", 4, "a"},
		{"four byte kept whole", "a😀", 5, "a😀"},
		{"first char too wide", "世", 2, ""},
	}
	for _, c := range cases {
		got := truncateUTF8(c.in, c.max)
		if got != c.want {
			t.Errorf("%s: truncateUTF8(%q, %d) = %q, want %q", c.name, c.in, c.max, got, c.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("%s: result %q is not valid UTF-8", c.name, got)
		}
	}
}

func TestTruncateUTF8_ErrorHeaderBound(t *testing.T) {
	long := strings.Repeat("世", 400)
	got := truncateUTF8(long, maxErrorHeaderBytes)
	if len(got) > maxErrorHeaderBytes {
		t.Fatalf("len = %d, want <= %d", len(got), maxErrorHeaderBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncated header is not valid UTF-8")
	}
}
