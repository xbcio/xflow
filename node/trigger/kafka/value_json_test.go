package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/types"
)

// groupExecTestRuntime is entrySeedTestRuntime plus the group-execution
// capability, which is the exact pair resolveValueJSON looks for.
type groupExecTestRuntime struct {
	entrySeedTestRuntime
}

func (r *groupExecTestRuntime) ExecuteGroup(context.Context, map[string]any) (types.GroupExecResult, error) {
	return types.GroupExecResult{Outcome: "success"}, nil
}

// TestMessageData_ValueJSONSplicesRatherThanEscapes asserts the property the
// flag exists for, at the only place it is observable: the bytes that come out
// of json.Marshal.
//
// Asserting the FIELD's Go type would pass for a []byte too, and json.Marshal
// writes a []byte as base64 — a field no consumer can read, produced by a
// change that looked correct in the debugger. So the assertion is on the
// marshalled payload: the record appears in it verbatim, once, unescaped.
func TestMessageData_ValueJSONSplicesRatherThanEscapes(t *testing.T) {
	const record = `{"request":{"method":"GET","body":"a\"b"},"response":{"status":200}}`
	msg := Message{Topic: "t", Partition: 1, Offset: 7, Value: []byte(record)}

	spliced, err := json.Marshal(messageData(msg, true))
	if err != nil {
		t.Fatalf("marshal spliced item: %v", err)
	}
	escaped, err := json.Marshal(messageData(msg, false))
	if err != nil {
		t.Fatalf("marshal escaped item: %v", err)
	}

	if !bytes.Contains(spliced, []byte(`"value":`+record)) {
		t.Fatalf("spliced item does not carry the record verbatim:\n%s", spliced)
	}
	// The escaped form is the baseline this is measured against; if it ever
	// stopped escaping, the two would be identical and this test would be
	// asserting nothing.
	if bytes.Contains(escaped, []byte(`"value":`+record)) {
		t.Fatalf("the default form no longer escapes; there is nothing left to save:\n%s", escaped)
	}
	if len(spliced) >= len(escaped) {
		t.Fatalf("spliced item (%d bytes) is not smaller than the escaped one (%d bytes)", len(spliced), len(escaped))
	}

	// Round-trips to the same document either way. A cheaper encoding that
	// carried a different message would be the worst outcome available here.
	var fromSpliced, fromEscaped map[string]any
	if err := json.Unmarshal(spliced, &fromSpliced); err != nil {
		t.Fatalf("spliced item does not parse: %v", err)
	}
	if err := json.Unmarshal(escaped, &fromEscaped); err != nil {
		t.Fatalf("escaped item does not parse: %v", err)
	}
	inner, err := json.Marshal(fromSpliced["value"])
	if err != nil {
		t.Fatalf("re-marshal spliced value: %v", err)
	}
	var want, got any
	if err := json.Unmarshal([]byte(record), &want); err != nil {
		t.Fatalf("the record itself does not parse, fix the test: %v", err)
	}
	if err := json.Unmarshal(inner, &got); err != nil {
		t.Fatalf("spliced value does not parse: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("the spliced value is not the record")
	}
	if s, _ := fromEscaped["value"].(string); s != record {
		t.Fatalf("escaped value = %q, want the record", s)
	}
}

// TestMessageData_ValueJSONFallsBackOnNonJSON covers the mixed topic.
//
// The flag is a property of the trigger, not of the message, so a topic that
// carries some JSON and some not must keep working: a payload that would not
// splice stays a string rather than failing the batch.
func TestMessageData_ValueJSONFallsBackOnNonJSON(t *testing.T) {
	for _, payload := range []string{"plain text", `{"truncated":`, ""} {
		item := messageData(Message{Value: []byte(payload)}, true)
		if _, ok := item["value"].(string); !ok {
			t.Fatalf("value for %q = %T, want a string", payload, item["value"])
		}
		if _, err := json.Marshal(item); err != nil {
			t.Fatalf("item carrying %q does not marshal: %v", payload, err)
		}
	}
}

// TestResolveValueJSON_OnlyOnTheGroupPath is the safety half of the feature.
//
// The spliced form is only sound where the item never crosses a JSON boundary,
// which is the entry-seed group path. Everywhere else it must come back off —
// so this asserts the refusals, not just the acceptance. A flag that quietly
// stayed on for a per-message activation would ship an object into Redis where
// every reader expects a string.
func TestResolveValueJSON_OnlyOnTheGroupPath(t *testing.T) {
	groupRT := &groupExecTestRuntime{}
	plainRT := &entrySeedTestRuntime{}

	cases := []struct {
		name      string
		cfg       ConsumerConfig
		runtime   types.TriggerRuntime
		entrySeed bool
		want      bool
	}{
		{"group path, flag on", ConsumerConfig{ValueJSON: true}, groupRT, true, true},
		{"group path, flag off", ConsumerConfig{}, groupRT, true, false},
		{"entry seed without group exec", ConsumerConfig{ValueJSON: true}, plainRT, true, false},
		{"group-capable runtime but not entry seed", ConsumerConfig{ValueJSON: true}, groupRT, false, false},
		{"no runtime at all", ConsumerConfig{ValueJSON: true}, nil, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := &types.TriggerActivateInput{NodeName: "trig", Runtime: tc.runtime}
			if got := resolveValueJSON(tc.cfg, in, tc.entrySeed); got != tc.want {
				t.Fatalf("resolveValueJSON = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestValueAsJSON_RoundTripsThroughParams checks the builder reaches the
// consumer, which is a separate fact from either of the two above: the field
// exists, the resolver reads it, and nothing in between carries it.
//
// It also pins the absent-when-false encoding. The param is omitted rather than
// written as false so that adding this field does not move the hash of a
// workflow definition that never asked for it.
func TestValueAsJSON_RoundTripsThroughParams(t *testing.T) {
	rawParams := func(n *Node) map[string]any {
		p, ok := n.RawParams().(map[string]any)
		if !ok {
			t.Fatalf("RawParams() = %T, want a map", n.RawParams())
		}
		return p
	}
	consumerConfig := func(params map[string]any) ConsumerConfig {
		cfg, err := configFromParams(params, nil, false)
		if err != nil {
			t.Fatalf("configFromParams: %v", err)
		}
		return cfg
	}

	off := rawParams(New().Brokers("b").Topic("t").Group("g"))
	if v, ok := off["value_json"]; ok {
		t.Fatalf("value_json = %v on a node that never set it; every existing definition's hash just moved", v)
	}
	if consumerConfig(off).ValueJSON {
		t.Fatal("ValueJSON is on without the builder call")
	}

	on := rawParams(New().Brokers("b").Topic("t").Group("g").ValueAsJSON())
	if on["value_json"] != true {
		t.Fatalf("value_json = %v, want true", on["value_json"])
	}
	if !consumerConfig(on).ValueJSON {
		t.Fatal("ValueAsJSON() does not reach ConsumerConfig")
	}
}

// TestSeedEntryBatchViaGroupExec_CarriesSplicedValue is the wiring assertion:
// the flag has to survive all the way to the messages ExecuteGroup is handed,
// which is the only place a member node can see it.
func TestSeedEntryBatchViaGroupExec_CarriesSplicedValue(t *testing.T) {
	const record = `{"response":{"status":200}}`
	rt := &mockGroupExecRuntime{
		mockEntrySeedRuntime: mockEntrySeedRuntime{response: types.EntrySeedResponse{Accepted: true}},
		execResult:           types.GroupExecResult{Outcome: "success"},
	}
	in := &types.TriggerActivateInput{NodeName: "trig", WorkflowID: "wf-1", Params: map[string]any{}}
	msgs := []Message{{Topic: "t", Value: []byte(record)}}

	if !seedEntryBatchViaGroupExec(context.Background(), in, rt, msgs, true) {
		t.Fatal("accepted admission must allow commit")
	}
	got := rt.gotInput["messages"].([]map[string]any)[0]["value"]
	if raw, ok := got.(json.RawMessage); !ok || string(raw) != record {
		t.Fatalf("value handed to ExecuteGroup = %T %v, want the record spliced", got, got)
	}
}
