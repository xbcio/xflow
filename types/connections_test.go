package types

import (
	"encoding/json"
	"testing"
)

// 存量定义全是数组形状。这条断言的是「改类型后，同一份 JSON 往返回来逐字节不变」——
// 它是 runtimeHash 稳定性的前提，也是本次唯一波及全部存量定义的改动。
func TestConnections_LegacyArrayRoundTripsByteIdentical(t *testing.T) {
	const legacy = `{"consume":{"main":[{"node":"cleanse","input":"main"}]}}`

	var c Connections
	if err := json.Unmarshal([]byte(legacy), &c); err != nil {
		t.Fatalf("unmarshal legacy array form: %v", err)
	}
	if got := c["consume"]["main"].Type; got != ConnectionTypeData {
		t.Errorf("legacy array must decode to Type=data, got %q", got)
	}
	if got := len(c["consume"]["main"].Targets); got != 1 {
		t.Fatalf("Targets length = %d, want 1", got)
	}

	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != legacy {
		t.Errorf("round trip changed bytes:\n got: %s\nwant: %s", out, legacy)
	}
}

// Type 被显式设为 "data"（而非留空）时也必须回吐数组。omitempty 只挡空串，
// 挡不住显式的 "data" —— 而旧 JSON 解析后 Type 正是被设为 "data"。
func TestConnections_ExplicitDataTypeStillMarshalsAsArray(t *testing.T) {
	c := Connections{
		"a": {"main": PortConnections{
			Type:    ConnectionTypeData,
			Targets: []Connection{{Node: "b", Input: "main"}},
		}},
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"a":{"main":[{"node":"b","input":"main"}]}}`
	if string(out) != want {
		t.Errorf("explicit Type=data must still emit an array:\n got: %s\nwant: %s", out, want)
	}
}

// 依赖边用对象形状，必须原样保留。
func TestConnections_DependencyObjectRoundTrips(t *testing.T) {
	const dep = `{"rules":{"supply":{"type":"dependency","targets":[{"node":"cleanse"}]}}}`

	var c Connections
	if err := json.Unmarshal([]byte(dep), &c); err != nil {
		t.Fatalf("unmarshal dependency form: %v", err)
	}
	if got := c["rules"]["supply"].Type; got != ConnectionTypeDependency {
		t.Errorf("Type = %q, want dependency", got)
	}

	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != dep {
		t.Errorf("round trip changed bytes:\n got: %s\nwant: %s", out, dep)
	}
}

// MarshalJSON 必须是值 receiver。Connections 是 map[string]map[string]PortConnections，
// map 的值不可寻址，encoding/json 不会对它取指针方法集 —— 指针 receiver 的
// MarshalJSON 根本不会被调用，退化成默认结构体编码 {"targets":[...]}，
// 且无编译错误、无运行时报错。
//
// 这个测试必须走真实的 Connections 值。拿单个 PortConnections 变量 marshal
// 是假探针：单值可寻址，指针 receiver 在它上面会被正常调用，于是恰好放过
// 唯一会出事的那种写法。
func TestPortConnections_MarshalJSONIsReachableThroughMapValue(t *testing.T) {
	c := Connections{
		"a": {"main": PortConnections{Targets: []Connection{{Node: "b"}}}},
	}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// 若 MarshalJSON 写成指针 receiver，这里会得到 {"a":{"main":{"targets":[...]}}}
	if string(out) == `{"a":{"main":{"targets":[{"node":"b"}]}}}` {
		t.Fatal("custom MarshalJSON was not invoked through the map value: " +
			"it must use a VALUE receiver, not a pointer receiver")
	}
	const want = `{"a":{"main":[{"node":"b"}]}}`
	if string(out) != want {
		t.Errorf("got %s, want %s", out, want)
	}
}
