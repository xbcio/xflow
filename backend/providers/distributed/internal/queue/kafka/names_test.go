package kafka

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
)

func TestNewTopicNames_EmptySuffixOmitsSegment(t *testing.T) {
	n, err := newTopicNames("")
	if err != nil {
		t.Fatalf("newTopicNames(\"\"): %v", err)
	}
	want := []string{
		"xflow.task.default",
		"xflow.task.batch",
		"xflow.task.delay",
		"xflow.task.dlq",
	}
	if got := n.All(); !reflect.DeepEqual(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
}

func TestNewTopicNames_SuffixIsInsertedAfterPrefix(t *testing.T) {
	n, err := newTopicNames("tenant-1")
	if err != nil {
		t.Fatalf("newTopicNames: %v", err)
	}
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"Default", n.Default(), "xflow.tenant-1.task.default"},
		{"Batch", n.Batch(), "xflow.tenant-1.task.batch"},
		{"Delay", n.Delay(), "xflow.tenant-1.task.delay"},
		{"DLQ", n.DLQ(), "xflow.tenant-1.task.dlq"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s() = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestNewTopicNames_AcceptsFullAlphabet(t *testing.T) {
	for _, suffix := range []string{"a", "Z", "0", "a.b", "a_b", "a-b", "Dev_Team.v2-x", "..a", "a.."} {
		if _, err := newTopicNames(suffix); err != nil {
			t.Errorf("newTopicNames(%q): unexpected error %v", suffix, err)
		}
	}
}

func TestNewTopicNames_RejectsCharactersOutsideAlphabet(t *testing.T) {
	for _, suffix := range []string{
		"a:b", "a/b", "a b", "a\tb", "a*b", "a{b}", "a#b",
		"café", "命名空间", "a b", "a\x00b", "a\nb",
	} {
		_, err := newTopicNames(suffix)
		if err == nil {
			t.Errorf("newTopicNames(%q): want error, got nil", suffix)
			continue
		}
		// The error quotes the suffix with %q, so control characters appear escaped.
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", suffix)) {
			t.Errorf("newTopicNames(%q): error %q does not name the suffix", suffix, err)
		}
	}
}

func TestNewTopicNames_RejectsReservedDotNames(t *testing.T) {
	for _, suffix := range []string{".", ".."} {
		_, err := newTopicNames(suffix)
		if err == nil {
			t.Errorf("newTopicNames(%q): want error, got nil", suffix)
			continue
		}
		if !strings.Contains(err.Error(), suffix) {
			t.Errorf("newTopicNames(%q): error %q does not name the suffix", suffix, err)
		}
	}
}

// The longest name is the group ID "<prefix>consumer.default": "xflow." +
// suffix + "." + "consumer.default" is 23 bytes plus the suffix, 4 more than the
// longest topic, so 226 is the largest suffix that keeps every topic and every
// group ID within 249 bytes.
func TestNewTopicNames_LengthLimit(t *testing.T) {
	atLimit := strings.Repeat("a", maxTopicLen-len("xflow.")-len(".consumer.default"))
	n, err := newTopicNames(atLimit)
	if err != nil {
		t.Fatalf("newTopicNames(len %d): %v", len(atLimit), err)
	}
	for _, topic := range n.All() {
		if len(topic) > maxTopicLen {
			t.Errorf("topic %q is %d bytes, over the %d limit", topic, len(topic), maxTopicLen)
		}
	}
	for _, kind := range []string{groupDefault, groupBatch, groupDelay} {
		if g := n.Group(kind); len(g) > maxTopicLen {
			t.Errorf("group %q is %d bytes, over the %d limit", g, len(g), maxTopicLen)
		}
	}
	if got := len(n.Group(groupDefault)); got != maxTopicLen {
		t.Errorf("len(Group(default)) = %d, want exactly %d at the boundary", got, maxTopicLen)
	}

	over := atLimit + "a"
	_, err = newTopicNames(over)
	if err == nil {
		t.Fatalf("newTopicNames(len %d): want error, got nil", len(over))
	}
	if !strings.Contains(err.Error(), over) {
		t.Errorf("error does not name the over-long suffix: %v", err)
	}
}

// Group IDs follow the topic naming: "xflow.<suffix>.consumer.<kind>", with the
// suffix segment omitted for the default deployment.
func TestTopicNames_Group(t *testing.T) {
	cases := []struct {
		suffix, kind, want string
	}{
		{"", groupDefault, "xflow.consumer.default"},
		{"", groupBatch, "xflow.consumer.batch"},
		{"", groupDelay, "xflow.consumer.delay"},
		{"ns1", groupDefault, "xflow.ns1.consumer.default"},
		{"ns1", groupBatch, "xflow.ns1.consumer.batch"},
		{"ns1", groupDelay, "xflow.ns1.consumer.delay"},
	}
	for _, c := range cases {
		n, err := newTopicNames(c.suffix)
		if err != nil {
			t.Fatalf("newTopicNames(%q): %v", c.suffix, err)
		}
		if got := n.Group(c.kind); got != c.want {
			t.Errorf("Group(%q) with suffix %q = %q, want %q", c.kind, c.suffix, got, c.want)
		}
	}
}

func TestTopicNames_For(t *testing.T) {
	n, err := newTopicNames("ns1")
	if err != nil {
		t.Fatalf("newTopicNames: %v", err)
	}
	cases := []struct {
		name string
		task *engine.Task
		want string
	}{
		{"batch", &engine.Task{Type: engine.TaskTypeNodeBatch}, "xflow.ns1.task.batch"},
		{"exec", &engine.Task{Type: engine.TaskTypeNodeExec}, "xflow.ns1.task.default"},
		{"resume", &engine.Task{Type: engine.TaskTypeNodeResume}, "xflow.ns1.task.default"},
		{"advance", &engine.Task{Type: engine.TaskTypeNodeAdvance}, "xflow.ns1.task.default"},
		{"group exec", &engine.Task{Type: engine.TaskTypeGroupExec}, "xflow.ns1.task.default"},
		{"nil", nil, "xflow.ns1.task.default"},
	}
	for _, c := range cases {
		if got := n.For(c.task); got != c.want {
			t.Errorf("For(%s) = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestTopicNames_ForNeverReturnsDelayOrDLQ(t *testing.T) {
	n, err := newTopicNames("")
	if err != nil {
		t.Fatalf("newTopicNames: %v", err)
	}
	for _, tt := range []engine.TaskType{
		engine.TaskTypeNodeExec, engine.TaskTypeNodeResume, engine.TaskTypeNodeAdvance,
		engine.TaskTypeNodeSkip, engine.TaskTypeNodeBatch, engine.TaskTypeGroupExec,
	} {
		got := n.For(&engine.Task{Type: tt})
		if got == n.Delay() || got == n.DLQ() {
			t.Errorf("For(type %d) = %q, a producer must not route into delay or dlq", tt, got)
		}
	}
}

func TestTopicNames_AllOrder(t *testing.T) {
	n, err := newTopicNames("x")
	if err != nil {
		t.Fatalf("newTopicNames: %v", err)
	}
	want := []string{n.Default(), n.Batch(), n.Delay(), n.DLQ()}
	if got := n.All(); !reflect.DeepEqual(got, want) {
		t.Fatalf("All() = %v, want default, batch, delay, dlq order %v", got, want)
	}
	seen := map[string]bool{}
	for _, topic := range n.All() {
		if seen[topic] {
			t.Errorf("All() lists %q twice", topic)
		}
		seen[topic] = true
	}
}

func TestSuffixCollisionRisk(t *testing.T) {
	cases := []struct {
		suffix string
		want   bool
	}{
		{"", false},
		{"prod", false},
		{"tenant-1", false},
		{"Team42", false},
		{"a.b", true},
		{"a_b", true},
		{"a.b_c", true},
		{".", true},
		{"_", true},
		{"-", false},
	}
	for _, c := range cases {
		if got := suffixCollisionRisk(c.suffix); got != c.want {
			t.Errorf("suffixCollisionRisk(%q) = %v, want %v", c.suffix, got, c.want)
		}
	}
}

// Distinct suffixes must never be folded onto one topic: the validator rejects
// rather than replaces, so no rejected input has a name to collide with.
func TestNewTopicNames_RejectionDoesNotSubstitute(t *testing.T) {
	good, err := newTopicNames("a_b")
	if err != nil {
		t.Fatalf("newTopicNames(a_b): %v", err)
	}
	for _, bad := range []string{"a:b", "a b", "a/b"} {
		n, err := newTopicNames(bad)
		if err == nil {
			t.Fatalf("newTopicNames(%q): want error", bad)
		}
		if n != (topicNames{}) {
			t.Errorf("newTopicNames(%q) returned non-zero names %+v alongside an error", bad, n)
		}
		if reflect.DeepEqual(n.All(), good.All()) {
			t.Errorf("rejected suffix %q resolves to the topics of %q", bad, "a_b")
		}
	}
}
