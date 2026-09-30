package control

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

type recordingDescriptorObserver struct {
	mu      sync.Mutex
	reasons []string
}

func (o *recordingDescriptorObserver) OnRunnerDescriptorRejected(_ context.Context, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reasons = append(o.reasons, reason)
}

func descriptorEnvelope(t *testing.T, entries ...protocol.RunnerDescriptorEntry) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(protocol.RunnerDescriptorEnvelope{Schema: protocol.RunnerDescriptorSchema, Descriptors: entries})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return raw
}

func descriptorEntry(t *testing.T, nodeType string, version int, d types.Descriptor) protocol.RunnerDescriptorEntry {
	t.Helper()
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	return protocol.RunnerDescriptorEntry{Type: nodeType, Version: version, Descriptor: raw}
}

func descriptorCaps(nodeTypes ...string) []protocol.Capability {
	out := make([]protocol.Capability, 0, len(nodeTypes))
	for _, t := range nodeTypes {
		out = append(out, protocol.Capability{NodeType: t, NodeVersion: 1})
	}
	return out
}

func TestValidateRunnerDescriptors(t *testing.T) {
	allowAll := RunnerPolicy{AllowedNodeTypes: []string{"*"}}
	good := func(nodeType string) types.Descriptor {
		return types.Descriptor{Type: nodeType, DisplayName: nodeType}
	}
	oversized := types.Descriptor{Type: "acme.big", Docs: strings.Repeat("x", protocol.MaxRunnerDescriptorBytes)}

	cases := []struct {
		name     string
		raw      func(t *testing.T) json.RawMessage
		caps     []protocol.Capability
		policy   RunnerPolicy
		wantKept []string
		wantDrop []string
	}{
		{
			name:     "absent envelope reports none",
			raw:      func(*testing.T) json.RawMessage { return nil },
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantKept: nil,
		},
		{
			name: "declared entries kept sorted",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.b", 1, good("acme.b")),
					descriptorEntry(t, "acme.a", 2, good("acme.a")),
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
				)
			},
			caps:     descriptorCaps("acme.a", "acme.b"),
			policy:   allowAll,
			wantKept: []string{"acme.a@1", "acme.a@2", "acme.b@1"},
		},
		{
			name: "undeclared type dropped",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
					descriptorEntry(t, "acme.other", 1, good("acme.other")),
				)
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantKept: []string{"acme.a@1"},
			wantDrop: []string{RunnerDescriptorRejectedUndeclaredType},
		},
		{
			name: "unentitled type dropped",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
					descriptorEntry(t, "acme.b", 1, good("acme.b")),
				)
			},
			caps:     descriptorCaps("acme.a", "acme.b"),
			policy:   RunnerPolicy{AllowedNodeTypes: []string{"acme.a"}},
			wantKept: []string{"acme.a@1"},
			wantDrop: []string{RunnerDescriptorRejectedTypeNotGranted},
		},
		{
			name: "descriptor type mismatch dropped",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t, descriptorEntry(t, "acme.a", 1, good("acme.impostor")))
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedTypeMismatch},
		},
		{
			name: "non-positive version dropped",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t, descriptorEntry(t, "acme.a", 0, good("acme.a")))
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedInvalidVersion},
		},
		{
			name: "duplicate type version keeps first",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
					descriptorEntry(t, "acme.a", 1, types.Descriptor{Type: "acme.a", DisplayName: "second"}),
				)
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantKept: []string{"acme.a@1"},
			wantDrop: []string{RunnerDescriptorRejectedDuplicate},
		},
		{
			name: "over-limit entry dropped alone",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
					descriptorEntry(t, "acme.big", 1, oversized),
				)
			},
			caps:     descriptorCaps("acme.a", "acme.big"),
			policy:   allowAll,
			wantKept: []string{"acme.a@1"},
			wantDrop: []string{RunnerDescriptorRejectedDescriptorTooLarge},
		},
		{
			name: "malformed entry dropped alone",
			raw: func(t *testing.T) json.RawMessage {
				return descriptorEnvelope(t,
					descriptorEntry(t, "acme.a", 1, good("acme.a")),
					protocol.RunnerDescriptorEntry{Type: "acme.b", Version: 1, Descriptor: json.RawMessage(`"not an object"`)},
				)
			},
			caps:     descriptorCaps("acme.a", "acme.b"),
			policy:   allowAll,
			wantKept: []string{"acme.a@1"},
			wantDrop: []string{RunnerDescriptorRejectedMalformed},
		},
		{
			name: "too many entries drops all",
			raw: func(t *testing.T) json.RawMessage {
				entries := make([]protocol.RunnerDescriptorEntry, 0, protocol.MaxRunnerDescriptorEntries+1)
				for i := 0; i <= protocol.MaxRunnerDescriptorEntries; i++ {
					entries = append(entries, descriptorEntry(t, "acme.a", i+1, good("acme.a")))
				}
				return descriptorEnvelope(t, entries...)
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedTooManyEntries},
		},
		{
			name: "oversized envelope drops all",
			raw: func(t *testing.T) json.RawMessage {
				return json.RawMessage(`{"schema":"` + protocol.RunnerDescriptorSchema + `","descriptors":[],"pad":"` +
					strings.Repeat("x", protocol.MaxRunnerDescriptorEnvelopeBytes) + `"}`)
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedEnvelopeTooLarge},
		},
		{
			name: "unknown schema drops all",
			raw: func(*testing.T) json.RawMessage {
				return json.RawMessage(`{"schema":"xflow.runner-descriptors/v99","descriptors":[]}`)
			},
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedUnknownSchema},
		},
		{
			name:     "bad json drops all",
			raw:      func(*testing.T) json.RawMessage { return json.RawMessage(`{"schema":`) },
			caps:     descriptorCaps("acme.a"),
			policy:   allowAll,
			wantDrop: []string{RunnerDescriptorRejectedMalformedEnvelope},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kept, rejected := validateRunnerDescriptors(tc.raw(t), tc.caps, tc.policy)
			var gotKept []string
			for _, d := range kept {
				gotKept = append(gotKept, fmt.Sprintf("%s@%d", d.Type, d.Version))
			}
			var gotDrop []string
			for _, r := range rejected {
				gotDrop = append(gotDrop, r.Reason)
			}
			if fmt.Sprint(gotKept) != fmt.Sprint(tc.wantKept) {
				t.Fatalf("kept = %v, want %v", gotKept, tc.wantKept)
			}
			if fmt.Sprint(gotDrop) != fmt.Sprint(tc.wantDrop) {
				t.Fatalf("dropped = %v, want %v", gotDrop, tc.wantDrop)
			}
		})
	}
}

// TestValidateRunnerDescriptorsCanonicalizes pins that the stored bytes and
// hash depend on the descriptor, not on how the runner formatted it.
func TestValidateRunnerDescriptorsCanonicalizes(t *testing.T) {
	compact := descriptorEnvelope(t, protocol.RunnerDescriptorEntry{
		Type: "acme.a", Version: 1,
		Descriptor: json.RawMessage(`{"Type":"acme.a","DisplayName":"A","Params":[{"Name":"n","Default":9007199254740993}]}`),
	})
	spaced := descriptorEnvelope(t, protocol.RunnerDescriptorEntry{
		Type: "acme.a", Version: 1,
		Descriptor: json.RawMessage("{ \"DisplayName\" : \"A\",\n \"Params\":[{\"Default\":9007199254740993,\"Name\":\"n\"}], \"Type\":\"acme.a\" }"),
	})
	caps := descriptorCaps("acme.a")
	policy := RunnerPolicy{AllowedNodeTypes: []string{"*"}}
	a, _ := validateRunnerDescriptors(compact, caps, policy)
	b, _ := validateRunnerDescriptors(spaced, caps, policy)
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("kept %d/%d, want 1/1", len(a), len(b))
	}
	if a[0].Hash != b[0].Hash || string(a[0].JSON) != string(b[0].JSON) {
		t.Fatalf("canonical forms differ:\n%s\n%s", a[0].JSON, b[0].JSON)
	}
	if !strings.Contains(string(a[0].JSON), "9007199254740993") {
		t.Fatalf("large integer lost precision: %s", a[0].JSON)
	}
	if len(a[0].Hash) != 64 {
		t.Fatalf("hash = %q, want hex sha256", a[0].Hash)
	}
}

// recordingDescriptorDirectory captures the directory request Core built.
type recordingDescriptorDirectory struct {
	*MemoryRunnerDirectory
	last RegisterRunnerRequest
}

func (d *recordingDescriptorDirectory) Register(ctx context.Context, req RegisterRunnerRequest) (RunnerSession, error) {
	d.last = req
	return d.MemoryRunnerDirectory.Register(ctx, req)
}

func TestRegisterDropsBadDescriptorsWithoutFailing(t *testing.T) {
	dir := &recordingDescriptorDirectory{MemoryRunnerDirectory: NewMemoryRunnerDirectory()}
	obs := &recordingDescriptorObserver{}
	logger := &recordingLogger{}
	c := &Core{runners: dir, runnerDescriptorObserver: obs, logger: logger}

	raw := descriptorEnvelope(t,
		descriptorEntry(t, "acme.a", 1, types.Descriptor{Type: "acme.a"}),
		descriptorEntry(t, "acme.undeclared", 1, types.Descriptor{Type: "acme.undeclared"}),
	)
	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		InstanceUID:     "test-instance",
		RunnerID:        "runner-1",
		Concurrency:     1,
		Capabilities:    descriptorCaps("acme.a"),
		DescriptorsJSON: raw,
	}, TransportInfo{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(dir.last.Descriptors) != 1 || dir.last.Descriptors[0].Type != "acme.a" {
		t.Fatalf("directory descriptors = %+v, want only acme.a", dir.last.Descriptors)
	}
	if fmt.Sprint(obs.reasons) != fmt.Sprint([]string{RunnerDescriptorRejectedUndeclaredType}) {
		t.Fatalf("observed reasons = %v", obs.reasons)
	}
	warns := logger.withMsg("runner_descriptor_rejected")
	if len(warns) != 1 || warns[0].level != "warn" || warns[0].field("node_type") != "acme.undeclared" {
		t.Fatalf("warn records = %+v", warns)
	}

	// A malformed envelope still registers, with no descriptors.
	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		InstanceUID:     "test-instance",
		RunnerID:        "runner-1",
		Concurrency:     1,
		Capabilities:    descriptorCaps("acme.a"),
		DescriptorsJSON: json.RawMessage(`{not json`),
	}, TransportInfo{}); err != nil {
		t.Fatalf("register with malformed envelope failed: %v", err)
	}
	if dir.last.Descriptors != nil {
		t.Fatalf("malformed envelope stored descriptors %+v", dir.last.Descriptors)
	}
	if got := obs.reasons[len(obs.reasons)-1]; got != RunnerDescriptorRejectedMalformedEnvelope {
		t.Fatalf("last reason = %q, want %q", got, RunnerDescriptorRejectedMalformedEnvelope)
	}
}
