package control

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// descriptorContractDirectory is what the descriptor contract needs from a
// directory under test.
type descriptorContractDirectory interface {
	RunnerDirectory
	RunnerDescriptorDirectory
	RunnerRemover
	RunnerDeregisterer
}

// descriptorContractMarker is embedded in every stored descriptor so the
// snapshot-leak check can search for it.
const descriptorContractMarker = "descriptor-contract-marker"

func contractDescriptor(nodeType string, version int, displayName string) RunnerNodeDescriptor {
	raw := json.RawMessage(`{"Type":"` + nodeType + `","DisplayName":"` + displayName + `","Docs":"` + descriptorContractMarker + `"}`)
	return RunnerNodeDescriptor{Type: nodeType, Version: version, Hash: "hash-" + displayName, JSON: raw}
}

func contractRegister(t *testing.T, dir RunnerDirectory, runnerID string, now time.Time, descriptors ...RunnerNodeDescriptor) RunnerSession {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "acme.a", NodeVersion: 1}},
		Descriptors:  descriptors,
		PoolID:       "pool-1",
		PoolName:     "gpu",
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{"*"}},
		Namespaces:   []namespace.Namespace{"tenant-a"},
		InstanceUID:  "instance-" + runnerID,
		Now:          now,
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", runnerID, err)
	}
	return session
}

func liveDescriptors(t *testing.T, dir RunnerDescriptorDirectory, now time.Time) []RunnerDescriptorRecord {
	t.Helper()
	records, err := dir.LiveRunnerDescriptors(context.Background(), now)
	if err != nil {
		t.Fatalf("LiveRunnerDescriptors: %v", err)
	}
	return records
}

// runRunnerDescriptorDirectoryContract is the behaviour every
// RunnerDescriptorDirectory must share. newDir returns a fresh, empty
// directory.
func runRunnerDescriptorDirectoryContract(t *testing.T, newDir func(t *testing.T) descriptorContractDirectory) {
	base := time.Unix(1_900_000_000, 0).UTC()

	t.Run("register stores descriptors with attribution", func(t *testing.T) {
		dir := newDir(t)
		want := []RunnerNodeDescriptor{contractDescriptor("acme.a", 1, "A1"), contractDescriptor("acme.a", 2, "A2")}
		contractRegister(t, dir, "runner-1", base, want...)
		contractRegister(t, dir, "runner-0", base) // reports none

		records := liveDescriptors(t, dir, base.Add(time.Second))
		if len(records) != 1 {
			t.Fatalf("records = %+v, want only runner-1", records)
		}
		got := records[0]
		if got.RunnerID != "runner-1" || got.PoolID != "pool-1" || got.PoolName != "gpu" {
			t.Fatalf("attribution = %+v", got)
		}
		if !reflect.DeepEqual(got.Namespaces, []namespace.Namespace{"tenant-a"}) {
			t.Fatalf("namespaces = %v, want [tenant-a]", got.Namespaces)
		}
		if !got.RegisteredAt.Equal(base) {
			t.Fatalf("RegisteredAt = %v, want %v", got.RegisteredAt, base)
		}
		if len(got.Descriptors) != len(want) {
			t.Fatalf("descriptors = %+v, want %+v", got.Descriptors, want)
		}
		for i := range want {
			g, w := got.Descriptors[i], want[i]
			if g.Type != w.Type || g.Version != w.Version || g.Hash != w.Hash || string(g.JSON) != string(w.JSON) {
				t.Fatalf("descriptor[%d] = %+v, want %+v", i, g, w)
			}
		}
	})

	t.Run("records sorted by runner id", func(t *testing.T) {
		dir := newDir(t)
		for _, id := range []string{"runner-c", "runner-a", "runner-b"} {
			contractRegister(t, dir, id, base, contractDescriptor("acme.a", 1, "A"))
		}
		records := liveDescriptors(t, dir, base)
		var ids []string
		for _, r := range records {
			ids = append(ids, r.RunnerID)
		}
		if !reflect.DeepEqual(ids, []string{"runner-a", "runner-b", "runner-c"}) {
			t.Fatalf("ids = %v", ids)
		}
	})

	t.Run("re-register replaces descriptors", func(t *testing.T) {
		dir := newDir(t)
		contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "old"))
		later := base.Add(time.Second)
		contractRegister(t, dir, "runner-1", later, contractDescriptor("acme.a", 2, "new"))
		records := liveDescriptors(t, dir, later)
		if len(records) != 1 || len(records[0].Descriptors) != 1 || records[0].Descriptors[0].Hash != "hash-new" {
			t.Fatalf("records = %+v, want only the replacement set", records)
		}
		if !records[0].RegisteredAt.Equal(later) {
			t.Fatalf("RegisteredAt = %v, want %v", records[0].RegisteredAt, later)
		}
	})

	t.Run("register without descriptors clears them", func(t *testing.T) {
		dir := newDir(t)
		contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		contractRegister(t, dir, "runner-1", base.Add(time.Second))
		if records := liveDescriptors(t, dir, base.Add(time.Second)); len(records) != 0 {
			t.Fatalf("records = %+v, want none after an old runner re-registered", records)
		}
	})

	t.Run("remove runner deletes descriptors", func(t *testing.T) {
		dir := newDir(t)
		contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		if err := dir.RemoveRunner(context.Background(), "runner-1"); err != nil {
			t.Fatalf("RemoveRunner: %v", err)
		}
		if records := liveDescriptors(t, dir, base); len(records) != 0 {
			t.Fatalf("records = %+v, want none after RemoveRunner", records)
		}
		// A fresh registration after removal does not resurrect the old set.
		contractRegister(t, dir, "runner-1", base)
		if records := liveDescriptors(t, dir, base); len(records) != 0 {
			t.Fatalf("records = %+v, want none after re-register", records)
		}
	})

	t.Run("live only", func(t *testing.T) {
		dir := newDir(t)
		session := contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		contractRegister(t, dir, "runner-2", base, contractDescriptor("acme.a", 1, "A"))
		if records := liveDescriptors(t, dir, base.Add(DefaultRunnerLiveTTL-time.Second)); len(records) != 2 {
			t.Fatalf("records inside TTL = %d, want 2", len(records))
		}
		heartbeatAt := base.Add(DefaultRunnerLiveTTL / 2)
		if err := dir.Heartbeat(context.Background(), HeartbeatRequest{
			RunnerID: "runner-1", SessionID: session.SessionID, Capacity: 1, Now: heartbeatAt,
		}); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
		records := liveDescriptors(t, dir, base.Add(DefaultRunnerLiveTTL+time.Second))
		if len(records) != 1 || records[0].RunnerID != "runner-1" {
			t.Fatalf("records past TTL = %+v, want only the heartbeating runner-1", records)
		}
		if !records[0].RegisteredAt.Equal(base) {
			t.Fatalf("heartbeat moved RegisteredAt to %v", records[0].RegisteredAt)
		}
	})

	t.Run("deregister ends liveness", func(t *testing.T) {
		dir := newDir(t)
		session := contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		if err := dir.Deregister(context.Background(), "runner-1", session.SessionID); err != nil {
			t.Fatalf("Deregister: %v", err)
		}
		if records := liveDescriptors(t, dir, base); len(records) != 0 {
			t.Fatalf("records = %+v, want none after Deregister", records)
		}
	})

	t.Run("no leak into runner snapshot", func(t *testing.T) {
		dir := newDir(t)
		contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		snapshot, ok := dir.Runner(context.Background(), "runner-1")
		if !ok {
			t.Fatal("runner not found")
		}
		raw, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		if strings.Contains(string(raw), descriptorContractMarker) || strings.Contains(string(raw), "hash-A") {
			t.Fatalf("management snapshot carries descriptors: %s", raw)
		}
	})

	t.Run("returned records are copies", func(t *testing.T) {
		dir := newDir(t)
		contractRegister(t, dir, "runner-1", base, contractDescriptor("acme.a", 1, "A"))
		first := liveDescriptors(t, dir, base)
		first[0].Descriptors[0].JSON[0] = 'X'
		first[0].Descriptors[0].Hash = "mutated"
		second := liveDescriptors(t, dir, base)
		if second[0].Descriptors[0].Hash != "hash-A" || second[0].Descriptors[0].JSON[0] != '{' {
			t.Fatalf("caller mutation reached the directory: %+v", second[0].Descriptors[0])
		}
	})

	t.Run("concurrent register and read", func(t *testing.T) {
		dir := newDir(t)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				if _, err := dir.Register(context.Background(), RegisterRunnerRequest{
					RunnerID: "runner-1", Capacity: 1, InstanceUID: "instance-runner-1", Now: base,
					Descriptors: []RunnerNodeDescriptor{contractDescriptor("acme.a", 1, "A")},
				}); err != nil {
					t.Errorf("Register: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				records, err := dir.LiveRunnerDescriptors(context.Background(), base)
				if err != nil {
					t.Errorf("LiveRunnerDescriptors: %v", err)
				}
				for _, r := range records {
					if len(r.Descriptors) != 1 {
						t.Errorf("torn record %+v", r)
					}
				}
			}()
		}
		wg.Wait()
	})
}

func TestMemoryRunnerDirectoryDescriptorContract(t *testing.T) {
	runRunnerDescriptorDirectoryContract(t, func(*testing.T) descriptorContractDirectory {
		return NewMemoryRunnerDirectory()
	})
}

func encodeContractEnvelope(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := protocol.EncodeRunnerDescriptors([]protocol.RunnerDescriptor{
		{Type: "acme.a", Version: 1, Descriptor: types.Descriptor{Type: "acme.a"}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return raw
}

func TestRegisterAttributesDescriptorsToIssuedIdentityPool(t *testing.T) {
	core, directory, token := newPoolRegisterCore(t)
	raw := encodeContractEnvelope(t)
	if _, err := core.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID: "runner-pool", InstanceUID: "instance-pool", AuthToken: token, Concurrency: 1,
		Capabilities:    []protocol.Capability{{NodeType: "acme.a", NodeVersion: 1}},
		DescriptorsJSON: raw,
	}, TransportInfo{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	records, err := directory.LiveRunnerDescriptors(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("LiveRunnerDescriptors: %v", err)
	}
	if len(records) != 1 || records[0].PoolID != "pool-register" || len(records[0].Descriptors) != 1 {
		t.Fatalf("records = %+v, want one record attributed to pool-register", records)
	}
}

func TestRegisterWithoutPoolRecordsEmptyPool(t *testing.T) {
	directory := NewMemoryRunnerDirectory()
	core := &Core{runners: directory}
	raw := encodeContractEnvelope(t)
	if _, err := core.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID: "runner-static", InstanceUID: "instance", Concurrency: 1,
		Capabilities:    []protocol.Capability{{NodeType: "acme.a", NodeVersion: 1}},
		DescriptorsJSON: raw,
	}, TransportInfo{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	records, err := directory.LiveRunnerDescriptors(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("LiveRunnerDescriptors: %v", err)
	}
	if len(records) != 1 || records[0].PoolID != "" || records[0].PoolName != "" {
		t.Fatalf("records = %+v, want one pool-less record", records)
	}
}
