package workflowhash

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

func reconcileDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Namespace: "default", Name: "wf", Version: "v1",
		Nodes: []types.NodeDef{{Name: "start", Type: "xflow.start", Kind: types.NodeKindAction, Version: 1}},
	}
}

// staleV1 is the hash a pre-v2 binary stored for def: the v1 algorithm
// ignored node Timeout, so it is the hash of def with every Timeout cleared.
func staleV1(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	stripped := *def
	stripped.Nodes = append([]types.NodeDef(nil), def.Nodes...)
	for i := range stripped.Nodes {
		stripped.Nodes[i].Timeout = 0
	}
	return mustRuntime(t, &stripped)
}

func TestReconcile(t *testing.T) {
	v1Def := reconcileDef()
	v1 := mustRuntime(t, v1Def)
	v2Def := reconcileDef()
	v2Def.Nodes[0].Timeout = time.Minute
	v2 := mustRuntime(t, v2Def)

	tests := []struct {
		name        string
		stored      string
		storedDef   *types.WorkflowDef
		wantHash    string
		wantUpgrade bool
		wantErr     bool
	}{
		{"bare sha256 is recomputed", "sha256:deadbeef", v1Def, v1, true, false},
		{"audit fingerprint is recomputed", AuditPrefix + "feedface", v1Def, v1, true, false},
		{"stale v1 is recomputed", staleV1(t, v2Def), v2Def, v2, true, false},
		{"current v1 needs no upgrade", v1, v1Def, v1, false, false},
		{"v1 with nil def is kept", v1, nil, v1, false, false},
		{"v2 is current", v2, v2Def, v2, false, false},
		{"v2 with nil def is current", v2, nil, v2, false, false},
		{"legacy with nil def fails", "sha256:deadbeef", nil, "", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, upgrade, err := Reconcile(tt.stored, tt.storedDef, nil)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Reconcile(%q) error = nil, want error", tt.stored)
				}
				return
			}
			if err != nil {
				t.Fatalf("Reconcile(%q): %v", tt.stored, err)
			}
			if got != tt.wantHash || upgrade != tt.wantUpgrade {
				t.Fatalf("Reconcile(%q) = (%q, %v), want (%q, %v)", tt.stored, got, upgrade, tt.wantHash, tt.wantUpgrade)
			}
		})
	}
}

// fakeRegistry is a single-key registry with the backend's plain hash
// string compare. casErr, when set, fails the next UpdateDefinitionHash and
// lets the test rewrite the stored hash as a concurrent registrar would.
type fakeRegistry struct {
	rec     *backend.WorkflowRecord
	casErr  error
	onCAS   func(*backend.WorkflowRecord)
	updates int
}

func (f *fakeRegistry) AddWorkflow(_ context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	if f.rec == nil {
		rec.ID = "wf-new"
		f.rec = &rec
		return rec, nil
	}
	if f.rec.DefinitionHash != rec.DefinitionHash {
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	}
	return *f.rec, nil
}

func (f *fakeRegistry) GetWorkflowByKey(_ context.Context, key string) (backend.WorkflowRecord, error) {
	if f.rec == nil || f.rec.Key != key {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return *f.rec, nil
}

func (f *fakeRegistry) UpdateDefinitionHash(_ context.Context, id types.WorkflowID, oldHash, newHash string) error {
	f.updates++
	if f.casErr != nil {
		err := f.casErr
		f.casErr = nil
		if f.onCAS != nil {
			f.onCAS(f.rec)
		}
		return err
	}
	if f.rec == nil || f.rec.ID != id || f.rec.DefinitionHash != oldHash {
		return errors.New("cas mismatch")
	}
	f.rec.DefinitionHash = newHash
	return nil
}

func newRecord(t *testing.T, def *types.WorkflowDef) backend.WorkflowRecord {
	t.Helper()
	return backend.WorkflowRecord{Key: "default/wf@v1", DefinitionHash: mustRuntime(t, def), Definition: def}
}

func TestReconcileAdd(t *testing.T) {
	ctx := context.Background()

	t.Run("fresh record is created", func(t *testing.T) {
		reg := &fakeRegistry{}
		got, created, err := ReconcileAdd(ctx, reg, newRecord(t, reconcileDef()), nil)
		if err != nil || !created || got.ID != "wf-new" {
			t.Fatalf("ReconcileAdd = (%q, %v, %v), want (wf-new, true, nil)", got.ID, created, err)
		}
	})

	t.Run("legacy hash is upgraded", func(t *testing.T) {
		reg := &fakeRegistry{rec: &backend.WorkflowRecord{ID: "wf-old", Key: "default/wf@v1", DefinitionHash: "sha256:deadbeef", Definition: reconcileDef()}}
		rec := newRecord(t, reconcileDef())
		got, created, err := ReconcileAdd(ctx, reg, rec, nil)
		if err != nil || created || got.ID != "wf-old" {
			t.Fatalf("ReconcileAdd = (%q, %v, %v), want (wf-old, false, nil)", got.ID, created, err)
		}
		if reg.rec.DefinitionHash != rec.DefinitionHash || got.DefinitionHash != rec.DefinitionHash {
			t.Fatalf("stored hash = %q, returned %q, want upgraded %q", reg.rec.DefinitionHash, got.DefinitionHash, rec.DefinitionHash)
		}
	})

	t.Run("legacy hash of a different definition conflicts", func(t *testing.T) {
		other := reconcileDef()
		other.Nodes[0].Type = "xflow.end"
		reg := &fakeRegistry{rec: &backend.WorkflowRecord{ID: "wf-old", Key: "default/wf@v1", DefinitionHash: "sha256:deadbeef", Definition: other}}
		if _, _, err := ReconcileAdd(ctx, reg, newRecord(t, reconcileDef()), nil); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("ReconcileAdd error = %v, want ErrWorkflowConflict", err)
		}
		if reg.updates != 0 {
			t.Fatalf("UpdateDefinitionHash calls = %d, want 0", reg.updates)
		}
	})

	t.Run("lost CAS re-fetches a concurrent upgrade", func(t *testing.T) {
		rec := newRecord(t, reconcileDef())
		reg := &fakeRegistry{
			rec:    &backend.WorkflowRecord{ID: "wf-old", Key: "default/wf@v1", DefinitionHash: "sha256:deadbeef", Definition: reconcileDef()},
			casErr: errors.New("cas mismatch"),
			onCAS:  func(r *backend.WorkflowRecord) { r.DefinitionHash = rec.DefinitionHash },
		}
		got, created, err := ReconcileAdd(ctx, reg, rec, nil)
		if err != nil || created || got.ID != "wf-old" {
			t.Fatalf("ReconcileAdd = (%q, %v, %v), want (wf-old, false, nil)", got.ID, created, err)
		}
	})

	t.Run("lost CAS to a replacement conflicts", func(t *testing.T) {
		reg := &fakeRegistry{
			rec:    &backend.WorkflowRecord{ID: "wf-old", Key: "default/wf@v1", DefinitionHash: "sha256:deadbeef", Definition: reconcileDef()},
			casErr: errors.New("cas mismatch"),
			onCAS:  func(r *backend.WorkflowRecord) { r.DefinitionHash = "sha256:replaced" },
		}
		if _, _, err := ReconcileAdd(ctx, reg, newRecord(t, reconcileDef()), nil); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("ReconcileAdd error = %v, want ErrWorkflowConflict", err)
		}
	})

	t.Run("stale v1 hit on a real change conflicts and corrects the hash", func(t *testing.T) {
		withTimeout := reconcileDef()
		withTimeout.Nodes[0].Timeout = time.Minute
		stale := staleV1(t, withTimeout)
		reg := &fakeRegistry{rec: &backend.WorkflowRecord{ID: "wf-old", Key: "default/wf@v1", DefinitionHash: stale, Definition: withTimeout}}
		// Dropping the timeout hashes to the stale v1 hash: a plain hash hit.
		if _, _, err := ReconcileAdd(ctx, reg, newRecord(t, reconcileDef()), nil); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("ReconcileAdd error = %v, want ErrWorkflowConflict", err)
		}
		if want := mustRuntime(t, withTimeout); reg.rec.DefinitionHash != want {
			t.Fatalf("stored hash = %q, want corrected %q", reg.rec.DefinitionHash, want)
		}
	})

	t.Run("non-conflict registry error passes through", func(t *testing.T) {
		boom := errors.New("boom")
		reg := &errRegistry{err: boom}
		if _, _, err := ReconcileAdd(ctx, reg, newRecord(t, reconcileDef()), nil); !errors.Is(err, boom) {
			t.Fatalf("ReconcileAdd error = %v, want %v", err, boom)
		}
	})
}

type errRegistry struct {
	fakeRegistry
	err error
}

func (e *errRegistry) AddWorkflow(context.Context, backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	return backend.WorkflowRecord{}, e.err
}
