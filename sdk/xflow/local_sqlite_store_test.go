package xflow

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store/sqlstore/sqlitestore"
	"github.com/xbcio/xflow/types"
)

// TestNewLocalWithSQLiteStoreSurvivesRestart is the local-first deployment
// shape: an embedded engine mirrors into a SQLite file, the engine and the
// database are closed, and a fresh process-equivalent reopens the same file and
// still reads the finished execution and its nodes.
func TestNewLocalWithSQLiteStoreSurvivesRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "xflow.db")

	st, err := sqlitestore.New(path, sqlitestore.WithAutoMigrate())
	if err != nil {
		t.Fatalf("sqlitestore.New() error = %v", err)
	}
	eng, err := NewLocal(WithStore(st))
	if err != nil {
		t.Fatalf("NewLocal(WithStore) error = %v", err)
	}

	wf := Workflow("local-sqlite-mirror")
	start := wf.Node("start", node.Start())
	set := wf.Node("set", node.Set(map[string]any{"done": true}))
	wf.Connect(start, set)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "T-7"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(ctx, id); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	eng.Stop()
	sqlDB, err := st.DB().DB()
	if err != nil {
		t.Fatalf("resolve *sql.DB: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	reopened, err := sqlitestore.New(path)
	if err != nil {
		t.Fatalf("reopen sqlitestore: %v", err)
	}
	t.Cleanup(func() {
		if db, err := reopened.DB().DB(); err == nil {
			_ = db.Close()
		}
	})
	rec, err := reopened.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution after reopen: %v", err)
	}
	if rec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status after reopen = %q, want success", rec.Status)
	}
	for _, name := range []string{"start", "set"} {
		n, err := reopened.GetNode(ctx, id, name)
		if err != nil {
			t.Fatalf("GetNode(%q) after reopen: %v", name, err)
		}
		if n.Status != types.NodeStatusSuccess {
			t.Fatalf("node %q status after reopen = %q, want success", name, n.Status)
		}
	}
}
