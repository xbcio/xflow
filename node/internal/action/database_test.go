package action_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
)
func TestDatabase_Factory(t *testing.T) {
	b := node.Database("select", "users", "my_db").
		SetWhere(map[string]any{"id": 1}).
		SetColumns("id", "name").
		SetLimit(10)

	if b.NodeType() != "xflow.database" {
		t.Fatalf("expected xflow.database, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["operation"] != "select" {
		t.Fatalf("expected select, got %v", params["operation"])
	}
	if params["limit"] != 10 {
		t.Fatalf("expected limit=10, got %v", params["limit"])
	}
}

func TestDatabase_MissingCredential(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	input := &types.Input{
		Params: map[string]any{
			"operation": "select",
			"table":     "users",
		},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing credential param")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("missing credential must be permanent; got %v", err)
	}
}

func TestDatabase_CredentialNotFound(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	b := node.Database("select", "users", "my_db")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for credential not found")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("credential not found must be permanent; got %v", err)
	}
}

func TestDatabase_InvalidTable(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	input := &types.Input{
		Params: map[string]any{
			"operation":  "select",
			"table":      "users; DROP TABLE",
			"credential": "db",
		},
	}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("invalid table name must be permanent; got %v", err)
	}
}

func TestDatabase_UnknownOperation(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	input := &types.Input{
		Params: map[string]any{
			"operation":  "truncate",
			"table":      "users",
			"credential": "db",
		},
	}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unknown operation")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("unknown operation must be permanent; got %v", err)
	}
}

func TestDatabase_UpdateRequiresWhere(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	b := node.Database("update", "users", "db").SetData(map[string]any{"name": "test"})
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for update without where")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("missing where must be permanent; got %v", err)
	}
}

func TestDatabase_DeleteRequiresWhere(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	b := node.Database("delete", "users", "db")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for delete without where")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("missing where must be permanent; got %v", err)
	}
}

// TestDatabase_NoPoolIsPermanent pins the contract: when no ResourcePool is
// attached to the context, DatabaseNode.Execute must return a permanent
// classified error — NOT route to the error port. Missing pool is a deployment
// configuration error that retry will never fix.
//
// The input is constructed to pass all upstream validation gates (credential
// present + found, dsn non-empty, valid table, known operation) so the only
// failure point is the pool lookup inside acquireSQL.
func TestDatabase_NoPoolIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.database")
	b := node.Database("select", "users", "db")
	input := &types.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})

	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error when no resource pool is configured")
	}
	if !strings.Contains(err.Error(), "no resource pool configured") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "no resource pool configured")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("no-pool error must be permanent (not retried); got %v", err)
	}
}
