package node_test

import (
	"context"
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
	h, _ := node.Lookup("xflow.database")
	input := &node.Input{
		Params: map[string]any{
			"operation": "select",
			"table":     "users",
		},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing credential param")
	}
}

func TestDatabase_CredentialNotFound(t *testing.T) {
	h, _ := node.Lookup("xflow.database")
	b := node.Database("select", "users", "my_db")
	input := &node.Input{
		Params: b.RawParams().(map[string]any),
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for credential not found")
	}
}

func TestDatabase_InvalidTable(t *testing.T) {
	h, _ := node.Lookup("xflow.database")
	input := &node.Input{
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
}

func TestDatabase_UnknownOperation(t *testing.T) {
	h, _ := node.Lookup("xflow.database")
	input := &node.Input{
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
}

func TestDatabase_UpdateRequiresWhere(t *testing.T) {
	h, _ := node.Lookup("xflow.database")
	b := node.Database("update", "users", "db").SetData(map[string]any{"name": "test"})
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for update without where")
	}
}

func TestDatabase_DeleteRequiresWhere(t *testing.T) {
	h, _ := node.Lookup("xflow.database")
	b := node.Database("delete", "users", "db")
	input := &node.Input{Params: b.RawParams().(map[string]any)}
	input.SetCredentialResolver(func(name string) map[string]any {
		return map[string]any{"dsn": "user:pass@tcp(localhost)/test", "driver": "mysql"}
	})
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for delete without where")
	}
}
