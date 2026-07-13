package node_test

import (
	"context"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestFacadeExposesBuiltInNodesAndTriggers(t *testing.T) {
	if got := node.HTTP("GET", "https://example.com").NodeType(); got != "xflow.http" {
		t.Fatalf("HTTP().NodeType() = %q, want xflow.http", got)
	}
	if got := node.End().NodeType(); got != "xflow.end" {
		t.Fatalf("End().NodeType() = %q, want xflow.end", got)
	}
	if got := node.KafkaTrigger().NodeType(); got != "xflow.trigger.kafka" {
		t.Fatalf("KafkaTrigger().NodeType() = %q, want xflow.trigger.kafka", got)
	}
	if got := node.Set(map[string]any{"status": "ok"}).NodeType(); got != "xflow.transform.set" {
		t.Fatalf("Set().NodeType() = %q, want xflow.transform.set", got)
	}
	if got := node.Filter("items", "item.enabled").NodeType(); got != "xflow.transform.filter" {
		t.Fatalf("Filter().NodeType() = %q, want xflow.transform.filter", got)
	}
}

func TestFacadeExposesCustomNodeDefinition(t *testing.T) {
	def := node.Define("test.facade.echo", func(_ context.Context, input *types.Input) (*types.Output, error) {
		return &types.Output{Data: input.Data}, nil
	})

	builder := def.New(map[string]any{"k": "v"})
	if got := builder.NodeType(); got != "test.facade.echo" {
		t.Fatalf("NodeType() = %q, want test.facade.echo", got)
	}
}
