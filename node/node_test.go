package node_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestFacadeExposesBuiltInNodes(t *testing.T) {
	if got := node.HTTP("GET", "https://example.com").NodeType(); got != "xflow.http" {
		t.Fatalf("HTTP().NodeType() = %q, want xflow.http", got)
	}
	if got := node.End().NodeType(); got != "xflow.end" {
		t.Fatalf("End().NodeType() = %q, want xflow.end", got)
	}
	if got := node.Set(map[string]any{"status": "ok"}).NodeType(); got != "xflow.transform.set" {
		t.Fatalf("Set().NodeType() = %q, want xflow.transform.set", got)
	}
	if got := node.Filter("items", "item.enabled").NodeType(); got != "xflow.transform.filter" {
		t.Fatalf("Filter().NodeType() = %q, want xflow.transform.filter", got)
	}
	params := map[string]any{
		"debugging_url": "http://chrome.example.test:9222",
		"entry_url":     "https://app.example.test/login",
	}
	browser := node.BrowserCDP(params)
	if got := browser.NodeType(); got != "xflow.browser.cdp" {
		t.Fatalf("BrowserCDP().NodeType() = %q, want xflow.browser.cdp", got)
	}
	if got := browser.RawParams(); !reflect.DeepEqual(got, params) {
		t.Fatalf("BrowserCDP().RawParams() = %#v, want %#v", got, params)
	}
	if got := browser.OnError(types.OnErrorOutput); got != browser || browser.OnErrorStrategy() != types.OnErrorOutput {
		t.Fatalf("BrowserCDP().OnError() did not preserve builder and strategy")
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
