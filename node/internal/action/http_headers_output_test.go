package action_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestHTTP_ResponseHeadersReachTheOutput closes the wiring half of the
// flattenHeaders gap. The white-box tests in http_flatten_headers_test.go pin
// what flattenHeaders computes, but they cannot see the one line that puts it
// in the node's output (`"headers": flattenHeaders(resp.Header)`): delete that
// key from the data map and every one of them stays green, while a workflow
// branching on a response header sees nothing at all.
//
// The existing jsonResponse helper already accepts a header set and no test
// in the package ever read one back, so this is the first assertion that the
// response's headers survive the trip into out.Data.
func TestHTTP_ResponseHeadersReachTheOutput(t *testing.T) {
	withHTTPClient(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusCreated, `{"ok":true}`, http.Header{
			"Location": []string{"https://api.example.test/users/42"},
			"Etag":     []string{`W/"v7"`},
		}), nil
	})

	h, ok := registry.Lookup("xflow.http")
	if !ok {
		t.Fatal("xflow.http is not registered")
	}
	b := node.HTTP("POST", "https://api.example.test/users")
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	headers, ok := out.Data["headers"].(map[string]any)
	if !ok {
		t.Fatalf("out.Data[\"headers\"] = %#v (%T), want map[string]any: the response "+
			"headers never reached the node's output, so no downstream node can read "+
			"Location, ETag or any other header", out.Data["headers"], out.Data["headers"])
	}
	if got := headers["Location"]; got != "https://api.example.test/users/42" {
		t.Fatalf("headers[\"Location\"] = %#v, want the created resource's URL", got)
	}
	if got := headers["Etag"]; got != `W/"v7"` {
		t.Fatalf("headers[\"Etag\"] = %#v, want W/\"v7\"", got)
	}
}
