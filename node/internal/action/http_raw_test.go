package action_test

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	actionimpl "github.com/xbcio/xflow/node/internal/action"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// rawInput builds the params a raw-mode node would receive. It goes through the
// builder rather than constructing the map by hand so the builder and the
// handler cannot drift apart.
func rawInput(b *node.HTTPNode) *types.Input {
	return &types.Input{Params: b.RawParams().(map[string]any)}
}

func TestHTTPRaw_FactorySerializesModeAndBody(t *testing.T) {
	b := node.HTTP("POST", "https://example.test").
		SetMode("raw").
		SetBodyB64("aGVsbG8=").
		SetHeaders(map[string]any{"Content-Type": "text/plain"}).
		DisableRedirect().
		InsecureSkipVerify()

	params := b.RawParams().(map[string]any)
	if params["mode"] != "raw" {
		t.Fatalf("expected mode=raw, got %v", params["mode"])
	}
	if params["body_b64"] != "aGVsbG8=" {
		t.Fatalf("expected body_b64 to round-trip, got %v", params["body_b64"])
	}
	opts := params["options"].(map[string]any)
	if opts["disable_redirect"] != true {
		t.Fatalf("expected disable_redirect=true, got %v", opts["disable_redirect"])
	}
	if opts["insecure_skip_verify"] != true {
		t.Fatalf("expected insecure_skip_verify=true, got %v", opts["insecure_skip_verify"])
	}
}

// TestHTTPRaw_BodyBytesAreSentVerbatim pins the reason raw mode exists at all:
// a form-urlencoded body must reach the target as the exact bytes the caller
// produced. JSON mode marshals the body and forces application/json, which
// cannot express this request.
func TestHTTPRaw_BodyBytesAreSentVerbatim(t *testing.T) {
	const body = "user=admin&pass=a%2Bb%20c"
	var received string
	var contentType string
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		received = string(raw)
		contentType = r.Header.Get("Content-Type")
		return jsonResponse(200, `ok`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("POST", "https://example.test").
		SetMode("raw").
		SetBodyB64(base64.StdEncoding.EncodeToString([]byte(body)))
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received != body {
		t.Fatalf("body must be sent verbatim; want %q, got %q", body, received)
	}
	if contentType != "" {
		t.Fatalf("raw mode must not inject a Content-Type, got %q", contentType)
	}
	if out.Port != "main" {
		t.Fatalf("expected port main, got %q", out.Port)
	}
}

// TestHTTPRaw_CallerContentTypeIsPreserved is the complement of the test above:
// the caller owns every header, and an explicit Content-Type must survive.
func TestHTTPRaw_CallerContentTypeIsPreserved(t *testing.T) {
	const contentType = "application/x-www-form-urlencoded"
	var received string
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		received = r.Header.Get("Content-Type")
		return jsonResponse(200, `ok`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("POST", "https://example.test").
		SetMode("raw").
		SetHeaders(map[string]any{"Content-Type": contentType}).
		SetBodyB64(base64.StdEncoding.EncodeToString([]byte("a=b")))
	if _, err := h.Execute(context.Background(), rawInput(b)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received != contentType {
		t.Fatalf("want %q, got %q", contentType, received)
	}
}

// TestHTTPRaw_SetBodyIsIgnoredInRawMode pins that the two body parameters do
// not blend: a workflow that sets both must get the raw one, because silently
// JSON-marshalling the other would send a body the caller never described.
func TestHTTPRaw_SetBodyIsIgnoredInRawMode(t *testing.T) {
	var received string
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		received = string(raw)
		return jsonResponse(200, `ok`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("POST", "https://example.test").
		SetMode("raw").
		SetBody(map[string]any{"ignored": true}).
		SetBodyB64(base64.StdEncoding.EncodeToString([]byte("chosen")))
	if _, err := h.Execute(context.Background(), rawInput(b)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received != "chosen" {
		t.Fatalf("body_b64 must win in raw mode; got %q", received)
	}
}

// TestHTTPRaw_EveryStatusStaysOnMainPort is the central contract of raw mode.
// JSON mode turns a 4xx into a permanent error and a 5xx into a retryable one;
// a scanner needs all of them as results, and the retryable classification
// would re-send a state-changing request.
func TestHTTPRaw_EveryStatusStaysOnMainPort(t *testing.T) {
	for _, code := range []int{200, 201, 301, 400, 401, 403, 404, 408, 429, 500, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
				return jsonResponse(code, `{"error":"x"}`, nil), nil
			})

			h, _ := registry.Lookup("xflow.http")
			b := node.HTTP("GET", "https://example.test").SetMode("raw")
			out, err := h.Execute(context.Background(), rawInput(b))
			if err != nil {
				t.Fatalf("status %d must be a result, not an error; got %v", code, err)
			}
			if out.Port != "main" {
				t.Fatalf("status %d must stay on main, got port %q", code, out.Port)
			}
			if out.Data["status"] != code {
				t.Fatalf("want status %d, got %v", code, out.Data["status"])
			}
			if _, ok := out.Data["error"]; ok {
				t.Fatalf("status %d must not carry an error field: %v", code, out.Data["error"])
			}
		})
	}
}

// TestHTTPRaw_TransportFailureIsAResultNotAnError pins that a refused
// connection cannot replace the output of the batch it belongs to. A batch of N
// requests has to yield N ordered results, so the failure is encoded as status
// 0 -- the same encoding the local executor uses.
func TestHTTPRaw_TransportFailureIsAResultNotAnError(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection refused")
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("transport failure must be a result, got error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port main, got %q", out.Port)
	}
	if out.Data["status"] != 0 {
		t.Fatalf("expected status 0 for a failure, got %v", out.Data["status"])
	}
	if out.Data["error"] == "" || out.Data["error"] == nil {
		t.Fatal("expected a non-empty error field")
	}
	if out.Data["body_b64"] != "" {
		t.Fatalf("expected empty body, got %v", out.Data["body_b64"])
	}
	// The node-level failure path must not be reachable: reaching it would abort
	// the whole map batch instead of filling this item's slot.
	if types.IsPermanent(err) {
		t.Fatal("failure must not be classified permanent")
	}
}

// TestHTTPRaw_PolicyViolationIsStillAudible pins the one exception to "raw mode
// never errors": a host-policy rejection is a security decision, and burying it
// as a per-request status-0 result would make a blocked SSRF attempt
// indistinguishable from a host that was simply down.
func TestHTTPRaw_PolicyViolationIsStillAudible(t *testing.T) {
	withHostPolicy(t, actionimpl.NewHostPolicy([]string{"example.test"}, nil))
	called := false
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		called = true
		return jsonResponse(200, `{}`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://evil.test/x").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err == nil {
		t.Fatalf("host-policy rejection must error, got output %+v", out)
	}
	if !types.IsPermanent(err) {
		t.Fatalf("host-policy rejection must be permanent; got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "http.host_not_allowed" {
		t.Fatalf("expected code=http.host_not_allowed, got %T %v", err, err)
	}
	if called {
		t.Fatal("the request must not be dispatched")
	}
}

// TestHTTPRaw_OversizedBodyIsTruncatedAndFlagged documents a deliberate
// divergence from JSON mode: exceeding max_response_bytes truncates and flags
// rather than failing, so the status and headers that did come back survive.
func TestHTTPRaw_OversizedBodyIsTruncatedAndFlagged(t *testing.T) {
	const limit = 64
	oversized := strings.Repeat("A", limit*4)
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(201, oversized, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	input := rawInput(b)
	input.Params["options"] = map[string]any{"max_response_bytes": limit}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("oversized body must not fail the node in raw mode; got %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port main, got %q", out.Port)
	}
	if out.Data["body_truncated"] != true {
		t.Fatalf("expected body_truncated=true, got %v", out.Data["body_truncated"])
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Data["body_b64"].(string))
	if err != nil {
		t.Fatalf("body_b64 must decode: %v", err)
	}
	if len(decoded) != limit {
		t.Fatalf("expected the body truncated to %d bytes, got %d", limit, len(decoded))
	}
	// The status of the truncated response is still reportable.
	if out.Data["status"] != 201 {
		t.Fatalf("expected status 201 to survive truncation, got %v", out.Data["status"])
	}
}

func TestHTTPRaw_WithinLimitIsNotFlagged(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(200, "short", nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["body_truncated"] != false {
		t.Fatalf("expected body_truncated=false, got %v", out.Data["body_truncated"])
	}
}

// TestHTTPRaw_BodyIsAlwaysBase64EvenWhenItIsValidJSON pins that the node does
// not "helpfully" parse a JSON response. A re-serialized document loses key
// order and number formatting, so the bytes would no longer match what the
// target sent.
func TestHTTPRaw_BodyIsAlwaysBase64EvenWhenItIsValidJSON(t *testing.T) {
	const body = `{"z":1,"a":2.50}`
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(200, body, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out.Data["body"]; ok {
		t.Fatalf("raw mode must not expose a decoded body, got %v", out.Data["body"])
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Data["body_b64"].(string))
	if err != nil {
		t.Fatalf("body_b64 must decode: %v", err)
	}
	if string(decoded) != body {
		t.Fatalf("want %q, got %q", body, string(decoded))
	}
}

// TestHTTPRaw_BinaryBodyRoundTripsExactly covers the payloads JSON mode can only
// mangle: bytes that are not valid UTF-8.
func TestHTTPRaw_BinaryBodyRoundTripsExactly(t *testing.T) {
	payload := []byte{0x00, 0xff, 0xfe, 0x80, 0x01, 0x7f, 0xc3, 0x28}
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(string(payload))),
		}, nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Data["body_b64"].(string))
	if err != nil {
		t.Fatalf("body_b64 must decode: %v", err)
	}
	if string(decoded) != string(payload) {
		t.Fatalf("binary body must round-trip; want %v, got %v", payload, decoded)
	}
}

func TestHTTPRaw_TimingIsReportedPerRequest(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(200, "ok", nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	started, okStart := out.Data["started_at"].(int64)
	finished, okFinish := out.Data["finished_at"].(int64)
	if !okStart || !okFinish {
		t.Fatalf("expected integer timestamps, got %T and %T", out.Data["started_at"], out.Data["finished_at"])
	}
	if started <= 0 || finished < started {
		t.Fatalf("expected 0 < started <= finished, got %d and %d", started, finished)
	}
	duration, ok := out.Data["duration_ms"].(int64)
	if !ok || duration < 0 {
		t.Fatalf("expected a non-negative duration_ms, got %v", out.Data["duration_ms"])
	}
}

// TestHTTPRaw_DisableRedirectReturnsTheFirstResponse covers the credential-leak
// control: following a redirect re-sends the request to a host the caller never
// chose, so a scanner must be able to observe the 3xx itself.
func TestHTTPRaw_DisableRedirectReturnsTheFirstResponse(t *testing.T) {
	calls := 0
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		return jsonResponse(302, ``, http.Header{"Location": []string{"https://other.test/next"}}), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test/start").SetMode("raw").DisableRedirect()
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["status"] != 302 {
		t.Fatalf("expected the 302 to be returned, got %v", out.Data["status"])
	}
	if calls != 1 {
		t.Fatalf("expected exactly one dispatch, got %d", calls)
	}
}

// TestHTTPRaw_RedirectIsFollowedByDefault is the control for the test above: the
// flag, not raw mode, is what changes redirect behaviour.
func TestHTTPRaw_RedirectIsFollowedByDefault(t *testing.T) {
	var hosts []string
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		hosts = append(hosts, r.URL.Host)
		if len(hosts) == 1 {
			return jsonResponse(302, ``, http.Header{"Location": []string{"https://other.test/next"}}), nil
		}
		return jsonResponse(200, `arrived`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test/start").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["status"] != 200 {
		t.Fatalf("expected the redirect to be followed, got %v", out.Data["status"])
	}
	if len(hosts) != 2 || hosts[1] != "other.test" {
		t.Fatalf("expected a second hop to other.test, got %v", hosts)
	}
}

// TestHTTPRaw_InsecureSkipVerifyAcceptsSelfSignedHost covers the actual target
// state -- a certificate the scanner cannot validate -- using a real TLS
// handshake rather than an injected transport.
func TestHTTPRaw_InsecureSkipVerifyAcceptsSelfSignedHost(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	withHostPolicy(t, nil)

	h, _ := registry.Lookup("xflow.http")

	// Control: with verification on, the same host fails to handshake.
	verified := node.HTTP("GET", server.URL).SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(verified))
	if err != nil {
		t.Fatalf("a TLS failure must still be a result, got error: %v", err)
	}
	if out.Data["status"] != 0 {
		t.Fatalf("expected the self-signed host to fail verification, got status %v", out.Data["status"])
	}

	relaxed := node.HTTP("GET", server.URL).SetMode("raw").InsecureSkipVerify()
	out, err = h.Execute(context.Background(), rawInput(relaxed))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["status"] != 200 {
		t.Fatalf("expected status 200 with verification disabled, got %v (error=%v)", out.Data["status"], out.Data["error"])
	}
	decoded, err := base64.StdEncoding.DecodeString(out.Data["body_b64"].(string))
	if err != nil {
		t.Fatalf("body_b64 must decode: %v", err)
	}
	if string(decoded) != "hello" {
		t.Fatalf("want %q, got %q", "hello", string(decoded))
	}
}

func TestHTTPRaw_InvalidModeIsRejected(t *testing.T) {
	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test")
	input := rawInput(b)
	input.Params["mode"] = "binary"

	out, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatalf("expected an unknown mode to be rejected, got output %+v", out)
	}
	if !types.IsPermanent(err) {
		t.Fatalf("unknown mode must be permanent; got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "http.invalid_mode" {
		t.Fatalf("expected code=http.invalid_mode, got %T %v", err, err)
	}
}

// TestHTTPRaw_InvalidBase64DoesNotEchoThePayload pins that the rejected value is
// not quoted back: for a scanner that payload can be a credential or a stored
// attack string, and the error message is written to the execution audit trail.
func TestHTTPRaw_InvalidBase64DoesNotEchoThePayload(t *testing.T) {
	const secret = "super-secret-token!!"
	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("POST", "https://example.test").SetMode("raw")
	input := rawInput(b)
	input.Params["body_b64"] = secret

	out, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatalf("expected invalid base64 to be rejected, got output %+v", out)
	}
	if !types.IsPermanent(err) {
		t.Fatalf("invalid base64 must be permanent; got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "http.invalid_body_b64" {
		t.Fatalf("expected code=http.invalid_body_b64, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("the rejected payload must not be echoed: %v", err)
	}
}

// TestHTTPRaw_QueryParamsStillApply pins that raw mode reuses the shared request
// construction rather than forking it.
func TestHTTPRaw_QueryParamsStillApply(t *testing.T) {
	var got string
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		got = r.URL.Query().Get("page")
		return jsonResponse(200, "ok", nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw").SetQuery(map[string]any{"page": "7"})
	if _, err := h.Execute(context.Background(), rawInput(b)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "7" {
		t.Fatalf("expected query page=7, got %q", got)
	}
}

// TestHTTPRaw_HeadersAreFlattenedLikeJSONMode keeps the two modes' output shape
// consistent for the fields they share, so a consumer can read `headers` the
// same way regardless of mode.
func TestHTTPRaw_HeadersAreFlattenedLikeJSONMode(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(200, "ok", http.Header{
			"Content-Type": []string{"text/plain"},
			"Set-Cookie":   []string{"a=1", "b=2"},
		}), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test").SetMode("raw")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	headers, ok := out.Data["headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected a headers map, got %T", out.Data["headers"])
	}
	if headers["Content-Type"] != "text/plain" {
		t.Fatalf("single-valued header must unwrap to a scalar, got %v", headers["Content-Type"])
	}
	if _, isSlice := headers["Set-Cookie"].([]string); !isSlice {
		t.Fatalf("multi-valued header must stay a slice, got %T", headers["Set-Cookie"])
	}
}

// TestHTTPJSONMode_IsUnaffectedByRawModeExisting is a guard on the default path:
// every raw-mode branch must be opt-in, so a workflow that never sets `mode`
// keeps the parsed-body and error-classification behaviour its tests pin.
func TestHTTPJSONMode_IsUnaffectedByRawModeExisting(t *testing.T) {
	withHTTPClient(t, func(r *http.Request) (*http.Response, error) {
		return jsonResponse(404, `{"error":"not found"}`, nil), nil
	})

	h, _ := registry.Lookup("xflow.http")
	b := node.HTTP("GET", "https://example.test")
	out, err := h.Execute(context.Background(), rawInput(b))
	if err == nil || out != nil {
		t.Fatalf("json mode must still fail on 4xx; got out=%+v err=%v", out, err)
	}
	if !types.IsPermanent(err) {
		t.Fatalf("json-mode 4xx must remain permanent; got %v", err)
	}
}
