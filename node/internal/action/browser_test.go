package action

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestBrowserCDPDescriptorAndRegistration(t *testing.T) {
	handler, ok := registry.Lookup(browserCDPNodeType)
	if !ok {
		t.Fatal("xflow.browser.cdp is not registered")
	}
	d := handler.Descriptor()
	if d.Type != browserCDPNodeType || d.Kind != types.NodeKindAction || !reflect.DeepEqual(d.Capabilities, []string{"browser.cdp.v1"}) {
		t.Fatalf("descriptor = %+v", d)
	}
	if len(d.Inputs) != 1 || len(d.Outputs) != 2 {
		t.Fatalf("ports = inputs %v outputs %v", d.Inputs, d.Outputs)
	}
}

func TestBrowserCDPConfigDefaultsAndLeases(t *testing.T) {
	defaults := BrowserCDPConfigDefaults()
	if len(defaults.EndpointAllowlist) != 0 || defaults.MaxContexts != 1 || defaults.QueueTimeout != 5*time.Second || defaults.ConnectTimeout != 5*time.Second {
		t.Fatalf("defaults = %+v", defaults)
	}
	cfg := defaults
	cfg.EndpointAllowlist = []string{"CHROME-B.TEST", "chrome-a.test", "chrome-b.test"}
	release1, err := AcquireBrowserCDPConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer release1()
	snapshot := currentBrowserConfigSnapshot()
	if want := []string{"chrome-a.test", "chrome-b.test"}; !reflect.DeepEqual(snapshot.config.EndpointAllowlist, want) {
		t.Fatalf("allowlist = %v, want %v", snapshot.config.EndpointAllowlist, want)
	}
	release2, err := AcquireBrowserCDPConfig(cfg)
	if err != nil {
		t.Fatalf("identical acquire: %v", err)
	}
	release2()
	conflict := cfg
	conflict.MaxContexts++
	if _, err := AcquireBrowserCDPConfig(conflict); err == nil {
		t.Fatal("conflicting active config accepted")
	}
}

func TestBrowserCDPConfigRejectsEndpointURLs(t *testing.T) {
	for _, host := range []string{"", "https://chrome.test", "chrome.test:9222", "chrome.test/path", " chrome.test"} {
		cfg := BrowserCDPConfigDefaults()
		cfg.EndpointAllowlist = []string{host}
		if _, err := AcquireBrowserCDPConfig(cfg); err == nil {
			t.Fatalf("allowlist host %q accepted", host)
		}
	}
}

func TestParseBrowserCDPParamsValidatesDebuggingEndpoint(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	policy := NewHostPolicy([]string{"app.test"}, nil)

	valid := []string{
		"http://chrome.test:9222",
		"http://chrome.test:9222/",
		"https://chrome.test:9222/json/version",
		"ws://chrome.test:9222/devtools/browser/browser-id",
		"wss://chrome.test/devtools/browser/01234567-89ab-cdef",
	}
	for _, endpoint := range valid {
		t.Run("accept_"+strings.NewReplacer(":", "_", "/", "_").Replace(endpoint), func(t *testing.T) {
			params := validBrowserParams()
			params["debugging_url"] = endpoint
			parsed, err := parseBrowserCDPParams(params, snapshot, policy)
			if err != nil {
				t.Fatalf("parse debugging_url %q: %v", endpoint, err)
			}
			if got := parsed.DebuggingURL.String(); got != endpoint {
				t.Fatalf("DebuggingURL = %q, want %q", got, endpoint)
			}
		})
	}

	invalid := []string{
		"http://user@chrome.test:9222",
		"http://chrome.test:9222?token=secret",
		"http://chrome.test:9222/#fragment",
		"http://chrome.test:9222/json/list",
		"http://chrome.test:9222/json/version/",
		"https://chrome.test:9222/devtools/browser/browser-id",
		"ws://user@chrome.test:9222/devtools/browser/browser-id",
		"ws://chrome.test:9222/devtools/browser/browser-id?token=secret",
		"ws://chrome.test:9222/devtools/browser/browser-id#fragment",
		"ws://chrome.test:9222",
		"ws://chrome.test:9222/",
		"ws://chrome.test:9222/devtools/browser",
		"ws://chrome.test:9222/devtools/browser/",
		"ws://chrome.test:9222/devtools/browser/browser-id/extra",
		"ws://chrome.test:9222/devtools/page/page-id",
		"ws://chrome.test:9222/devtools/browser/browser-id%2Fextra",
	}
	for _, endpoint := range invalid {
		t.Run("reject_"+strings.NewReplacer(":", "_", "/", "_").Replace(endpoint), func(t *testing.T) {
			params := validBrowserParams()
			params["debugging_url"] = endpoint
			_, err := parseBrowserCDPParams(params, snapshot, policy)
			var invalidParam *browserParamError
			if !errors.As(err, &invalidParam) || invalidParam.field != "debugging_url" {
				t.Fatalf("parse debugging_url %q: error = %v, want invalid debugging_url", endpoint, err)
			}
		})
	}
}

func TestResolveBrowserWebSocketEndpointPassesThroughCompleteEndpoint(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	for _, endpoint := range []string{
		"ws://chrome.test:9222/devtools/browser/browser-id",
		"wss://chrome.test/devtools/browser/01234567-89ab-cdef",
	} {
		t.Run(endpoint[:strings.Index(endpoint, ":")], func(t *testing.T) {
			debuggingURL := mustParseBrowserTestURL(t, endpoint)
			resolved, err := resolveBrowserWebSocketEndpoint(context.Background(), debuggingURL, snapshot)
			if err != nil {
				t.Fatalf("resolve %q: %v", endpoint, err)
			}
			if got := fmt.Sprint(resolved); got != endpoint {
				t.Fatalf("resolved endpoint = %q, want %q", got, endpoint)
			}
		})
	}
}

func TestResolveBrowserWebSocketEndpointDiscoversJSONVersion(t *testing.T) {
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet || r.URL.RequestURI() != "/json/version" {
			http.Error(w, "unexpected discovery request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"webSocketDebuggerUrl":%q}`, browserTestWebSocketURL(t, server.URL, "/devtools/browser/discovered-id"))
	}))
	defer server.Close()

	serverURL := mustParseBrowserTestURL(t, server.URL)
	snapshot := testBrowserSnapshot(serverURL.Hostname())
	for _, path := range []string{"", "/", "/json/version"} {
		t.Run("path_"+strings.ReplaceAll(path, "/", "_"), func(t *testing.T) {
			debuggingURL := *serverURL
			debuggingURL.Path = path
			resolved, err := resolveBrowserWebSocketEndpoint(context.Background(), &debuggingURL, snapshot)
			if err != nil {
				t.Fatalf("resolve %q: %v", debuggingURL.String(), err)
			}
			want := browserTestWebSocketURL(t, server.URL, "/devtools/browser/discovered-id")
			if got := fmt.Sprint(resolved); got != want {
				t.Fatalf("resolved endpoint = %q, want %q", got, want)
			}
		})
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("discovery requests = %d, want 3", got)
	}
}

func TestResolveBrowserWebSocketEndpointBoundsDiscoveryByConnectTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	debuggingURL := mustParseBrowserTestURL(t, server.URL)
	snapshot := testBrowserSnapshot(debuggingURL.Hostname())
	snapshot.config.ConnectTimeout = 75 * time.Millisecond
	outerCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	started := time.Now()
	_, err := resolveBrowserWebSocketEndpoint(outerCtx, debuggingURL, snapshot)
	elapsed := time.Since(started)

	select {
	case <-entered:
	default:
		t.Fatal("discovery handler was not reached")
	}
	var timedOut *browserTimeoutError
	if !errors.As(err, &timedOut) {
		t.Fatalf("resolve error = %v, want browserTimeoutError", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("discovery returned after %s, want connect-timeout-bounded return", elapsed)
	}
	if outerCtx.Err() != nil {
		t.Fatalf("outer context expired: %v", outerCtx.Err())
	}
}

func TestResolveBrowserWebSocketEndpointClassifiesStalledDiscoveryBodyAsTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"webSocketDebuggerUrl":`))
		w.(http.Flusher).Flush()
		close(entered)
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	debuggingURL := mustParseBrowserTestURL(t, server.URL)
	snapshot := testBrowserSnapshot(debuggingURL.Hostname())
	snapshot.config.ConnectTimeout = 75 * time.Millisecond

	_, err := resolveBrowserWebSocketEndpoint(context.Background(), debuggingURL, snapshot)
	select {
	case <-entered:
	default:
		t.Fatal("discovery response body was not reached")
	}
	var timedOut *browserTimeoutError
	if !errors.As(err, &timedOut) {
		t.Fatalf("resolve error = %v, want browserTimeoutError", err)
	}
}

func TestResolveBrowserWebSocketEndpointRejectsRedirect(t *testing.T) {
	var redirectedRequests atomic.Int32
	var target *httptest.Server
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		_, _ = fmt.Fprintf(w, `{"webSocketDebuggerUrl":%q}`, browserTestWebSocketURL(t, target.URL, "/devtools/browser/redirected-id"))
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/redirected", http.StatusFound)
	}))
	defer source.Close()

	debuggingURL := mustParseBrowserTestURL(t, source.URL)
	snapshot := testBrowserSnapshot(debuggingURL.Hostname())
	if _, err := resolveBrowserWebSocketEndpoint(context.Background(), debuggingURL, snapshot); err == nil {
		t.Fatal("discovery redirect was accepted")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, want 0", got)
	}
}

func TestResolveBrowserWebSocketEndpointRejectsInvalidDiscoveryResponse(t *testing.T) {
	const oversizedDiscoveryResponseBytes = 2 << 20

	tests := []struct {
		name           string
		body           func(serverURL string) string
		wantHostDenied bool
	}{
		{name: "malformed_json", body: func(string) string { return `{"webSocketDebuggerUrl":` }},
		{name: "missing_websocket_url", body: func(string) string { return `{"Browser":"Chrome"}` }},
		{name: "wrong_websocket_url_type", body: func(string) string { return `{"webSocketDebuggerUrl":42}` }},
		{name: "oversized_json", body: func(serverURL string) string {
			return `{"padding":"` + strings.Repeat("x", oversizedDiscoveryResponseBytes) + `","webSocketDebuggerUrl":"` + browserTestWebSocketURL(t, serverURL, "/devtools/browser/browser-id") + `"}`
		}},
		{name: "non_websocket_scheme", body: func(serverURL string) string {
			return fmt.Sprintf(`{"webSocketDebuggerUrl":%q}`, serverURL+"/devtools/browser/browser-id")
		}},
		{name: "non_browser_path", body: func(serverURL string) string {
			return fmt.Sprintf(`{"webSocketDebuggerUrl":%q}`, browserTestWebSocketURL(t, serverURL, "/devtools/page/page-id"))
		}},
		{name: "websocket_query", body: func(serverURL string) string {
			return fmt.Sprintf(`{"webSocketDebuggerUrl":%q}`, browserTestWebSocketURL(t, serverURL, "/devtools/browser/browser-id")+"?token=secret")
		}},
		{name: "unallowlisted_websocket_host", wantHostDenied: true, body: func(string) string {
			return `{"webSocketDebuggerUrl":"ws://outside.test/devtools/browser/browser-id"}`
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RequestURI() != "/json/version" {
					http.Error(w, "unexpected discovery request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body(server.URL)))
			}))
			defer server.Close()

			debuggingURL := mustParseBrowserTestURL(t, server.URL)
			snapshot := testBrowserSnapshot(debuggingURL.Hostname())
			_, err := resolveBrowserWebSocketEndpoint(context.Background(), debuggingURL, snapshot)
			if err == nil {
				t.Fatal("invalid discovery response was accepted")
			}
			if test.wantHostDenied {
				var denied *browserHostDeniedError
				if !errors.As(err, &denied) {
					t.Fatalf("error = %v, want browserHostDeniedError", err)
				}
			}
		})
	}
}

func TestParseBrowserCDPParamsDefaultsAndStrictTypes(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	policy := NewHostPolicy([]string{"app.test"}, nil)
	params, err := parseBrowserCDPParams(validBrowserParams(), snapshot, policy)
	if err != nil {
		t.Fatal(err)
	}
	if params.TargetHost != "app.test" || params.Plan.WaitSelector != "body" || params.Plan.Retries != 3 || params.Timeout != 30*time.Second || params.TotalTimeout != 45*time.Second {
		t.Fatalf("params defaults = %+v", params)
	}
	if params.TTL.EarlyExpireRatio != .1 || params.TTL.EarlyExpireMin != 5*time.Minute || params.TTL.CredentialTTLCap != 4*time.Hour {
		t.Fatalf("ttl defaults = %+v", params.TTL)
	}

	invalid := []func(map[string]any){
		func(m map[string]any) { m["timeout_ms"] = "30000" },
		func(m map[string]any) { m["timeout_ms"] = 1.5 },
		func(m map[string]any) { m["timeout_ms"] = -1 },
		func(m map[string]any) { m["plan"] = []any{} },
		func(m map[string]any) { m["seed_cookies"] = map[string]any{} },
		func(m map[string]any) { m["unknown"] = true },
		func(m map[string]any) { m["target_host"] = "app.test:443" },
		func(m map[string]any) { m["plan"] = map[string]any{"retries": -1} },
		func(m map[string]any) { m["plan"] = map[string]any{"retries": maxBrowserRetries + 1} },
	}
	for i, mutate := range invalid {
		m := validBrowserParams()
		mutate(m)
		if _, err := parseBrowserCDPParams(m, snapshot, policy); err == nil {
			t.Fatalf("invalid case %d accepted: %#v", i, m)
		}
	}
	zeroRetries := validBrowserParams()
	zeroRetries["plan"] = map[string]any{"retries": 0}
	parsed, err := parseBrowserCDPParams(zeroRetries, snapshot, policy)
	if err != nil || parsed.Plan.Retries != 0 {
		t.Fatalf("zero retries: params=%+v err=%v", parsed, err)
	}
}

func TestParseBrowserCDPParamsRejectsCookieURLOutsideTargetHost(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	params := validBrowserParams()
	params["harvest"] = map[string]any{"cookie_urls": []any{"https://other.test/session"}}
	_, err := parseBrowserCDPParams(params, snapshot, NewHostPolicy([]string{"app.test", "other.test"}, nil))
	var invalid *browserParamError
	if !errors.As(err, &invalid) || invalid.field != "harvest.cookie_urls" {
		t.Fatalf("error = %v, want invalid harvest.cookie_urls", err)
	}
}

func TestBrowserRequestScopeMatchesAPIPathPatternsCaseInsensitively(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	params := validBrowserParams()
	params["harvest"] = map[string]any{"request_scope": map[string]any{
		"api_path_patterns": []any{"/API/"},
	}}
	parsed, err := parseBrowserCDPParams(params, snapshot, NewHostPolicy([]string{"app.test"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	event := &fetch.EventRequestPaused{
		Request:      &network.Request{URL: "https://app.test/v1/api/session"},
		ResourceType: network.ResourceTypeDocument,
	}
	if !browserRequestInHarvestScope(event, parsed.Harvest) {
		t.Fatal("mixed-case API path pattern did not match case-insensitively")
	}
}

func TestParseBrowserCDPParamsValidatesAllHostsBeforeExecute(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	params := validBrowserParams()
	params["entry_url"] = "https://app.test/login?ticket=TOP_SECRET"
	params["seed_cookies"] = []any{map[string]any{
		"name": "seed", "value": "COOKIE_SECRET", "url": "https://denied.test/", "secure": true,
	}}
	policy := NewHostPolicy([]string{"app.test"}, nil)
	_, err := parseBrowserCDPParams(params, snapshot, policy)
	var denied *browserHostDeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("error = %v, want host denial", err)
	}
	if strings.Contains(err.Error(), "TOP_SECRET") || strings.Contains(err.Error(), "COOKIE_SECRET") {
		t.Fatalf("error leaked a secret: %v", err)
	}
}

func TestBrowserCDPExecuteDenyBeforeExecutor(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, 100*time.Millisecond)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = NewHostPolicy([]string{"somewhere.test"}, nil)
	var calls atomic.Int32
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		calls.Add(1)
		return nil, nil
	}
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: validBrowserParams()})
	assertBrowserError(t, err, "browser.host_denied", true)
	if calls.Load() != 0 {
		t.Fatalf("executor called %d times", calls.Load())
	}
}

func TestBrowserCDPExecuteDeniesNilHostPolicyBeforeExecutor(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, 100*time.Millisecond)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = nil
	var calls atomic.Int32
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		calls.Add(1)
		return nil, nil
	}
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: validBrowserParams()})
	assertBrowserError(t, err, "browser.host_denied", true)
	if calls.Load() != 0 {
		t.Fatalf("executor called %d times", calls.Load())
	}
}

func TestBrowserCDPExecuteRetriesAndBuildsFilteredOutput(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, time.Second)
	defer release()
	oldPolicy, oldExecutor, oldNow := HTTPHostPolicy, executeBrowserCDPAttempt, browserCDPNow
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt, browserCDPNow = oldPolicy, oldExecutor, oldNow }()
	HTTPHostPolicy = NewHostPolicy([]string{"app.test"}, nil)
	now := time.Unix(1_800_000_000, 0)
	browserCDPNow = func() time.Time { return now }
	var calls atomic.Int32
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("net::ERR_INSUFFICIENT_RESOURCES secret-that-must-not-escape")
		}
		return &browserAttemptResult{
			Headers: http.Header{"authorization": []string{"Bearer " + testJWT(now.Add(2*time.Hour).Unix())}, "Cookie": []string{"untrusted=leak"}},
			Cookies: []*network.Cookie{
				{Name: "keep", Value: "yes", Expires: float64(now.Add(time.Hour).Unix())},
				{Name: "drop_me", Value: "no", Session: true},
			},
			FallbackUsed: true,
		}, nil
	}
	params := validBrowserParams()
	params["plan"] = map[string]any{"retry_backoff_ms": 0, "retries": 3}
	params["harvest"] = map[string]any{"cookie_name_exclude_prefixes": []any{"drop_"}}
	out, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: params})
	if err != nil {
		t.Fatal(err)
	}
	headers := out.Data["headers"].(map[string][]string)
	if got := headers["Cookie"]; !reflect.DeepEqual(got, []string{"keep=yes"}) {
		t.Fatalf("Cookie = %v", got)
	}
	if _, leaked := headers["cookie"]; leaked || strings.Contains(strings.Join(headers["Cookie"], ""), "untrusted") {
		t.Fatalf("copied untrusted request Cookie: %v", headers)
	}
	diag := out.Data["diag"].(map[string]any)
	if diag["retries"] != 1 || diag["cookie_count"] != 1 || diag["header_count"] != 1 || diag["fallback_used"] != true {
		t.Fatalf("diag = %#v", diag)
	}
}

func TestBrowserCDPExecuteAllowsZeroRetries(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, time.Second)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = NewHostPolicy([]string{"app.test"}, nil)
	var calls atomic.Int32
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		calls.Add(1)
		return nil, errors.New("net::ERR_INSUFFICIENT_RESOURCES")
	}
	params := validBrowserParams()
	params["plan"] = map[string]any{"retries": 0, "retry_backoff_ms": 0}
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: params})
	assertBrowserError(t, err, "browser.resource_exhausted", false)
	if calls.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", calls.Load())
	}
}

func TestBrowserCDPExecuteClassifiesAndRedacts(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, time.Second)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = NewHostPolicy([]string{"app.test"}, nil)
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		return nil, errors.New("dial failed with ticket=TOP_SECRET and Cookie=COOKIE_SECRET")
	}
	params := validBrowserParams()
	params["entry_url"] = "https://app.test/login?ticket=TOP_SECRET"
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: params})
	assertBrowserError(t, err, "browser.unavailable", true)
	if strings.Contains(err.Error(), "TOP_SECRET") || strings.Contains(err.Error(), "COOKIE_SECRET") {
		t.Fatalf("classified error leaked secret: %v", err)
	}
	var classified *types.ClassifiedError
	if !errors.As(err, &classified) || classified.Details != nil {
		t.Fatalf("details = %#v", classified.Details)
	}
}

func TestBrowserCDPExecuteRecoversPanicAndReleasesSlot(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, 50*time.Millisecond)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = NewHostPolicy([]string{"app.test"}, nil)
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		panic("PANIC_SECRET")
	}
	node := &CDPNode{}
	_, err := node.Execute(context.Background(), &types.Input{Params: validBrowserParams()})
	assertBrowserError(t, err, "browser.unavailable", true)
	if strings.Contains(err.Error(), "PANIC_SECRET") {
		t.Fatalf("panic leaked: %v", err)
	}
	executeBrowserCDPAttempt = func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error) {
		return &browserAttemptResult{Headers: http.Header{"Authorization": []string{"opaque"}}}, nil
	}
	if _, err := node.Execute(context.Background(), &types.Input{Params: validBrowserParams()}); err != nil {
		t.Fatalf("slot was not released after panic: %v", err)
	}
}

func TestBrowserCDPSemaphoreQueueTimeout(t *testing.T) {
	release := acquireTestBrowserConfig(t, "chrome.test", 1, 20*time.Millisecond)
	defer release()
	oldPolicy, oldExecutor := HTTPHostPolicy, executeBrowserCDPAttempt
	defer func() { HTTPHostPolicy, executeBrowserCDPAttempt = oldPolicy, oldExecutor }()
	HTTPHostPolicy = NewHostPolicy([]string{"app.test"}, nil)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	executeBrowserCDPAttempt = func(ctx context.Context, _ *browserParams, _ *browserConfigSnapshot) (*browserAttemptResult, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {
		case <-unblock:
			return &browserAttemptResult{Headers: http.Header{"Authorization": []string{"opaque"}}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: validBrowserParams()})
		firstDone <- err
	}()
	<-entered
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: validBrowserParams()})
	assertBrowserError(t, err, "browser.timeout", false)
	close(unblock)
	if err := <-firstDone; err != nil {
		t.Fatalf("first execute: %v", err)
	}
}

func TestCalculateBrowserCredentialExpireTime(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	rules := browserTTLRules{EarlyExpireRatio: .1, EarlyExpireMin: 5 * time.Minute, CredentialTTLCap: 4 * time.Hour}
	cookies := []*network.Cookie{{Name: "session", Session: true}, {Name: "persistent", Expires: float64(now.Add(2 * time.Hour).Unix())}}
	headers := http.Header{"Authorization": []string{"Bearer " + testJWT(now.Add(time.Hour).Unix())}}
	if got, want := calculateBrowserCredentialExpireTime(now, cookies, headers, rules), now.Add(54*time.Minute).Unix(); got != want {
		t.Fatalf("expire = %d, want %d", got, want)
	}
	if got, want := calculateBrowserCredentialExpireTime(now, nil, nil, rules), now.Add(3*time.Hour+36*time.Minute).Unix(); got != want {
		t.Fatalf("cap fallback = %d, want %d", got, want)
	}
	short := []*network.Cookie{{Name: "short", Expires: float64(now.Add(2 * time.Minute).Unix())}}
	if got, want := calculateBrowserCredentialExpireTime(now, short, nil, rules), now.Add(time.Minute).Unix(); got != want {
		t.Fatalf("half fallback = %d, want %d", got, want)
	}
}

func TestBrowserCDPLive(t *testing.T) {
	endpoint := os.Getenv("XFLOW_BROWSER_CDP_URL")
	entry := os.Getenv("XFLOW_BROWSER_CDP_ENTRY_URL")
	if endpoint == "" || entry == "" {
		t.Skip("XFLOW_BROWSER_CDP_URL and XFLOW_BROWSER_CDP_ENTRY_URL are not set")
	}
	endpointHost := mustBrowserURLHost(t, endpoint)
	entryHost := mustBrowserURLHost(t, entry)
	release := acquireTestBrowserConfig(t, endpointHost, 1, 5*time.Second)
	defer release()
	oldPolicy := HTTPHostPolicy
	defer func() { HTTPHostPolicy = oldPolicy }()
	HTTPHostPolicy = NewHostPolicy([]string{entryHost}, nil)
	params := validBrowserParams()
	params["debugging_url"] = endpoint
	params["entry_url"] = entry
	params["target_host"] = entryHost
	params["total_timeout_ms"] = 45000
	_, err := (&CDPNode{}).Execute(context.Background(), &types.Input{Params: params})
	if err != nil {
		t.Fatalf("live CDP execute: %v", err)
	}
}

func validBrowserParams() map[string]any {
	return map[string]any{"debugging_url": "http://chrome.test:9222", "entry_url": "https://app.test/login"}
}

func testBrowserSnapshot(host string) *browserConfigSnapshot {
	cfg := BrowserCDPConfigDefaults()
	cfg.EndpointAllowlist = []string{host}
	normalized, err := normalizeBrowserCDPConfig(cfg)
	if err != nil {
		panic(err)
	}
	return newBrowserConfigSnapshot(normalized)
}

func acquireTestBrowserConfig(t *testing.T, host string, max int, queue time.Duration) func() {
	t.Helper()
	cfg := BrowserCDPConfigDefaults()
	cfg.EndpointAllowlist = []string{host}
	cfg.MaxContexts = max
	cfg.QueueTimeout = queue
	release, err := AcquireBrowserCDPConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func assertBrowserError(t *testing.T, err error, code string, permanent bool) {
	t.Helper()
	var classified *types.ClassifiedError
	if !errors.As(err, &classified) {
		t.Fatalf("error = %v (%T), want ClassifiedError", err, err)
	}
	if classified.Code != code || classified.Permanent != permanent || classified.Retryable == permanent || classified.Details != nil {
		t.Fatalf("classified error = %+v", classified)
	}
}

func testJWT(exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":` + strconv.FormatInt(exp, 10) + `}`))
	return header + "." + payload + "."
}

func mustBrowserURLHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		t.Fatalf("invalid test URL")
	}
	return u.Hostname()
}

func mustParseBrowserTestURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse test URL %q: %v", raw, err)
	}
	return u
}

func browserTestWebSocketURL(t *testing.T, serverURL, path string) string {
	t.Helper()
	u := mustParseBrowserTestURL(t, serverURL)
	u.Scheme = "ws"
	u.Path = path
	return u.String()
}
