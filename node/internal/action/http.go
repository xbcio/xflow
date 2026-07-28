package action

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/types"

	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
	"github.com/spf13/cast"
)

// HTTPNode implements xflow.http — executes an HTTP request.
type HTTPNode struct {
	nodeinternal.BaseNode
	Method         string
	URL            string
	Headers        map[string]any
	Body           any
	Query          map[string]any
	Authentication string
	Options        map[string]any
}

// DefaultHTTPClient is used by HTTPNode.Execute. Tests and embedded runtimes
// may replace it to inject custom transports. It ships with a transport that
// caps per-host and idle connections so a misbehaving endpoint cannot exhaust
// the process's file descriptors under the default client.
var defaultHTTPTransport = &http.Transport{
	MaxConnsPerHost:     100,
	MaxIdleConnsPerHost: 10,
	MaxIdleConns:        100,
	IdleConnTimeout:     90 * time.Second,
}

var DefaultHTTPClient = &http.Client{Transport: defaultHTTPTransport}

// defaultMaxResponseBytes caps how much of a response body HTTPNode.Execute
// will buffer into memory. Without a cap a malicious or misbehaving endpoint
// could stream an unbounded body and exhaust the process's memory. It can be
// overridden per node via options["max_response_bytes"].
const defaultMaxResponseBytes int64 = 10 << 20 // 10 MiB

// HostPolicy decides whether a request may be dispatched to a given host. A nil
// return permits the request; a non-nil error aborts it before any bytes are
// sent. The supplied host is the URL hostname with any port stripped.
type HostPolicy func(host string) error

// HTTPHostPolicy, when non-nil, is consulted with the destination host before
// the initial request is dispatched and again for every redirect hop, so a
// redirect cannot smuggle a request to a host the policy would reject. It is
// nil by default: with no policy configured the node performs no host filtering
// and behavior is fully backward compatible. Embedded runtimes may set it to
// enforce SSRF allow/deny policy.
var HTTPHostPolicy HostPolicy

// NewHostPolicy builds a HostPolicy from optional allow and deny lists. When
// allow is non-empty, only hosts present in it are permitted; deny always
// rejects matching hosts and takes precedence over allow. Host matching is
// case-insensitive and compares hostnames only (ports are ignored). It returns
// nil when both lists are empty, preserving the no-filtering default.
func NewHostPolicy(allow, deny []string) HostPolicy {
	allowSet := make(map[string]struct{}, len(allow))
	for _, h := range allow {
		allowSet[strings.ToLower(h)] = struct{}{}
	}
	denySet := make(map[string]struct{}, len(deny))
	for _, h := range deny {
		denySet[strings.ToLower(h)] = struct{}{}
	}
	if len(allowSet) == 0 && len(denySet) == 0 {
		return nil
	}
	return func(host string) error {
		h := strings.ToLower(host)
		if _, denied := denySet[h]; denied {
			return fmt.Errorf("host %q is denied", host)
		}
		if len(allowSet) > 0 {
			if _, ok := allowSet[h]; !ok {
				return fmt.Errorf("host %q is not in the allowlist", host)
			}
		}
		return nil
	}
}

// HTTP creates an HTTP request node.
//
//	node.HTTP("POST", "https://api.example.com/users").Body(data).Auth("cred")
func HTTP(method, rawURL string) *HTTPNode {
	return &HTTPNode{Method: method, URL: rawURL}
}

func (n *HTTPNode) SetHeaders(h map[string]any) *HTTPNode { n.Headers = h; return n }
func (n *HTTPNode) SetBody(body any) *HTTPNode            { n.Body = body; return n }
func (n *HTTPNode) SetQuery(q map[string]any) *HTTPNode   { n.Query = q; return n }
func (n *HTTPNode) Auth(credential string) *HTTPNode      { n.Authentication = credential; return n }
func (n *HTTPNode) Timeout(d string) *HTTPNode {
	if n.Options == nil {
		n.Options = map[string]any{}
	}
	n.Options["timeout"] = d
	return n
}

func (n *HTTPNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.http",
		DisplayName: "HTTP Request",
		Credentials: []string{"http_auth"},
		Params: []types.ParamSpec{
			{Name: "method", DisplayName: "Method", Type: types.ParamString, Required: true, Default: "GET", Description: "HTTP method: GET/POST/PUT/DELETE/PATCH"},
			{Name: "url", DisplayName: "URL", Type: types.ParamString, Required: true, Description: "Target URL"},
			{Name: "authentication", DisplayName: "Authentication", Type: types.ParamString, Required: false, Description: "Credential reference name"},
			{Name: "body", DisplayName: "Body", Type: types.ParamObject, Required: false, Description: "Request body"},
			{Name: "headers", DisplayName: "Headers", Type: types.ParamObject, Required: false, Description: "Request headers"},
			{Name: "query", DisplayName: "Query Params", Type: types.ParamObject, Required: false, Description: "URL query parameters"},
			{Name: "options", DisplayName: "Options", Type: types.ParamObject, Required: false, Description: "Additional options (timeout, retry, etc.)"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *HTTPNode) NodeType() string { return "xflow.http" }
func (n *HTTPNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *HTTPNode) RawParams() any {
	params := map[string]any{"method": n.Method, "url": n.URL}
	if n.Headers != nil {
		params["headers"] = n.Headers
	}
	if n.Body != nil {
		params["body"] = n.Body
	}
	if n.Query != nil {
		params["query"] = n.Query
	}
	if n.Authentication != "" {
		params["authentication"] = n.Authentication
	}
	if n.Options != nil {
		params["options"] = n.Options
	}
	return params
}

func (n *HTTPNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	method := cast.ToString(input.Params["method"])
	if method == "" {
		method = "GET"
	}
	method = strings.ToUpper(method)

	rawURL := cast.ToString(input.Params["url"])
	if rawURL == "" {
		return nil, types.NewPermanentError("http.url_required", "url parameter is required")
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, types.NewPermanentError("http.invalid_url", err.Error())
	}

	if query, ok := input.Params["query"].(map[string]any); ok {
		q := parsedURL.Query()
		for k, v := range query {
			q.Set(k, cast.ToString(v))
		}
		parsedURL.RawQuery = q.Encode()
	}

	var bodyReader io.Reader
	if body := input.Params["body"]; body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, types.NewPermanentError("http.marshal_body", err.Error())
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), bodyReader)
	if err != nil {
		return nil, types.NewPermanentError("http.create_request", err.Error())
	}

	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if headers, ok := input.Params["headers"].(map[string]any); ok {
		for k, v := range headers {
			req.Header.Set(k, cast.ToString(v))
		}
	}

	if authName := cast.ToString(input.Params["authentication"]); authName != "" {
		cred := input.Credential(authName)
		applyHTTPAuth(req, cred)
	}

	timeout := 30 * time.Second
	maxResponseBytes := defaultMaxResponseBytes
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t := cast.ToString(options["timeout"]); t != "" {
			if d, err := time.ParseDuration(t); err == nil {
				timeout = d
			}
		}
		if _, ok := options["max_response_bytes"]; ok {
			if n := cast.ToInt64(options["max_response_bytes"]); n > 0 {
				maxResponseBytes = n
			}
		}
	}
	if input.Timeout > 0 {
		timeout = input.Timeout
	}

	client := *DefaultHTTPClient
	client.Timeout = timeout

	// Enforce the host policy (SSRF allow/deny) after the final URL is resolved
	// but before any request is dispatched. When no policy is configured the
	// node performs no filtering, preserving backward compatibility.
	if policy := HTTPHostPolicy; policy != nil {
		if err := policy(parsedURL.Hostname()); err != nil {
			return nil, types.NewPermanentError("http.host_not_allowed", err.Error())
		}
		// The default client follows redirects, so re-check every redirect hop
		// with the same policy; otherwise a redirect could smuggle a request to
		// a disallowed host and bypass the allowlist.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if err := policy(req.URL.Hostname()); err != nil {
				return types.NewPermanentError("http.host_not_allowed", err.Error())
			}
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			return nil
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		// A redirect rejected by the host policy surfaces here wrapped in a
		// *url.Error; preserve its permanent classification instead of masking
		// it as a transient connection failure.
		var ce *types.ClassifiedError
		if errors.As(err, &ce) && ce.Permanent {
			return nil, ce
		}
		return nil, types.NewTransientError("http.connection", err.Error())
	}
	defer func() { _ = resp.Body.Close() }()

	// Cap the buffered response body so an oversized response cannot exhaust
	// memory. Read one extra byte to detect an overflow past the limit.
	limited := io.LimitReader(resp.Body, maxResponseBytes+1)
	respBody, err := io.ReadAll(limited)
	if err != nil {
		return nil, types.NewTransientError("http.read_response", fmt.Sprintf("read response: %v", err))
	}
	if int64(len(respBody)) > maxResponseBytes {
		return nil, types.NewPermanentError("http.response_too_large", fmt.Sprintf("response body exceeds %d bytes", maxResponseBytes))
	}

	data := map[string]any{
		"status":      resp.StatusCode,
		"status_text": resp.Status,
		"headers":     flattenHeaders(resp.Header),
	}
	var jsonBody any
	if err := json.Unmarshal(respBody, &jsonBody); err == nil {
		data["body"] = jsonBody
	} else {
		data["body"] = string(respBody)
	}

	switch {
	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode == http.StatusTooManyRequests:
		// 408 and 429 have explicit retry semantics per RFC; they are NOT
		// permanent 4xx config errors. Classify by a stable table, not by the
		// leading digit, so a backoff can succeed.
		return nil, &types.ClassifiedError{
			Kind:      types.ErrorKindTransient,
			Code:      fmt.Sprintf("http.%d", resp.StatusCode),
			Message:   fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status),
			Retryable: true,
			Details:   data,
		}
	case resp.StatusCode >= 500:
		return nil, &types.ClassifiedError{
			Kind:      types.ErrorKindTransient,
			Code:      "http.5xx",
			Message:   fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status),
			Retryable: true,
			Details:   data,
		}
	case resp.StatusCode >= 400:
		return nil, &types.ClassifiedError{
			Kind:      types.ErrorKindPermanent,
			Code:      "http.4xx",
			Message:   fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status),
			Permanent: true,
			Details:   data,
		}
	}
	return &types.Output{Data: data, Port: "main"}, nil
}

func applyHTTPAuth(req *http.Request, cred map[string]any) {
	if cred == nil {
		return
	}
	switch cred["type"] {
	case "bearer":
		if token := cast.ToString(cred["token"]); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case "basic":
		user := cast.ToString(cred["username"])
		pass := cast.ToString(cred["password"])
		req.SetBasicAuth(user, pass)
	case "api_key":
		header := cast.ToString(cred["header"])
		value := cast.ToString(cred["value"])
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, value)
	}
}

func flattenHeaders(h http.Header) map[string]any {
	result := make(map[string]any, len(h))
	for k, v := range h {
		if len(v) == 1 {
			result[k] = v[0]
		} else {
			result[k] = v
		}
	}
	return result
}

func init() { registry.Register(&HTTPNode{}) }
