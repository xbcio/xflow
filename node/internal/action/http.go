package action

import (
	"bytes"
	"context"
	"encoding/json"
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
		return nil, fmt.Errorf("xflow.http: url parameter is required")
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("xflow.http: invalid url: %w", err)
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
			return nil, fmt.Errorf("xflow.http: marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("xflow.http: create request: %w", err)
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
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t := cast.ToString(options["timeout"]); t != "" {
			if d, err := time.ParseDuration(t); err == nil {
				timeout = d
			}
		}
	}
	if input.Timeout > 0 {
		timeout = input.Timeout
	}

	client := *DefaultHTTPClient
	client.Timeout = timeout
	resp, err := client.Do(req)
	if err != nil {
		return &types.Output{
			Data: map[string]any{"error": err.Error()},
			Port: "error",
		}, nil
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &types.Output{
			Data: map[string]any{"error": fmt.Sprintf("read response: %v", err)},
			Port: "error",
		}, nil
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

	if resp.StatusCode >= 400 {
		return &types.Output{Data: data, Port: "error"}, nil
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
