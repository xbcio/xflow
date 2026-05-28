package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPNode implements xflow.http — executes an HTTP request.
type HTTPNode struct {
	BaseNode
	Method         string
	URL            string
	Headers        map[string]any
	Body           any
	Query          map[string]any
	Authentication string
	Options        map[string]any
}

// HTTP creates an HTTP request node.
//
//	node.HTTP("POST", "https://api.example.com/users").Body(data).Auth("cred")
func HTTP(method, rawURL string) *HTTPNode {
	return &HTTPNode{Method: method, URL: rawURL}
}

func (n *HTTPNode) SetHeaders(h map[string]any) *HTTPNode  { n.Headers = h; return n }
func (n *HTTPNode) SetBody(body any) *HTTPNode             { n.Body = body; return n }
func (n *HTTPNode) SetQuery(q map[string]any) *HTTPNode    { n.Query = q; return n }
func (n *HTTPNode) Auth(credential string) *HTTPNode       { n.Authentication = credential; return n }
func (n *HTTPNode) Timeout(d string) *HTTPNode {
	if n.Options == nil {
		n.Options = map[string]any{}
	}
	n.Options["timeout"] = d
	return n
}

func (n *HTTPNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.http",
		DisplayName: "HTTP Request",
		Credentials: []string{"http_auth"},
		Params: []ParamSpec{
			{Name: "method", DisplayName: "Method", Type: ParamString, Required: true, Default: "GET", Description: "HTTP method: GET/POST/PUT/DELETE/PATCH"},
			{Name: "url", DisplayName: "URL", Type: ParamString, Required: true, Description: "Target URL"},
			{Name: "authentication", DisplayName: "Authentication", Type: ParamString, Required: false, Description: "Credential reference name"},
			{Name: "body", DisplayName: "Body", Type: ParamObject, Required: false, Description: "Request body"},
			{Name: "headers", DisplayName: "Headers", Type: ParamObject, Required: false, Description: "Request headers"},
			{Name: "query", DisplayName: "Query Params", Type: ParamObject, Required: false, Description: "URL query parameters"},
			{Name: "options", DisplayName: "Options", Type: ParamObject, Required: false, Description: "Additional options (timeout, retry, etc.)"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *HTTPNode) NodeType() string { return "xflow.http" }
func (n *HTTPNode) OnError(s OnError) Builder {
	n.onError = s
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

func (n *HTTPNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	method, _ := input.Params["method"].(string)
	if method == "" {
		method = "GET"
	}
	method = strings.ToUpper(method)

	rawURL, _ := input.Params["url"].(string)
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
			q.Set(k, fmt.Sprintf("%v", v))
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
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	if authName, _ := input.Params["authentication"].(string); authName != "" {
		cred := input.Credential(authName)
		applyHTTPAuth(req, cred)
	}

	timeout := 30 * time.Second
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t, ok := options["timeout"].(string); ok {
			if d, err := time.ParseDuration(t); err == nil {
				timeout = d
			}
		}
	}
	if input.Timeout > 0 {
		timeout = input.Timeout
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return &Output{
			Data: map[string]any{"error": err.Error()},
			Port: "error",
		}, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Output{
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
		return &Output{Data: data, Port: "error"}, nil
	}
	return &Output{Data: data, Port: "main"}, nil
}

func applyHTTPAuth(req *http.Request, cred map[string]any) {
	if cred == nil {
		return
	}
	switch cred["type"] {
	case "bearer":
		if token, ok := cred["token"].(string); ok {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case "basic":
		user, _ := cred["username"].(string)
		pass, _ := cred["password"].(string)
		req.SetBasicAuth(user, pass)
	case "api_key":
		header, _ := cred["header"].(string)
		value, _ := cred["value"].(string)
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

func init() { Register(&HTTPNode{}) }
