package node

import "context"

// HTTPHandler implements xflow.http — executes an HTTP request.
// Execute is a stub; the real implementation lives in the Worker layer.
type HTTPHandler struct{}

func (h *HTTPHandler) Descriptor() Descriptor {
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

func (h *HTTPHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.http", "_stub": true}}, nil
}

func init() { Register(&HTTPHandler{}) }
