package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"

	"github.com/xbcio/xflow/types"
	"github.com/spf13/cast"
)

const defaultWebhookMaxBodyBytes = int64(1 << 20)

// Node is the xflow.trigger.webhook trigger: it registers an HTTP route on the
// host's WebhookRuntime and emits one TriggerEvent per accepted request.
type Node struct {
	nodeinternal.BaseTrigger
	MethodValue        string
	PathValue          string
	EventIDHeaderValue string
	MaxBodyBytesValue  int64
}

// New returns a webhook trigger with the default 1 MiB body cap.
func New() *Node {
	return &Node{MaxBodyBytesValue: defaultWebhookMaxBodyBytes}
}

func (n *Node) Method(method string) *Node {
	n.MethodValue = method
	return n
}

func (n *Node) Path(path string) *Node {
	n.PathValue = path
	return n
}

func (n *Node) EventIDHeader(header string) *Node {
	n.EventIDHeaderValue = header
	return n
}

func (n *Node) MaxBodyBytes(max int64) *Node {
	n.MaxBodyBytesValue = max
	return n
}

func (n *Node) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.trigger.webhook",
		Kind:        types.NodeKindTrigger,
		DisplayName: "Webhook Trigger",
		Params: []types.ParamSpec{
			{Name: "method", DisplayName: "Method", Type: types.ParamString, Required: true},
			{Name: "path", DisplayName: "Path", Type: types.ParamString, Required: true},
			{Name: "event_id_header", DisplayName: "Event ID Header", Type: types.ParamString},
			{Name: "max_body_bytes", DisplayName: "Max Body Bytes", Type: types.ParamNumber, Default: float64(defaultWebhookMaxBodyBytes)},
		},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *Node) NodeType() string { return "xflow.trigger.webhook" }
func (n *Node) RawParams() any {
	max := n.MaxBodyBytesValue
	if max <= 0 {
		max = defaultWebhookMaxBodyBytes
	}
	return map[string]any{
		"method":          n.MethodValue,
		"path":            n.PathValue,
		"event_id_header": n.EventIDHeaderValue,
		"max_body_bytes":  max,
	}
}
func (n *Node) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *Node) TriggerHandler() types.TriggerHandler { return n }

func init() { registry.RegisterTrigger(&Node{}) }

func (n *Node) Activate(ctx context.Context, in *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	method := strings.ToUpper(in.GetString("method"))
	path := in.GetString("path")
	if method == "" || path == "" {
		return nil, fmt.Errorf("webhook method and path are required")
	}
	eventIDHeader := in.GetString("event_id_header")
	maxBodyBytes := webhookMaxBodyBytes(in.Params["max_body_bytes"])
	webhooks, err := in.Webhooks()
	if err != nil {
		return nil, err
	}
	return webhooks.Handle(method, path, func(reqCtx context.Context, req *http.Request) (*types.TriggerEvent, error) {
		body, err := readWebhookBody(req.Body, maxBodyBytes)
		if err != nil {
			return nil, err
		}
		headers := make(map[string]string, len(req.Header))
		for name, values := range req.Header {
			if len(values) > 0 {
				headers[name] = values[0]
			}
		}
		eventID := ""
		if eventIDHeader != "" {
			eventID = req.Header.Get(eventIDHeader)
		}
		if eventID == "" {
			sum := sha256.Sum256(body)
			eventID = hex.EncodeToString(sum[:]) + fmt.Sprintf("/%d", time.Now().Unix()/60)
		}
		event := &types.TriggerEvent{
			ID:      eventID,
			Kind:    "webhook",
			Source:  in.NodeName,
			Time:    time.Now(),
			Headers: headers,
			Data: map[string]any{
				"method": req.Method,
				"path":   req.URL.Path,
				"body":   string(body),
			},
			Raw: body,
		}
		ok, err := in.Runtime.Dedup(reqCtx, "trigger:"+string(in.WorkflowID)+":"+in.NodeName+":"+event.ID, 24*time.Hour)
		if err != nil || !ok {
			return event, err
		}
		_, err = in.Emit(reqCtx, event)
		return event, err
	})
}

func webhookMaxBodyBytes(v any) int64 {
	n, err := cast.ToInt64E(v)
	if err == nil && n > 0 {
		return n
	}
	return defaultWebhookMaxBodyBytes
}

func readWebhookBody(r io.Reader, max int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	limited := io.LimitReader(r, max+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("webhook body exceeds %d bytes", max)
	}
	return body, nil
}
