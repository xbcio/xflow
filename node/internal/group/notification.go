package group

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	"github.com/spf13/cast"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
)

// NotificationNode implements xflow.notification — records a normalized
// notification request for downstream workflow steps.
type NotificationNode struct {
	nodeinternal.BaseNode
	Channel string
	To      any
	Subj    string
	Msg     string
	Data    map[string]any
}

// Notification creates a notification node.
//
//	node.Notification("email", "ops@example.com").Subject("Order blocked")
func Notification(channel string, to any) *NotificationNode {
	return &NotificationNode{Channel: channel, To: to}
}

func (n *NotificationNode) Subject(subject string) *NotificationNode {
	n.Subj = subject
	return n
}

func (n *NotificationNode) Message(message string) *NotificationNode {
	n.Msg = message
	return n
}

func (n *NotificationNode) SetData(data map[string]any) *NotificationNode {
	n.Data = data
	return n
}

func (n *NotificationNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.notification",
		DisplayName: "Notification",
		Params: []types.ParamSpec{
			{Name: "channel", DisplayName: "Channel", Type: types.ParamString, Required: true, Description: "Notification channel such as email, sms, slack, webhook"},
			{Name: "to", DisplayName: "To", Type: types.ParamString, Required: true, Description: "Recipient address or identifier"},
			{Name: "subject", DisplayName: "Subject", Type: types.ParamString, Required: false, Description: "Notification subject or title"},
			{Name: "message", DisplayName: "Message", Type: types.ParamString, Required: false, Description: "Notification body"},
			{Name: "data", DisplayName: "Data", Type: types.ParamObject, Required: false, Description: "Additional structured notification data"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *NotificationNode) NodeType() string { return "xflow.notification" }
func (n *NotificationNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *NotificationNode) RawParams() any {
	params := map[string]any{
		"channel": n.Channel,
		"to":      n.To,
	}
	if n.Subj != "" {
		params["subject"] = n.Subj
	}
	if n.Msg != "" {
		params["message"] = n.Msg
	}
	if n.Data != nil {
		params["data"] = n.Data
	}
	return params
}

func (n *NotificationNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	if input == nil {
		input = &types.Input{}
	}
	channel := cast.ToString(input.Params["channel"])
	if channel == "" {
		return nil, fmt.Errorf("xflow.notification: channel parameter is required")
	}
	to := input.Params["to"]
	if isEmptyNotificationRecipient(to) {
		return nil, fmt.Errorf("xflow.notification: to parameter is required")
	}

	data := make(map[string]any)
	if input.Data != nil {
		for k, v := range input.Data {
			data[k] = v
		}
	}
	if extra, ok := input.Params["data"].(map[string]any); ok {
		for k, v := range extra {
			data[k] = v
		}
	}

	data["channel"] = channel
	data["to"] = to
	data["subject"] = cast.ToString(input.Params["subject"])
	data["message"] = cast.ToString(input.Params["message"])
	data["sent"] = true

	return &types.Output{Data: data, Port: "main"}, nil
}

func isEmptyNotificationRecipient(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []string:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return cast.ToString(t) == ""
	}
}

func init() { registry.Register(&NotificationNode{}) }
