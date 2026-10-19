package group

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
)

// NotificationNode is the xflow.notification action. It stays registered so
// definitions that name the type still compile and appear in the catalog, but
// it has no delivery backend: Execute fails rather than reporting a
// notification that was never sent.
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

func (n *NotificationNode) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return nil, fmt.Errorf("xflow.notification is not implemented; use xflow.http or a webhook trigger")
}

func init() { registry.Register(&NotificationNode{}) }
