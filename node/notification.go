package node

import "context"

// NotificationHandler implements xflow.notification — sends a notification via email, SMS, or Slack.
// Execute is a stub; the real implementation lives in the Worker layer.
type NotificationHandler struct{}

func (h *NotificationHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.notification",
		DisplayName: "Notification",
		Params: []ParamSpec{
			{Name: "channel", DisplayName: "Channel", Type: ParamString, Required: true, Description: "Delivery channel: \"email\"/\"sms\"/\"slack\""},
			{Name: "to", DisplayName: "To", Type: ParamString, Required: false, Description: "Recipient address or identifier"},
			{Name: "subject", DisplayName: "Subject", Type: ParamString, Required: false, Description: "Message subject (email)"},
			{Name: "body", DisplayName: "Body", Type: ParamString, Required: false, Description: "Plain-text or HTML message body"},
			{Name: "template", DisplayName: "Template", Type: ParamString, Required: false, Description: "Template name for pre-defined message templates"},
			{Name: "message", DisplayName: "Message", Type: ParamString, Required: false, Description: "Short message text (SMS/Slack)"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *NotificationHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.notification", "_stub": true}}, nil
}

func init() { Register(&NotificationHandler{}) }
