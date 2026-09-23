package workflows

import (
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
)

// BrowserDebuggingURL is the CDP endpoint the browser tier names. It is a
// reserved, non-resolving host: the tier asserts that the node refuses to
// connect to it, so the value must never point at a real Chrome.
const BrowserDebuggingURL = "http://cdp.invalid:9222"

// BrowserEntryURL is the page the browser tier would navigate to.
const BrowserEntryURL = "https://example.test/"

// BrowserTargetHost is the host whose credentials the harvest would target.
const BrowserTargetHost = "example.test"

// BrowserCDPWorkflow covers the xflow.browser.cdp node through its authoritative
// outcome: a denial.
//
//	start → subject(browser.cdp) ─main→ done
//	                             └error→ recover → done
//
// Endpoint admission defaults to an empty allowlist and the node's navigation
// policy blocks private addresses, so there is no arrangement in which a test
// environment reaches the harvest path without disabling the control the node
// exists to enforce. The reachable, meaningful behaviour is the refusal, and the
// tier drives it through the error port so both the classification and the
// routing are asserted rather than assumed.
//
// Node types: xflow.start, xflow.browser.cdp, xflow.transform.set, xflow.end.
func BrowserCDPWorkflow() *xflow.WorkflowBuilder {
	return ErrorPortWorkflow(node.BrowserCDP(map[string]any{
		"debugging_url": BrowserDebuggingURL,
		"entry_url":     BrowserEntryURL,
		"target_host":   BrowserTargetHost,
	}))
}
