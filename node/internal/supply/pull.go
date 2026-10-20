package supply

import (
	nodeinternal "github.com/xbcio/xflow/node/internal"
	xsupply "github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// PullNode declares that this workflow consumes supply content a RUNNER
// fetches from an external HTTPS source, on a schedule the runner owns.
//
// It has no Execute and no registered handler, like the other two supply node
// types: the node is a declaration, and the fetch happens in the runner's
// supply gate, never in a task. What is different is where the content comes
// from — not the control plane's SupplyResource store (external) and not the
// definition itself (static), but an operator-run HTTP endpoint reached with
// the runner's own network position and credentials.
//
// Two consequences are deliberate and documented rather than hidden:
//
//   - The fetched content never enters the store, so there is no cluster-wide
//     version authority: two runners hosting this supply may briefly serve
//     different versions. Convergence is per-runner (TTL poll + activation-time
//     fetch), not a single global version.
//   - A pull supply never appears in a heartbeat hint (only store-backed
//     supplies have hints), so the runner's poll interval is its freshness SLO.
type PullNode struct {
	nodeinternal.BaseNode
	URLValue            string
	AuthenticationValue string
	RequireReadyValue   bool
	PollIntervalValue   string
}

// Pull returns a supply declaration whose content the runner fetches from url.
// require_ready defaults to true, matching every other supply type.
func Pull(url string) *PullNode {
	return &PullNode{URLValue: url, RequireReadyValue: true}
}

// Authentication names a runner-side credential to send with the fetch. The
// credential itself lives in the runner's process configuration; the definition
// carries only its name. An unresolvable name fails the fetch rather than
// degrading to an anonymous request.
func (n *PullNode) Authentication(name string) *PullNode {
	n.AuthenticationValue = name
	return n
}

// RequireReady sets the readiness policy. Same meaning as on the other supply
// types: false lets the runner take over the entry activation before any
// content has ever been fetched.
func (n *PullNode) RequireReady(v bool) *PullNode {
	n.RequireReadyValue = v
	return n
}

// PollInterval overrides the runner's global poll interval for this supply. The
// value is a Go duration string ("30s"); the floor is types.MinSupplyPullInterval
// and the compile-time validator rejects anything below it. It can only make
// this supply refresh faster than the runner's global cadence — polling cannot
// be switched off.
func (n *PullNode) PollInterval(d string) *PullNode {
	n.PollIntervalValue = d
	return n
}

func (n *PullNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        xsupply.NodeTypeHTTPPull,
		Kind:        types.NodeKindSupply,
		DisplayName: "HTTP Supply (Runner Pull)",
		Category:    "unknown",
		CardSummary: []string{"url", "authentication", "poll_interval"},
		Params: []types.ParamSpec{
			{Name: xsupply.ParamURL, DisplayName: "URL", Type: types.ParamString, Required: true,
				Description: "HTTPS source the runner fetches. Only https is accepted and redirects are not followed",
				Constraints: nodeinternal.Format(nodeinternal.FormatURL)},
			{Name: xsupply.ParamAuthentication, DisplayName: "Authentication", Type: types.ParamString,
				Description: "Name of a runner-side credential to send with the fetch; the credential never appears in the definition",
				Widget:      nodeinternal.WidgetCredentialSelect},
			{Name: xsupply.ParamRequireReady, DisplayName: "Require Ready", Type: types.ParamBool, Default: true,
				Description: "When true (default) a runner will not take over the entry activation until this supply has content"},
			{Name: xsupply.ParamPollInterval, DisplayName: "Poll Interval", Type: types.ParamString,
				Description: "Refresh cadence for this supply (e.g. \"30s\"), at least 10s. Defaults to the runner's global interval and can only make it faster",
				Widget:      nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
		},
		// No Outputs: a supply node is never connected by a dataflow edge.
	}
}

func (n *PullNode) NodeType() string { return xsupply.NodeTypeHTTPPull }

func (n *PullNode) RawParams() any {
	params := map[string]any{
		xsupply.ParamURL:          n.URLValue,
		xsupply.ParamRequireReady: n.RequireReadyValue,
	}
	if n.AuthenticationValue != "" {
		params[xsupply.ParamAuthentication] = n.AuthenticationValue
	}
	if n.PollIntervalValue != "" {
		params[xsupply.ParamPollInterval] = n.PollIntervalValue
	}
	return params
}

// OnError satisfies types.Builder. A supply node never executes, so the strategy
// is inert; the method exists only to complete the interface.
func (n *PullNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
