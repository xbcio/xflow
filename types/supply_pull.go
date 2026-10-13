package types

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// SupplyPullNodeType is the DSL node type of a pull-mode supply declaration: a
// supply whose content a RUNNER fetches from an external HTTPS source, rather
// than reading it from the control plane's store (xflow.supply.external) or
// carrying it in the definition (xflow.supply.static).
//
// It lives in types rather than next to the node implementation because three
// layers must agree on the exact string and cannot import each other:
// node/internal/supply declares the node, engine/graph validates it at compile
// time, and service/control derives the fetch source for the activation.
const SupplyPullNodeType = "xflow.supply.http"

// Parameter names of a pull supply node, plus the two shared supply parameters
// the pull shape also accepts. node/supply re-exports these so the node layer
// and the compiler validate one spelling.
const (
	SupplyPullParamURL            = "url"
	SupplyPullParamAuthentication = "authentication"
	SupplyPullParamPollInterval   = "poll_interval"
	// SupplyParamRequireReady is the readiness policy shared by every supply
	// type. It is not validated here (see node/supply.RequireReady) but must be
	// accepted rather than rejected as unknown.
	SupplyParamRequireReady = "require_ready"
	// SupplyParamResource is the store-key override of xflow.supply.external.
	// A pull supply REJECTS it: pull content has no store record, so a resource
	// key would name something that can never exist.
	SupplyParamResource = "resource"
)

// MinSupplyPullInterval is the lowest per-node poll interval the compiler
// accepts, mirroring runner.MinSupplyPollInterval (which cannot be imported
// here: engine and types must not depend on service/). Polling is the only
// refresh path a pull supply has, so it cannot be switched off or turned into
// a hammering cadence.
const MinSupplyPullInterval = 10 * time.Second

// SupplyPullSource is the validated fetch plan for one pull supply: everything
// the runner needs to fetch the content, and nothing it must look up in the
// definition again.
//
// It travels to the runner inside the activation directive (engine.
// SupplyRequirement.PullSource), so every field is JSON-tagged. The credential
// is referenced by NAME only — the credential map itself lives in the runner's
// process configuration and never appears in a definition or on the wire.
type SupplyPullSource struct {
	// URL is the https source. Validated at compile time: parseable, https,
	// host present, no embedded credentials.
	URL string `json:"url"`
	// Authentication names a runner-side credential to send with the fetch.
	// Empty means the source is anonymous.
	Authentication string `json:"authentication,omitempty"`
	// PollInterval, when non-zero, overrides the runner's global poll interval
	// for this supply. It can only make the poll faster (the runner takes the
	// smaller of the two), never slower or off.
	PollInterval time.Duration `json:"poll_interval,omitempty"`
}

// ParseSupplyPullSource validates the parameters of an xflow.supply.http node
// and returns the fetch plan they describe. It is a pure function shared by the
// graph compiler (which rejects a malformed node at registration time, before
// anything is stored) and the control plane (which derives the activation),
// so the two can never disagree about what a definition means.
//
// The rules, all fail-closed:
//
//   - url is required, must be a well-formed absolute https URL with a host,
//     and must not embed credentials (userinfo). Plain http has no opt-out.
//   - authentication, when present, must name a credential (non-empty string).
//   - poll_interval, when present, must parse as a Go duration and be at least
//     MinSupplyPullInterval.
//   - resource is rejected outright: it is the store key of the external
//     supply shape, and a pull supply has no store record. Accepting it
//     silently would let an author believe two nodes share a store-backed
//     source when the runner actually fetches each from its own URL.
//   - any other key is rejected: an unknown parameter is a typo or a feature
//     nobody implemented, and both are cheaper to catch here than in
//     production.
//
// require_ready is accepted and not validated further: it is read by the
// readiness gate through node/supply.RequireReady, whose own malformed-input
// rule is "keep the safe default".
func ParseSupplyPullSource(params map[string]any) (SupplyPullSource, error) {
	var src SupplyPullSource

	if raw, ok := params[SupplyPullParamURL]; ok {
		s, err := pullStringParam(SupplyPullParamURL, raw)
		if err != nil {
			return SupplyPullSource{}, err
		}
		src.URL = s
	}
	if raw, ok := params[SupplyPullParamAuthentication]; ok {
		s, err := pullStringParam(SupplyPullParamAuthentication, raw)
		if err != nil {
			return SupplyPullSource{}, err
		}
		if s == "" {
			return SupplyPullSource{}, fmt.Errorf("%s: %q must name a credential (leave the parameter out for an anonymous source)",
				SupplyPullNodeType, SupplyPullParamAuthentication)
		}
		src.Authentication = s
	}
	if raw, ok := params[SupplyPullParamPollInterval]; ok {
		d, err := parseSupplyPullInterval(raw)
		if err != nil {
			return SupplyPullSource{}, err
		}
		src.PollInterval = d
	}

	// Reject the store-key parameter of the external shape before anything
	// else unknown: it has a specific explanation, and reporting it depends on
	// nothing but its own presence. require_ready is accepted and read
	// elsewhere (node/supply.RequireReady keeps the safe default on anything
	// malformed, so there is nothing to validate here).
	if _, ok := params[SupplyParamResource]; ok {
		return SupplyPullSource{}, fmt.Errorf("%s: %q is not accepted on a pull node: it names a store record, and pull content is fetched per runner and never written to the store",
			SupplyPullNodeType, SupplyParamResource)
	}
	// Unknown keys are reported in sorted order: iterating the map directly
	// would make which typo the author hears about depend on Go's map order.
	var unknown []string
	for key := range params {
		switch key {
		case SupplyPullParamURL, SupplyPullParamAuthentication, SupplyPullParamPollInterval, SupplyParamRequireReady:
		default:
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return SupplyPullSource{}, fmt.Errorf("%s: unknown parameter %q", SupplyPullNodeType, unknown[0])
	}

	if src.URL == "" {
		return SupplyPullSource{}, fmt.Errorf("%s: %q parameter is required", SupplyPullNodeType, SupplyPullParamURL)
	}
	if err := validateSupplyPullURL(src.URL); err != nil {
		return SupplyPullSource{}, err
	}
	return src, nil
}

// pullStringParam reads a parameter that must be a string. A non-string is an
// error rather than cast.ToString's best guess: this function decides what a
// production fetch does, so "442" silently becoming a URL or a credential name
// is exactly the kind of coercion that hides an author's mistake.
func pullStringParam(key string, raw any) (string, error) {
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s: %q must be a string, got %T", SupplyPullNodeType, key, raw)
	}
	return s, nil
}

// parseSupplyPullInterval reads the optional poll_interval parameter. The form
// is a Go duration string ("30s"), the same spelling every other duration
// parameter in the DSL uses.
func parseSupplyPullInterval(raw any) (time.Duration, error) {
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("%s: %q must be a duration string such as \"30s\", got %T",
			SupplyPullNodeType, SupplyPullParamPollInterval, raw)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %q: %w", SupplyPullNodeType, SupplyPullParamPollInterval, err)
	}
	if d < MinSupplyPullInterval {
		return 0, fmt.Errorf("%s: %q %s is below the %s minimum (the poll cannot be disabled or slowed below the floor)",
			SupplyPullNodeType, SupplyPullParamPollInterval, d, MinSupplyPullInterval)
	}
	return d, nil
}

// validateSupplyPullURL enforces the transport rules of a pull source. https
// only, no plaintext opt-out. userinfo (user:pass@host) is rejected because a
// credential embedded in a URL is a credential in the definition, in logs, and
// in a redirect Location — the `authentication` parameter (a runner-side
// credential name) is the supported way to authenticate.
func validateSupplyPullURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid URL: %w", SupplyPullNodeType, SupplyPullParamURL, err)
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return fmt.Errorf("%s: %q must be an https URL (got scheme %q); plaintext sources are not supported",
			SupplyPullNodeType, SupplyPullParamURL, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%s: %q has no host", SupplyPullNodeType, SupplyPullParamURL)
	}
	if u.User != nil {
		return fmt.Errorf("%s: %q must not embed credentials; name a runner-side credential in %q instead",
			SupplyPullNodeType, SupplyPullParamURL, SupplyPullParamAuthentication)
	}
	return nil
}
