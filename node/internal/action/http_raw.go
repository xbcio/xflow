package action

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/types"
)

// httpMode selects how HTTPNode encodes the request body and shapes the result.
//
// It exists because the two encodings are not interchangeable and neither is a
// superset of the other. modeJSON (the default) marshals the body to JSON,
// decodes the response into JSON when it parses, and classifies a 4xx as a
// permanent node error. modeRaw ships pre-encoded bytes and returns the raw
// response bytes with no classification at all -- every HTTP status is a
// result, not a failure. A scanner needs what modeJSON throws away (verbatim
// bytes, and 4xx/5xx as findings), and a workflow that wants a parsed JSON body
// needs what modeRaw deliberately refuses to do.
type httpMode string

const (
	// modeJSON is the default encoding and the behaviour every existing
	// workflow depends on. The zero value selects it.
	modeJSON httpMode = "json"
	// modeRaw treats the body as opaque bytes in both directions.
	modeRaw httpMode = "raw"

	httpModeParam    = "mode"
	httpRawBodyParam = "body_b64"
)

// parseHTTPMode resolves the request's encoding mode. An unrecognized value is
// rejected rather than silently treated as the default: a workflow that asks
// for a mode the node does not implement has a bug the author can fix, and
// falling back to modeJSON would send a materially different request (a
// base64 string as a JSON body) with no diagnostic anywhere.
func parseHTTPMode(params map[string]any) (httpMode, error) {
	raw := strings.TrimSpace(cast.ToString(params[httpModeParam]))
	switch raw {
	case "":
		return modeJSON, nil
	case string(modeJSON):
		return modeJSON, nil
	case string(modeRaw):
		return modeRaw, nil
	default:
		return "", types.NewPermanentError("http.invalid_mode",
			fmt.Sprintf("%s parameter must be %q or %q, got %q", httpModeParam, modeJSON, modeRaw, raw))
	}
}

// rawRequestBody decodes the base64 request body. An absent parameter means no
// body. Content-Type is deliberately NOT set here: in raw mode the caller owns
// every header, and injecting one would corrupt exactly the form-urlencoded and
// multipart bodies this mode exists to send.
func rawRequestBody(params map[string]any) ([]byte, error) {
	encoded := strings.TrimSpace(cast.ToString(params[httpRawBodyParam]))
	if encoded == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// The offending value is not echoed: it is request payload, and for a
		// scanner that payload can be a credential or a stored attack string.
		return nil, types.NewPermanentError("http.invalid_body_b64",
			fmt.Sprintf("%s parameter is not valid base64", httpRawBodyParam))
	}
	return decoded, nil
}

// httpOptions holds the transport-level knobs shared by both modes.
type httpOptions struct {
	timeout            time.Duration
	maxResponseBytes   int64
	disableRedirect    bool
	insecureSkipVerify bool
}

// parseHTTPOptions reads options{} and the boundary-supplied timeout. Every
// field defaults to the pre-mode behaviour, so a workflow that sets none of
// them is unaffected by raw mode having been added.
func parseHTTPOptions(input *types.Input) httpOptions {
	opts := httpOptions{
		timeout:          30 * time.Second,
		maxResponseBytes: defaultMaxResponseBytes,
	}
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t := cast.ToString(options["timeout"]); t != "" {
			if d, err := time.ParseDuration(t); err == nil {
				opts.timeout = d
			}
		}
		if _, ok := options["max_response_bytes"]; ok {
			if n := cast.ToInt64(options["max_response_bytes"]); n > 0 {
				opts.maxResponseBytes = n
			}
		}
		opts.disableRedirect = cast.ToBool(options["disable_redirect"])
		opts.insecureSkipVerify = cast.ToBool(options["insecure_skip_verify"])
	}
	if input.Timeout > 0 {
		opts.timeout = input.Timeout
	}
	return opts
}

// configureRedirects installs the redirect policy on client.
//
// disableRedirect wins when both apply: it means the caller needs to observe the
// redirect itself (a scanner records a 302 as a finding, and a redirect that
// re-sends a credential-bearing request to a new host is the leak the flag
// exists to prevent), so following the chain would both hide the status and
// perform the leak.
func configureRedirects(client *http.Client, opts httpOptions, policy HostPolicy) {
	if opts.disableRedirect {
		client.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
		return
	}
	if policy == nil {
		return
	}
	// Re-check every redirect hop with the same policy; otherwise a redirect
	// could smuggle a request to a disallowed host and bypass the allowlist.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := policy(req.URL.Hostname()); err != nil {
			return types.NewPermanentError("http.host_not_allowed", err.Error())
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
}

// relaxedTLSRoundTripper clones base and turns off certificate verification.
//
// This is a deliberate, opt-in capability for scanning a host whose certificate
// is self-signed or expired -- a state the target cannot fix from the scanner's
// side, and one that must be reported as a finding rather than as a connection
// failure. It is NOT a general-purpose escape hatch: the caller opts in per
// request, and a request that carries credentials must never set it, because
// disabling verification is what makes the credential replayable to an
// on-path attacker.
//
// A caller that asks for it and cannot get it is failed loudly rather than
// silently downgraded to verified TLS: a silent downgrade surfaces as an
// unexplained handshake failure at the target, which is the hardest possible
// shape to diagnose.
func relaxedTLSRoundTripper(base http.RoundTripper) (*http.Transport, error) {
	var transport *http.Transport
	switch t := base.(type) {
	case *http.Transport:
		transport = t.Clone()
	case nil:
		defaulted, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, types.NewPermanentError("http.insecure_transport_unavailable",
				"insecure_skip_verify requires an http.Transport, and the default transport is not one")
		}
		transport = defaulted.Clone()
	default:
		return nil, types.NewPermanentError("http.insecure_transport_unavailable",
			fmt.Sprintf("insecure_skip_verify requires an http.Transport, got %T", base))
	}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // opt-in, per-request; see doc comment
	return transport, nil
}

// rawResponseData builds the modeRaw success payload.
//
// body_b64 is always base64, including for bodies that happen to be valid UTF-8
// or valid JSON. Decoding is the consumer's decision: a scanner must compare
// bytes, and a node that "helpfully" parsed a JSON body would hand back a
// re-serialized document whose key order and number formatting no longer match
// what the target sent.
func rawResponseData(resp *http.Response, body []byte, truncated bool, startedAt, finishedAt time.Time) map[string]any {
	return map[string]any{
		"status":         resp.StatusCode,
		"status_text":    resp.Status,
		"headers":        flattenHeaders(resp.Header),
		"body_b64":       base64.StdEncoding.EncodeToString(body),
		"body_truncated": truncated,
		"started_at":     startedAt.UnixMilli(),
		"finished_at":    finishedAt.UnixMilli(),
		"duration_ms":    finishedAt.Sub(startedAt).Milliseconds(),
	}
}

// rawFailureData builds the modeRaw payload for a request that never produced a
// response.
//
// A transport failure is returned on the main port with status 0, matching how
// an executor that owns a batch of requests must behave: a batch of N requests
// has to yield exactly N ordered results, so a refused connection cannot be
// allowed to become an error that replaces the whole node's output. status 0 is
// the same encoding the local executor already uses for a failed request.
func rawFailureData(message string, startedAt, finishedAt time.Time) map[string]any {
	return map[string]any{
		"status":         0,
		"status_text":    "",
		"headers":        map[string]any{},
		"body_b64":       "",
		"body_truncated": false,
		"started_at":     startedAt.UnixMilli(),
		"finished_at":    finishedAt.UnixMilli(),
		"duration_ms":    finishedAt.Sub(startedAt).Milliseconds(),
		"error":          message,
	}
}
