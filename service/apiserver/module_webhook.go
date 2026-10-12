package apiserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// webhookModule serves the server-hosted webhook entry route
// (POST|<method> /v1/hooks/{namespace}/{workflow_id}/{entry_unit}).
//
// A runner cannot host this route: it is an outbound execution node with no
// stable address and no inbound port, and the agent that would serve the route
// does not exist in cluster mode. So the control plane serves it and admits the
// request through the same entry-seed admission a runner would have used —
// same dedup, same concurrency admission, same downstream topology resolution.
//
// # Authentication
//
// The route is deliberately OUTSIDE the /v1 authz wrapper. Its authentication is
// the per-entry HMAC signature: an external sender (a code host, a payment
// provider) cannot present a platform principal, so requiring one would make the
// route unusable for the senders it exists for. The signature is mandatory, the
// secret is never taken from the request, and a workflow that declares no secret
// cannot be registered as server-hosted at all — the fail-closed rule that keeps
// this from being an unauthenticated execution-creating endpoint.
//
// # What is never logged or returned
//
// The secret, the signature, the request body and the request headers never
// reach a log line or an error message. Failures answer with a generic code and
// a fixed message; the operator-visible detail is the log line's workflow and
// entry unit, which is what debugging this needs.
type webhookModule struct {
	cp       *control.ControlPlane
	supplies store.Supplies
	log      engine.Logger
}

func newWebhookModule(cp *control.ControlPlane) *webhookModule {
	return &webhookModule{cp: cp}
}

func (m *webhookModule) Name() string { return "webhook" }

// RegisterHTTP mounts the route with NO method qualifier: the HTTP method is a
// property of the workflow's webhook node (its `method` parameter), not of the
// route, so the mux cannot select on it and the handler compares the request
// method against the declared one.
func (m *webhookModule) RegisterHTTP(mux *http.ServeMux) {
	if m.cp == nil {
		return
	}
	mux.HandleFunc(PathWebhookEntry, m.handleWebhookEntry)
}

const (
	// webhookSignatureHeader carries the hex HMAC-SHA256 of the request body.
	webhookSignatureHeader = "X-XFlow-Signature"
	// webhookSignatureScheme is the only accepted scheme tag. Requiring it keeps
	// the header self-describing: a future algorithm can be added as a second
	// tag without guessing what an unstamped value means.
	webhookSignatureScheme = "sha256="
	// webhookKind is this route's segment in the admission key, so a webhook
	// event's key can never collide with another trigger kind's for the same
	// entry unit (see node/trigger/internal/seedadmit.FireKey).
	webhookKind = "webhook"
	// defaultWebhookMaxBodyBytes mirrors node/trigger/webhook's own default: a
	// definition that sets no max_body_bytes gets the same 1 MiB the local
	// runtime would have enforced.
	defaultWebhookMaxBodyBytes = int64(1 << 20)
	// webhookBodyOverhead is the extra byte read past the limit so "exactly at
	// the limit" is distinguishable from "over it".
	webhookBodyOverhead = 1
)

// Parameter names read from the entry unit's own configuration. They are the
// webhook node's DSL parameters, resolved server-side from the registered
// definition — never from the request.
const (
	webhookMethodParam        = "method"
	webhookEventIDHeaderParam = "event_id_header"
	webhookMaxBodyBytesParam  = "max_body_bytes"
	webhookSecretSupplyParam  = "secret_supply"
)

func (m *webhookModule) handleWebhookEntry(w http.ResponseWriter, r *http.Request) {
	claimedNS := namespace.Namespace(r.PathValue("namespace"))
	workflowID := types.WorkflowID(r.PathValue("workflow_id"))
	entryUnitID := r.PathValue("entry_unit")
	if claimedNS == "" || workflowID == "" || entryUnitID == "" {
		// The registered pattern always supplies all three; this guards a
		// pattern edit that stops populating one of them, which would otherwise
		// resolve a workflow by an empty id.
		writeWebhookError(w, http.StatusNotFound, "route_not_found")
		return
	}
	registry := m.cp.WorkflowRegistry()
	if registry == nil {
		writeWebhookError(w, http.StatusInternalServerError, "internal_error")
		return
	}
	rec, err := registry.GetWorkflow(r.Context(), workflowID)
	if err != nil {
		if !errors.Is(err, backend.ErrWorkflowNotFound) {
			m.logError("webhook_entry_lookup_failed", "workflow_id", string(workflowID), "err", err)
			writeWebhookError(w, http.StatusInternalServerError, "internal_error")
			return
		}
		writeWebhookError(w, http.StatusNotFound, "workflow_not_found")
		return
	}
	// The path's namespace is a CLAIM and the record's is authoritative: a
	// request that names another tenant's namespace for a workflow it can reach
	// by id is answered as not-found rather than served. The admission itself
	// runs under the record's namespace, so the store scoping and the write
	// cannot disagree with the read.
	if rec.Namespace == "" || rec.Namespace != string(claimedNS) {
		writeWebhookError(w, http.StatusNotFound, "workflow_not_found")
		return
	}
	if rec.Graph == nil {
		writeWebhookError(w, http.StatusNotFound, "workflow_not_found")
		return
	}
	params, ok := serverHostedWebhookParams(rec.Graph, entryUnitID)
	if !ok {
		// Either the entry unit does not exist, or it exists and is not a
		// server-hosted webhook. Both are "this route serves no such entry", and
		// neither may leak which of the two it was.
		writeWebhookError(w, http.StatusNotFound, "entry_unit_not_found")
		return
	}
	if method := strings.ToUpper(strings.TrimSpace(paramString(params, webhookMethodParam))); method != "" && r.Method != method {
		w.Header().Set("Allow", method)
		writeWebhookError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	body, ok := readWebhookBody(w, r, webhookMaxBodyBytes(params))
	if !ok {
		return
	}
	secret, ok := m.webhookSecret(r.Context(), rec.Namespace, paramString(params, webhookSecretSupplyParam))
	if !ok {
		// Fail closed: no resolvable secret means no way to authenticate the
		// caller, and admitting would make this an unauthenticated
		// execution-creating endpoint. The caller gets a retryable status; the
		// operator gets the log line that says which entry is unconfigured.
		m.logError("webhook_entry_secret_unavailable",
			"namespace", rec.Namespace, "workflow_id", string(workflowID), "entry_unit", entryUnitID)
		writeWebhookError(w, http.StatusServiceUnavailable, "entry_unavailable")
		return
	}
	if !verifyWebhookSignature(secret, body, r.Header.Get(webhookSignatureHeader)) {
		// One generic answer for missing, malformed and mismatched signatures:
		// distinguishing them would tell a prober which part to fix.
		m.logError("webhook_entry_signature_rejected",
			"namespace", rec.Namespace, "workflow_id", string(workflowID), "entry_unit", entryUnitID)
		writeWebhookError(w, http.StatusUnauthorized, "signature_invalid")
		return
	}

	now := time.Now()
	event := &types.TriggerEvent{
		ID:      webhookEventID(params, r, body, now),
		Kind:    webhookKind,
		Source:  entryUnitID,
		Time:    now,
		Headers: webhookHeaders(r),
		Data: map[string]any{
			"method": r.Method,
			"path":   r.URL.Path,
			"body":   string(body),
		},
		Raw: body,
	}
	exits := []engine.BoundaryExit{{
		NodeName: entryUnitID,
		// The output port the trigger nodes declare. The server resolves the
		// entry unit's out-edges by (node index, port), so an exit naming a port
		// the node does not have would be admitted and fan out to nothing.
		Port: "main",
		Data: map[string]any{"trigger": types.TriggerEventData(event)},
	}}
	outcome := engine.GroupOutcomeSuccess
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey("trigger:" + string(workflowID) + ":" + entryUnitID + ":" + webhookKind + ":" + event.ID),
		WorkflowID:      workflowID,
		WorkflowVersion: rec.Version,
		EntryUnitID:     entryUnitID,
		Outcome:         outcome,
		Exits:           exits,
		// ResultHash is computed server-side, exactly as on the runner face: the
		// caller supplies nothing it could be derived from.
		ResultHash: engine.ComputeResultHash(outcome, exits),
	}
	// No activation record exists for a server-hosted unit, so the generation
	// fence finds nothing to enforce and the seed is admitted on its own key.
	ctx := namespace.WithNamespace(r.Context(), namespace.Namespace(rec.Namespace))
	resp, err := seedAdmission(ctx, m.cp, nil, req)
	writeSeedAdmissionResult(w, m.log, resp, err)
}

// validateServerHostedWebhookSecrets rejects a compiled definition whose
// server-hosted webhook entry unit declares no signing secret (spec §6,
// decision 5: fail closed at registration).
//
// Registration is the last moment the operator is present. A definition accepted
// without a secret would run, answer every request with 503, and look like a
// platform fault from the sender's side; refusing it here names the node that
// has to change while the person who wrote it is still looking at it.
//
// It returns a *WorkflowCompileError so the existing registration contract
// applies unchanged: HTTP answers 400 workflow_compile_failed with the
// compiler's message, and the in-process SDK path reports a caller-fixable
// registration error. A derivation failure is returned as-is (a plain error), not
// wrapped as caller-fixable: the definition compiled, so the failure is the
// server's.
func validateServerHostedWebhookSecrets(g *graph.Graph) error {
	units, err := control.DeriveEntryActivations(g)
	if err != nil {
		return err
	}
	for _, eu := range units {
		if !eu.ServerHosted {
			continue
		}
		if paramString(eu.Params, webhookSecretSupplyParam) == "" {
			return &WorkflowCompileError{err: fmt.Errorf(
				"webhook entry unit %q declares no %s: a server-hosted webhook is "+
					"authenticated by its HMAC signature, so the entry must name the "+
					"supply resource holding its signing secret",
				eu.EntryUnitID, webhookSecretSupplyParam)}
		}
	}
	return nil
}

// serverHostedWebhookParams resolves the entry unit's RENDERED parameters when
// that unit is a server-hosted webhook, and reports false for anything else.
//
// It reads through control.DeriveEntryActivations rather than the raw node so
// the parameters are the same ones the platform would have delivered to a
// runner: a $config or $vars template in the node's params is resolved there,
// and this route must not read a template it would then compare a signature
// against. DeriveEntryActivations covers both entry-unit shapes (a standalone
// trigger node and a group whose entry member is a trigger); only the
// standalone webhook node is server-hosted, so only it can be served here.
func serverHostedWebhookParams(g *graph.Graph, entryUnitID string) (map[string]any, bool) {
	units, err := control.DeriveEntryActivations(g)
	if err != nil {
		return nil, false
	}
	for _, eu := range units {
		if eu.EntryUnitID == entryUnitID && eu.ServerHosted {
			return eu.Params, true
		}
	}
	return nil, false
}

// webhookSecret reads the entry's HMAC secret from the supply resource its
// definition names. The secret lives in the supply store rather than in the
// workflow definition because a definition is listed, diffed, exported and
// copied between environments — a signing secret must not travel with it.
//
// A missing store, an empty or absent name, a not-found resource and an
// unreadable one all report false: the caller cannot authenticate, so the
// outcome is the same refusal and the log line is the only place the distinction
// belongs.
func (m *webhookModule) webhookSecret(ctx context.Context, ns, supplyName string) ([]byte, bool) {
	if m.supplies == nil || supplyName == "" {
		return nil, false
	}
	rec, err := m.supplies.GetSupply(ctx, ns, supplyName)
	if err != nil {
		return nil, false
	}
	if len(rec.Content) == 0 {
		return nil, false
	}
	return rec.Content, true
}

// verifyWebhookSignature reports whether signature is the HMAC-SHA256 of body
// under secret, compared in constant time.
//
// hmac.Equal is what makes the comparison constant-time; comparing the decoded
// bytes with == would leak the position of the first differing byte and let a
// caller recover a valid signature one byte at a time. The header is only ever
// parsed, never logged.
func verifyWebhookSignature(secret, body []byte, signature string) bool {
	if len(secret) == 0 {
		return false
	}
	tag, ok := strings.CutPrefix(signature, webhookSignatureScheme)
	if !ok {
		return false
	}
	provided, err := hex.DecodeString(tag)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

// readWebhookBody reads the request body under the entry's declared limit and
// reports whether the caller may continue.
//
// The limit is enforced while reading (limit+1 bytes) so an oversized body is
// refused without being buffered in full, and the answer is 413: the request was
// well-formed but too large, which tells the sender to split or compress rather
// than to retry the same payload.
func readWebhookBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, bool) {
	if max <= 0 {
		max = defaultWebhookMaxBodyBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, max+webhookBodyOverhead))
	_ = r.Body.Close()
	if err != nil {
		writeWebhookError(w, http.StatusBadRequest, "body_unreadable")
		return nil, false
	}
	if int64(len(body)) > max {
		writeWebhookError(w, http.StatusRequestEntityTooLarge, "payload_too_large")
		return nil, false
	}
	return body, true
}

// webhookMaxBodyBytes resolves the entry's declared limit, falling back to the
// same default the local webhook runtime applies.
func webhookMaxBodyBytes(params map[string]any) int64 {
	if n, ok := paramInt64(params, webhookMaxBodyBytesParam); ok && n > 0 {
		return n
	}
	return defaultWebhookMaxBodyBytes
}

// webhookEventID is the event's identity, and therefore the deduplication key:
// the same event delivered twice must produce one execution.
//
// A configured event_id_header wins: it is the sender's own delivery identity
// (a delivery GUID, an event id), so a redelivery of THAT event collapses even
// across minutes. Without one the identity is the body hash bucketed to the
// minute, which mirrors the local webhook runtime — including its limit: two
// genuinely distinct deliveries of the same body within one minute collapse into
// one, so a sender that needs them distinguished must set the header.
func webhookEventID(params map[string]any, r *http.Request, body []byte, now time.Time) string {
	if header := paramString(params, webhookEventIDHeaderParam); header != "" {
		if id := r.Header.Get(header); id != "" {
			return id
		}
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]) + "/" + strconv.FormatInt(now.Unix()/60, 10)
}

// webhookHeaders flattens the request headers to the same first-value-per-name
// shape the local webhook runtime produces, so a downstream node reading
// $input.trigger.headers sees one shape in both modes.
func webhookHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string, len(r.Header))
	for name, values := range r.Header {
		if len(values) > 0 {
			headers[name] = values[0]
		}
	}
	return headers
}

func paramString(params map[string]any, name string) string {
	if params == nil {
		return ""
	}
	s, _ := params[name].(string)
	return s
}

func paramInt64(params map[string]any, name string) (int64, bool) {
	if params == nil {
		return 0, false
	}
	switch v := params[name].(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

// writeWebhookError answers with the bare errorResponse shape the entry-seed
// face uses (spec §0.1/§8.2), which is what this route's admission result
// already speaks: one face, one body shape. message is always generic — no
// internal detail, no echo of the request.
func writeWebhookError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, errorResponse{Error: code})
}

func (m *webhookModule) logError(msg string, args ...any) {
	if m.log == nil {
		return
	}
	m.log.Error(msg, args...)
}
