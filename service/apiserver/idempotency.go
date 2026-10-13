package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// IdempotencyKeyHeader is the header a caller sends to make a submission safe to
// retry (the IETF's "Idempotency-Key" name). It is a header rather than a body
// field so it does not change any request schema and a gateway can inject it.
const IdempotencyKeyHeader = "Idempotency-Key"

// idempotencyKeyFromRequest returns the request's Idempotency-Key, or "" when
// it carries none, reporting false (and writing the 400) for a malformed one.
func idempotencyKeyFromRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get(IdempotencyKeyHeader))
	if key == "" {
		return "", true
	}
	if err := engine.ValidateIdempotencyKey(key); err != nil {
		writeFail(w, r, http.StatusBadRequest, "idempotency_key_invalid",
			"Idempotency-Key must be 1-255 characters of A-Za-z0-9._:-")
		return "", false
	}
	return key, true
}

// idempotencyKeyForResolve returns the request's Idempotency-Key when it is
// present and well-formed, and "" otherwise. Unlike idempotencyKeyFromRequest it
// writes nothing: it is called by resource resolvers, which run before the
// handler and must not answer the request. A malformed key falls through to the
// handler's 400 and to a fresh, unused execution id on the audit rows — deriving
// an id from a key the server refuses would put that made-up id on the admission
// row.
func idempotencyKeyForResolve(r *http.Request) string {
	key := strings.TrimSpace(r.Header.Get(IdempotencyKeyHeader))
	if key == "" || engine.ValidateIdempotencyKey(key) != nil {
		return ""
	}
	return key
}

// resolveExecutionID is the execution id a submission will run under, decided
// before the handler runs so the admission audit row can carry it (R3.1): the id
// derived from the request's Idempotency-Key when it carries a usable one, and a
// fresh id otherwise.
//
// The keyed case must be engine.IdempotentExecutionID over the same namespace,
// scope and key the engine will use — the engine derives the id itself when it
// claims the key, so anything else here would leave the audit rows naming an id
// no execution ever runs under (reconcile Probe would settle the submission as
// absent while its execution ran). scope is the exact scope this route's handler
// passes to keyedSubmission; both sides call the same scope constructor. The
// namespace is the verified principal's, attached to the request by authzWrap
// before resolvers run.
func resolveExecutionID(r *http.Request, scope string) types.ExecutionID {
	if key := idempotencyKeyForResolve(r); key != "" {
		return engine.IdempotentExecutionID(namespace.FromContext(r.Context()), scope, key)
	}
	return engine.NewExecutionID()
}

// keyedSubmission builds the keyed-submission request for one route, or nil when
// the request carries no key — the same submission every caller sent before the
// header existed.
//
// A malformed key is answered here and reported as not-ok: the shape is a
// client-visible contract (engine.ValidateIdempotencyKey), so a server that
// silently dropped a key it did not like would answer "accepted" to a request
// whose retry it cannot recognize.
func keyedSubmission(w http.ResponseWriter, r *http.Request, scope, digest string) (*engine.Idempotency, bool) {
	key, ok := idempotencyKeyFromRequest(w, r)
	if !ok || key == "" {
		return nil, ok
	}
	return &engine.Idempotency{Key: key, Scope: scope, Digest: digest}, true
}

// inlineIdempotencyScope is the key scope of an inline definition: its namespace
// and its name. A definition submitted twice with the same key and the same name
// is one submission, however the caller re-encoded the body; the definition's
// digest is part of the fingerprint instead, so an edited definition under the
// same key is a conflict rather than a silent duplicate.
func inlineIdempotencyScope(ctx context.Context, def *types.WorkflowDef) string {
	return engine.DefinitionIdempotencyScope(string(namespace.FromContext(ctx)), def.Name)
}

// definitionDigest is the canonical digest of an inline definition: the request
// body's own content, hashed through the decoding the handler already did.
//
// Two equal definitions decode to equal values whatever their key order or
// whitespace (encoding/json orders map keys), so a re-sent body hashes the same;
// an edited one does not, and is what turns a reused key into a conflict.
func definitionDigest(def *types.WorkflowDef) (string, error) {
	encoded, err := json.Marshal(def)
	if err != nil {
		return "", fmt.Errorf("digest inline definition: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// writeIdempotencyFailure maps the refusals a keyed submission produces onto
// their responses, reporting whether it wrote one.
//
// The two 409s are deliberately distinct. A conflict (the key was used for a
// different request) is a caller bug that retrying cannot fix; an in-progress
// claim belongs to a request that has the same key and the same fingerprint and
// is still working, so the answer says "ask again" — with the same Retry-After
// floor a concurrency refusal carries, because the server likewise cannot know
// when the first request will finish.
//
// The 503 is the fail-closed case: a server whose store cannot hold a claim
// refuses a keyed request rather than accepting it and losing the guarantee the
// key was sent for.
func writeIdempotencyFailure(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case errors.Is(err, engine.ErrIdempotencyConflict):
		writeFail(w, r, http.StatusConflict, "idempotency_key_conflict",
			"the idempotency key was already used for a different request")
	case errors.Is(err, engine.ErrIdempotencyInProgress):
		w.Header().Set("Retry-After", strconv.Itoa(concurrencyRetryAfterSeconds))
		writeFail(w, r, http.StatusConflict, "idempotency_key_in_progress",
			"a request with this idempotency key is still in progress")
	case errors.Is(err, engine.ErrIdempotencyUnsupported):
		writeFail(w, r, http.StatusServiceUnavailable, "idempotency_unsupported",
			"idempotency keys are not available on this server")
	default:
		return false
	}
	return true
}

// writeSubmitFailure maps a failed Submit/Invoke onto the execute routes'
// refusals, reporting whether it wrote one; the caller writes internal_error
// when it returns false.
//
// The four submit/invoke call sites (the inline and the by-id route, each with a
// Submit and an Invoke branch) must not drift: a refusal added for one has to be
// added for all, and the idempotency refusals are exactly such a set.
//
// An unknown entry node (ErrEntryNotFound) is a 400 rather than a 404: the
// missing resource is the entry in the submitted graph, not an execution in the
// store.
func writeSubmitFailure(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case writeIdempotencyFailure(w, r, err):
		return true
	case writeConcurrencyFailure(w, r, err):
		return true
	case errors.Is(err, engine.ErrFAFRequiresDirectDispatch):
		writeFail(w, r, http.StatusBadRequest, "workflow_faf_unsupported", "FAF workflows require direct dispatch")
	case errors.Is(err, graph.ErrInvalidParams):
		writeFail(w, r, http.StatusBadRequest, "workflow_params_invalid", err.Error())
	case errors.Is(err, engine.ErrEntryNotFound):
		writeFail(w, r, http.StatusBadRequest, "workflow_entry_not_found", err.Error())
	default:
		return false
	}
	return true
}
