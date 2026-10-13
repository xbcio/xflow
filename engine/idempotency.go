package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ErrIdempotencyUnsupported is returned by a keyed submission when the wired
// StateStore cannot hold an idempotency claim.
//
// It fails closed for the same reason a concurrency limit does: a key the
// caller sent is a promise that a retry will not run the work twice, and
// silently ignoring it would let the caller believe a retry is safe when it is
// not.
var ErrIdempotencyUnsupported = errors.New("state store does not support idempotency keys")

// ErrIdempotencyConflict is returned when an idempotency key that already
// started an execution is presented again with a different request. Nothing is
// created for the second request: reusing a key for different work is a
// caller bug, and answering it with the original execution would hand back the
// result of work the caller did not ask for this time.
var ErrIdempotencyConflict = errors.New("idempotency key was already used for a different request")

// ErrIdempotencyInProgress is returned when another request holding the same
// key and fingerprint is still creating its execution. The caller retries; the
// retry is answered as a duplicate once the first request has finished.
//
// It exists so two concurrent requests can never both create the execution: a
// claim that is merely young is held by a request that is still working, and
// only a claim older than its lease is treated as abandoned (see
// IdempotencyClaimTakeover).
var ErrIdempotencyInProgress = errors.New("a request with this idempotency key is still in progress")

// ErrIdempotencyKeyInvalid reports a key outside the accepted shape (see
// ValidateIdempotencyKey).
var ErrIdempotencyKeyInvalid = errors.New("idempotency key must be 1-255 characters of [A-Za-z0-9._:-]")

// errIdempotencyScopeMissing reports a keyed submission with no scope. The
// scope (WorkflowIdempotencyScope / DefinitionIdempotencyScope) is what the
// caller knows and the engine does not: an empty one is a wiring bug, so it is
// refused instead of quietly pooling every workflow's keys together.
var errIdempotencyScopeMissing = errors.New("idempotency key requires a scope")

// DefaultIdempotencyTTL is how long a key keeps answering with the execution it
// created when the engine is not configured otherwise. The effective window is
// further capped by the execution's own retention, so a key never outlives the
// execution it points at.
const DefaultIdempotencyTTL = 24 * time.Hour

// DefaultIdempotencyClaimLease is how long a claim whose execution has not
// appeared yet is presumed to belong to a request that is still creating it.
//
// Past the lease the claim is presumed abandoned — the request that took it
// crashed between the claim and the creation — and the next request with the
// same key and fingerprint takes it over and redoes the creation under the same
// deterministic execution ID. The lease is long relative to a creation (a few
// store round trips) so a slow request is not overtaken while it works.
const DefaultIdempotencyClaimLease = 30 * time.Second

// maxIdempotencyKeyLen bounds the key's length. The key is hashed into the
// execution ID, so the bound is about request hygiene, not storage.
const maxIdempotencyKeyLen = 255

// ValidateIdempotencyKey reports whether key has the accepted shape: 1-255
// characters drawn from letters, digits and ". _ : -". The character set is
// deliberately narrow so a key can travel in a header, a log line and a URL
// without escaping.
func ValidateIdempotencyKey(key string) error {
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return ErrIdempotencyKeyInvalid
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == ':', c == '-':
		default:
			return ErrIdempotencyKeyInvalid
		}
	}
	return nil
}

// WorkflowIdempotencyScope is the key scope of a registered workflow: every
// face that invokes the workflow by its registered ID (the SDK and the by-id
// HTTP route) uses it, so one key means one execution whichever face retries.
//
// The version is deliberately not part of the scope: a replay after the
// workflow was upgraded must find the original execution, not start a new one.
// It is part of the fingerprint instead, so such a replay is a conflict rather
// than a silent duplicate of work done under another definition.
func WorkflowIdempotencyScope(id types.WorkflowID) string {
	return "workflow:" + string(id)
}

// DefinitionIdempotencyScope is the key scope of an inline definition, which
// has no registered ID: the definition's own namespace and name stand in for
// it. The definition digest belongs in the fingerprint (Idempotency.Digest), so
// the same key with an edited definition is a conflict.
func DefinitionIdempotencyScope(definitionNamespace, name string) string {
	return "definition:" + definitionNamespace + "/" + name
}

// rerunIdempotencyScope scopes a rerun's key to the execution it derives from:
// the same key on two different parents is two different reruns.
func rerunIdempotencyScope(parent types.ExecutionID) string {
	return "rerun:" + string(parent)
}

// IdempotentExecutionID derives the execution ID a keyed request creates.
//
// The ID is a pure function of the namespace, the scope and the key, which is
// what lets a retry find the execution its first attempt created, and lets the
// claim live in the execution's own Redis hash slot ({execID}) so it can be
// read atomically with the execution's status. The exec-ik- prefix keeps the
// space disjoint from entry-seed IDs (exec-tg-) and from minted ones.
func IdempotentExecutionID(ns namespace.Namespace, scope, key string) types.ExecutionID {
	h := sha256.New()
	h.Write([]byte(ns))
	h.Write([]byte{0})
	h.Write([]byte(scope))
	h.Write([]byte{0})
	h.Write([]byte(key))
	sum := h.Sum(nil)
	return types.ExecutionID("exec-ik-" + hex.EncodeToString(sum[:16]))
}

// IdempotencyFingerprint identifies the content of a keyed request: the same
// key with the same fingerprint is a retry, with a different one a conflict.
type IdempotencyFingerprint string

// fingerprintPayload is the canonical form a fingerprint hashes. It is JSON
// because encoding/json orders map keys, so two equal parameter maps hash the
// same whatever order they were built in, and a number decoded from a request
// (float64) and the same number built in Go (int) encode identically.
//
// Trace and span IDs and runtime vars are deliberately absent: a client's retry
// carries a fresh trace, and making that a conflict would defeat the key. The
// test flag is present because it changes what the execution does (pinned
// nodes serve their mock).
type fingerprintPayload struct {
	Operation      string            `json:"op"`
	Version        string            `json:"version,omitempty"`
	Digest         string            `json:"digest,omitempty"`
	Entry          string            `json:"entry,omitempty"`
	Params         map[string]any    `json:"params,omitempty"`
	Test           bool              `json:"test,omitempty"`
	Parent         types.ExecutionID `json:"parent,omitempty"`
	FromNode       string            `json:"from_node,omitempty"`
	IncludeSuccess bool              `json:"include_success,omitempty"`
}

func (p fingerprintPayload) hash() (IdempotencyFingerprint, error) {
	if len(p.Params) == 0 {
		p.Params = nil
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("fingerprint idempotent request: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return IdempotencyFingerprint(hex.EncodeToString(sum[:])), nil
}

// SubmissionFingerprint is the fingerprint of a keyed Submit (entry empty) or
// Invoke. params are the parameters after graph.ApplyParams, so a request that
// spells out a default and one that relies on it are the same request. digest
// is the caller-supplied extra component (Idempotency.Digest).
func SubmissionFingerprint(g *graph.Graph, entry string, params map[string]any, test bool, digest string) (IdempotencyFingerprint, error) {
	op := "submit"
	if entry != "" {
		op = "invoke"
	}
	version := ""
	if g != nil {
		version = g.WorkflowVersion()
	}
	return fingerprintPayload{
		Operation: op,
		Version:   version,
		Digest:    digest,
		Entry:     entry,
		Params:    params,
		Test:      test,
	}.hash()
}

// rerunFingerprint is the fingerprint of a keyed rerun: which execution, from
// which node, and whether a successful parent was accepted. The parameters are
// the parent's and so are implied by the parent.
func rerunFingerprint(req RerunRequest) (IdempotencyFingerprint, error) {
	return fingerprintPayload{
		Operation:      "rerun",
		Parent:         req.ExecutionID,
		FromNode:       req.FromNode,
		IncludeSuccess: req.IncludeSuccess,
	}.hash()
}

// Idempotency is a keyed submission's request and, once Submit or Invoke
// returns, its outcome.
//
// It travels by pointer on the context because Submit and Invoke are public
// entry points whose signatures are fixed: the caller keeps the pointer and
// reads Duplicate afterwards, the same way it reads any other detail of a
// submission it did not get in the return value.
type Idempotency struct {
	// Key is the caller's idempotency key (see ValidateIdempotencyKey).
	Key string
	// Scope is what the key is unique within, below the namespace: one of
	// WorkflowIdempotencyScope or DefinitionIdempotencyScope.
	Scope string
	// Digest is an extra fingerprint component the caller vouches for — the
	// inline definition's digest, so an edited definition under the same key is
	// a conflict rather than a duplicate.
	Digest string

	// Duplicate is set by the engine when the key had already created an
	// execution: Submit/Invoke returned that execution's ID and created nothing.
	Duplicate bool
}

type idempotencyCtxKey struct{}

// WithIdempotency marks the submission on ctx as keyed. Submit and Invoke then
// derive the execution ID from the key instead of minting one, and a retry
// returns the execution the first attempt created.
func WithIdempotency(ctx context.Context, idem *Idempotency) context.Context {
	if idem == nil {
		return ctx
	}
	return context.WithValue(ctx, idempotencyCtxKey{}, idem)
}

// IdempotencyFromContext returns the keyed-submission request on ctx, if any.
func IdempotencyFromContext(ctx context.Context) (*Idempotency, bool) {
	idem, ok := ctx.Value(idempotencyCtxKey{}).(*Idempotency)
	return idem, ok && idem != nil
}

// IdempotencyClaimState is a store's answer to one claim.
type IdempotencyClaimState string

const (
	// IdempotencyClaimAccepted: the key was free and is now claimed by this
	// request, which goes on to create the execution.
	IdempotencyClaimAccepted IdempotencyClaimState = "accepted"
	// IdempotencyClaimTakeover: the key was claimed by a request that never
	// created its execution and whose lease has run out. The claim now belongs
	// to this request, which redoes the creation under the same ID.
	IdempotencyClaimTakeover IdempotencyClaimState = "claimed_pending"
	// IdempotencyClaimDuplicate: the key already created its execution, and the
	// fingerprint matches. Nothing is created; the original ID is the answer.
	IdempotencyClaimDuplicate IdempotencyClaimState = "duplicate"
	// IdempotencyClaimConflict: the key is held for a different fingerprint.
	IdempotencyClaimConflict IdempotencyClaimState = "conflict"
	// IdempotencyClaimInProgress: the key is held, with the same fingerprint,
	// by a request still inside its lease and whose execution does not exist
	// yet.
	IdempotencyClaimInProgress IdempotencyClaimState = "in_progress"
)

// IdempotencyClaimRequest is one claim on a keyed request's execution ID.
type IdempotencyClaimRequest struct {
	ExecutionID types.ExecutionID
	Fingerprint IdempotencyFingerprint
	// TTL is how long the claim answers for its execution. A store caps it at
	// the retention the execution will get, so the claim never outlives the
	// execution it names.
	TTL time.Duration
	// Lease is how long an unconfirmed claim with no execution behind it is
	// presumed to belong to a request that is still working.
	Lease time.Duration
	// Token identifies this request's hold on the claim when it is accepted or
	// taken over. Confirm and Release act only on the token that holds the
	// claim, so a request whose claim was taken over cannot confirm or release
	// the claim of the request that took it.
	Token string
}

// IdempotencyClaim is a store's answer to a claim.
type IdempotencyClaim struct {
	State IdempotencyClaimState
	// Status is the existing execution's status on a duplicate. It is empty when
	// the execution was created but has since expired.
	Status types.ExecutionStatus
}

// IdempotentStateStore is the optional StateStore capability behind keyed
// submissions.
//
// A claim is first-writer-wins on the execution ID and must be atomic with the
// read of that execution's status, so a store can never report a key free (or
// abandoned) while its execution exists. A key with no claim whose execution
// nevertheless exists — the execution outlived its key, which a queue wait or a
// suspension can cause — is answered as a duplicate, never re-created: creating
// it again would overwrite the execution in place.
//
// A claim lives in one of two states. PENDING is taken by Claim and means "a
// request is creating this execution"; CONFIRMED is set by Confirm after the
// creation succeeded and means "this execution exists, or did". The two are
// distinct so an execution that expired before its claim did is still answered
// as a duplicate, never re-created.
type IdempotentStateStore interface {
	ClaimIdempotency(ctx context.Context, req IdempotencyClaimRequest) (IdempotencyClaim, error)
	// ConfirmIdempotency marks the claim held under token as having created its
	// execution. A token that no longer holds the claim confirms nothing.
	ConfirmIdempotency(ctx context.Context, id types.ExecutionID, token string) error
	// ReleaseIdempotency frees a pending claim held under token, after a
	// refusal that persisted nothing. A confirmed claim, or a token that no
	// longer holds the claim, is left alone.
	ReleaseIdempotency(ctx context.Context, id types.ExecutionID, token string) error
}

// WithIdempotencyTTL overrides how long an idempotency key answers for the
// execution it created (DefaultIdempotencyTTL). The window is still capped by
// the execution's own retention. A non-positive value keeps the default.
func WithIdempotencyTTL(ttl time.Duration) Option {
	return func(e *Engine) {
		if ttl > 0 {
			e.idempotencyTTL = ttl
		}
	}
}

// WithIdempotencyClaimLease overrides DefaultIdempotencyClaimLease. A
// non-positive value keeps the default.
func WithIdempotencyClaimLease(lease time.Duration) Option {
	return func(e *Engine) {
		if lease > 0 {
			e.idempotencyLease = lease
		}
	}
}

// idempotentStart is the decision a claim produced for one keyed request.
type idempotentStart struct {
	id        types.ExecutionID
	token     string
	duplicate bool
}

// beginSubmission resolves the execution ID a submission will use and, when the
// request carries an idempotency key, claims it before anything is written.
//
// Without a key this is the ID the caller pre-allocated on the context, or a
// fresh one — unchanged. With a key the key's deterministic ID wins over any
// pre-allocation, because a retry has to find the execution its first attempt
// created and a per-request pre-allocation cannot be that.
//
// entry is the node an Invoke starts at and empty for a Submit; it is part of
// the request's fingerprint, so a key used once for a submit cannot be reused
// for an invoke of the same graph.
func (e *Engine) beginSubmission(ctx context.Context, g *graph.Graph, entry string, params map[string]any) (idempotentStart, error) {
	idem, ok := IdempotencyFromContext(ctx)
	if !ok {
		return idempotentStart{id: preallocOrNewExecutionID(ctx)}, nil
	}
	fp, err := SubmissionFingerprint(g, entry, params, TestRunFromContext(ctx), idem.Digest)
	if err != nil {
		return idempotentStart{}, err
	}
	start, err := e.claimIdempotency(ctx, g, idem.Scope, idem.Key, fp)
	if err != nil {
		return idempotentStart{}, err
	}
	if start.duplicate {
		// Reported on the caller's own request, which is the only handle it has
		// on the outcome: Submit and Invoke return the ID alone.
		idem.Duplicate = true
	}
	return start, nil
}

// claimIdempotency claims the keyed request's deterministic execution ID.
//
// A duplicate comes back as a decision rather than an error: the request
// succeeded, it just created nothing. A conflict, an in-progress claim and a
// store without the capability are errors, because the caller has nothing to
// act on but the refusal.
//
// Residual window: the claim is not atomic with the execution's creation. A
// request can claim, stall past the lease without creating anything, lose the
// claim to a takeover, and then resume and create anyway. Its create names the
// SAME derived id (the key determines it), and every backend's create is an
// unconditional write (local's CreateExecution / CreateExecutionWithOutbox,
// rstate's createExecution), so the late write lands on top of the execution the
// takeover request already created and confirmed. The claim token guards
// settling — a stale token can neither confirm nor release — but not creating,
// so the overwrite is a deliberate, documented residual window until a store
// offers a token-conditional create (create only while the claim still holds
// this request's token; on refusal, re-read and answer duplicate) and the
// engine asks for it.
func (e *Engine) claimIdempotency(ctx context.Context, g *graph.Graph, scope, key string, fp IdempotencyFingerprint) (idempotentStart, error) {
	if err := ValidateIdempotencyKey(key); err != nil {
		return idempotentStart{}, err
	}
	if scope == "" {
		// Refused rather than defaulted: an empty scope would silently make one
		// key mean one execution across every workflow of the namespace, which is
		// a caller's wiring bug, not a request this engine can honor.
		return idempotentStart{}, fmt.Errorf("idempotency key %q: %w", key, errIdempotencyScopeMissing)
	}
	store, ok := e.state.(IdempotentStateStore)
	if !ok {
		return idempotentStart{}, ErrIdempotencyUnsupported
	}
	id := IdempotentExecutionID(namespace.FromContext(ctx), scope, key)
	token := uuid.NewString()
	claim, err := store.ClaimIdempotency(ctx, IdempotencyClaimRequest{
		ExecutionID: id,
		Fingerprint: fp,
		TTL:         e.idempotencyWindow(ctx, g),
		Lease:       e.idempotencyClaimLease(),
		Token:       token,
	})
	if err != nil {
		return idempotentStart{}, fmt.Errorf("claim idempotency key for %q: %w", id, err)
	}
	switch claim.State {
	case IdempotencyClaimAccepted, IdempotencyClaimTakeover:
		return idempotentStart{id: id, token: token}, nil
	case IdempotencyClaimDuplicate:
		return idempotentStart{id: id, duplicate: true}, nil
	case IdempotencyClaimConflict:
		return idempotentStart{}, ErrIdempotencyConflict
	case IdempotencyClaimInProgress:
		return idempotentStart{}, ErrIdempotencyInProgress
	}
	return idempotentStart{}, fmt.Errorf("claim idempotency key for %q: unknown claim state %q", id, claim.State)
}

// settleIdempotency records how the creation under a claim ended.
//
// Success confirms the claim. A refusal that persisted nothing (a reached
// concurrency limit, a full queue, a missing capability) releases it, so the
// caller's retry after Retry-After is judged afresh instead of being answered
// as a duplicate of an execution that does not exist. Any other error keeps the
// claim pending: the creation may have landed with its reply lost, and a later
// request either finds the execution (duplicate) or, once the lease has run
// out, takes the claim over and redoes the creation under the same ID.
//
// Both writes are best effort. A confirm that failed leaves the claim pending,
// which still answers duplicate while the execution exists; a release that
// failed leaves a pending claim the lease frees.
func (e *Engine) settleIdempotency(ctx context.Context, start idempotentStart, createErr error) {
	store, ok := e.state.(IdempotentStateStore)
	if !ok || start.token == "" {
		return
	}
	var err error
	switch {
	case createErr == nil:
		err = store.ConfirmIdempotency(ctx, start.id, start.token)
	case persistedNothing(createErr):
		err = store.ReleaseIdempotency(ctx, start.id, start.token)
	default:
		return
	}
	if err != nil && e.logger != nil {
		e.logger.Error("idempotency claim settle failed", "execution_id", string(start.id), "err", err)
	}
}

// persistedNothing reports whether a creation error is one of the refusals the
// engine raises before the store writes anything for the execution.
func persistedNothing(err error) bool {
	return errors.Is(err, ErrConcurrencyLimit) ||
		errors.Is(err, ErrConcurrencyQueueFull) ||
		errors.Is(err, ErrConcurrencyQueueUnsupported) ||
		errors.Is(err, ErrConcurrencyUnsupported)
}

// idempotencyWindow is the TTL a claim is asked for: the configured window,
// shortened to the execution's own retention when the engine knows it is
// shorter (a per-execution TTL override, or a transient graph's TTL). The store
// applies its own default retention on top.
func (e *Engine) idempotencyWindow(ctx context.Context, g *graph.Graph) time.Duration {
	ttl := e.idempotencyTTL
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}
	if override, ok := ExecutionTTLFromContext(ctx); ok && override > 0 && override < ttl {
		ttl = override
	}
	if g != nil && g.Transient() {
		if transient := g.TransientTTL(); transient > 0 && transient < ttl {
			ttl = transient
		}
	}
	return ttl
}

func (e *Engine) idempotencyClaimLease() time.Duration {
	if e.idempotencyLease > 0 {
		return e.idempotencyLease
	}
	return DefaultIdempotencyClaimLease
}
