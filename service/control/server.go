package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// SubmitWorkflowPath is the HTTP path of the inline workflow execute endpoint
// (POST /v1/workflows/execute after the §9.1 semantic inversion moved the old
// compile-and-execute POST /v1/workflows to /v1/workflows/execute, and the
// register semantics onto POST /v1/workflows).
// Stage 3 moved the workflow/control routes out of Server.Handler (which now
// serves only the runner protocol) and into the apiserver workflow-control
// module. The constant is retained so external callers (sdk, integration and
// perf tests) that build URLs against this path keep compiling; the route
// itself is hosted by service/apiserver.
const SubmitWorkflowPath = "/v1/workflows/execute"

// EngineFacade is the subset of *engine.Engine the runner-protocol Server and
// the apiserver control modules need. *engine.Engine implements every method
// below (Submit/Invoke/Inspect/DeliverSignal/RevokeSignal/Cancel plus the
// embedded execution.Engine lease/routing surface), so it satisfies this
// interface without any adapter.
type EngineFacade interface {
	execution.Engine
	Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error)
	// Invoke starts a new execution from an explicit entry node. Implemented
	// by *engine.Engine (engine.go).
	Invoke(ctx context.Context, g *graph.Graph, entryNode string, input map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error)
	Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error)
	DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
	// RevokeSignal atomically revokes a delivered-but-unconsumed signal.
	// Implemented by *engine.Engine (engine.go).
	RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error
	Cancel(ctx context.Context, id types.ExecutionID) error
	BuildTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error)
	CommitTaskResultWithOutcome(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) (engine.CommitOutcome, error)
	// SeedExecutionFromEntry atomically seeds an execution from an entry unit
	// (single node or group node) result. Implemented by *engine.Engine
	// (entry_admission.go), delegating to the backend EntryAdmissionStore.
	SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error)
}

type Server struct {
	core           *Core
	trustedProxies []netip.Prefix
}

type errorResponse struct {
	Error string `json:"error"`
}

// ServerOption configures a control-plane Server.
type ServerOption func(*Server)

// WithAuthenticator installs a runner-protocol authenticator. Default is the
// permissive DisabledAuthenticator so today's zero-config behavior stays
// unchanged.
func WithAuthenticator(a Authenticator) ServerOption {
	return func(s *Server) {
		if a != nil {
			s.core.auth = a
		}
	}
}

// WithEnroll turns on the enrollment endpoint. Passing a nil store leaves it
// off — enroll is opt-in, and a server that never calls this rejects every
// attempt with the standard message.
func WithEnroll(codes RegistrationCodeStore, ids IssuedIdentityStore) ServerOption {
	return func(s *Server) {
		if codes == nil || ids == nil {
			return
		}
		s.core.registrationCodes = codes
		s.core.issuedIdentities = ids
		s.core.enrollLimiter = newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout)
	}
}

// WithRunnerPools installs the pool store used by pool-bound enrollment and
// issued-identity registration labels. Nil leaves pool enrollment disabled.
func WithRunnerPools(pools RunnerPoolStore) ServerOption {
	return func(s *Server) {
		if pools != nil {
			s.core.pools = pools
		}
	}
}

// WithEnrollRotationGrace sets how long the immediately previous issued token
// remains valid after an idempotent re-enroll. Negative durations are ignored.
func WithEnrollRotationGrace(d time.Duration) ServerOption {
	return func(s *Server) {
		if d >= 0 {
			s.core.rotationGrace = d
		}
	}
}

// WithTrustedProxies enables X-Forwarded-For processing for requests whose
// direct peer is in one of the supplied CIDRs. With no prefixes, forwarding
// headers remain ignored exactly as before.
func WithTrustedProxies(prefixes []netip.Prefix) ServerOption {
	return func(s *Server) {
		s.trustedProxies = append([]netip.Prefix(nil), prefixes...)
	}
}

// withEnrollmentRunnerIDPrefix installs the already-normalized prefix supplied
// by Config. It is intentionally private: control.NewControlPlane is the
// construction boundary that can return an invalid-prefix error.
func withEnrollmentRunnerIDPrefix(prefix string) ServerOption {
	return func(s *Server) { s.core.enrollmentRunnerIDPrefix = prefix }
}

// WithIdentityTTL sets how long a newly enrolled identity authenticates
// before it must renew. Zero (the default) means the identity never expires,
// which is the pre-feature behavior: switching a running fleet onto a TTL
// must be a deliberate act, not something an upgrade does to it.
func WithIdentityTTL(d time.Duration) ServerOption {
	return func(s *Server) {
		if d <= 0 {
			return
		}
		s.core.identityTTL = d
	}
}

// WithControlLogger sets the logger used for auth decisions and other
// runner-protocol diagnostics. Optional.
func WithControlLogger(l engine.Logger) ServerOption {
	return func(s *Server) { s.core.logger = l }
}

// WithAuthObserver installs a non-blocking observer for runner auth decisions.
func WithAuthObserver(observer AuthObserver) ServerOption {
	return func(s *Server) { s.core.authObserver = observer }
}

// WithReportRejectionObserver installs a non-blocking observer for rejected
// result reports, so the reason a 409 was returned is attributable from metrics
// rather than only from a log post-mortem. nil or unset leaves the report path
// byte-identical to before (see reportRejectionObserver).
func WithReportRejectionObserver(observer ReportRejectionObserver) ServerOption {
	return func(s *Server) { s.core.reportRejectionObserver = observer }
}

// WithRunnerDescriptorObserver installs a non-blocking observer for
// runner-reported descriptors dropped at registration. nil or unset is a no-op.
func WithRunnerDescriptorObserver(observer RunnerDescriptorObserver) ServerOption {
	return func(s *Server) { s.core.runnerDescriptorObserver = observer }
}

// WithNodeTimeoutObserver installs the observer for node execution timeout
// events emitted from the server side (the renewLease backstop). nil or unset
// leaves the Core with a nil observer, which renewLease nil-guards so legacy
// behavior is byte-identical. The observer must avoid high-cardinality labels
// (see execution.TimeoutObserver).
func WithNodeTimeoutObserver(observer execution.TimeoutObserver) ServerOption {
	return func(s *Server) {
		if observer != nil {
			s.core.timeoutObserver = observer
		}
	}
}

// WithEntryActivationStore installs the durable EntryActivation store used to
// fence entry seeds by activation generation (spec §11.6). When set, a seed
// carrying a generation older than the currently-assigned generation is
// rejected for a not-yet-accepted admission key and duplicate-accepted for an
// already-accepted one. Nil (the default) disables generation fencing.
func WithEntryActivationStore(store engine.EntryActivationStore) ServerOption {
	return func(s *Server) {
		if store != nil {
			s.core.entryActivations = store
		}
	}
}

// WithWorkflowRegistry installs the durable registry of compiled workflow
// graphs onto the control Core so later tasks can resolve a graph on the seed
// path to derive entry activations. Nil (the default) leaves the Core without a
// registry.
func WithWorkflowRegistry(reg backend.WorkflowRegistry) ServerOption {
	return func(s *Server) {
		if reg != nil {
			s.core.workflowRegistry = reg
		}
	}
}

// WithTracer installs a distributed tracing implementation on the control
// plane's runner-protocol server. The tracer instruments task dispatch and
// commit spans and injects W3C trace carriers into TaskLease so runners can
// create properly-parented execution spans. Default is NoopTracer.
func WithTracer(t tracing.Tracer) ServerOption {
	return func(s *Server) {
		if t != nil {
			s.core.tracer = t
		}
	}
}

// WithHTTPPollWait sets the long-poll wait duration returned to runners when
// no task is available. Default is one second.
func WithHTTPPollWait(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.core.pollWait = d
		}
	}
}

func NewServer(engine EngineFacade, runners RunnerDirectory, opts ...ServerOption) *Server {
	if runners == nil {
		log.Printf("control: runners directory is nil; using in-memory runner directory; not safe for multi-replica deployments")
		runners = NewMemoryRunnerDirectory()
	}
	srv := &Server{
		core: &Core{
			engine:        engine,
			runners:       runners,
			pollWait:      time.Second,
			tracer:        tracing.NoopTracer{},
			rotationGrace: 60 * time.Second,
		},
	}
	for _, o := range opts {
		o(srv)
	}
	return srv
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	protocol.RegisterRunnerRoutes(mux, s)
	return mux
}

// MaxRegisterRunnerBodyBytes caps an HTTP register body at 8 MiB: the
// descriptor envelope limit plus 7 MiB for the rest of the request. Without a
// cap the one request that carries descriptors would be the only uncapped
// runner-protocol read.
//
// The headroom is sized for the activation inventory, the only unbounded
// field: nothing limits how many trigger activations one runner hosts, and a
// realistic item (workflow ID, 64-char version, entry unit, replica,
// generation) encodes to ~225 bytes. A 1 MiB headroom therefore stopped at
// ~4.5k activations, and an old runner with no descriptors at all hit 413
// near ~9k where it used to register. 7 MiB carries ~32k activations (plus
// capabilities and labels, which stay in the tens of KiB) while still
// bounding a single decode. Raise it rather than tighten it if a fleet ever
// hosts more per runner.
//
// The gRPC transport applies the same bound as its receive limit; see
// RunnerGRPCServerOptions.
const MaxRegisterRunnerBodyBytes = protocol.MaxRunnerDescriptorEnvelopeBytes + 7<<20

func (s *Server) HandleRegisterRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	defer func() { _ = r.Body.Close() }()
	var req protocol.RegisterRunnerRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRegisterRunnerBodyBytes)).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeRunnerError(w, ErrRegisterBodyTooLarge)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.register(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.HeartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.heartbeat(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandlePollTask(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.PollTaskRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.pollTask(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	body, err := encodeJSONBody(resp)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrInternalServer.Error())
		return
	}
	// Same bound as the gRPC transport (see GRPCServer.PollTask), measured on
	// the exact bytes the runner would read. A runner's HTTP client caps its
	// response reads at this size, so an oversize lease would be refused and
	// redelivered forever; it is failed here instead.
	if size := len(body); resp.Lease != nil && size > MaxRegisterRunnerBodyBytes {
		writeJSON(w, http.StatusOK, s.core.failOversizeLease(r.Context(), req, resp, size, "runner HTTP response limit"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// encodeJSONBody encodes body exactly as writeJSON would write it, trailing
// newline included, so a handler can measure a response before sending it.
func encodeJSONBody(body any) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Server) HandleReportResult(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.ReportResultRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.reportResult(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			writeJSON(w, http.StatusConflict, resp)
			return
		}
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleRenewLease(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.RenewLeaseRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.renewLease(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleActivationAck(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.ActivationAck
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	if err := s.core.activationAck(r.Context(), req, s.httpTransportInfo(r)); err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// HandleReportMetrics receives a gzip'd delimited-protobuf metrics snapshot.
//
// It does not use decodeJSON: the body is an opaque binary stream. It also does
// not log or echo any request header — the Authorization header is on the
// organization's absolute log blacklist, so there is no wholesale header dump
// even on a malformed request.
func (s *Server) HandleReportMetrics(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	if !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		writeRunnerError(w, ErrMetricsEncodingUnsupported)
		return
	}

	var token string
	overrideTokenFromHeader(r, &token)

	// MaxBytesReader caps the COMPRESSED size before anything is retained. The
	// decompressed size is bounded separately on the read path, so a gzip bomb
	// cannot turn 1 MiB accepted into unbounded memory at scrape time.
	limited := http.MaxBytesReader(w, r.Body, int64(protocol.MaxRunnerMetricsBytes))
	body, err := io.ReadAll(limited)
	if err != nil {
		// Any read failure at this point is either the cap tripping or a broken
		// connection; treat both as too-large rather than guessing, and never
		// include err (which can quote request state) in the response.
		writeRunnerError(w, ErrMetricsPayloadTooLarge)
		return
	}

	if err := s.core.reportMetrics(r.Context(),
		r.Header.Get(protocol.RunnerIDHeader),
		r.Header.Get(protocol.SessionIDHeader),
		token, body, s.httpTransportInfo(r)); err != nil {
		writeRunnerError(w, err)
		return
	}
	// 204: the runner has nothing to read back, and an empty JSON object would
	// only cost a round trip's worth of bytes 4 times a minute per runner.
	w.WriteHeader(http.StatusNoContent)
}

// HandleEnroll serves the unauthenticated enrollment endpoint. Every rejection
// — unknown code, revoked code, out-of-scope, rate-limited, and "enroll is not
// configured" — returns the same status and the same body.
func (s *Server) HandleEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// This is the one endpoint in the runner protocol that requires no
	// credential to reach, so an unbounded body read here is a
	// memory-exhaustion surface open to anyone. Same cap and shape as
	// HandleReportMetrics's MaxBytesReader — reusing the existing limit
	// rather than inventing a new number.
	limited := http.MaxBytesReader(w, r.Body, int64(protocol.MaxRunnerMetricsBytes))
	var req protocol.EnrollRequest
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		// A malformed body is still a rejected enrollment attempt as far as the
		// caller can tell. Reporting "bad JSON" separately would distinguish
		// "your request was well-formed but wrong" from "your request was
		// malformed", which is a small oracle but an oracle.
		writeError(w, http.StatusForbidden, "enrollment rejected")
		return
	}
	resp, err := s.core.Enroll(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeError(w, http.StatusForbidden, "enrollment rejected")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleRenewIdentity serves the runner-facing identity renewal endpoint. It
// runs behind the same authenticator as every other ongoing runner endpoint
// (see Core.renewIdentity), so by the time renewIdentity returns success the
// token has already been proven to belong to req.RunnerID — this handler adds
// no authorization logic of its own, exactly like HandleHeartbeat.
func (s *Server) HandleRenewIdentity(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.RenewIdentityRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.renewIdentity(r.Context(), req, s.httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleDeregister serves the runner-facing graceful-shutdown endpoint. Like
// HandleRenewIdentity it adds no authorization of its own; Core.deregister
// authenticates and the directory fences on the session.
func (s *Server) HandleDeregister(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.DeregisterRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	if err := s.core.deregister(r.Context(), req, s.httpTransportInfo(r)); err != nil {
		writeRunnerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// overrideTokenFromHeader gives Authorization: Bearer priority over the body
// AuthToken field. Header transport is preferred per the spec.
func overrideTokenFromHeader(r *http.Request, dst *string) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		*dst = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
}

// httpTransportInfo extracts TLS peer identity from the request when the
// connection is a verified client mTLS session. SourceIP is always populated
// (empty TLS fields on plaintext HTTP so the authenticator's mTLS branch will
// reject).
func httpTransportInfo(r *http.Request) TransportInfo {
	return httpTransportInfoWithTrustedProxies(r, nil)
}

func (s *Server) httpTransportInfo(r *http.Request) TransportInfo {
	return httpTransportInfoWithTrustedProxies(r, s.trustedProxies)
}

func httpTransportInfoWithTrustedProxies(r *http.Request, trustedProxies []netip.Prefix) TransportInfo {
	info := TransportInfo{Kind: TransportKindHTTP, SourceIP: sourceIPOf(r, trustedProxies)}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return info
	}
	cert := r.TLS.PeerCertificates[0]
	info.TLSPeerCN = cert.Subject.String()
	info.TLSPeerSAN = append(info.TLSPeerSAN, cert.DNSNames...)
	return info
}

// sourceIPOf strips the port from RemoteAddr. Without trustedProxies,
// X-Forwarded-For stays ignored exactly as before. With trusted proxies, the
// header is considered only when the direct peer is trusted; walking from the
// right then finds the first address outside the trusted proxy chain.
//
// It can return "": r == nil; RemoteAddr == "" (net.SplitHostPort errors, and
// the empty string is returned as-is); RemoteAddr with an empty host, e.g.
// ":1234" (SplitHostPort succeeds and yields host == ""); and Unix domain
// socket listeners, which commonly report RemoteAddr as "" or "@". Callers
// that bucket by SourceIP (the enroll rate limiter) must treat "" as "no
// source to bucket by" and refuse rather than share one bucket.
func sourceIPOf(r *http.Request, trustedProxies []netip.Prefix) string {
	if r == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if len(trustedProxies) == 0 {
		return host
	}

	peer, err := netip.ParseAddr(host)
	if err != nil || !addressInPrefixes(peer, trustedProxies) {
		return host
	}

	parts := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	forwarded := make([]netip.Addr, 0, len(parts))
	parseFailed := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			parseFailed = true
			continue
		}
		addr, parseErr := netip.ParseAddr(part)
		if parseErr != nil {
			parseFailed = true
			continue
		}
		forwarded = append(forwarded, addr)
	}
	if len(forwarded) == 0 {
		return host
	}
	if parseFailed {
		return forwarded[0].String()
	}
	for i := len(forwarded) - 1; i >= 0; i-- {
		if !addressInPrefixes(forwarded[i], trustedProxies) {
			return forwarded[i].String()
		}
	}
	return forwarded[0].String()
}

func addressInPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// decodeJSON decodes a runner-protocol request body capped at
// MaxRegisterRunnerBodyBytes, the same bound the gRPC transport applies as its
// receive limit, answering 413 when the cap trips and 400 for malformed JSON.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRegisterRunnerBodyBytes)).Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeRunnerError(w, ErrRequestBodyTooLarge)
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return false
	}
	return true
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// writeRunnerError maps transport-agnostic Core sentinel errors to HTTP status
// codes. Known sentinels carry an actionable message; the catch-all default
// returns a generic 500 so internal error details are not leaked.
func writeRunnerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRunnerIDRequired), errors.Is(err, ErrRunnerSessionRequired), errors.Is(err, ErrConcurrencyRequired), errors.Is(err, ErrInstanceUIDRequired), errors.Is(err, ErrInvalidNamespace), errors.Is(err, ErrLabelConflict), errors.Is(err, ErrLeaseRequired), errors.Is(err, ErrMissingWorkflowVersion):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrInvalidCapability):
		writeError(w, http.StatusBadRequest, ErrInvalidCapability.Error())
	case errors.Is(err, ErrRunnerSessionStale), errors.Is(err, ErrRunnerIDConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRunnerNotFound):
		writeError(w, http.StatusNotFound, "runner not found")
	case errors.Is(err, ErrUnauthenticated):
		writeError(w, http.StatusUnauthorized, err.Error())
	case errors.Is(err, ErrAuthNamespaceDenied):
		writeError(w, http.StatusForbidden, ErrAuthNamespaceDenied.Error())
	case errors.Is(err, ErrAuthCapabilityDenied):
		writeError(w, http.StatusForbidden, ErrAuthCapabilityDenied.Error())
	case errors.Is(err, ErrMetricsProxyDisabled):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, ErrMetricsPayloadTooLarge), errors.Is(err, ErrRegisterBodyTooLarge), errors.Is(err, ErrRequestBodyTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, ErrMetricsEncodingUnsupported):
		writeError(w, http.StatusUnsupportedMediaType, err.Error())
	default:
		// Logged before it is generalized away, because the response cannot carry
		// it and nothing else will: an unrecognised error reaches the runner as a
		// bare 500, and a runner's pollLoop treats a poll error as fatal (see
		// core.go's claim-race comment) -- so this branch can permanently idle a
		// runner while leaving no record anywhere of what the cause was. Observed:
		// a SAS runner died on a 500 from this branch and the reason was
		// unrecoverable after the fact.
		//
		// The log goes to the server's own stderr, never to the response. The
		// generic body is unchanged: §7 forbids exposing internals to the caller.
		log.Printf("control: unmapped runner error, returning 500: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternalServer.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
