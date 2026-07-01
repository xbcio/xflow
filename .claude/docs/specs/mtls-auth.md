# Spec: Cluster Auth — Bearer Token + mTLS + Per-Runner Allowlist

**Status**: shipped @ 3fe7234
**Tracks**: review concern #9 — no auth, no TLS, no authorization in runner protocol
**Severity**: high (verifier-confirmed; critical in untrusted networks)

## Problem

`service/control/core.go:32-38` accepts any `RegisterRunnerRequest`. Any
process that can reach the server can register, poll for tasks, and report
results with a self-chosen `RunnerID`. There is no TLS, no auth field, and no
authorization check on which node types a runner may execute. This is acceptable
for an MVP on a trusted dev network; it is a blocker for any deployment
exposed to less-trusted callers.

## Goals

- Add **bearer token authentication** with per-token policy (allowed runner ID
  prefix, allowed node types).
- Support **mTLS** for transport-level authentication; mTLS subject CN/SAN
  optionally feeds into the policy.
- Provide a **dry-run mode** so operators can deploy auth without immediately
  breaking unauthenticated runners.
- Keep dev experience: zero-config dev mode stays the default.

## Non-goals

- Workflow-level authorization (who can submit workflows). Separate spec.
- Multi-tenancy / namespace isolation. Implies more invasive engine changes.
- Token rotation infrastructure. We support hot-reload of a token file; the
  rotation mechanism is operator-owned.

## Design

### Layers

Two orthogonal layers:

1. **Transport security** (mTLS) — server and runner each provide certs;
   mutual verification via CA roots.
2. **Application auth** (bearer tokens) — runner sends `Authorization: Bearer
   <token>` on every request; server looks it up in a policy file.

Operators choose: TLS alone, tokens alone, or both. Default for `--auth-mode`:
`disabled` (today's behavior).

### Wire changes

`service/protocol/types.go`:

```go
type RegisterRunnerRequest struct {
    // ... existing
    AuthToken string `json:"auth_token,omitempty"`
}
```

For HTTP transport the token also accepted via `Authorization: Bearer <token>`
header (preferred). The body field is for clients/transports that can't set
headers easily.

For gRPC the token is carried in metadata under `authorization`.

Same on `HeartbeatRequest`, `PollTaskRequest`, `ReportResultRequest` — every
call validates the token, not just register. Rationale: token revocation
without forcing re-register.

### Policy file

`runners.yaml`:

```yaml
version: 1
runners:
  - id_prefix: order-runner-
    token: ${ORDER_RUNNER_TOKEN}   # env-expanded at load
    allowed_node_types:
      - xflow.http
      - xflow.function
      - xflow.script
  - id_prefix: payment-runner-
    token_file: /etc/xflow/payment-runner.token
    allowed_node_types:
      - "*"   # all
  - id_prefix: mtls-only-
    mtls_subject: CN=runner-prod,OU=eng,O=acme
    allowed_node_types:
      - xflow.database
```

Rules:
- Lookup order: bearer token match → mTLS subject match → fail.
- If both are configured for a runner, **both** must match (defense in depth).
- `id_prefix` is enforced: runner's self-declared ID must start with the
  configured prefix to bind it to the policy.
- `allowed_node_types: ["*"]` means all. Empty list means none (runner can
  register but won't be assigned anything).

### Authenticator

`service/control/auth.go` (new):

```go
type Authenticator interface {
    AuthenticateRegister(req RegisterRunnerRequest, transportInfo TransportInfo) (RunnerPolicy, error)
    AuthenticateOngoing(runnerID, token string, transportInfo TransportInfo) (RunnerPolicy, error)
}

type RunnerPolicy struct {
    IDPrefix         string
    AllowedNodeTypes []string // empty == none; "*" == all
}

type TransportInfo struct {
    TLSPeerCN  string
    TLSPeerSAN []string
}
```

`FilePolicyStore`:

- Loads `runners.yaml`, expands env vars / token files.
- Watches the file via `fsnotify`; on change, reloads atomically with a
  copy-on-write swap of an `atomic.Pointer[policySnapshot]`.
- Constant-time token comparison (`subtle.ConstantTimeCompare`) to avoid
  timing attacks.

### Dispatcher integration

`service/control/runner_pool.go::canRun`:

```go
func canRun(state *runnerState, lease *engine.TaskLease) bool {
    if !matchesCapability(state.snapshot.Capabilities, lease) {
        return false
    }
    if !state.policy.Allows(lease.NodeType) {
        return false
    }
    return true
}
```

When a runner registers, its policy snapshot is bound to `runnerState`. Policy
reload triggers a re-evaluation only on the next assign — already-dispatched
leases aren't recalled.

### TLS configuration

`cmd/server/main.go` flags:
- `--tls-cert /path/to/server.crt`
- `--tls-key /path/to/server.key`
- `--tls-client-ca /path/to/clients.ca` (presence enables mTLS)
- `--auth-mode {disabled,token,mtls,both,dry-run}` (default `disabled`)

`cmd/runner/run.go` env / flags:
- `XFLOW_RUNNER_TLS_SERVER_CA`
- `XFLOW_RUNNER_TLS_CLIENT_CERT`
- `XFLOW_RUNNER_TLS_CLIENT_KEY`
- `XFLOW_RUNNER_TOKEN` (preferred) or `--token`

### Dry-run mode

`--auth-mode dry-run`:

- Authenticator runs normally and computes a verdict.
- On failure: log `auth_dry_run_violation` with full context but **allow** the
  call to proceed.
- Useful when rolling out: deploy server with dry-run, observe logs, fix
  misconfigured runners, then flip to `--auth-mode token` (or `both`).

### gRPC interceptor

```go
func unaryAuthInterceptor(auth Authenticator) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        token := tokenFromMetadata(ctx)
        tinfo := tlsInfoFromContext(ctx)
        if _, err := auth.AuthenticateOngoing("", token, tinfo); err != nil {
            return nil, status.Error(codes.Unauthenticated, err.Error())
        }
        return handler(ctx, req)
    }
}
```

HTTP middleware mirrors this.

### Audit logs

Every auth decision (allow / deny / dry-run-allow) emits a structured log line.
Counters surface in #6.

## Security considerations

- Token files: validate mode 0600; refuse to load if world-readable.
- Server cert/key: same — refuse insecure permissions.
- Never log raw tokens. Log SHA-256 prefixes (first 8 hex chars) for
  debugging.
- Constant-time comparison everywhere.
- TLS minimum version 1.2; prefer 1.3.
- Document that `--auth-mode disabled` is **dev only**. Loud startup warning
  when listening on a non-loopback address with auth disabled.
- Don't roll our own crypto; lean on `crypto/tls` and stdlib `crypto/subtle`.

## Testing

- Unit: `FilePolicyStore` parses YAML, expands env, reloads on change.
- Unit: matching logic — token-only, mTLS-only, both, wildcard.
- Unit: constant-time compare verified against known token.
- Integration (HTTP): register with valid token → 200; invalid → 401; valid
  token but disallowed node type → poll returns empty.
- Integration (gRPC + mTLS): valid cert + token → success; missing cert →
  handshake failure; cert mismatch → `Unauthenticated`.
- Negative: forged `RunnerID` not matching token's `id_prefix` → rejected.
- Dry-run mode: invalid token → call succeeds, dry-run violation logged.

## Rollout

1. Ship the code with default `--auth-mode disabled`. No behavior change.
2. Operators add a `runners.yaml`, flip to `dry-run`, observe logs.
3. After all runners migrate, flip to `token` or `both`.
4. Document the migration in `.claude/docs/security.md`.

## Acceptance

- `--auth-mode disabled` keeps current behavior; existing E2E tests pass.
- `--auth-mode token` + valid YAML + valid runner token: cluster passes
  existing E2E tests.
- `--auth-mode token` with no token or wrong token: register returns 401
  / `Unauthenticated`.
- mTLS with bad client cert fails TLS handshake before the protocol layer.
- Hot reload: changing `allowed_node_types` for a runner in `runners.yaml`
  is reflected on the next dispatch without restart.
- Lint: no plaintext tokens in any log output.
