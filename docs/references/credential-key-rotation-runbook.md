# Credential & Key Rotation Runbook

xflow has **three distinct rotation axes**. They protect different things, they
are rotated by different actors, and their feasibility is completely different.
Conflating them is the main way this document could mislead an operator, so the
axis table in §1 comes first and the at-rest verdict is stated there rather than
buried in its section.

> **Status: this runbook decides nothing.** [RELEASE-GATES.md](../design/RELEASE-GATES.md)
> §6.1 now records "密钥轮换（runner token / mTLS / supply KEK）" as an **existing**
> runbook (that row previously read *missing*), and
> D6 (owner / deadline) is still `OPEN`. This document supplies the missing
> *content* so that only the owner/deadline decision remains. No rotation
> described here has ever been exercised in a real environment in this
> repository, and nothing below should be read as an approval of any procedure.

## 1. Which axis are you rotating?

| Axis | What it protects | Who / what rotates it | Safe today? |
|---|---|---|---|
| **1. Supply transport key** | supply content in flight from control plane to runner (`GET /v1/supplies/{name}` with `Accept: application/x-xflow-encrypted`) | the control plane, unattended, on a schedule (`DefaultSupplyKeyRotationPeriod`, 24h); a runner adopts a rotation on its next heartbeat | **Yes.** Automated, in-flight only, reissuable at will, no stored data depends on it. See §2. |
| **2. Supply at-rest KEK** (`XFLOW_MASTER_KEY` / `--master-key-file`) | the `content` column of every stored supply row in MySQL | manual, offline-window — `xflow supply reseal` (`cmd/xflow/supply_reseal.go`) | **Yes, offline-window only.** A previous-key configuration surface (`masterkey.LoadPrevious`) and a re-encryption path (`(*sqlstore.Provider).ResealSupplies`, wired to `xflow supply reseal`) now exist. Rotating **without** loading the previous key still destroys access to every stored row (§3.4 is unchanged and still describes that failure). This path has never been rehearsed in a real environment; see §3. |
| **3. Runner credentials** (static bearer token, enrollment-issued identity, mTLS material) | a runner's authentication to the control plane | manual, per runner (or per fleet) | **Partly.** A static token and a client certificate can each be replaced. The **server side** of both (the policy file and the TLS material) hot-reloads via `SIGHUP` with no restart; the **runner side** of both still requires a restart per runner. An enrollment-issued token **cannot** be rotated in place — renewal extends its expiry and never changes the token. See §4. |

> **Operators: if you came here to rotate a key, find your axis first.** Axis 1
> and axis 3 have procedures. Axis 2 also has one now, but it is
> **offline-window only** and has never been rehearsed in a real environment —
> read §3 in full before starting, and do not treat a generic KMS
> key-rotation playbook written for a different system as a substitute for
> the procedure below. Skipping the previous-key step (§3.4) still destroys
> access to all stored supply content, with no way back.

Cross-cutting: axis 2 is an **offline change** (it needs at least a process
restart on every server), so it couples to the maintenance window in
[maintenance-window-runbook.md](maintenance-window-runbook.md). Axis 3's
**server-side** material (runner auth policy, server TLS cert/key/client-CA)
now hot-reloads via `SIGHUP` and needs no window; axis 3's **runner-side**
material still requires restarting each runner. Axis 1 needs no window at all.

## 2. Axis 1 — supply transport key (automated, safe)

### 2.1 What it protects, and how it is delivered

The transport key encrypts supply content on the server→runner hop only. It is
generated at control-plane startup, and in a multi-replica deployment it is
shared through Redis so that a runner registering against replica A can still
decrypt content fetched from replica B — without the shared key that fetch fails,
the supply gate declines, and the runner hosts no triggers while heartbeating
perfectly healthily (`service/control/supply_encryption.go:53-57`, `:205-213`).

- Shared key: `SET NX` on `xflow:supply:transport-key`
  (`service/control/supply_encryption.go:202-203`, `:69-91`); without Redis the
  backend is single-replica and a process-local key is used
  (`:214-219`).
- Delivery to a runner: the key rides the registration response and is installed
  into the fetcher's keyring (`service/runner/runner.go:230-236`, `:694-708`).
- Rotation delivery: the runner reports the key ID it holds on every heartbeat
  (`service/protocol/types.go:120-129`; `service/runner/runner.go:619`) and the
  server returns the full key **only when the IDs differ**
  (`service/control/core.go:386-388`, `service/control/supply_encryption.go:122-141`).
  Delivery is convergent, not bookkept: a runner that was down during a
  rotation, restarted, or joined afterwards catches up on its next heartbeat
  (`service/control/supply_encryption.go:18-25`).
- The runner's keyring keeps current + previous, so content already encrypted
  under the superseded key still decrypts
  (`service/crypto/supplyenc/supplyenc.go:137-142`;
  `service/runner/runner.go:710-712`).

This key is deliberately the **only** key kept in Redis: it is short-lived and
self-healing, so losing it costs a re-registration. The comment is explicit that
the at-rest key must never live there, because losing *that* one would make
stored ciphertext permanently unreadable
(`service/control/supply_encryption.go:64-68`).

### 2.2 When to rotate

- **Default: you do nothing.** The control plane rotates on a schedule; the
  period defaults to 24h because the hop is TLS-protected in any real
  deployment, so rotation limits the blast radius of a *leaked* key rather than
  stopping a live eavesdropper, and rotating harder only adds heartbeat churn
  (`service/control/supply_key_rotation.go:11-19`).
- **Manually: after a suspected read of the Redis key value.** The key is stored
  base64-encoded in Redis (`service/control/supply_encryption.go:74`), so anyone
  who can read that key has it.
- **Do not rotate it aggressively.** A misconfigured period is floored at one
  minute (`service/control/supply_key_rotation.go:21-24`, `:60-74`) because key
  churn that outruns the heartbeat interval makes runners spend their time
  adopting keys instead of converging on one.

### 2.3 Preconditions

- Redis reachable from every replica. `Rotate` publishes to Redis **before**
  swapping the local key, so a failed publish leaves the rotation a no-op rather
  than a partial one (`service/control/supply_encryption.go:143-166`).
- Runners heartbeating — that is the delivery channel.
- Know the configured period. `--supply-key-rotation` (`cmd/server/main.go:222-223`)
  is `0` = 24h default, **negative = rotation disabled**, positive = floored to
  1 minute (`service/control/supply_key_rotation.go:63-74`; threaded through
  `service/apiserver/apiserver.go:107-110` and `sdk/xflow/server.go:620-621`).

### 2.4 Procedure

Rotating this key is not an operator procedure in the normal case — it is a
background loop. Two ways to act on it:

1. **Preferred: wait.** The schedule is the procedure.
2. **Force a rotation now** (e.g. after a suspected Redis read): delete the
   rotation slot and let the loop claim it.
   ```
   redis-cli DEL xflow:supply:transport-key:rotation-slot
   ```
   The slot key is `xflow:supply:transport-key:rotation-slot`
   (`service/control/supply_key_rotation.go:26-29`). Each replica re-checks it
   every 30s (`:31-36`) and the one that takes the slot rotates
   (`:106-133`). Do **not** delete `xflow:supply:transport-key` itself: a
   missing shared key is treated as an error, not a reason to generate a
   replacement — two replicas each minting one after an eviction would diverge,
   which is the exact failure the shared key exists to prevent
   (`service/control/supply_encryption.go:168-177`).
3. Setting a shorter `--supply-key-rotation` and restarting also works, but note
   the slot is *seeded* at startup so the first rotation lands one full period
   after start (`service/control/supply_key_rotation.go:86-92`). Restarting the
   whole fleet inside one period therefore *postpones* a rotation rather than
   forcing one.

### 2.5 Verification

Rotation is confirmed from logs only — there is no metric for it (§6).

- The rotating replica logs `supply transport key rotated` with a `key_id`
  attribute (`service/control/supply_key_rotation.go:129-131`).
- Every other replica logs `adopted rotated supply transport key` with the same
  `key_id` (`service/control/supply_key_rotation.go:141-143`). Same `key_id`
  across the fleet is the convergence check.
- The `key_id` is the first 4 bytes of SHA-256(key), hex-encoded — a fingerprint,
  safe on the wire and in logs (`service/crypto/supplyenc/supplyenc.go:41-47`;
  `service/control/supply_encryption.go:110-120`).
- Runners need no inspection: once a runner's reported ID matches current, the
  server stops sending it anything (`service/control/supply_encryption.go:122-141`).

### 2.6 What breaks if you get it wrong

- **A replica encrypting with a key runners have already replaced** declines the
  supply fetch, so it hosts no triggers while heartbeating healthily
  (`service/control/supply_key_rotation.go:76-83`). In practice this is the
  failure of *not* having a shared key, not of rotating.
- **Two rotations inside one keyring window.** The runner keyring holds only
  current + previous and `Rotate` evicts the oldest
  (`service/crypto/supplyenc/supplyenc.go:137-142`, `:155-165`), so a third key
  can evict the key an in-flight encrypted response was sealed under, and that
  one fetch fails. The 1-minute floor
  (`service/control/supply_key_rotation.go:21-24`) bounds how fast this can be
  driven. Redelivering the *same* key is worse and is structurally prevented:
  `installSupplyKey` calls `Rotate` on every delivery, so a duplicate would
  demote the key it just promoted (`service/protocol/types.go:147-156`).
- **No data loss is possible on this axis.** Nothing at rest depends on the
  transport key (`service/crypto/supplyenc/atrest.go:12-15`).

### 2.7 Rollback

There is nothing to roll back to: `Rotate` mints a fresh random key and the
previous value is not retained (`service/control/supply_encryption.go:143-166`).
"Rollback" means rotating again, and between the two rotations the superseded
key is still decryptable only while it occupies the runner keyring's previous
slot (`service/crypto/supplyenc/supplyenc.go:155-165`).

## 3. Axis 2 — supply at-rest KEK: offline-window rotation (unrehearsed)

### 3.1 Summary

> **This axis is rotatable, but only through an offline maintenance window,
> and the procedure below has never been exercised in a real environment in
> this repository.** Treat every step as unverified until it has been
> rehearsed once against a non-production database.
>
> The KEK (`XFLOW_MASTER_KEY` or `--master-key-file`) still protects data with
> the same weight as the supply content itself: losing it with no previous
> key configured, or skipping the reseal step below, destroys that data with
> no way back. The two capabilities that used to be missing now exist:
>
> - a previous-key configuration surface
>   (`masterkey.LoadPrevious`, `service/crypto/masterkey/masterkey.go`;
>   `supplyenc.NewAtRestWithPrevious`, `service/crypto/supplyenc/atrest.go`), and
> - a re-encryption path (`(*sqlstore.Provider).ResealSupplies`,
>   `store/sqlstore/supply_reseal.go`) exposed as an operator command
>   (`xflow supply reseal`, `cmd/xflow/supply_reseal.go`).
>
> Rotating **without** loading the previous key is exactly the failure this
> runbook warned about before these existed, and §3.4 below still describes
> that chain unchanged — it is now the documented consequence of skipping
> step (b), not a description of a missing capability.

### 3.2 What it protects

- Every stored supply row's `content` column in MySQL, sealed on write and
  opened on read (`store/sqlstore/supply.go:123-129`, `:46-54`).
- It is derived, not stored: `supplyenc.NewAtRest(mk.Derive(supplyenc.SupplyContentInfo))`
  (`cmd/server/supply_keys.go`) from the single master key loaded at server
  startup. `Derive` is deterministic HKDF-SHA256 scoped by an info string,
  which is what makes previously stored ciphertext readable after a restart
  (`service/crypto/masterkey/masterkey.go`).
- The transport key is *not* this key, and the two have opposite lifetimes by
  design: "the transport key is short-lived and reissued at will, while this one
  must stay derivable for the lifetime of the stored data"
  (`service/crypto/supplyenc/atrest.go`).

### 3.3 The two keys the rotation window uses

Two keys cover exactly **one** rotation. A previous key is accepted by
`AtRest.Open` for reading only; every `Seal` (new write, and every row a
reseal pass rewrites) always uses the current key
(`service/crypto/supplyenc/atrest.go`, `NewAtRestWithPrevious`). This is a hard
ceiling, not a convenience limit: `Keyring` holds at most current + previous,
so starting a second rotation before the first one's reseal pass has reported
`failed=0` strands whatever is still sealed under the oldest key with no key
left in the keyring that can open it.

- `masterkey.LoadPrevious` rejects a previous value equal to the current key
  (`ErrPreviousEqualsCurrent`) — a rotation that installs the same key twice
  would "succeed" while resealing nothing.
- `masterkey.LoadPrevious` rejects a previous key configured without a current
  key — there is nothing to rotate *to*.
- Neither loader ever echoes a bad key value in its error text.

### 3.4 What happens if you replace the KEK WITHOUT loading the previous key

This is not a hypothetical: it is the concrete failure that skipping step (b)
in §3.5, or letting `XFLOW_MASTER_KEY_PREVIOUS` / `--master-key-previous-file`
go unset during a cutover, still produces today. It is worth reading in full
before touching this key, because the symptom appears far from the cause and
the server does not tell you why.

1. Server: `GET /v1/supplies/{name}` → `GetSupply` → `AtRest.Open` fails with
   `ErrUnknownKey`, wrapped as
   `get supply "<ns>"/"<name>": decrypt: supplyenc: open stored content: supplyenc: no key matches kid: <kid>`
   (`store/sqlstore/supply.go:46-54`; `service/crypto/supplyenc/atrest.go:49-58`;
   `service/crypto/supplyenc/supplyenc.go:207-210`).
2. Server HTTP surface: **500** `{"success":false,"code":"internal_error","message":"internal server error"}`
   (`service/apiserver/module_supply.go:104-107`, envelope at
   `service/apiserver/envelope.go:58-64`). The decrypt reason is not in the body
   and is not logged by this handler — the 500 is indistinguishable from any
   other 500.
3. Runner: the fetch fails (`supply fetch: unexpected status 500`,
   `service/runner/supply_client.go:90-98`), the gate records a fetch error and
   logs a WARN whose message is exactly `supply fetch failed`
   (`service/runner/supply_gate.go:170-180`), and for a `require_ready` supply the
   node is added to the missing set (`:181-188`) and `Admit` returns
   `NotReadyError` (`:229-233`, message `supply not ready: <nodes>`, `:34-40`).
4. Activation is declined **before** the subscription starts — deliberately, so
   traffic stays in Kafka with consumer-group lag as the signal
   (`service/runner/trigger_activation_handler.go:184-192`).
5. Runner readiness never flips: `/readyz` reports not ready with reason
   `no supply has been fetched yet`, because only a successful fetch moves that
   state (`sdk/runner/lifecycle.go:95-102`, `:111-129`).
6. **Recovery is not possible from the new key alone.** The rows are still
   sealed under the old `kid`; the only way to read them again is to restart
   with the old key loaded as the previous key
   (`XFLOW_MASTER_KEY_PREVIOUS` / `--master-key-previous-file`) and then run
   `xflow supply reseal`. If the old key value itself is gone, the content is
   gone — and remember that supply rows are the input to the supply gate, so
   the workflows they feed do not run either.

One asymmetry is worth stating, because it makes the *upgrade* direction look
safe and the *rotation-without-previous-key* direction fatal: `Open` passes
through anything that does not look like an envelope, which is what lets a
deployment with pre-encryption plaintext rows start encrypting without
breaking them (`service/crypto/supplyenc/atrest.go:33-52`;
`service/crypto/supplyenc/supplyenc.go:248-268`). Plaintext→encrypted is safe
with no previous key needed. Encrypted→different-key is only safe with the
old key loaded as previous through the window below.

### 3.5 Procedure — offline maintenance window (unrehearsed)

This is an **offline-window** operation: every server that talks to this
MySQL database restarts twice. Coordinate it as a
[maintenance-window-runbook.md](maintenance-window-runbook.md) event, not as a
live change.

**(a) Back up the old KEK out of band, and generate the new one.**

The old KEK value must survive somewhere outside this procedure (secret
manager, offline vault) until step (d) below has completed with `failed=0`.
Generate the replacement the same way the current one was generated:

```
openssl rand -base64 32
```

**(b) Restart every server with both keys configured.**

Set the new value as the current key and the old value as the previous key,
by environment variable:

```
XFLOW_MASTER_KEY=<new base64 key>
XFLOW_MASTER_KEY_PREVIOUS=<old base64 key>
```

or by file (`--master-key-file` / `--master-key-previous-file`, each `0600`).
Env takes precedence over its corresponding file, exactly like the existing
`XFLOW_MASTER_KEY` / `--master-key-file` precedence
(`service/crypto/masterkey/masterkey.go`).

`loadSupplyAtRest` (`cmd/server/supply_keys.go`) rejects a previous value equal
to the current one and a previous value configured without a current one —
both are always startup errors, in every mode, never a silent fallback.

On success the server logs a startup line naming both facts out loud:

```
WARNING supply at-rest KEK rotation window open: sealing under kid=<new kid>, also accepting the previous key; run `xflow supply reseal` then remove XFLOW_MASTER_KEY_PREVIOUS / --master-key-previous-file
```

Every server instance pointed at this MySQL database must be restarted with
both keys before proceeding — a server still running with only the old key
still writes new rows correctly (old key = its only key), but a server
already on the new key needs the previous key to *read* rows written before
the cutover.

**(c) Reseal.**

Dry run first, to see the classification without writing anything:

```
xflow supply reseal --dry-run --mysql-dsn "$XFLOW_MYSQL_DSN"
```

Then run it for real, unscoped (no `--namespace`) — a rotation is only
finished by an unscoped pass, because a scoped pass leaves every row outside
that namespace still sealed under the previous key:

```
xflow supply reseal --mysql-dsn "$XFLOW_MYSQL_DSN"
```

The command prints a JSON report (`sqlstore.ResealReport`) with `scanned`,
`resealed`, `already_current`, `failed`, `dry_run`, and — when `failed > 0` —
a `failures` list naming each row's `namespace`, `name`, and `reason` (never
the stored bytes, which may be ciphertext of a credential). It exits non-zero
whenever `failed > 0`, specifically so a script cannot mistake a partial pass
for a completed rotation.

**Require `failed=0` before continuing.** If any row failed, it is still
sealed under the previous key (or, for a genuinely corrupted row, under
neither) — the previous key must stay configured, and the reseal command
re-run, until this reports zero failures. `xflow supply reseal` is idempotent
and resumable: rows already sealed under the current key are counted as
`already_current` and left untouched, so re-running only touches what is
still outstanding.

Resealing leaves `content_hash` and `revision` unchanged — the reseal pass
rewrites only the envelope, not the plaintext, so consumers that compare
`content_hash` never see a reseal as a content change
(`store/sqlstore/supply_reseal.go`). Each row is locked (`SELECT ... FOR
UPDATE`) and resealed in its own transaction, the same lock `PutSupply` takes,
so the pass is safe to run against a database taking concurrent
`PutSupply` writes — but run it inside the maintenance window regardless,
since the point of the window is the two restarts, not the reseal pass
itself.

**(d) Restart without the previous key.**

Once (c) has reported `failed=0`, restart every server again with
`XFLOW_MASTER_KEY_PREVIOUS` / `--master-key-previous-file` removed. This
closes the rotation window: from this point the previous key is no longer
accepted for `Open`, and a stray row still sealed under it (there should be
none, if (c) reported zero failures) becomes unreadable again.

**(e) Verify.**

- `GET /v1/supplies/{name}` for a representative supply in each namespace
  returns 200, not 500.
- Runner `/readyz` reports ready, not `no supply has been fetched yet`
  (`sdk/runner/lifecycle.go:111-129`).
- `xflow_supply_fetch_total{result="error"}` is not rising and
  `xflow_supply_not_ready` is at its pre-rotation baseline (§6).

**Do not start another rotation before this one's reseal has reported
`failed=0`.** Two keys cover exactly one rotation (§3.3); starting a second
one while the first is still in its window stacks a third key against a
keyring that only holds two.

### 3.6 What this procedure does not give you

- **A rehearsal.** This procedure has never been exercised end to end in a
  real environment in this repository. Rehearse it against a disposable copy
  of the database before relying on it in production.
- **A backup for the KEK itself.** Nothing in this repository provides one;
  step (a) is a deployment-side responsibility, not a code path.
- **Owner and deadline.** D6 in
  [RELEASE-GATES.md](../design/RELEASE-GATES.md) §6.1 records that a code path
  now exists for this axis and remains `OPEN` for the owner/deadline decision
  and for whether a rehearsal is required before this runbook counts as
  approved procedure.

### 3.7 The one operational rule for this axis

**Never lose the KEK, and never change it outside this procedure.**
Concretely: back it up out of band, inject it from secret management rather
than a shell history or an image (`service/crypto/masterkey/masterkey.go`),
keep every key file at `0600` (enforced: a group/world-readable file is a
startup error), and remember that a *bad* key value is fatal in every mode
rather than quietly ignored, precisely because silently ignoring it "writes
plaintext to disk while everything appears to work".

Also know which direction a missing key fails in:

- `--mode=production`: the server **refuses to start**, because
  `RequireSupplyEncryptionAtRest` is unmet
  (`service/apiserver/production.go`; remediation text
  `XFLOW_MASTER_KEY or --master-key-file`, `cmd/server/main.go`).
- `--mode=dev`: it starts and warns on stderr —
  `WARNING no XFLOW_MASTER_KEY: supply content is stored in plaintext`
  (`cmd/server/main.go`) — and supply content is then stored
  unencrypted (`store/sqlstore/supply.go:15-22`, `:123-129`).

## 4. Axis 3 — runner credentials (partially possible)

### 4.1 Static bearer token

**Where it comes from and which source wins.** Precedence is
**YAML < environment < flag**, and it is decided in code, not by convention:
`resolveRunnerConfig` loads the YAML file first
(`sdk/runner/config.go:798-802`), overlays environment variables
(`:804`, `:302-421`), and then applies a flag **only when that flag was
explicitly set** (`:808+`; for the token, `:873-875`). The token's three sources
are the YAML `token` key, `XFLOW_RUNNER_TOKEN`
(`sdk/runner/config.go:364-366`), and `--token` (`sdk/runner/run.go:158`).
The token may also be resolved from a stored identity or enrollment *after* this
resolution, which is why the "token required" check runs later
(`sdk/runner/profile.go:180-189`).

**How the server validates it.** The runner sends it as
`Authorization: Bearer <token>` (`service/protocol/client.go:29-33`, `:95`, `:143`)
and the server reads it off the header in preference to the body
(`service/control/server.go:219`, `:418-423`). Validation is by the **runner auth
policy** — `--auth-policy` pointing at `runners.yaml`
(`cmd/server/main.go:200`, `:781`; `service/control/auth.go:147-156`) — or by the
issued-identity store for enrolled runners. The policy store hashes the expected
token at load time (`sha256`, `service/control/auth.go:257`) and hashes the
presented token per request (`:321-324`), comparing in constant time (`:332`);
it re-validates on **every** request, so revoking a policy takes effect without a
runner-side reconnect (`:298-300`).

> **Correction worth knowing:** the runner token is **not** validated by
> `BearerTokenAuth`. That type (`service/apiserver/auth.go:34-68`) guards the
> workflow/management HTTP API (`--api-auth-token`,
> `cmd/server/main.go:105`, `:570`) and `sdk/xflow`'s `WithServerWorkflowAuth`.
> An operator debugging a runner 401 should be reading the policy store and the
> `auth_denied` log line (§4.4), not the workflow authenticator.

**Is there a dual-token acceptance window? Yes, at the policy layer.**
`authenticate` iterates **all** entries and returns the first that matches
(`service/control/auth.go:316-356`), so two entries carrying different tokens with
the same `id_prefix` are both accepted. Each entry needs `id_prefix` (`:239-241`)
plus at least one of `token`, `token_file`, `mtls_subject` (`:260-262`), and a
token may come from a `0600` file instead of inline (`:129-138`, `:268-282`).

**But the window used to cost two restarts — it no longer does.** Hot reload
is implemented: `FilePolicyStore.Reload` (`service/control/auth.go`) re-parses
`runners.yaml` and atomically swaps in the new snapshot — but only after every
check (policy file permission, YAML parse, `resolveConfig` including the
`token_file` 0600 check) has succeeded; a failed reload leaves the previous
snapshot serving unchanged. `cmd/server` wires this to `SIGHUP`: sending the
signal to the server process re-reads the file named by `--auth-policy` (only
when that flag is set) without restarting anything (`cmd/server/reload.go`,
`cmd/server/main.go`). A changed `runners.yaml` therefore takes effect on the
next `SIGHUP`, not at the next server start.

**When to rotate.** A leaked or suspected token; a person leaving; a periodic
credential policy; or tidying a token that was injected by hand.

**Procedure (no auth-failure window, no restart).**

1. Generate a new high-entropy token. Do not put it in the policy file's history,
   a ticket, an image, or source — the repository's own deployment example says
   to inject it from secret management
   (`docs/references/deployment-examples.md:136-149`).
2. Add a **second** entry to `runners.yaml`: same `id_prefix`, same
   `allowed_node_types` / `allowed_namespaces`, a new `token`, and the same
   `tls_subject` if the runner is mTLS-bound.
3. Send `SIGHUP` to the server process (`kill -HUP <server pid>`; the dual-token
   window is opening: both tokens now authenticate). Check the log for the
   reload outcome (§4.4) before proceeding — a reload that failed (malformed
   YAML, a `token_file` that regressed to 0644, a moved/missing file) logs the
   error and leaves the **previous** policy in force, so the new entry is not
   live yet and runners on the old token are unaffected.
4. Move runners onto the new token one at a time
   (`XFLOW_RUNNER_TOKEN`, or `--token`), restarting each. Verify each one
   authenticates before moving on (§4.4).
5. Remove the old entry from `runners.yaml` and send `SIGHUP` again (window
   closing). Re-check the log line for the same reload-outcome confirmation.

**Multi-replica deployments:** `SIGHUP` is process-local. Every server replica
that talks to this `runners.yaml` (or its own copy of it) must be signalled
individually — there is no fan-out. A replica that is not signalled keeps
serving its previous snapshot until it is, which for step 3 above means it
still accepts only the old token, and for step 5 means it still accepts the
old token too. Confirm the reload log line on **each** replica, not just one,
before treating a step as complete fleet-wide.

**Fast path (accepts an auth-failure window).** Replace the token in place and do
step 3 (`SIGHUP`) and step 4 together; every runner that has not yet restarted
fails authentication until it does. Acceptable only for a runner you can afford
to have down.

**What breaks if you get it wrong.** Mismatch presents as a plain authentication
failure: `unknown auth token` / `missing auth token`
(`service/control/auth.go:20-21`), the runner's registration is rejected, and its
reconnect loop retries. By design you cannot tell expired, revoked, and wrong
from the caller's side (see `docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:118-121`);
the distinction lives in the server-side log.

**Rollback.** Put the old entry back in `runners.yaml` and send `SIGHUP` —
no restart needed for the policy to take effect again. Because revocation is
immediate, there is no "old token still valid somewhere" hazard to chase.

**Precondition that must be true before any of this matters:** if `--auth-policy`
is empty and no enrollment is configured, the runner protocol runs with
`DisabledAuthenticator` — **all runners allowed** — and the only signal is a
warning (`service/control/controlplane.go:316-328`). Rotating a token that is not
being checked is theatre; confirm the posture first (`--mode=production` makes
`RequireRunnerAuth` a hard error, `:322-323`; remediation hint
`--auth-policy or --enroll`, `cmd/server/main.go:875`).

### 4.2 Enrollment-issued identity

- A runner enrolled with a registration code receives a runner ID and token
  (`sdk/runner/run.go:164`, `sdk/runner/enroll.go:97-113`).
- That token **cannot be rotated in place**. Renewal extends the expiry only:
  `renewIdentity` authenticates the existing `(runnerID, token)` pair and writes
  a new `ExpiresAt` (`service/control/enroll.go:134-173`). The reason is stated
  where the revoke operation is defined: renewal "deliberately does not rotate
  the token (design R9), because a thief can renew too"
  (`service/apiserver/authz.go:113-117`).
- **Rotation therefore means revoke + re-enroll:**
  `POST /v1/management/runners/{id}/revoke-identity`
  (`service/apiserver/paths.go:43-49`; route and scope at
  `service/apiserver/module_management.go:208`), which requires the
  `management.runner.revoke_identity` scope (`service/apiserver/authz.go:121`).
  Revoking a *registration code* is a different operation and does not narrow
  identities already issued from it (`service/apiserver/authz.go:115-117`); the
  runner then needs a fresh code to enroll again.
- Expiry is opt-in: `--runner-identity-ttl` defaults to `0`, which means an
  enrolled identity never expires (`cmd/server/main.go:203-204`;
  `service/apiserver/apiserver.go:56-59`), and the repository's own notes call
  that default deliberate rather than an omission
  (`docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:354-364`).
- The renewal loop only starts when there is an issued identity with an expiry;
  a static `--token` deployment never starts it
  (`sdk/runner/run.go:342-352`, `:606-623`;
  `docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:398-404`).

### 4.3 mTLS / TLS material

- Runner side: `--tls-server-ca`, `--tls-client-cert`, `--tls-client-key`,
  and `--allow-plaintext` (`sdk/runner/run.go:159-161`, `:165`). A plaintext
  connection is refused unless TLS material or an `https://` server URL is
  configured, or `--allow-plaintext` is passed explicitly
  (`sdk/runner/config.go:648-687`).
- Server side: `--tls-cert`, `--tls-key`, `--tls-client-ca`
  (`cmd/server/main.go:211-212`; client CA at `:213`); the client-CA file enables mutual TLS with
  `tls.RequireAndVerifyClientCert` (`service/apiserver/run.go:77-88`).
- **Both sides used to read their material once, at startup — the server side
  no longer does.** Runner: `buildRunnerTLSConfig`
  (`sdk/xflow/runner.go:994-1023`) still reads once; there is no runner-side
  hot reload or automatic certificate renewal, and every runner-side
  certificate or CA change is still a restart. Server: `loadTLS`
  (`service/apiserver/run.go`) now builds a `*TLSReloader`
  (`service/apiserver/tls_reload.go`) instead of loading a static
  `tls.Config`; `tls.Config.GetCertificate` and `GetConfigForClient` read
  through it on every handshake, and `cmd/server` wires a `SIGHUP` handler to
  its `Reload` method. Sending `SIGHUP` to the server process re-reads
  `--tls-cert` / `--tls-key` / `--tls-client-ca` from the same paths and swaps
  them in **only if every file parses**; a bad file leaves the previous
  certificate and CA pool serving unchanged and logs the error. This closes
  the server-side half of the gap this runbook used to describe — the
  runner-side half (`buildRunnerTLSConfig`) is unchanged.
- **A dual-CA window still exists, and it no longer needs a restart on the
  server side.** Both sides build their pool with `AppendCertsFromPEM`, which
  appends *every* certificate in the file (runner `sdk/xflow/runner.go:1004-1008`;
  server `service/apiserver/tls_reload.go`). So: put old and new CA in the
  bundle → `SIGHUP` the server (no restart) → roll runner leaves (still a
  restart per runner) → shrink the bundle → `SIGHUP` again. If the new leaf
  keeps the same issuing CA, only the leaf changes and the CA step is
  unnecessary.
- **mTLS pins the subject separately from the token.** A policy entry may carry
  `mtls_subject` (`service/control/auth.go:129-138`), matched case-insensitively
  against the peer CN (`:325`, `:336-340`). A new certificate with a *different*
  CN needs that policy entry updated in `runners.yaml` and reloaded — the
  `SIGHUP` from §4.1 and the `SIGHUP` for TLS material in this section are
  independent registrations on the same signal (`cmd/server/reload.go`), so one
  `kill -HUP` picks up both if both files changed; the second-entry trick from
  §4.1 only helps if you also accept the new CN.
- **Procedure (server side, no restart; runner side, still a restart).**
  1. Stage the new certificate/key (and CA bundle, for a CA rollover) at the
     paths named by `--tls-cert` / `--tls-key` / `--tls-client-ca`.
  2. `kill -HUP <server pid>`.
  3. Check the log for the TLS reload outcome (§4.4) before treating it as
     done — a bad file (mismatched key, corrupt PEM, a moved/missing path)
     logs the error and the server keeps serving the **previous** certificate
     and CA pool; nothing is torn down.
  4. Roll runner leaves that need to trust a new CA or present a new client
     cert, restarting each (`buildRunnerTLSConfig` reads once, at process
     start — see above).
  5. Once every runner has converged, shrink a dual-CA bundle if one was used,
     and `SIGHUP` again.
- **Multi-replica deployments:** exactly like §4.1, `SIGHUP` is process-local.
  Signal every server replica that terminates TLS, and confirm the reload log
  line on each one — a replica that is not signalled keeps serving its
  previous certificate/CA pool.
- **What a TLS mistake looks like:** authentication simply fails, and for the
  supply path the failure is worse than a 401 — if the supply fetch cannot be
  made, the readiness gate declines forever and the runner "never hosts its
  triggers at all" (`sdk/xflow/runner.go:1028-1033`; `service/runner/doc.go:92-98`).
  A *rejected reload* is a different, milder failure: the server logs the
  error and keeps its previous material, so a mistyped path or a bad file
  produces a log line, not an outage.
- **Rollback.** Server side: restore the previous certificate/CA files at the
  same paths and `SIGHUP` again — no restart needed either way. Runner side:
  restore the previous certificate/CA files and restart. Keep the old
  material until the fleet has converged.

### 4.4 Verification for axis 3

- Metric: `xflow_runner_auth_decisions_total`, labelled `result` and `auth_mode`
  (`observability/metrics/control.go:21`, `:103`; help text
  `observability/metrics/metrics.go:421`). A rotation in progress shows as
  `result="deny"` until the last runner has moved.
- Log (server-side TLS reload): `xflow-server: tls material reload succeeded`
  or `xflow-server: tls material reload failed, keeping previous
  configuration: <err>` (`cmd/server/reload.go`). The same reloader also logs
  `xflow-server: auth policy reload succeeded` / `... reload failed, keeping
  previous configuration: <err>` for the policy axis (§4.1), and
  `xflow-server: auth policy entry name=... id_prefix=... token=<fingerprint>`
  per entry on a successful policy reload — never the token itself or file
  contents.
- Log: `auth_denied` with `op`, `runner`, `token`, `cn`, `err`
  (`service/control/core.go:181-200`). The `token` field is a fingerprint, not
  the token — `TokenFingerprint` is the first 8 hex characters of SHA-256
  (`service/control/auth.go:305-314`) — so it is safe to correlate on and safe to
  paste into a ticket.

## 5. Blast radius per axis

| Axis | If rotated correctly | If mishandled | If the material is destroyed | Reversible? |
|---|---|---|---|---|
| 1. Transport key | In-flight content re-encrypts; runners converge on their next heartbeat; nothing at rest is affected | A replica that encrypts with a superseded key declines supply fetches and hosts no triggers while heartbeating healthily (`service/control/supply_key_rotation.go:76-83`); back-to-back rotations can evict a keyring slot (bounded by the 1-minute floor) | Redis key lost → runners re-register; self-healing by design (`service/control/supply_encryption.go:64-68`) | Yes, by rotating again |
| 2. At-rest KEK | Every stored row stays readable through the window (§3.5) and ends up sealed under the new key once `xflow supply reseal` reports `failed=0` | Skipping the previous-key step: every stored supply row becomes undecryptable → 500 on every supply fetch → every activation declined on every runner → no trigger hosted fleet-wide (§3.4) | If the previous key value is lost before reseal finishes: same outcome as mishandled, with no way back for the rows still sealed under it | **Only if the previous key was loaded during the window.** Without it, restoring the old KEK is the only recovery, and rows already resealed under the new key are then the unreadable ones |
| 3a. Static runner token | Server side: `SIGHUP` reload, no restart; fleet-wide credential replacement as runners restart onto the new token | Affected runners fail auth (`unknown auth token`) and retry; blast radius is exactly the runners you did not update yet. A **rejected reload** (malformed YAML, 0644 `token_file`, missing file) is not a blast radius at all — the server logs the error and keeps the previous policy in force, so no runner is affected until the file is fixed and reloaded | Token lost = rotate again (new entries, new `SIGHUP`, new per-runner restarts) | Yes |
| 3b. Enrollment-issued identity | Revoke + re-enroll per runner | Expired/revoked/unknown are indistinguishable to the caller by design; a whole fleet can fail auth at once if a TTL is introduced without planning (`docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:129-132`) | Revoked identity needs a new registration code | Yes, but not in place |
| 3c. mTLS material | Server side: bundle-based CA rollover via `SIGHUP`, no restart; runner side: leaf-by-leaf, still a restart | Runner cannot reach the control plane; on the supply path that means it never hosts a trigger (`sdk/xflow/runner.go:1028-1033`). A **rejected server-side reload** (bad cert/key/CA file) is not a blast radius — the server keeps serving its previous certificate/CA pool and logs the error | Re-issue from the CA; if the CA private key is lost, the whole mTLS fleet must be re-issued | Yes, server side with `SIGHUP`, runner side with restarts |

The asymmetry is the point: axis 1 and axis 3 mistakes are **recoverable
credential problems**. Axis 2 is recoverable only when the offline-window
procedure in §3.5 is followed in full; skipping the previous-key step turns it
into the same unrecoverable data-access problem it used to always be.

## 6. Observability: what actually exists

There is **no metric for key rotation.** Do not alert on one you invented.
There **is** now a metric for a decryption failure, but only on the control
plane's hint path, and it is selected by a label rather than by its name — see
the table below and the note after it. What exists and is relevant:

| Metric | Type | What it tells you here |
|---|---|---|
| `xflow_supply_fetch_total{name,result}` | counter | A fetch attempt per supply, `result` = `ok` or `error` (`observability/metrics/supply.go:182-186`). A decrypt failure on either axis surfaces as `error`. |
| `xflow_supply_not_ready{workflow,supply}` | gauge | An activation is currently being declined for a missing required supply (`observability/metrics/supply.go:196-204`). This is the fleet-impact signal for an axis-2 mistake. |
| `xflow_supply_unavailable_serving{name}` | gauge | Traffic is being served with content that was never successfully fetched, for `require_ready:false` supplies (`observability/metrics/supply.go:214-220`). |
| `xflow_supply_hint_read_errors_total{namespace,cause}` | counter | Supply reads that failed while the control plane computed heartbeat hints, `cause` = `decrypt` or `other` (the `SupplyHintMetrics` method in `observability/metrics/control.go`). The expected "not written yet" case is excluded. The only direct decryption-failure signal in the tree. |
| `xflow_runner_auth_decisions_total{result,auth_mode}` | counter | Runner authorization outcomes (`observability/metrics/control.go:21`, `:103`). Axis-3 verification signal. |
| `xflow_runner_up` | gauge | Runner liveness by heartbeat TTL (`observability/metrics/metrics.go:428`) — distinguishes "runner gone" from "runner healthy but hosting nothing". |

Metric names are confirmed against the help-text map in
`observability/metrics/metrics.go` (`:440-445` for the supply family, `:421` for
runner auth). On the **negative** side: a search for any
rotation/decryption/key-management metric *by name* still returns nothing —
`grep -rniE "xflow_[a-z_]*(rotat|decrypt|kek|masterkey|key_rot)" --include=*.go .`
— but that grep is no longer sufficient evidence for the claim it used to
support. A decryption failure is now reported, and its name
(`xflow_supply_hint_read_errors_total`) matches none of those stems because the
distinction lives in the `cause` **label**. Search labels too, or the negative
will read as "nothing reports decryption failures" while something does.

**Log signals (exact text to grep for):**

| Where | Message text | Notes |
|---|---|---|
| server | `supply transport key rotated` / `adopted rotated supply transport key` | Carries `key_id` (`service/control/supply_key_rotation.go:129-131`, `:141-143`). Axis-1 verification. |
| server | `supply key rotation failed`, `supply key rotation slot check failed`, `supply key refresh failed`, `supply key rotation slot unavailable at startup` | `service/control/supply_key_rotation.go:91`, `:113`, `:122`, `:137`. Never contain the key itself (`:119-122`). |
| server | `auth_denied` | `service/control/core.go:196-197`; fields `op`, `runner`, `token` (fingerprint), `cn`, `err`. Axis-3 diagnosis. |
| runner | `supply fetch failed` | `service/runner/supply_gate.go:179`. Attributes are `supply_node`, `resource`, `require_ready` (`:270-279`) — **the error text is not in the line**. |
| runner | `supply hint: fetch failed` | `service/runner/supply_gate.go:343`, same attributes. |
| runner | `supply key rotation decode failed` | `service/runner/runner.go:719`. |

**Literal error strings, and where they do and do not surface.** The
discriminators exist in the code —
`ErrUnknownKey` = `supplyenc: no key matches kid` and `ErrDecryptFailed` =
`supplyenc: decryption failed`
(`service/crypto/supplyenc/supplyenc.go:129-135`) — and downstream they appear as
`supply fetch: decrypt: ...` (`service/runner/supply_client.go:111-116`),
`supplyenc: open stored content: ...`
(`service/crypto/supplyenc/atrest.go:49-58`) and
`get supply "<ns>"/"<name>": decrypt: ...`
(`store/sqlstore/supply.go:46-54`). Be aware of where they do **not** appear on
the fetch path: the server answers `GET /v1/supplies/{name}` with a generic
`internal_error` body and does not log the reason
(`service/apiserver/module_supply.go:104-107`), and the runner's WARN line omits
the error text (`service/runner/supply_gate.go:270-279`). So a decrypt failure is
**not** greppable end to end; its fingerprint is the metric pair plus the runner's
`/readyz` reason string:

- `xflow_supply_fetch_total{result="error"}` rising with
  `xflow_supply_not_ready > 0`, and
- runner `/readyz` reporting not ready with reason
  `no supply has been fetched yet` (`sdk/runner/lifecycle.go:111-129`).

Treat "fetch errors + not-ready + a recent KEK or credential change" as the
signature, and check the *most recent configuration change* first.

### Alerts

- **Supply fetch failures** — `rate(xflow_supply_fetch_total{result="error"}[5m]) > 0`.
  Content could not be delivered; activations that require it are declining.
  *Does not cover:* which failure it is. A decrypt mismatch (axis 2), a
  transport-key mismatch (axis 1) and an ordinary 5xx all produce this same
  series, and the HTTP 500 body is generic
  (`service/apiserver/module_supply.go:104-107`). Correlate with a recent key or
  credential change, and with the runner-side log line `supply fetch failed`.
- **Activations declined for a missing required supply** —
  `max by (namespace, workflow) (xflow_supply_not_ready) > 0` for 5m.
  A standing decline means traffic is piling up in Kafka rather than being
  processed, because the gate declines before the subscription starts
  (`service/runner/trigger_activation_handler.go:184-192`). This is the alert
  that catches an at-rest-KEK mistake; it pages someone rather than waiting for
  retention to run out.
- **Serving with content that was never fetched** —
  `xflow_supply_unavailable_serving > 0`. Only possible for
  `require_ready:false` supplies (`service/runner/supply_gate.go:184-187`); it
  means the process is running on content it never obtained.
- **Runner authorization denials** —
  `rate(xflow_runner_auth_decisions_total{result="deny"}[5m]) > 0`, with
  `auth_mode` on the series (the `AuthMetrics` methods, `observability/metrics/control.go:103`). Expected in
  bursts during a token rotation (§4.1); sustained after a rotation is complete
  means a runner was missed.
- **Decryption failures on the hint path** —
  `rate(xflow_supply_hint_read_errors_total{cause="decrypt"}[5m]) > 0`.
  This is the closest thing to a direct "the at-rest key is wrong on this
  replica" alert, and the one to reach for after a rotation step. The series
  carries `namespace`, so a single tenant failing points at data rather than at
  a replica's keyring.
  *Scope, stated precisely:* it covers reads the **control plane** makes while
  computing heartbeat hints, not reads on the HTTP GET path — that path still
  answers a generic 500 with no metric
  (`service/apiserver/module_supply.go:104-107`). A zero rate is therefore not
  proof that every row is readable, only that the hint path saw nothing wrong.
  `cause="other"` covers corruption and database faults, which have no sentinel
  to be told apart from each other.
- **No rotation alert is possible.** Nothing emits a *rotation* metric — no
  counter or gauge reports that a key changed — so "the key rotated" cannot be
  alerted on from what this repository exports. Axis-1 rotation is observed in
  the server log (`supply transport key rotated`, `key_id`).
  *Updated:* decryption **failures** are no longer in this category. Since the
  hint path stopped swallowing read errors, they can be alerted on directly (see
  the bullet above); axis-2 damage on other paths is still visible only
  indirectly, through the two supply metrics above.

## 7. What this runbook does not cover

- **The D6 decision itself** — owner and deadline. `RELEASE-GATES.md` §6.1
  remains the place where that is recorded, and this document does not change it.
- **Runner replacement / drain**, the other §6.1 runbook (also previously
  listed as missing). Drain and resume are separate controls
  (`service/apiserver/paths.go:50-52`; `xflow_runner_control_transitions_total`,
  `observability/metrics/metrics.go:423`).
- **KMS integration.** There is none: the design records "无 KMS 集成，KEK 由部署方
  注入" (`docs/design/SUPPLY-NODE.md:626`).
- **Redis backup/restore for the transport key.** Covered by
  [maintenance-window-runbook.md](maintenance-window-runbook.md) §4. Worth
  knowing that a Redis restore can resurrect an older transport key; that is
  harmless, because runners converge on whatever the current fleet uses
  (`service/control/supply_encryption.go:18-25`).
- **The supply at-rest encryption story in general** — the storage contract in
  [STORAGE-CONTRACT.md](../design/STORAGE-CONTRACT.md) is about Redis as the
  system of record and does not mention supply content at all; supply rows live in
  MySQL (`store/sqlstore/supply.go`), and that contract has no
  encryption/rotation clause and no repair path for a changed key.
- **Deployment configuration for these credentials.** See
  [deployment-examples.md](deployment-examples.md) §2 (`runners.yaml`), §4
  (alert rules) and §5 (pre-flight checklist). The §5 checklist covers the
  *current* master key (`--master-key-file` / `XFLOW_MASTER_KEY`) but has no
  entry for the previous-key flag. Adding one (`--master-key-previous-file` /
  `XFLOW_MASTER_KEY_PREVIOUS`, rotation window only) is outside this runbook's
  scope; §3.5 is the rotation procedure itself, not a deployment checklist.
