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
| **2. Supply at-rest KEK** (`XFLOW_MASTER_KEY` / `--master-key-file`) | the `content` column of every stored supply row in MySQL | manual — and **there is no way to do it today** | **NO — NOT POSSIBLE.** Replacing the master key makes every previously stored supply row undecryptable, which stops every runner from hosting any trigger. It requires a code change (a previous-key configuration surface **and** a re-encryption path) plus an owner decision. Read §3 before touching this key. |
| **3. Runner credentials** (static bearer token, enrollment-issued identity, mTLS material) | a runner's authentication to the control plane | manual, per runner (or per fleet) | **Partly.** A static token and a client certificate can each be replaced, at the cost of restarts. An enrollment-issued token **cannot** be rotated in place — renewal extends its expiry and never changes the token. See §4. |

> **Operators: if you came here to rotate a key, find your axis first.** Axis 1
> and axis 3 have procedures. Axis 2 has a refusal, and following a
> "how to rotate the at-rest key" procedure — from anywhere, including a generic
> KMS key-rotation playbook written for a different system — destroys access to
> all stored supply content. There is no recovery without the old key.

Cross-cutting: axes 2 and 3 are **offline changes** (each needs at least a
process restart), so they couple to the maintenance window in
[maintenance-window-runbook.md](maintenance-window-runbook.md). Axis 1 needs no
window at all.

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

## 3. Axis 2 — supply at-rest KEK: **ROTATION IS NOT POSSIBLE TODAY**

### 3.1 The refusal

> **Do not rotate the at-rest KEK. There is no procedure that leaves the stored
> data readable, because none exists in the code.**
>
> The KEK (`XFLOW_MASTER_KEY` or `--master-key-file`) is, today, a
> **write-once, never-change** secret: it is exactly as load-bearing as the
> supply data itself. Treat the two with the same care, and treat "the KEK must
> never be lost" as an operational invariant of the deployment.
>
> Making rotation possible is **a code change plus an owner decision** (§3.5),
> not an operator procedure. This section therefore documents what is missing
> and what would have to be built first. It deliberately contains no
> step-by-step rotation to follow.

### 3.2 What it protects

- Every stored supply row's `content` column in MySQL, sealed on write and
  opened on read (`store/sqlstore/supply.go:123-129`, `:46-54`).
- It is derived, not stored: `supplyenc.NewAtRest(mk.Derive(supplyenc.SupplyContentInfo))`
  (`cmd/server/main.go:583-585`) from the single master key loaded at
  `cmd/server/main.go:581`. `Derive` is deterministic HKDF-SHA256 scoped by an
  info string, which is what makes previously stored ciphertext readable after a
  restart (`service/crypto/masterkey/masterkey.go:92-110`).
- The transport key is *not* this key, and the two have opposite lifetimes by
  design: "the transport key is short-lived and reissued at will, while this one
  must stay derivable for the lifetime of the stored data"
  (`service/crypto/supplyenc/atrest.go:12-15`).

### 3.3 Why rotation is impossible today — link by link

Each link below was verified against the code, and the two negative links were
proved by the searches named inline.

1. **One master key, from one value.** `masterkey.Load(os.Getenv("XFLOW_MASTER_KEY"), cfg.masterKeyFile)`
   returns a single `*Key` (`cmd/server/main.go:581`;
   `service/crypto/masterkey/masterkey.go:53-90`). `--master-key-file` is one
   path holding one value (`cmd/server/main.go:133-135`, `:214`).
2. **The at-rest encryptor is built with exactly one key.**
   `NewAtRest` does `NewKeyring(key)` with a single key
   (`service/crypto/supplyenc/atrest.go:21-26`). It exposes only `Seal` (`:28-31`)
   and `Open` (`:49-58`) — no way to add a second key, and no way to reach the
   keyring it holds.
3. **Nothing constructs a two-key at-rest keyring.** `NewKeyring` takes variadic
   keys (`service/crypto/supplyenc/supplyenc.go:144-153`) and the keyring holds
   up to current + previous (`:137-142`), but the only two-key construction in
   production code is the runner's **transport** keyring
   (`service/runner/runner.go:701-706`). The other callers are
   `service/crypto/supplyenc/atrest.go:25`
   (one key) and tests.
4. **Even a two-key keyring would not survive a second rotation.** `Rotate`
   installs `[]*Key{newKey, keys[0]}` (`service/crypto/supplyenc/supplyenc.go:155-165`,
   the assignment at `:163`): the oldest key is evicted, and a second rotation
   drops the key the oldest rows are sealed under.
5. **No configuration surface for a previous / old master key exists.**
   *Search:* `grep -rniE "previous.?master|old.?master|master.?key.?previous|prev.?kek|fallback.?key|secondary.?master|master.?key.?ring" --include=*.go .`
   → no matches. The only master-key inputs anywhere in the tree are
   `XFLOW_MASTER_KEY` and `--master-key-file`, each a single value
   (`cmd/server/main.go:581`, `:214`, `:882`;
   `service/crypto/masterkey/masterkey.go:45-47`).
6. **No re-encryption / re-wrap / migration path exists for stored supply rows.**
   *Search:* `grep -rni "reencrypt|re-encrypt|rewrap|re-wrap|rekey|re-key|migrat" --include=*.go service/crypto store cmd`
   → the only hits are the word "migration" in comments about unrelated schema
   `AutoMigrate` work and about a *damaged* envelope
   (`service/crypto/supplyenc/atrest.go:45`). Nothing iterates supply rows to
   re-seal them; `Seal` is called only from the write path
   (`store/sqlstore/supply.go:123-129`), and `cmd/xflow` ships only
   `dead-letter list|replay|reconcile` (`cmd/xflow/command.go:14-16`,
   `cmd/xflow/dead_letter.go:139-140`, `:187-188`,
   `cmd/xflow/dead_letter_reconcile.go:34-35`).
   Note the second consequence: because `PUT /v1/supplies/{name}` is sealed and
   the write path is in-process only
   (`service/apiserver/module_supply.go:60-65`;
   `service/apiserver/authz.go:127-130`), an operator cannot even re-put the
   content by hand through the API.
7. **The key ID is in the envelope and decryption is keyed on it.** The envelope
   carries `kid` (`service/crypto/supplyenc/supplyenc.go:96-101`) and `Decrypt`
   looks the key up by it, failing with `ErrUnknownKey` =
   `"supplyenc: no key matches kid"` (`:129-135`, `:207-210`).
8. **The info string is a one-way door, by intent.** `SupplyContentInfo` carries
   a version suffix "instead of ever being edited in place" because changing it
   "makes every previously stored row unreadable"
   (`service/crypto/supplyenc/atrest.go:7-10`), and the guard test states the
   same thing: it "re-keys the store and makes every supply row already written
   undecryptable, with no migration path"
   (`service/crypto/supplyenc/key_scope_and_sniff_test.go:39-44`).
9. **The design document already says rotation needs a full re-encryption.**
   `docs/design/SUPPLY-NODE.md` §10 (`:554-626`) lists the KEK's loss consequence
   as "需重发 + 重包 DEK" (`:560`) and notes that hardcoding a key would make
   rotation "需要发版加全量重新加密"; the same section records
   "**未做**：无 KMS 集成，KEK 由部署方注入" (`:626`).

**Conclusion: CONFIRMED — at-rest KEK rotation is not possible today.** Two
capabilities are missing, and neither is a configuration matter: a way to load a
*previous* key so stored rows remain decryptable, and a way to *re-encrypt*
stored rows so the previous key stops being needed.

### 3.4 What actually happens if you replace the KEK anyway

For an operator who does it regardless, this is the concrete chain. It is worth
reading before acting, because the symptom appears far from the cause and the
server does not tell you why.

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
   (`service/runner/trigger_activation_handler.go:184-192`). By `AtRest.Open`'s
   own reasoning, "the supply gate would then decline every activation, so no
   runner would host any trigger" (`service/crypto/supplyenc/atrest.go:33-48`).
5. Runner readiness never flips: `/readyz` reports not ready with reason
   `no supply has been fetched yet`, because only a successful fetch moves that
   state (`sdk/runner/lifecycle.go:95-102`, `:111-129`).
6. **Recovery is not possible from the new key.** The rows are still sealed
   under the old `kid`; putting the old key back is the only way to read them
   again (`service/crypto/supplyenc/supplyenc.go:207-210`). If the old key value
   is gone, the content is gone — and remember that supply rows are the input to
   the supply gate, so the workflows they feed do not run either. The design's
   own answer to KEK loss is to re-issue content and re-wrap, not to decrypt
   (`docs/design/SUPPLY-NODE.md` §10) — and re-issuing means an in-process
   `sdk/xflow.Server.UpdateSupply`, because the HTTP write verb is sealed
   (`service/apiserver/authz.go:127-130`).

One asymmetry is worth stating, because it makes the *upgrade* direction look
safe and the *rotation* direction fatal: `Open` passes through anything that
does not look like an envelope, which is what lets a deployment with
pre-encryption plaintext rows start encrypting without breaking them
(`service/crypto/supplyenc/atrest.go:33-52`;
`service/crypto/supplyenc/supplyenc.go:248-268`). Plaintext→encrypted is safe.
Encrypted→different-key is not.

### 3.5 What must happen before a KEK rotation is possible (owner decision)

This runbook does **not** assign an owner or a deadline; that is D6 in
[RELEASE-GATES.md](../design/RELEASE-GATES.md) §6.1 and it remains `OPEN`. What
follows is the shape of the work, described where it would go. Nothing here is
implemented, and none of it should be started without that decision.

1. **A configuration surface for the previous key.** Loading two keys is not a
   flag on `masterkey.Load`: it returns one `*Key`
   (`service/crypto/masterkey/masterkey.go:53-90`). A previous-key input would
   need a new API in `service/crypto/masterkey` (e.g. a pair-loading entry point)
   with the same validation obligations the existing loader already carries —
   reject a group/world-readable key file (`:63-65`), accept exactly 32 decoded
   bytes (`:84-86`), and never echo the value in an error (`:79-83`). The
   at-rest encryptor would then be built with a two-key keyring
   (`service/crypto/supplyenc/atrest.go:21-26`), the current key for `Seal` and
   both for `Open`. Note the constraint at
   `service/crypto/supplyenc/supplyenc.go:155-165`: a two-key keyring is enough
   for exactly **one** rotation, so the re-encryption step below is not optional
   even with the config surface in place.
2. **A re-encryption path for already-stored rows.** Something must read each
   supply row, decrypt it with the previous key, re-seal it with the current
   one, and write it back. It has to live where the row and the store are
   (`store/sqlstore/supply.go`) and be exposed as an operator entry point
   (`cmd/xflow`, which today has only the dead-letter commands). Design
   constraints from the existing code: `content_hash` is computed over the
   **plaintext** (`store/sqlstore/supply.go:116-121`) and is verified on every
   read (`:55-75`), so re-sealing must leave the plaintext and the hash
   unchanged; the job must be idempotent and resumable; and `PutSupply`'s
   `If-Match`/revision guard (`:81-108`) is the existing concurrency contract to
   respect.
3. **An offline operating window.** The cutover needs both keys accepted while
   the job runs and a restart to load the new one, which is exactly what
   [maintenance-window-runbook.md](maintenance-window-runbook.md) covers.
4. **A decision about the KEK's own backup.** Nothing in this repository
   provides one, and a KEK that exists only as one environment variable is a
   single point of failure for all stored supply content. This is a deployment
   decision, not a code change.
5. **A rehearsal.** No rotation on any axis has been exercised in a real
   environment here, and this axis is the one where a mistake is unrecoverable.

### 3.6 The one operational rule for this axis today

**Never lose the KEK, and never change it.** Concretely: back it up out of band,
inject it from secret management rather than a shell history or an image
(`service/crypto/masterkey/masterkey.go:3-8`), keep the key file at `0600`
(enforced: a group/world-readable file is a startup error, `:63-65`), and
remember that a *bad* key value is fatal in every mode rather than quietly
ignored, precisely because silently ignoring it "writes plaintext to disk while
everything appears to work" (`:49-52`, `:79-86`).

Also know which direction a missing key fails in:

- `--mode=production`: the server **refuses to start**, because
  `RequireSupplyEncryptionAtRest` is unmet
  (`service/apiserver/production.go:47-50`, `:102-104`, `:209`; remediation text
  `XFLOW_MASTER_KEY or --master-key-file`, `cmd/server/main.go:882`).
- `--mode=dev`: it starts and warns on stderr —
  `WARNING no XFLOW_MASTER_KEY: supply content is stored in plaintext`
  (`cmd/server/main.go:704-706`) — and supply content is then stored
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

**But the window costs two restarts.** Hot reload "is not implemented yet"
(`service/control/auth.go:147-150`): the file is read once in
`NewFilePolicyStore` (`:177-187`, called at `cmd/server/main.go:781`), `Reload`
(`:205-231`) has no production call site, and no SIGHUP handler exists anywhere in
the repository. A changed `runners.yaml` therefore takes effect only at server
start, i.e. inside a maintenance window
([maintenance-window-runbook.md](maintenance-window-runbook.md)).

**When to rotate.** A leaked or suspected token; a person leaving; a periodic
credential policy; or tidying a token that was injected by hand.

**Procedure (no auth-failure window, two maintenance windows).**

1. Generate a new high-entropy token. Do not put it in the policy file's history,
   a ticket, an image, or source — the repository's own deployment example says
   to inject it from secret management
   (`docs/references/deployment-examples.md:136-149`).
2. Add a **second** entry to `runners.yaml`: same `id_prefix`, same
   `allowed_node_types` / `allowed_namespaces`, a new `token`, and the same
   `tls_subject` if the runner is mTLS-bound.
3. Restart the server in a maintenance window (this is the dual-token window
   opening: both tokens now authenticate).
4. Move runners onto the new token one at a time
   (`XFLOW_RUNNER_TOKEN`, or `--token`), restarting each. Verify each one
   authenticates before moving on (§4.4).
5. Remove the old entry and restart the server again (window closing).

**Fast path (accepts an auth-failure window).** Replace the token in place and do
steps 3–4 together; every runner that has not yet restarted fails authentication
until it does. Acceptable only for a runner you can afford to have down.

**What breaks if you get it wrong.** Mismatch presents as a plain authentication
failure: `unknown auth token` / `missing auth token`
(`service/control/auth.go:20-21`), the runner's registration is rejected, and its
reconnect loop retries. By design you cannot tell expired, revoked, and wrong
from the caller's side (see `docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:118-121`);
the distinction lives in the server-side log.

**Rollback.** Put the old entry back and restart. Because revocation is
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
- **Both sides read their material once, at startup.** Runner:
  `buildRunnerTLSConfig` (`sdk/xflow/runner.go:994-1023`). Server: `loadTLS`
  (`service/apiserver/run.go:58-90`). There is no hot reload and no automatic
  certificate renewal anywhere. Every certificate or CA change is a restart.
- **A dual-CA window exists, and it comes from PEM-bundle semantics.** Both sides
  build their pool with `AppendCertsFromPEM`, which appends *every* certificate in
  the file (runner `sdk/xflow/runner.go:1004-1008`; server
  `service/apiserver/run.go:82-86`). So: put old and new CA in the bundle →
  restart the server → roll runner leaves → shrink the bundle → restart again.
  If the new leaf keeps the same issuing CA, only the leaf changes and the CA
  step is unnecessary.
- **mTLS pins the subject separately from the token.** A policy entry may carry
  `mtls_subject` (`service/control/auth.go:129-138`), matched case-insensitively
  against the peer CN (`:325`, `:336-340`). A new certificate with a *different*
  CN needs that policy entry updated in the same restart — the second-entry trick
  from §4.1 only helps if you also accept the new CN.
- **What a TLS mistake looks like:** authentication simply fails, and for the
  supply path the failure is worse than a 401 — if the supply fetch cannot be
  made, the readiness gate declines forever and the runner "never hosts its
  triggers at all" (`sdk/xflow/runner.go:1028-1033`; `service/runner/doc.go:92-98`).
- **Rollback.** Restore the previous certificate/CA files and restart. Keep the
  old material until the fleet has converged.

### 4.4 Verification for axis 3

- Metric: `xflow_runner_auth_decisions_total`, labelled `result` and `auth_mode`
  (`observability/metrics/control.go:21`, `:63`; help text
  `observability/metrics/metrics.go:421`). A rotation in progress shows as
  `result="deny"` until the last runner has moved.
- Log: `auth_denied` with `op`, `runner`, `token`, `cn`, `err`
  (`service/control/core.go:181-200`). The `token` field is a fingerprint, not
  the token — `TokenFingerprint` is the first 8 hex characters of SHA-256
  (`service/control/auth.go:305-314`) — so it is safe to correlate on and safe to
  paste into a ticket.

## 5. Blast radius per axis

| Axis | If rotated correctly | If mishandled | If the material is destroyed | Reversible? |
|---|---|---|---|---|
| 1. Transport key | In-flight content re-encrypts; runners converge on their next heartbeat; nothing at rest is affected | A replica that encrypts with a superseded key declines supply fetches and hosts no triggers while heartbeating healthily (`service/control/supply_key_rotation.go:76-83`); back-to-back rotations can evict a keyring slot (bounded by the 1-minute floor) | Redis key lost → runners re-register; self-healing by design (`service/control/supply_encryption.go:64-68`) | Yes, by rotating again |
| 2. At-rest KEK | **Not achievable today** (§3) | Every stored supply row becomes undecryptable → 500 on every supply fetch → every activation declined on every runner → no trigger hosted fleet-wide (§3.4) | Same outcome, with no way back: no re-encryption path, and the rows' only recovery is the old key or re-issuing the content | **No.** Only restoring the old KEK; rows written under the new key then become the unreadable ones |
| 3a. Static runner token | Fleet-wide credential replacement; runners restart onto the new token | Affected runners fail auth (`unknown auth token`) and retry; blast radius is exactly the runners you did not update yet | Token lost = rotate again (new entries, new restarts) | Yes |
| 3b. Enrollment-issued identity | Revoke + re-enroll per runner | Expired/revoked/unknown are indistinguishable to the caller by design; a whole fleet can fail auth at once if a TTL is introduced without planning (`docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md:129-132`) | Revoked identity needs a new registration code | Yes, but not in place |
| 3c. mTLS material | Bundle-based CA rollover, leaf-by-leaf | Runner cannot reach the control plane; on the supply path that means it never hosts a trigger (`sdk/xflow/runner.go:1028-1033`) | Re-issue from the CA; if the CA private key is lost, the whole mTLS fleet must be re-issued | Yes, with restarts |

The asymmetry is the point: axis 1 and axis 3 mistakes are **recoverable
credential problems**, while an axis 2 mistake is an **unrecoverable data-access
problem**.

## 6. Observability: what actually exists

There is **no metric for key rotation and no metric for a decryption failure.**
Do not alert on one you invented. What exists and is relevant:

| Metric | Type | What it tells you here |
|---|---|---|
| `xflow_supply_fetch_total{name,result}` | counter | A fetch attempt per supply, `result` = `ok` or `error` (`observability/metrics/supply.go:182-186`). A decrypt failure on either axis surfaces as `error`. |
| `xflow_supply_not_ready{workflow,supply}` | gauge | An activation is currently being declined for a missing required supply (`observability/metrics/supply.go:196-204`). This is the fleet-impact signal for an axis-2 mistake. |
| `xflow_supply_unavailable_serving{name}` | gauge | Traffic is being served with content that was never successfully fetched, for `require_ready:false` supplies (`observability/metrics/supply.go:214-220`). |
| `xflow_runner_auth_decisions_total{result,auth_mode}` | counter | Runner authorization outcomes (`observability/metrics/control.go:21`, `:63`). Axis-3 verification signal. |
| `xflow_runner_up` | gauge | Runner liveness by heartbeat TTL (`observability/metrics/metrics.go:428`) — distinguishes "runner gone" from "runner healthy but hosting nothing". |

Metric names are confirmed against the help-text map in
`observability/metrics/metrics.go` (`:440-444` for the supply family, `:421` for
runner auth). The **negative** was checked directly: the complete supply metric
name list is five names plus the wasm ones
(`observability/metrics/supply.go:12-16`), and a search for any
rotation/decryption/key-management metric —
`grep -rniE "xflow_[a-z_]*(rotat|decrypt|kek|masterkey|key_rot)" --include=*.go .`
— returns **no matches**.

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
  `auth_mode` on the series (`observability/metrics/control.go:63`). Expected in
  bursts during a token rotation (§4.1); sustained after a rotation is complete
  means a runner was missed.
- **No rotation alert is possible.** Nothing emits a rotation or
  decryption-failure metric, so an alert on "the key rotated" or "decryption
  failed" cannot be written from what this repository exports. Axis-1 rotation is
  observed in the server log (`supply transport key rotated`, `key_id`), and axis-2
  damage is observed indirectly through the two supply metrics above.

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
  (alert rules) and §5 (pre-flight checklist). Note that the §5 checklist does
  **not** mention the master key — the KEK has no checklist entry today, which is
  part of what §3.5 has to fix.
