# Browser / CDP Runner isolation policy — what is actually enforced

**Subject:** `xflow.browser.cdp` (`node/internal/action/browser.go`), the host
policy it shares with `xflow.http` (`node/internal/action/host_policy.go`),
and the runner-side configuration that installs them (`sdk/runner/`).

**Why this document exists.** The pre-UI remediation plan (TODO §0.2 **F**)
requires that the high-risk Browser/CDP Runner isolation policy be *verified*,
not asserted. This is a code-reading audit. Every claim carries a `file:line`
citation against `main` at the moment of writing; every gap is named as a gap
rather than smoothed into a claim of safety.

**Citation drift.** `node/internal/action/browser.go` was being edited by
another workstream while this audit was written (a diagnostics change in
`browserHostDeniedClassified` / `browserTimeoutClassified`), so line numbers
move by single digits. Each citation therefore names the *symbol* as well as the
line, and §6 gives grep-able anchors. Re-derive lines from the anchors, not from
this document's numbers.

**Verdict in one line:** the network *destination* control is real,
fail-closed, and covered by tests. Isolation in the sense the plan intends — a
separate runner pool, a minimum-egress network boundary, an approval and audit
trail — **is not implemented in this repository at all**; it is a deployment
property that nothing here enforces or checks.

---

## 1. What the node is, and what it deliberately is not

`CDPNode` implements `xflow.browser.cdp`. Its own doc comment states the
intent: *"It is deliberately a credential harvesting primitive, not a
general-purpose browser automation API"* (`browser.go:79-84`, above
`type CDPNode struct`).

It is not a bundled browser. It is a **remote** CDP client: it connects to an
external Chrome DevTools endpoint supplied as the `debugging_url` parameter
(`browser.go:104`, `Required: true`) and drives it over the wire. That fact
determines most of the isolation story, because it means the *browser* — its
sandbox, its container, its profile directory — is somebody else's process, and
this repository cannot make any statement about it.

**Consequence, stated plainly: nothing in this repository isolates the browser
process.** If an operator points `debugging_url` at a Chrome running on the
runner host with a default profile, that browser's filesystem, cookies and local
storage are exactly as exposed as that Chrome is configured to be. Nothing here
changes, requires, or checks that configuration.

## 2. What IS enforced

### 2.1 Endpoint allowlist — fail-closed, and the gate before any bytes move

The CDP endpoint is gated by a hostname allowlist in
`BrowserCDPConfig.Endpoints` (`browser.go:133`). The default is `nil`,
and the doc comment is explicit that **an empty list denies every CDP
endpoint** (`browser.go:130-131`); `BrowserCDPConfigDefaults` returns `nil`
(`browser.go:139-147`).

Enforcement happens in `resolveBrowserWebSocketEndpoint`, before the discovery
request is sent and before chromedp's transport exists: `browser.go:900-902`
rejects a non-allowlisted host with
`browserHostDeniedError{source: endpoints}`.

The doc comment on that function explains why the check sits there: the
discovery request *"happens before chromedp's CDP transport can enforce any of
the node's navigation guards"* (`browser.go:895-898`).

Pattern semantics are deliberately narrow, and are the strongest part of this
policy (`host_policy.go:27-71`):

- exact host (`example.com`) matches only itself (`host_policy.go:62-63`);
- `.example.com` is a **suffix** that includes the apex (`host_policy.go:64-65`);
- `*.example.com` is a **wildcard** requiring at least one extra label, so it
  excludes the apex (`host_policy.go:66-67`);
- IP literals are rejected inside suffix/wildcard patterns
  (`host_policy.go:42-44`), so `*.0.0.1` cannot smuggle a loopback address.

Suffix and wildcard being distinct rather than collapsed is the difference
between `.internal` and `*.internal` matching `internal` itself. That is a real
hardening choice, not a stylistic one.

### 2.2 Navigation and request policy — shared with `xflow.http`

Every destination the node navigates to, seeds a cookie for, or harvests from is
checked against the same process-wide `HostPolicy`:

- `entry_url` — `browser.go:423`
- `plan.fallback_url` — `browser.go:427`
- `harvest.request_host` — `browser.go:431`
- each `seed_cookies[].url` — `browser.go:436`
- each `harvest.cookie_urls[]` entry — `browser.go:442`

`applyBrowserHostPolicy` (`browser.go:877-886`) **denies when the policy is
nil** (`browser.go:878-880`). For the browser action, "no policy configured" is
a denial, not a bypass. This is the single most important line in the file for
isolation purposes; contrast it with `xflow.http`, where a nil policy is
documented as no filtering at all (`http.go:59-68`).

Intercepted requests — a page's sub-resources — are where the runtime control
lives, on the node's own worker rather than on the CDP transport:

- `fetch.Enable()` arms interception (`browser.go:1461`);
- every `EventRequestPaused` is validated (`browser.go:1421`);
- on failure the request is failed with `ErrorReasonBlockedByClient` and the
  attempt is aborted via `reportFatal` (`browser.go:1422-1425`); the same
  treatment applies when the header collector refuses to capture
  (`browser.go:1426-1430`).

So a page pulling in `<img src="http://169.254.169.254/...">` or a
non-allowlisted third-party script is not merely un-harvested — it does not
load, and the whole node fails.

Failing the node rather than silently skipping the request is the conservative
choice and reads as deliberate: a silently-skipped sub-resource would change the
page under observation and could yield a *wrong credential* rather than an
error.

`validateInterceptedBrowserURL` (`browser.go:1574-1605`) closes the scheme set to
`http`/`https`/`ws`/`wss` (`:1583`) plus `about`/`data` (`:1591`), and
special-cases `blob:` by re-parsing and policy-checking the inner URL
(`:1593-1601`). Everything else — `file:`, `ftp:`, custom schemes — is denied
(`:1602-1603`).

### 2.3 Runner-side wiring: browser-capable runners fail closed

`installRunnerHTTPHostPolicy` (`sdk/runner/run.go:377-395`):

- if no allow/deny lists are configured **and** the runner cannot receive
  `xflow.browser.cdp` work, it installs nothing — historical behavior preserved
  for ordinary runners (`run.go:380-382`);
- if the runner **is** browser-capable and no list is configured, it installs a
  policy that denies *every* host: *"host %q is denied: Browser CDP navigation
  has no HTTP host allowlist"* (`run.go:388-392`).

The closure is installed at `run.go:237` and removed on shutdown
(`run.go:394`). The capability check is `runnerConfigHasCapability`
(`run.go:397-408`), keyed on the literal `"xflow.browser.cdp"` (`run.go:367`).

Configuration surface (`sdk/runner/config.go`):

- `browser_cdp.endpoints` / `max_contexts` / `queue_timeout` /
  `connect_timeout` (`config.go:53-58`);
- `http_host_policy.allow` / `.deny` (`config.go:59-62`);
- env equivalents `XFLOW_BROWSER_CDP_ENDPOINTS` (`config.go:324`) and
  `XFLOW_HTTP_HOST_POLICY_ALLOW` / `_DENY` (`config.go:343-347`);
- an invalid pattern list fails config load (`config.go:544-551`), so a
  malformed policy is a startup failure rather than a silently permissive one.

Note the asymmetry in §2.3's first bullet: a *non*-browser runner keeps the
permissive historical default for `xflow.http`. Only the browser capability
turns "unconfigured" into "deny everything".

### 2.4 Resource bounding

Per-process concurrency is bounded by a semaphore of exactly `MaxContexts` slots
(`browser.go:162`), acquired under `QueueTimeout` (`acquireBrowserSlot`,
`browser.go:1099`) and released on the way out (`browser.go:1052`). Defaults:
one context, 5s queue wait, 5s connect (`browser.go:30-34`).

Total attempt time is bounded by `context.WithTimeout` (`browser.go:1043`),
per-attempt time at `browser.go:1055`, retries capped (`maxBrowserRetries = 100`,
`browser.go:45`). The intercepted-request queue is bounded at 4096 with overflow
treated as fatal rather than dropped (`browser.go:64-77`, `browser.go:1450-1455`).

Captured headers and cookies are bounded and sanitized: CR/LF/NUL rejection and
a per-value length ceiling (`browser.go:1307-1309`), a 128-header ceiling and a
1 MiB credential ceiling (`browser.go:1313-1316`, `browser.go:46-48`), and
cookie name/value validation (`browser.go:1611`).

**These are robustness bounds, not a security boundary.** They limit how much a
hostile page can make the *runner* do; they do not limit what a hostile page or
workflow can *reach* — which is §3's subject.

## 3. What a malicious workflow can still reach

Each gap is stated with the code that would have to change.

**G1 — The CDP endpoint's own reach is unbounded.** The only control on
`debugging_url` is a hostname allowlist (`browser.go:900-902`). A workflow author
who can name an allowlisted Chrome endpoint gets a full CDP session on it:
`Page.navigate` anywhere the *browser* can reach, `Runtime.evaluate` arbitrary
JS in any page of that browser context, `Network.getCookies`. The node's own
guards constrain only the requests flowing through *this node's* interception;
they are not a sandbox around CDP.

**G2 — No egress control, and no proxy support at all.**
`grep -ri proxy node/internal/action/browser.go sdk/runner/` returns nothing.
There is no proxy configuration, no egress firewall, no DNS pinning, and no
`NetworkPolicy`/iptables template in `test/env/` or `docs/`. `.github/workflows/`
carries no such control either. The plan's "最小 egress" requirement is
**entirely unaddressed by this repository** — a deployment requirement with no
artifact, no template, and no test.

**G3 — DNS rebinding is not defended against.** The policy matches the
*hostname* (`host_policy.go:104-115`) and the name is resolved only when the
request is actually issued. There is no resolve-then-pin step and no re-check of
the resolved address against the policy. A hostname that is allowlisted but
resolves to a private address — or that changes its answer between the policy
check and the connection — is not caught by anything here.

**G4 — Interception is bypassed by anything the page does outside the
intercepted request path.** WebSocket frames, `WebRTC` data channels, and
`EventSource` bodies are not hosts to be allowlisted after the initial
connection, and the initial WebSocket connect is checked only as a host
(`browser.go:1583`). G3 applies to every hop, and there is no hop-count bound
specific to redirects.

**G5 — Credential handling has no minimum exclusion and no namespace
boundary.** `buildBrowserOutput` returns harvested headers and cookies to the
workflow (`browser.go:1144-1182`); `Cookie` is stripped from captured *headers*
(`:1147-1149`) and re-added from the filtered cookie set (`:1157-1158`).
`harvest.cookie_name_exclude_prefixes` (`browser.go:599-604`) and
`harvest.header_exclude` (`:605-613`) let a workflow *narrow* what it takes;
nothing forces a minimum exclusion, and there is no per-namespace credential
boundary. A workflow that can run `xflow.browser.cdp` receives whatever the
browser's session holds for the target host. Whether that is acceptable is a
product decision, and it is currently an undeclared one.

**G6 — Seed cookies are written into a shared browser process.**
`network.SetCookie(...)` (`browser.go:1467-1470`) writes caller-supplied
credentials into the remote browser. The URL is policy-checked
(`browser.go:436-443`) and the context is per-attempt
(`chromedp.WithNewBrowserContext()`, `browser.go:1384`, torn down at
`:1386-1392`), but the *browser process* is shared with whatever else uses that
endpoint. Isolation between two workflows pointed at the same Chrome is
`MaxContexts` and nothing else.

**G7 — No approval, audit, or namespace scoping of the capability.** Nothing in
`sdk/runner/` or `node/internal/action/` requires an approval before a browser
node runs, records an audit event for a browser execution, or scopes the
capability to a namespace. Who may submit a workflow containing
`xflow.browser.cdp` is decided by whether the caller can reach a runner whose
`--capabilities` include it (`run.go:397-408`) — a deployment choice, not an
authorization decision made in this repository.

**G8 — Nothing separates a browser runner pool from any other pool.**
`MaxContexts` (`browser.go:132-137`) is the only knob read as "how many browsers
at once". There is no pool label, no scheduler constraint, no runner-pool
selector. The plan's "Browser/CDP 使用独立 Runner pool、独立 namespace/资源配额"
is a deployment template that does not exist in this repository.

## 4. Newly observed defect: a permanent condition classified as transient

Found during this audit. It is a correctness bug, not a hole, but a reader of the
security surface should know about it.

`realBrowserCDPAttempt` captures the **unfiltered** cookie slice from the browser
(`browser.go:1479-1482`, and again at `:1494-1497` after a fallback navigation)
and returns it in `browserAttemptResult{Cookies: cookies}` (`browser.go:1505`).
The filter is applied to that same data again later, in `buildBrowserOutput`
(`browser.go:1153`).

`filterBrowserCookies` deliberately does not overwrite its input; on overflow it
discards everything and returns `nil, "", &browserResourceError{}`
(`browser.go:1607-1627`, overflow at `:1626`). The overflow triggers are:

- `len(filtered) >= maxBrowserListItems` (256) — the *retained* count, so
  excluded cookies do not count toward it (`browser.go:1625`);
- or the retained `Name`+`Value` byte total exceeding
  `maxBrowserCredentialBytes` (1 MiB) (`browser.go:1626`).

`buildBrowserOutput` maps that error to `browserResourceClassified()`
(`browser.go:1154-1156`), and `browserResourceClassified` is:

```go
return types.NewTransientError("browser.resource_exhausted", "browser resources are exhausted")
```

(`browser.go:1273-1275`)

**The defect:** the node has already *succeeded* at harvesting. The browser holds
the credentials; the failure is purely the node's own output-size policy, which
is a deterministic function of the page and the declared `harvest` rules. It is
reported as a **transient** error whose message says the *browser's resources*
are exhausted. Two harms follow:

1. An operator reading `browser.resource_exhausted` will retry or scale the
   browser pool. Retrying cannot help — the same page overflows the same ceiling
   every time — and scaling the pool cannot help either, because the ceiling is
   per-node output, not browser capacity.
2. A transient classification invites the engine to re-run the node, which will
   re-drive the browser and re-harvest the same overflowing credential set. The
   in-node loop does return immediately on `exhausted`
   (`browser.go:1090-1093`), so the retry cost is paid at whatever layer
   consumes the transient class, not inside this node.

The honest fix is a classification decision — is an oversized credential set a
caller error (`browser.invalid_params`, permanent) or a genuinely distinct
permanent condition with its own code? — plus a test pinning it, because a
classification change with no test is exactly the kind of thing that silently
regresses. **Not fixed here:** choosing a production error taxonomy is not a
documentation task, and guessing would replace a known mislabel with an
unverified one.

## 5. Gap register

| # | Gap | Where it would have to be fixed | Status |
|---|---|---|---|
| G1 | CDP session reach is unbounded once the endpoint is allowlisted | deployment (dedicated, disposable browser) | **not in repo** |
| G2 | No egress control / proxy / network policy anywhere | deployment template + `test/env/` | **not in repo** — no artifact at all |
| G3 | DNS rebinding not defended (hostname-level check only) | `host_policy.go` (resolve-and-pin) | open |
| G4 | Non-HTTP channels (`ws` frames, WebRTC) carry no host semantics after connect | deployment + browser policy | open |
| G5 | No minimum credential exclusion, no per-namespace credential boundary | `browser.go` harvest rules | open — undeclared product decision |
| G6 | Seed cookies enter a shared browser process | deployment (per-workflow browser) | **not in repo** |
| G7 | No approval / audit / namespace scoping of the capability | runner admission + authz | **not in repo** |
| G8 | No separate browser runner pool / quota | deployment + scheduler constraint | **not in repo** |
| D1 | A permanent condition reported as `browser.resource_exhausted` (transient) | `browser.go:1154-1156`, `:1273-1275` | open, see §4 |

**Owner for G2, G6, G7, G8: deployment / platform.** None of them can be closed
by a change in this repository alone, which is exactly why the plan's exit
predicate ("高风险 Runner 的隔离由部署模板/策略和测试证据证明") is **not**
satisfiable from here and must be reported as an unclosed gap rather than
ticked off.

## 6. How to re-verify this document

Every anchor below is a literal string in the current tree; the line numbers in
this document were derived from them and will drift.

```sh
# Endpoint allowlist is fail-closed by default
grep -n "An empty list denies" node/internal/action/browser.go

# Nil policy denies (the load-bearing line)
grep -n -A 9 "func applyBrowserHostPolicy" node/internal/action/browser.go

# Intercepted sub-resources are failed, not skipped
grep -n -B 2 -A 6 "ErrorReasonBlockedByClient" node/internal/action/browser.go

# A permanent overflow is classified transient
grep -n -A 2 "func browserResourceClassified" node/internal/action/browser.go

# Browser-capable runners fail closed with no allowlist
grep -n -A 8 "func installRunnerHTTPHostPolicy" sdk/runner/run.go

# There is no egress/proxy control to find (expect no output)
grep -ri proxy node/internal/action/browser.go sdk/runner/ --include='*.go'
```

Tests that pin the enforced behaviour:
`node/internal/action/browser_test.go`,
`node/internal/action/host_policy_test.go`,
`sdk/runner/browser_cdp_config_test.go`,
`sdk/runner/http_host_policy_test.go`,
`sdk/xflow/runner_browser_cdp_wiring_test.go`.
