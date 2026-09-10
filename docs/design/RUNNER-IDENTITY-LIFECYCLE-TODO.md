# RUNNER 身份生命周期 遗留项

`feat/registration-code-ceiling-and-identity-lifecycle`(9 tasks，已 fast-forward 合入 main）
交付后未做的事，以及本次刻意不做的事。按「不做会怎样」排序，不按工作量。

**本文件自带背景，因为设计文档不在库里。** 本次的 spec 位于
`docs/specs/2026-09-08-registration-code-ceiling-and-identity-lifecycle-design.md`，
该路径被 `.gitignore` 忽略且从未跟踪——工作区一旦清理，其中的裁定即永久丢失。下面
「已知且接受的代价」一节是那份 spec §7 的完整转录，不是摘要。

本次交付的两件事：

- **H1 收口**——注册码的 scope 天花板。创建注册码时，请求的 namespace 集合被 principal
  自己的授权夹住；四个端点全部收口。落点 `service/apiserver/module_management.go` 的
  `resolveRequestedNamespaces`。
- **签发身份的生命周期**——`IssuedIdentity` 新增 `expires_at` / `revoked_at` 两列，
  认证时落闸（`service/control`），新增平台侧吊销端点与 runner 自助续期端点，
  runner 侧在到期前自动续期（`cmd/runner`）。

---

## 仓库状态：main 上有 10 个未走本计划 review 循环的提交

**这不是遗留项，是一笔需要知情的账。** 合入 main 的 25 个提交里，只有 15 个走过本计划的
逐任务 review 循环（14 个任务提交 + 1 个收尾的配置样例修正）。另外 10 个是在本计划执行
期间由本会话之外的写入者提交到同一分支上的，它们**随本次 fast-forward 一起进了 main**，
且从未经过本计划的任何一轮 review。

**批次一（六个，`fbc71f8`..`0dd26cb`）**——与本计划无关，落点 `Makefile`、
`backend/providers/local/*`、`execution/subgraph/*`、`node/internal/code/script/*`、
`test/integration/*`。这批是可分离的：把它们从 main 上摘掉不会破坏本计划的任何功能，
代价是一次 rebase 会重写 12 个 SHA。

**批次二（四个，`d246d584`..`c7d6bc3`）**——全部是本计划 T8 明文门禁的下游：

| 提交 | 落点 | 做了什么 |
|---|---|---|
| `d246d584` | `test/integration/` 三个 e2e | 给三个真进程 harness 补 `--allow-plaintext` |
| `870eac1` | `test/integration/runner_plaintext_gate_e2e_test.go` | **新增**一条真二进制的门禁 e2e |
| `cb1da42` | `Makefile` | 让 `make run-runner` 过门禁 |
| `c7d6bc3` | `cmd/runner/config.go`、`main_test.go` | 让配置样例过门禁 |

**这批不可与 T8 分离**：去掉它们而保留 T8，main 的 integration 套件会红。

其中 `870eac1` 值得单独看一眼——它补的正是本计划自己漏掉的那类覆盖。T8 加了明文门禁却
一次也没在真二进制上驱动过它，而 `make test` **不覆盖 integration 套件**
（`//go:build integration`），所以本计划的九轮 review 全程看不见这个洞。

两批都已随合并进入 main。若要拆分，只有批次一是安全的。

**一个值得记下的连锁**：批次二的 `c7d6bc3` 当初让配置样例过明文门禁的办法，是给样例加一句
生效的 `allow_plaintext: true`。收尾时这句被改掉了（`3eee28f`）——样例现在靠 `https` scheme
过门禁，`security:` 块整体注释掉，`url` 是 `https://REPLACE-ME:8080`。理由见下方
「配置样例为何不能直接跑」。

---

## 待办

按「不做会怎样」排序。**六条现已全部结案**——§4 只结了「过期」那一半（可复用是刻意保留的，
理由见下方「已知且接受的代价」中同名条目），§6 结的方式是加守卫而不是消除披露（那条披露
本身是 `list_global` 这个 scope 的用途，不是缺陷）。

六条都保留了原文。留着不是为了存档：修法只说做了什么，原文说的是那个洞为什么值得堵——
读不到后者的人，很容易在下一次重构里把守卫当成冗余而删掉。

### 1. ~~吊销端点没有 per-namespace 隔离（跨租户 DoS 通道）~~ 已修

> **已修**，见 `fix(enroll): confine identity revocation to the owning namespace`。
> 本条保留原文，因为它是 `owner_namespace` 列与
> `management.runner.revoke_identity_global` scope 存在的**唯一理由**：读不到这段的人，
> 很容易把这两样当成冗余而删掉，那等于把下面这条通道原样打开。

`POST /v1/management/runners/{id}/revoke-identity`（常量
`apiserver.PathManagementRunnerRevokeIdentity`）是平台级操作。持有
`management.runner.revoke_identity` scope 的调用方可以吊销**任意** namespace 下任意 runner
的身份；服务端不校验目标 runner 的签发身份属于哪个租户。

**不做会怎样：** 任何持有该 scope 的 principal——哪怕本意只服务一个租户——事实上拥有跨租户
吊销能力。若该 scope 将来被误发给租户级 principal，即是一条完整的横向 DoS 通道：一个租户
可以把另一个租户的整支 runner 机群踢下线。

**实际修法：** `IssuedIdentity` 新增 `OwnerNamespace`，在 enroll 时从注册码**快照**过来
（不是按 `CodeID` JOIN——身份一经签发就独立于注册码，删码不能抹掉归属）。吊销时
`ownerScopeFor` 把 principal 投影成 `store.OwnerScope`，store 层用它做谓词。三处要点：

1. **越权报 not-found 而非 forbidden**——否则该端点成了别的租户 runner id 的存在性预言机。
2. **`owner_namespace = ""` 是「归属未知」不是「归属所有人」**——该列存在之前写入的历史行只有
   `*_global` 能吊销。读成「公开」就是 fail-open。
3. **`RowsAffected == 0` 的 COUNT 补偿必须带上和 UPDATE 同一个 scope 谓词**——无谓词的 COUNT
   会数到别的租户的行，`n > 0` 于是返回 nil，等于告诉调用方「那个 runner 存在且已吊销」，
   而它根本无权看见该 runner。这是整个修复里最容易在重构中丢掉的一处。

上述三点各有定向变异用例把守（`service/control`、`service/apiserver`、`store/storecontract`
三处），逐处变异 7/7 同轴命中。

### 2. ~~身份过期/被吊销导致的认证失败，在服务端自己的日志里也无法与「token 不对」区分~~ 已修

> **已修**，见 `fix(control): name the lifecycle rejection reason in the server's own auth log`。
> 过期与吊销两条路径现在各 wrap 一层原因说明（`unknown auth token: issued identity expired`
> / `unknown auth token: issued identity was revoked`），照存储查询失败那条的先例。
> 本条保留原文，因为它是这个区分为什么存在的**唯一记录**：读不到这段的人，很容易在某次
> 「统一错误消息」的重构里把三条字符串重新拍平，那等于把下面这个缺口原样打开。
>
> 同时要注意的边界（原文「注意不要顺手改错地方」一段说的正是这个）：R7 的对外不可区分
> **不由这三个字符串保证**，而由 `Core.authDeny` 在硬拒绝时返回 `ErrUnauthenticated`
> 常量保证——两个 transport 的映射（`server.go` 的 `writeRunnerError`、`grpc_server.go`
> 的 `runnerStatus`）都只渲染那个常量。所以「日志里可区分」与「对外不可区分」互不冲突。
> 落点测试：`TestAuthenticateRejectsExpiredAndRevokedIdenticallyToCallers`（三条
> `errors.Is` 相同、三次 `authDeny` 返回同一常量、三条日志字符串两两不同）与
> `TestAuthDeniedLogNamesTheLifecycleReason`（走真实 `Core.heartbeat`，断言
> `auth_denied` 的 `err` 字段点名原因）。

`service/control/issued_identity.go` 的 `authenticate` 在四条拒绝路径上返回的 error：

| 拒绝原因 | `authDeny` 记进 `auth_denied` 的 `err` |
|---|---|
| 存储查询失败 | `unknown auth token: issued-identity lookup failed: <真实错误>` |
| **身份已被吊销** | `unknown auth token` |
| **身份已过期** | `unknown auth token` |
| token 不匹配 | `unknown auth token` |

查询失败那条被刻意 wrap 过（wrap 保留 `errors.Is(err, ErrAuthUnknownToken)` 可匹配性，
只在日志里多带信息），理由写在代码注释里：不 wrap 的话，身份存储一次故障会让每个 runner
的心跳都记成「unknown auth token」，把运维指向凭证而故障其实在数据库。

**过期与吊销这两条没有享受同样的待遇**，它们是裸 `return RunnerPolicy{}, ErrAuthUnknownToken`。

**不做会怎样：** 一支机群的身份因 TTL 到期而集体失效时，服务端日志是一片
`auth_denied ... err="unknown auth token"`——与「有人拿着错 token 来敲门」逐字相同。运维会
去查凭证分发，而真正该看的是 `--runner-identity-ttl` 和续期循环有没有在跑。**这个缺口正是
本次 TTL 特性的直接下游**：没有 TTL 就没有「身份过期」这种事，有了 TTL 才需要能看见它。

**注意不要顺手改错地方：** 对外响应不可区分是刻意的（R7，见下方「已知且接受的代价」中
「只能续自己」一条的括注）。可改的只有服务端内部日志，改法照查询失败那条已有的先例——
wrap 一层，`ErrAuthUnknownToken` 仍是唯一 `errors.Is` 可匹配的身份，`authDeny` 返回的
`ErrUnauthenticated` 常量一个字节都不变。

### 3. ~~`runServer` 零测试覆盖（本计划扩大了它）~~ 已修

> **已修，分两轮。第一轮**见 `refactor(server): extract buildServerOptions from runServer` /
> `test(server): guard the --runner-identity-ttl wiring hop`。本条保留原文，因为它记录了
> 缺口的完整形状，而第一轮只补上了其中一跳——读不到原文的人会以为「有测试了」就等于
> 「`runServer` 被测过了」，那不是第一轮做的事。
>
> **缺口的实际形状是两头有覆盖、中间没有：** `TestParseServerConfigRunnerIdentityTTLFlag`
> （`cmd/server/main_test.go`）早就证明了 `--runner-identity-ttl` 落到
> `cfg.runnerIdentityTTL`；`sdk/xflow/server_enroll_test.go` 的
> `TestNewServerReachesIdentityTTLEndToEnd` 证明了 `WithServerIdentityTTL` 一直传到
> `EnrollResponse.ExpiresAt`。没人守的是中间那一跳——`cfg.runnerIdentityTTL` 到
> `WithServerIdentityTTL(...)` 这次调用本身，把 TTL 错接到另一个同类型字段（例如
> `cfg.runnerMetricsInterval`）编译照样通过，全仓库测试照样全绿。
>
> **修法：** 把 `cmd/server/main.go` 里原来在 `runServer` 内联的选项装配块（原 545–612
> 行，从 `serverOpts := []xflowsdk.ServerOption{` 到 `cfg.management` 分支结束）原样提成
> `buildServerOptions(cfg serverConfig, deps serverDeps) []xflowsdk.ServerOption`，`deps`
> 装那块代码闭包捕获的、由 `runServer` 更早处构造好的依赖（logger、metrics、tracer、
> workflow/principal 认证器、audit sink、artifact store、runner 认证器、注册码/签发身份
> 两个 store、`singleToken`/`durableAudit` 两个布尔、`supplyAtRest`）。纯搬运，零行为变化，
> `cfg.management` 分支里原有的两句 `log.Println` 原样带过去。新测试
> `cmd/server/server_options_test.go` 从真实 argv 出发——`parseServerConfig` 走
> `-mode dev -memory -enroll -runner-identity-ttl 24h`——喂给 `buildServerOptions`，再把
> 拿到的 options 交给真正的 `xflowsdk.NewServer`（纯内存，不绑端口），对 enroll 端点发一个
> HTTP POST，断言 `EnrollResponse.ExpiresAt` 落在期望窗口内；配套的反向用例去掉
> `-runner-identity-ttl` 后断言 `ExpiresAt` 为空，防止把 TTL 硬编码成 24h 也能通过主用例。
> 三处定向变异（改接别的字段 / 删掉整行 / 硬编码 24h）逐一验证过会让对应用例变红。
>
> **`runServer` 本身仍然没有被任何测试整体调用过**——第一轮只守住了选项装配这一段
> （`buildServerOptions`）。`runServer` 剩下的部分（logger/tracer/认证器/store 的构造、
> production 门禁触发前的分支选择、`NewServer` 调用之后的 HTTP/gRPC 启动与优雅关闭）
> 仍然只有编译期保证。下一次有人往 `runServer` 里加接线，这条覆盖不会自动跟上。
>
> **第二轮把这一段也补上了**，见 `refactor(server): root runServer's signal context in a caller ctx`
> 与 `test(server): add the first tests that call runServer itself`。
>
> 挡路的从来不是「测试难写」，而是两处具体的不可驱动：`runServer` 自己造
> `signal.NotifyContext(context.Background(), ...)`，外部没有任何办法取消它；而
> `srv.Run(ctx)` 会真绑两个端口并阻塞。修法是把 ctx 提成参数、让 signal context 挂在
> 它下面。`main` 传 `context.Background()`——永不取消，所以生产的关闭仍然只由
> SIGINT/SIGTERM 驱动，行为一个字节没变；测试传一个可取消的 ctx，就能驱动同一条优雅
> 关闭路径，而不必给测试进程发真信号。
>
> 两条用例（`cmd/server/run_server_test.go`）都从真实 argv 出发，各走 `xflowsdk.NewServer`
> 的一条出口：
>
> - **起得来、关得掉**——`-mode dev -memory -management` 真绑 loopback 端口，轮询
>   `/readyz` 直到 `"ready":true`（不是睡一个猜的间隔；`/readyz` 是 apiserver 唯一对外的
>   就绪信号，且只住在 management 模块里，`-management` 因此是必需的），然后 cancel，
>   断言返回值**恰好是 nil**。不是「nil 或 `context.Canceled`」：`main` 对非 nil 会
>   `log.Fatal`，把取消本身当错误返回就等于让每次正常 SIGTERM 都以失败退出。
> - **production 门禁**——`-mode production` 缺 `--auth-tokens-file` 与 `--mysql-dsn`，
>   断言错误里点名这两个 flag。apiserver 自己的 `ProductionGateError` 只说「production
>   posture not met」、一个 flag 名都不提，所以断言 flag 名正是在证明 `runServer` 仍然把
>   这个错误路由过 `explainProductionGate`——拿到裸错误的运维知道缺什么，但不知道该敲什么。
>
> 端口一律由内核分配（先绑 `:0` 读出地址再放掉，交给服务端），不硬编码——`apiserver.Run`
> 把配置地址直接交给 `ListenAndServe`、从不公布实际拿到的地址，所以只能这样先占后放。
> 代价是一个固有的 TOCTOU 竞态，用三次换地址重试兜住，而不是让它偶发变红。
>
> 五处定向变异逐一验证过。其中「把 `explainProductionGate(err)` 换成 `return err`」只让
> 新的门禁用例变红、既有的 `TestExplainProductionGateNamesFlags` 保持绿——这个**唯一性**
> 才是这条覆盖确实补了新洞、而不是重复既有覆盖的证据。
>
> **仍然没被覆盖的两处，不要读成「`cmd/server` 已经测全了」：** `main()` 自己不可从测试
> 驱动（它 `log.Fatal`），以及 `runServer` 里所有非 dev/内存的分支——mysql store、OTLP
> tracer、文件认证器这些只在生产配置下才构造的路径，仍然只有编译期保证。

`--runner-identity-ttl` flag 一路穿过 `runServer` 接到 Core 字段，而 `runServer` 本身在此
之前就没有任何测试覆盖，本次也没有为它新增。

**不做会怎样：** flag 到 Core 字段这一段接线只有编译期保证——类型对得上就算通过。如果接线
接错了（例如把 TTL 错接到另一个无关字段），没有任何测试会红。

这个缺口先于本计划存在，但本计划扩大了它的表面积：现在有一个安全相关的值走这条无守卫的路。

### 4. ~~注册码本身仍没有过期机制~~ 已修（过期部分）

> **已修（过期部分）**，见 `feat(enroll): give registration codes a lifetime`。本条保留原文，
> 因为它记录的是**两个放大器**（可复用 + 无过期），而本次只拆掉了后一个——读不到原文的人
> 会以为「注册码有过期了」就等于「泄漏的注册码不再是无限放大器」，那不是本次做的事。
> 可复用仍然成立，见下方「已知且接受的代价」中「注册码可复用」一条。
>
> 落点：`RegistrationCode.ExpiresAt` + `IsExpired(now)`（零值＝永不过期，所以升级不会追溯
> 作废任何已发出的码）；过期判定放在 `ResolveByPlaintext` 内、**常量时间比对之后**，与
> `Revoked` 同一处——两处生命周期检查分家的话，后来的读者找到一处就不会再去找另一处。
> 两个 store 实现由 `store/storecontract` 的四条共享用例夹住，其中一条钉死「既吊销又过期
> 报 `ErrRegistrationCodeRevoked`」：对外这两种拒绝不可区分（`Core.Enroll` 统一收敛成
> `ErrEnrollRejected`，不给存在性预言机），但审计 `reason` 里必须可区分，那是运维唯一
> 能看见的记录。
>
> 时限来源是 `--registration-code-ttl`，它既是默认值**也是天花板**：create 请求只能往短里
> 收，要更长（含显式 `expires_in_seconds: 0`＝永不过期）一律 400 而不是静默夹到天花板——
> 被夹的调用方会以为自己拿到了请求的时限。天花板同样约束 `registration_code.create_global`
> 持有者，所以放宽它是一次**主机上的运维动作**（改 flag、进程可见），不是一个能被授予的
> scope。接线链每一跳都有测试守住（flag → `WithServerRegistrationCodeTTL` →
> `apiserver.Config` → `managementModule`），因为末两跳是构造后字段注入，断掉的话所有
> handler 测试仍然全绿。
>
> 顺带修正了一处 **OpenAPI 与实现不符**：spec 原先把注册码描述成 one-time-use，而实现从来
> 是无限复用。现在如实写明可复用，并指向 `expires_in_seconds` 与吊销作为限制爆炸半径的手段。

注册码只有 `Revoked`，没有 `ExpiresAt`。本次刻意不引入（本次的过期机制加在**签发身份**上，
不在注册码上）。

**不做会怎样：** 一枚签发出去的注册码永久有效直到有人手动吊销。它同时还是**可复用**的
（enroll 成功后不消费、不标记，这是既有设计），所以一枚泄漏的注册码可以被无限次用来
注册新 runner，且没有时间上限。

需另立条目。

### 5. ~~`decideIdentityRenewal` 与真实接线之间隔着一个未被驱动的分支~~ 已修

> **已修**，见 `test(runner): drive runRunner's identity-renewal wiring branch`。
> 本条保留原文，因为它是 `startIdentityRenewal` seam 为什么存在的**唯一记录**：读不到这段的
> 人，很容易把这个变量当成一层无意义的间接而内联掉，那等于把下面这条分支重新变回不可观测。

`cmd/runner/renew_test.go` 只驱动这个纯函数本身、断言它返回的四个值，没有驱动 `runRunner`
全程去观察续期 goroutine 是否真的按判定结果被启动或不被启动。

判定与启动之间的实际距离（`cmd/runner/run.go:294-297`）：

```go
if rc, start, warnMsg, warnErr := decideIdentityRenewal(cfg, store); warnErr != nil {
    slog.Warn(warnMsg, "runner_id", cfg.runnerID, "error", warnErr)
} else if start {
    go runIdentityRenewal(runCtx, rc, cfg.runnerID, cfg.token, slog.Default())
}
```

**不做会怎样：** 若将来有人在这个 `if/else if` 里再加一个条件（例如误加一个提前 return，
或者把 `else if start` 写成别的判据），`decideIdentityRenewal` 自身的单元测试仍然全绿——
它测的是纯函数的返回值，不是这个分支有没有照返回值行事。

**本次记档不修的理由：** 修它需要一条驱动 `runRunner` 全程（起真实 goroutine、断言其存在
或不存在）的测试，成本远超这几行代码本身的风险。列在待办而非「已接受的代价」里，是因为
一旦这个分支开始生长，成本收益比会翻转。这个判断后来翻转了：成本比估计的低得多，落地只是
一个包级 seam（`var startIdentityRenewal = runIdentityRenewal`，照 `newRunnerService` 的既有
先例）加三条驱动 `runRunner` 全程的用例（已签发身份+https、无已签发身份、已签发身份+明文被拒
三种分支结果各一条），没有触碰 `decideIdentityRenewal` 本身或 `runRunner` 的其它结构。三条
用例都用变异验证过：把 `else if start` 反转、在分支前插一个提前 return、把
`go startIdentityRenewal(...)` 整行删掉，三种变异都能让对应用例变红，随后各自还原并重新
确认全绿。

### 6. ~~若将来放宽注册码 list 的归属，须重新评估投影~~ 已加守卫

> **已加守卫**，见 `test(apiserver): pin what a global registration-code list discloses`。
> 本条保留原文，因为它是那条守卫为什么存在的**唯一记录**。
>
> **先纠正原文的一处事实：那个「将来」不是将来。** `management.registration_code.list_global`
> 这个 scope（常量 `apiserver.ScopeRegistrationCodeListGlobal`）今天就在代码里，持有它的
> 调用方现在就能列出全部租户的注册码，连同每条的 `allowed_namespaces`。原文说的
> 「本次收口后这个问题自然消失」只对**不持**该 scope 的调用方成立。
>
> **这不是「已修」，因为它不该被修。** 那个 scope 的全部目的就是一个跨租户的平台视图；
> 把披露收窄掉等于把 scope 变成一个没有用途的空壳。变的只是它从「没人守」变成「一改就红，
> 且红里写着为什么」。
>
> 新用例种两个不同租户的码（`namespaceA`，以及 `allowed_namespaces` 为
> `namespaceB-secret-scope` 的 `namespaceB`），用 `list_global` 列出，断言两条都在**且**
> 外租户那个 scope 名以明文出现在响应体里。它与既有的
> `TestRegistrationCodeReadsAreNamespaceScoped` 构成一对：非 global 看不见外租户，
> global 全看得见。两条一起才说清楚投影的形状，单看任何一条都会读成另一种设计。
>
> **两个方向的改动都会撞红：** 把 `list_global` 收窄成只返回自己拥有的码，或者从
> `newRegistrationCodeView` 里删掉 `AllowedNamespaces` 字段。前者破坏这个 scope 的用途，
> 后者悄悄改变平台视图能看见的东西。失败消息会把人指回本节。

`registrationCodeView` 的列表投影历史上会披露其它租户的 scope。本次收口后这个问题自然消失，
但该消失依赖于「list 只返回自己拥有的注册码」这条前提。

**不做会怎样：** 若将来为了运营视图放宽 list 归属（例如允许平台角色列出全部注册码），
这条披露会原样回来，而当前没有任何测试守住它。

---

## 已知且接受的代价（不打算改）

以下是本计划实施期间产出的裁定，每条附代价。与上面「待办」不矛盾：一条缺口可以同时出现在
两处——待办说明何时它不再可接受，这里说明现状为何可接受。

### `--runner-identity-ttl` 默认 0 = 永不过期，且这必须是刻意动作

把一支正在跑的机群从「不过期」切到「有 TTL」只能靠运维显式加这个 flag；升级本身（不加
任何 flag）不会让任何已签发身份开始过期。

*代价：* 想要「默认就有有效期」的场景（例如合规要求身份定期轮换）在今天的默认值下拿不到，
必须显式配置，且没有任何门禁提醒运维去配置它。

*为何可接受：* 反方向的错误更贵——一次不加 flag 的常规升级让整支机群的身份在 TTL 后集体
失效，是一次全面停机。

### runner 面不进 OpenAPI 契约

整个 `/v1/runners/*`（含新增的 `POST /v1/runners/renew-identity`，常量
`protocol.RenewIdentityPath`）按 spec §0.1/§10 不进 `api/openapi/xflow-v1.yaml`。身份续期
端点只有 Go doc comment。`TestContractPathsAreAllRegistered` 的覆盖范围本就不含 runner 面
路径——**这不是本次遗漏**。

*代价：* 第三方 runner 实现者只能读源码或 doc comment 得知续期端点的请求/响应形状；没有
机器可读的契约能在该端点变更时给他们提前预警。

### 「只能续自己」是认证方式换来的，不是一次显式比较

这条不变式由 `AuthenticateOngoing(runnerID, token, TransportInfo)` 的**成对认证**保证，
而不是续期 handler 里一次显式的 `renewRunnerID == authenticatedRunnerID` 比较。成对的含义
落在 `service/control/issued_identity.go` 的 `authenticate`：先 `Lookup(ctx, runnerID)` 取出
**那一条**记录，再 `subtle.ConstantTimeCompare(HashSecret(token), id.TokenHash)`——runnerID
与 token 必须同时指向同一条记录才能通过。

（同一个函数里还有两条相关的既定形状：生命周期检查刻意排在常数时间比较**之后**，且过期与
吊销两种拒绝都以 `ErrAuthUnknownToken` 作为唯一 `errors.Is` 可匹配的身份——但各自 wrap 了
一层只进服务端日志的原因说明（见上方「待办 §2 已修」）。前者避免向不持有效 token 的调用方
回答「这个 runner id 存在吗」；后者的 R7「过期与未知 token 对外不可区分」由 `authDeny` 硬拒绝
时返回的 `ErrUnauthenticated` 常量保证，不由错误消息的字面相同保证。改动 `authenticate` 时
这两条都不能松：wrap 里可以写原因，但不能引入第二个可匹配的 sentinel，也不能把原因写进
任何到达调用方的响应。）

*代价：* 若将来认证方式改成「只验 token、不绑 runner id」（例如为了支持 token 与 runner id
解耦的部署），这条推理会失效，续期端点会退化成一条**横向提权通道**：A 的 token 可以续 B 的
身份。

> **给未来重构者：** 依赖的那条回归测试——A 用 A 的 token 试图续 B 的身份，必须被拒——
> 必须在任何认证方式重构中保留，且必须继续通过。它是这条不变式唯一的守卫。

### 静态 `--token` 部署不启动续期循环

判据是 `store.Load()` 返回的 `ok`。身份来自静态文件而非 enroll 得到的 `ExpiresAt` 时，
续期循环不启动。

*代价：* 若不做这条区分，静态 token 部署会每隔约 30 秒打一条 Warn 日志直到进程死亡——因为
它的身份文件里从来没有 `ExpiresAt` 可续。这是既定裁定不是遗漏；错误方向是「让静态 token
也去续期」，那会向一个不支持续期语义的凭证源发起注定失败的请求。

### 明文 `--server` 下不启动续期循环，并记一条 Warn

指显式用 `--allow-plaintext` 放行明文的部署（未放行时会在更早阶段拒绝启动）。

*代价：* 明文部署若同时打开了服务端 TTL，其 runner 的身份会在 TTL 到期后失效死亡，而不是
靠明文续期机制续活——因为续期请求本身会把 token 亮在明文链路上，与 R7「过期与未知 token
对外不可区分」这条防线的精神相悖。出口是在 `--allow-plaintext` 之外再加密（TLS），而不是
放宽这条限制。

### 签发身份 `Revoke` 的 UPDATE-then-COUNT 非原子

**先分清是哪一个 `Revoke`。** `store/sqlstore/registration_code_repo.go` 这个文件里住着两个
repo，各有一个 `Revoke`：

- `registrationCodeRepo.Revoke`——`RowsAffected == 0` 直接返回 `ErrRegistrationCodeNotFound`，
  **没有** COUNT 补偿。「不存在」「属于别的 namespace」「已经吊销过」三种情况对外不可区分，
  这是刻意的：不建存在性预言机。这条不在本节讨论范围内。
- `issuedIdentityRepo.Revoke`——**本节说的是这一个。** 它先
  `UPDATE ... WHERE runner_id = ? AND revoked_at IS NULL`，若 `RowsAffected == 0` 再补一次
  `COUNT` 来区分「该 runner 不存在」（返回 `ErrIssuedIdentityNotFound`）与「已被吊销」
  （返回 nil，幂等 no-op）。这两条语句之间若发生同一 `runner_id` 的并发删除+重建，
  分类可能出错。

**不修的理由：** 吊销是运维手动动作，不是高并发路径；正确修法需要引入事务或
`SELECT ... FOR UPDATE`，复杂度远超收益。

*代价：* 极端并发下一次吊销调用可能把「不存在」误报成「已吊销成功」，或反之——但**两种
误报都不会让一个已经被吊销的身份重新通过认证**。落闸本身不受这个竞态影响，受影响的只是
该次 API 调用返回给调用方的状态描述是否精确。

### 注册码可复用

enroll 成功后不消费、不标记。这是既有设计，本次不改。与上面「待办 §4」配合读：可复用
**且**无过期，是同一枚泄漏注册码的两个放大器。

§4 的过期部分已修，所以第二个放大器现在有了时间上限——但**只在部署设了
`--registration-code-ttl` 或调用方自己传了 `expires_in_seconds` 时才有**。默认仍是永不过期，
因为让升级追溯作废已发出的码是更坏的一种意外。可复用本身一个字节都没变：在码的有效期内，
一枚泄漏的注册码仍然能注册任意多个 runner。

---

## 配置样例为何不能直接跑

`cmd/runner` 的 `config sample` 输出的 `url` 是 `https://REPLACE-ME:8080`，`security:` 块
整体注释掉。**样例故意不能以出厂形态运行。**

一个能直接跑的默认值只有两种选法，都更坏：

- **明文 http url** —— 要让它过门禁就得配一句生效的 `allow_plaintext: true`。读者把 url
  改成真实主机、漏看旁边那行，就会把 runner token 明文送上网。`validateTransportSecurity`
  **没有 loopback 豁免**，抓不到这个错配。
- **能跑的 https url** —— 指不到任何真实地址，等于还是要改。

所以样例选择「问你要主机」而不是猜。两种忘记之中，只有一种会告诉你它发生了：

| 忘记什么 | 后果 |
|---|---|
| 忘记取消注释 `allow_plaintext` | 启动时硬停，**立刻可见** |
| 把 url 挪离 loopback 后忘记重新注释掉 | 凭证静默泄漏，**永远不可见** |

样例默认倒向前者。两条测试守卫锁住这个形状（`cmd/runner/main_test.go`）：一条断言样例能通过
`validateTransportSecurity`，另一条断言它**不是靠** `allow_plaintext` 通过的。两条守卫落在
不同行、互不掩蔽——变异验证过。
