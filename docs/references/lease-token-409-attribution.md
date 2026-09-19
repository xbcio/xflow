# 409（`ErrInvalidLeaseToken`）成因归因：受控实验与结论

本文记录一次针对 `POST /runners/result` 返回 409 的受控实验，结论是
**v0.0.12 新增的 stranded-lease reaper 不可能是该 409 的成因**，并说明
为何如此、剩下的候选机制是什么、以及新增了哪些指标让下一次真实运行
可以直接归因。文中不含 runner ID、lease token、execution ID 或任何
连接参数。

> **关键结论**
> 1. stranded-lease reaper 的唯一闸门是「该 assignment 的 per-assignment
>    lease-metadata 键已不存在」。该键的 TTL 在 `FinalizeClaim` 时按
>    `lease TTL + directory claim TTL` 设置，**恒大于 lease 自身的存活窗口**；
>    每次成功续租又会把它推后一个同样的窗口。因此 reaper 永远不是第一个
>    行动者。
> 2. 一个 reaper 释放会让 `LookupLease` **miss**，于是报告路径走
>    「authoritative lease not found」分支（有 warn 日志）。而实测 409
>    出自**引擎提交**分支，要求 `LookupLease` **命中**——恰好是 reaper
>    拒绝触碰的那个状态。二者是互斥的。
> 3. outbox 的游标分页发现**不会**重复投递同一条 outbox 条目：
>    重复投递由 per-entry delivery lease 把关，与发现扫描无关。所以
>    「游标环绕导致同一 node 被派发两次」这条候选也不成立。
> 4. 剩下的机制是「引擎在 directory 记录仍在时重新签出了该 node 的租约」，
>    而它的唯一入口是**引擎侧租约窗口先于 directory 窗口到期**。本次实测的
>    响应耗时（1.6–3.6s）远小于 `lease_ttl`（3m），所以这条也需要下一次
>    真实运行用新指标来确认，而不是继续推理。

## 1. stranded-lease reaper 的闸门

`RedisRunnerDirectory.ReapStrandedLeases` 的两个 pass（per-runner 索引 +
全 fleet handoff 账本）最终都收敛到同一个判定：

```go
// service/control/redis_runner_directory_stranded.go
func (d *RedisRunnerDirectory) strandedLeaseIdentity(...) {
    if state != redisAssignmentLeased { return ..., false, nil }
    exists, _ := d.rdb.Exists(ctx, d.keys.assignmentLeaseMetaKey(assignmentID)).Result()
    if exists != 0 { return ..., false, nil }   // ← 唯一的安全性来源
    // ... 只有 metadata 已消失时才继续
}
```

也就是说：**reaper 判断的不是「持有者是否还活着」，而是「directory 还能否
解析这份 lease」**。这两件事在正常 TTL 算术下是同一件事，但在两个窗口发生
分叉时会分离。

## 2. 两个窗口的算术

| 窗口 | 何时开始 | 何时结束 | 由谁推进 |
|---|---|---|---|
| directory metadata | `FinalizeClaim` | `lease.TTL + claimTTL`（默认 30s margin） | `RefreshLeaseMeta`（成功续租后调用） |
| 引擎租约 | `BuildTaskLease` / `AcquireTaskLease` | `lease_deadline_ms` | `RenewTaskLease`（每次续租 +1 TTL） |

两者由同一次 `FinalizeClaim` 起算，由同一次续租一起推进，且前者的窗口恒为
`live + claimTTL`。由此得到一条可以直接断言的命题：

> **引擎侧的租约窗口在 metadata 窗口之前或同时结束。**

runner 的续租节奏是 `min(TTL/3, 10s)`（`service/runner/lease_renew.go`），
每次向引擎申请**整整一个 TTL** 的延长，并触发一次 `RefreshLeaseMeta`。
所以对一个正在续租的持有者，metadata 窗口始终领先引擎窗口一个
`claimTTL`（默认 30s）。

对**不续租**的持有者（gRPC runner、以及任何 `leaseRenewClient` 类型断言
不成立的路径），两个窗口在同一瞬间结束：此时引擎的 reclaim 路径
（sweeper，默认 10s 周期）已经可以把该 lease 回收并重投，而 reaper 要等到
`claimTTL` 之后才第一次有机会看到它。

`LeaseSweeperConfig.Period`（默认 `DefaultSweepPeriod = 10s`）在生产装配
`controlplane.go` 中从未被设置，`WithRedisRunnerDirectoryClaimTTL` 也没有
任何生产调用点，所以默认参数下 margin 恒为 30s，是 sweeper 周期的 3 倍。

## 3. 为什么 reaper 释放 ≠ 本次 409

`HandleReportResult` 只有一个 409 出口，但 `reportResult` 有四个不同的围栏，
其中三个要求目录查找**失败**，第四个要求它**成功**：

| 围栏 | 目录查找 | 日志 | 本次实测 |
|---|---|---|---|
| `directory_unavailable` | 能力缺失 | Error | 0 次 |
| `directory_lease_not_found` | miss | **Warn** | 0 次 |
| `directory_immutable_mismatch` | 命中但身份不符 | 无 | 不可观测 |
| `engine_stale_token` | **命中** | 无 | 即为本次 |

reaper 的释放只可能落到第二行：它删掉了 `assignment:lease:token` /
`assignment:lease:id` 索引与 `assignment:state`，`LookupLease` 必然 miss。
而实测中该分支的 warn 日志出现 0 次，说明报告路径**从未走到**那里。
反过来，第四行要求 `LookupLease` 命中，而命中的前提正是 metadata 仍然存在
——reaper 在这种情况下会直接返回 `stranded=false`。

结论：**在 reaper 的闸门诚实的前提下，它的释放与本次 409 互斥。**

## 4. 闸门唯一可能不诚实的形态

`strandedLeaseIdentity` 只看 metadata **是否存在**，不看它是如何消失的。
所以「持有者仍然存活」却被打成 stranded，只可能来自 TTL 之外的删除：

* metadata 被 evict / 被外部删除 / 被运维操作清掉。

这不来自 TTL 算术，也无法由续租节奏触发。本仓库没有任何代码路径会在租约
存续期间删除该键，所以这属于运维事故面而不是逻辑缺陷面；但它确实是
reaper 唯一能误杀存活租约的入口，因此实验里单独固化了一条测试。

## 5. 重复派发（outbox 游标分页）

`OutboxDispatcher.drain` 用 `ListOutboxExecutions(page)` 做发现，
`rstate` 侧以 per-namespace、per-master 的游标续传。游标走完一圈回到 0
时，同一个 execution ID **可以**被再次枚举到——这是 SCAN 语义本身允许的。

但它不会导致重复投递，因为投递的门是 per-entry 的 delivery lease：

```go
// engine/outbox_lease.go
func (e *Engine) claimOutbox(...) {
    if leaser, ok := state.(OutboxLeaser); ok {
        return leaser.LeaseOutbox(ctx, id, now, limit)   // ← 条目级租约
    }
    ...
}
```

`LeaseOutbox` 把条目移入 `OutboxDeliveryLeaseTTL = 5s` 的租约，由
`outboxLeaseKeeper`（每 `TTL/3` 续期）持续续租。因此**同一个 execution
被枚举多少次都无所谓**，最多多花一次 `FlushOutbox`，而那次 flush 会发现
所有条目都已被租走、什么都不投递。

再退一步：即使同一 node 真的被派发两次，`BuildTaskLease`
（`engine/lease.go`）**每次**都调用 `newLeaseCredentials()` 铸新 token，
而 `RecoverTaskLease` 走的是重放路径、复用同一个 token。前者在
`AcquireTaskLease` 被引擎自己拒绝（前一个租约还没死），后者不会产生新
token。所以「先提交者成功、后提交者被判 stale」需要一个**新 token 被签发
出来**，这仍然回到「引擎租约窗口已过期」这一条，而不是发现路径。

## 6. 新增指标（本次调查的直接产出）

原本四个围栏共用同一个 409、同一个响应体，归因只能靠日志事后重建，
而 `engine_stale_token` 与 `directory_immutable_mismatch` 两条路径
**不打任何日志**。新增两个 counter：

| 指标 | 标签 | 含义 |
|---|---|---|
| `xflow_report_rejections_total` | `reason`, `namespace` | 报告被哪个围栏拒绝。`reason` ∈ `directory_unavailable` / `directory_lease_not_found` / `directory_immutable_mismatch` / `engine_stale_token` |

两个 counter 都同时接在 **HTTP `Server` 与 gRPC `GRPCServer`** 上。
`NewGRPCServer` **自建一个独立的 `Core`**，所以 HTTP 侧的
`WithReportRejectionObserver` 不会传递到 gRPC 路径；少了
`WithGRPCReportRejectionObserver` 这一路，走 gRPC transport 的 409 仍然
无法归因——而 §7 指出这正是尚未确定的一点。
| `xflow_lease_view_divergence_total` | `namespace` | 引擎拒绝了 token，而**目录侧仍然能解析出同一份 lease（同 assignment、同 lease_id、同 token）** 的子集 |

`xflow_lease_view_divergence_total` 是本次调查最缺的那一个信号。它由
**重新询问目录**得出，而不是从引擎的回答推断：对同一个
`(assignment, lease_id, lease_token)` 三元组再查一次目录，命中才算分叉
（三个凭据分别校验；空值不参与比较，否则会恒真）。

```go
// service/control/core.go（提交之后、容量释放之前）
if errors.Is(err, engine.ErrInvalidLeaseToken) {
    directoryStillResolved = c.reportLeaseStillResolvable(ctx, req.RunnerID, req.SessionID, authoritativeLease)
}
...
if outcome.ReleasesLeasedCapacity() { ...ReleaseLeased... }
if errors.Is(err, engine.ErrInvalidLeaseToken) {
    c.observeReportRejected(ctx, ReportRejectedEngineStaleToken)
    if directoryStillResolved { c.observeReportDivergence(ctx) }
}
```

顺序是负载性的：`ReleaseLeased` 会把目录记录清掉，所以探针必须在它之前执行，
否则每个引擎拒绝的提交都会读到「找不到」，该指标恒为 0。

对本次真实发生的那 5 次 409，如果这些指标当时存在，预期会看到：

* `xflow_report_rejections_total{reason="engine_stale_token"} = 5`
  （前提是它们确实来自引擎提交路径，这一点已由日志排除法确认）；
* 另三个 `reason` 全为 0 —— 这会把「目录侧丢租约」这一整族可能一次性排除；
* `xflow_lease_view_divergence_total` 预期**与上一条相等**（5）。因为引擎
  拒绝的前一步刚刚 `LookupLease` 命中，而目录记录要到之后的 `ReleaseLeased`
  才被清除，所以探针在同一次请求内几乎必然命中。两者**不相等**才是异常信号：
  它表示目录记录在「查找命中」与「提交被拒」之间被第三方释放了，那是一个
  独立值得追查的竞态（候选来源包括 sweeper 的 directory release 与 stranded
  reaper）。

  > 该指标的定位因此是**分叉的存在性与稳定性**：它把「目录侧在引擎拒绝的
  > 瞬间仍持有同一个 lease」这件事从不可观测变为可观测，并顺带给出
  > 「提交期间记录被抢走」的发生率。它**不能**回答「reaper 是否误杀」——
  > 那一问由 §3 的闸门逻辑与 §2 的窗口算术回答。

## 7. 仍然未知的部分（诚实缺口）

1. **为什么那 5 次 409 集中在一个 63 秒窗口内**，本实验只排除了 reaper
   这一条解释，没有给出替代解释。窗口与 stranded reap 的第二次 pass 吻合
   仍然只是相关性；本实验否定的是因果，而不是这个相关性本身。
2. **`lease_ttl` 在真实运行时是否真的解析成 3m** 仍未直接采样验证。
   全部推演都依赖「引擎租约窗口的实际长度」，这一点建议下次复测时在首个
   assignment 被 claim 的瞬间密集采样 metadata 键的 `PTTL` 来确认。
3. **续租是否真的在落地**：`renewLeaseLoop` 只在协议客户端实现
   `leaseRenewClient` 时启动（`service/runner/runner.go`），gRPC 客户端
   不实现它。若真实环境走的是 gRPC，则 metadata 窗口与引擎窗口同时结束，
   §2 的 margin 完全不存在，`engine_stale_token` 会变成「引擎窗口到期后
   又被重投」的常规形态。这两个分支用 §6 的两个指标即可区分，不需要新的
   插桩。
4. **本实验没有把真实引擎接进来**。`service/control` 只与引擎的
   `EngineFacade` 交互，无法在不引入 rstate Lua 依赖的前提下构造真实的
   `AcquireTaskLease` 序列；因此 §2 的窗口算术是**代码核实 + 模型断言**，
   而不是端到端复现。要端到端复现需要真实 Redis（Lua）或对
   `acquireTaskLeaseLua` 的等效实现。
