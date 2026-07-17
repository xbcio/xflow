# 维护窗口与单副本 SLO Runbook

> G1 是**单租户、允许控制面维护窗口**的受限生产部署。本文档定义单副本 SLO 边界、维护窗口流程和恢复验证。
> 门槛定义见 [RELEASE-GATES.md](../design/RELEASE-GATES.md)，部署示例见 [deployment-examples.md](deployment-examples.md)。

## 1. 单副本 SLO 边界

G1 单副本控制面部署的明确边界：

| 项 | 承诺 |
|---|---|
| API 可用性 | 控制面进程重启期间 API 不可用；**不提供无中断 HA SLO** |
| 调度可靠性 | durable assignment/lease/outbox 在 Redis 持久化，重启后恢复；调度意图不丢失 |
| 执行语义 | at-least-once；重复提交不重复推进 DAG（fencing）；业务副作用由宿主幂等键保证 |
| 维护窗口 | 必须声明；drain 期间优雅停机，非硬终止 |

无中断 HA 属于 G2（B2），未完成前不得宣称。

## 2. 维护窗口流程

```
1. 通知      → 提前通知业务方维护窗口和预期不可用时长
2. Drain     → 发送 SIGTERM，触发 graceful shutdown
3. 升级      → 替换二进制 / 配置 / Redis 维护
4. 验证      → /healthz /readyz /v1/management/leader
5. 恢复流量   → 确认 leader 持有、ready 正常后恢复接入
```

### 2.1 Drain（graceful shutdown）

发送 `SIGTERM`（或 `SIGINT`）。`cmd/server` 的 `signal.NotifyContext` 捕获后触发有序停机。

**停机顺序**（`service/apiserver/run.go:Run`，超时默认 15s）：

1. **HTTP server** `httpSrv.Shutdown(shutdownCtx)` — drain 在飞请求
2. **gRPC server** `grpcServer.GracefulStop()` — drain 在飞 RPC
3. **metrics server** `metricsStop()` — 独立停机（5s 超时）
4. **control plane** `s.Shutdown(shutdownCtx)` → `ControlPlane.Shutdown`：
   - 取消 sweeper / claim recovery / leader campaign 的 context
   - 等待后台 goroutine 退出（受 ctx 限制，避免卡住停机）
   - `elector.Resign` — 释放 leadership（token 匹配才删 key，防止误释放）
   - `unbind()` — 解绑队列

每个步骤**无论前一步是否出错都执行**，错误聚合返回。

### 2.2 Drain 期间的安全保证

at-least-once 语义下，维护窗口是安全的：

- **durable assignment**：runner claim 前已持久化到 Redis；未 finalize 的 claim 有 TTL，过期回到 assignment queue，重启后重新 handoff。
- **leased handoff**：已构建 lease 但未 finalize 的，重启后恢复该精确 fenced lease 并完成 finalization，**不签发第二个 owner**。
- **outbox**：下游调度 intent 在 durable outbox 中；队列不可用时保留，恢复后 dispatcher 重放。
- **runner 视角**：response loss 后 runner reconnect 或重新注册会 replay 同一 lease；runner 必须把 handler 执行视为 at-least-once。

**业务侧要求**：handler 副作用必须幂等（宿主幂等键）。维护窗口期间被中断的 invocation 可能在重启后被重复执行一次。

## 3. 升级后验证

升级完成后，启动新进程并验证：

```bash
# 1. 进程存活（liveness）
curl -s https://server:8080/healthz
# {"status":"ok"}

# 2. 就绪 + leader 状态（readiness）
curl -s https://server:8080/readyz
# {"ready":true,"leader":true}

# 3. 显式 leader 确认
curl -s https://server:8080/v1/management/leader
# {"is_leader":true}

# 4. metrics 端点
curl -s https://server:8090/metrics | grep xflow_outbox_dead_letters
```

> `/healthz`、`/readyz`、`/v1/management/leader` 由 management 模块提供，`cmd/server` 通过 `--management` flag 启用。
> 启用后 `/v1/management/*` 由 `ManagementAuthMiddleware` 用 `--api-auth-token` 门控；`/healthz`、`/readyz` 保持开放供 Kubernetes 探针。未设 `--api-auth-token` 时 `/v1/management/*` 开放（仅 dev / 外部网关后），生产应同时配置 `--api-auth-token`。非 leader 副本也 ready，调用方可据 `leader` 字段路由写入。

验证通过后恢复流量接入。

## 4. Redis 备份/恢复演练

G1 要求（spec）：Redis 采用可恢复部署并完成备份/恢复演练。

- **持久化**：Redis 开启 RDB + AOF（`appendfsync everysec`），数据落盘。
- **备份**：定期 `BGSAVE` 后拷贝 RDB/AOF 到异地存储；或使用 Redis 快照能力。
- **恢复演练**（定期执行并记录）：
  1. 在测试环境用备份恢复一个 Redis 实例
  2. 启动 `xflow-server` 指向恢复的 Redis
  3. 验证既有 execution / assignment / outbox 状态可读
  4. 验证一个 suspended 审批工作流可继续 signal 并推进到终态
  5. 验证 dead-letter 存储完整
- **记录**：演练时间、RPO、恢复耗时、验证结果。

> xflow 的 Redis key 全部使用 `{id}` hash tag（`backend/distributed/internal/rstate/keys.go`），数据模型 cluster-ready；但当前客户端是单节点初始化（`redis.NewClient`），sentinel/cluster 客户端支持属 B2 范围。

## 5. dead-letter 人工处置

dead-letter 告警触发后，按 [dead-letter-runbook](dead-letter-runbook.md) 处置：

1. 确认底层投递故障已恢复（queue/runner 健康），否则 replay 后会再次 dead-letter。
2. `xflow dead-letter list` 检查积压。
3. `xflow dead-letter replay --reason ... --operator ...` 重放（特权写，有审计）。
4. terminal/expired 执行的 replay 是 no-op（`rejected_*` outcome），不是错误。
5. 重复 dead-letter 的条目升级到 queue/runner 诊断。

## 6. 可观测性关联

G1 要求 tracing/metrics/logging 具备值班所需关联能力：

| 维度 | 配置 | 关联 |
|---|---|---|
| tracing | `--trace otlp` | W3C `traceparent` 贯通 server→runner→server；`xflow.task.commit` span；HTTP middleware 为最外层。**敏感 signal/审批 payload 不进入 span attribute** |
| metrics | `--metrics-addr` | Prometheus；dead-letter/lease/auth/dispatcher/outbox 指标 |
| logging | `--log-format json` | 结构化日志；auth 拒绝、runner op 失败、leader campaign 等事件 |

故障排查时，从 metrics 告警定位时间窗 → tracing 按 traceparent 串起 server→runner→server 调用链 → logs 按时间窗和 span 过滤。
