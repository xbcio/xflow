# 部署配置示例（G1 受限单租户生产）

> 本文档给出 G1 生产部署的完整配置示例。门槛定义见 [RELEASE-GATES.md](../design/RELEASE-GATES.md)，拓扑见 [DEPLOYMENT-TOPOLOGIES.md](../design/DEPLOYMENT-TOPOLOGIES.md)。
> 所有 flag 名称与 `cmd/server/main.go` 精确一致。

## 1. 完整 cmd/server 启动示例

```bash
xflow-server \
  --addr :8080 \
  --grpc-addr :8090 \
  --redis redis://:password@redis.internal:6379/0 \
  --concurrency 10 \
  --auth-policy /etc/xflow/runners.yaml \
  --api-auth-token "$WORKFLOW_API_TOKEN" \
  --require-api-auth \
  --tls-cert /etc/xflow/tls/server.crt \
  --tls-key /etc/xflow/tls/server.key \
  --tls-client-ca /etc/xflow/tls/runner-ca.crt \
  --trace otlp \
  --trace-endpoint otel-collector.observability:4317 \
  --trace-insecure false \
  --metrics-addr :9090 \
  --metrics-path /metrics \
  --log-format json \
  --management
```

### Flag 说明

| Flag | 默认值 | G1 生产要求 | 说明 |
|---|---|---|---|
| `--addr` | `:8080` | 必设 | HTTP 控制面监听地址 |
| `--grpc-addr` | `""`（禁用） | 推荐启用 | gRPC Runner Protocol 监听地址；空则禁用 gRPC |
| `--redis` | `""` | **必设**（非 memory） | Redis 地址；空时回退 `--memory`（仅 dev） |
| `--memory` | `false` | **禁止**生产使用 | 进程内存 backend，进程退出即丢失 |
| `--concurrency` | `10` | 按容量调 | 队列消费者并发 |
| `--auth-policy` | `""` | **必设** | runners.yaml 路径；空=runner 协议鉴权禁用 |
| `--auth-dry-run` | `false` | 仅 rollout 过渡 | 日志记录违规但放行；enforce 前的过渡窗口 |
| `--api-auth-token` | `""` | **必设** | workflow API bearer token（`/v1/workflows`、`/v1/executions/*`） |
| `--require-api-auth` | `false` | **必设** | 无 `--api-auth-token` 时启动失败（fail-closed） |
| `--tls-cert` | `""` | **必设** | server TLS 证书；启用 TLS |
| `--tls-key` | `""` | **必设** | server TLS 私钥；须与 `--tls-cert` 成对 |
| `--tls-client-ca` | `""` | 推荐（mTLS） | 验证 runner 客户端证书的 CA；启用 mTLS |
| `--log-format` | `text` | `json` | 生产用 json |
| `--metrics-addr` | `""`（禁用） | **必设** | Prometheus metrics 监听地址 |
| `--metrics-path` | `/metrics` | 默认 | Prometheus metrics 路径 |
| `--trace` | `disabled` | `otlp` | tracing 模式：`disabled`/`stdout`/`otlp` |
| `--trace-endpoint` | `localhost:4317` | OTLP collector | OTLP gRPC endpoint（`--trace=otlp` 时） |
| `--trace-insecure` | `false` | `false` | 是否禁用 OTLP TLS 验证 |
| `--management` | `false` | **必设** | 启用 ops management 模块（`/healthz` `/readyz` `/v1/management/*`）；`/v1/management/*` 由 `--api-auth-token` 门控 |

## 1.1. Redis HA 模式启动示例（sentinel / cluster）

> **诚实性声明 [ENV-GATED]**：本节示例仅展示 Redis HA flag 的正确配置方式。Redis HA 客户端抽象代码（Task 1.1–3.1）与 `cmd/server` flag（Task 4.1）已实现，但真实 sentinel/cluster 环境的连通性、failover 行为与多副本 soak 验收依赖真实部署环境，**尚未完成**。在 [RELEASE-GATES.md](../design/RELEASE-GATES.md) §2 G2 行状态仍为 `⏳ 未完成` 前，不得宣称 "control-plane HA 生产可用"。完整 HA 验收方法论与故障矩阵见 [ha-soak-plan](ha-soak-plan.md)。

### Sentinel 模式

```bash
xflow-server \
  --addr :8080 \
  --grpc-addr :8090 \
  --redis-mode sentinel \
  --redis-sentinel-master mymaster \
  --redis-sentinel-addrs sentinel1:26379,sentinel2:26379,sentinel3:26379 \
  --redis-username xflow \
  --redis-password <password> \
  --redis-sentinel-username xflow-sentinel \
  --redis-sentinel-password <sentinel-password> \
  --redis-db 0 \
  --redis-tls \
  --concurrency 10 \
  --auth-policy /etc/xflow/runners.yaml \
  --api-auth-token "$WORKFLOW_API_TOKEN" \
  --require-api-auth \
  --tls-cert /etc/xflow/tls/server.crt \
  --tls-key /etc/xflow/tls/server.key \
  --tls-client-ca /etc/xflow/tls/runner-ca.crt \
  --trace otlp \
  --trace-endpoint otel-collector.observability:4317 \
  --trace-insecure false \
  --metrics-addr :9090 \
  --metrics-path /metrics \
  --log-format json \
  --management
```

### Cluster 模式

```bash
xflow-server \
  --addr :8080 \
  --grpc-addr :8090 \
  --redis-mode cluster \
  --redis-cluster-addrs node1:6379,node2:6379,node3:6379 \
  --redis-username xflow \
  --redis-password <password> \
  --redis-tls \
  --concurrency 10 \
  --auth-policy /etc/xflow/runners.yaml \
  --api-auth-token "$WORKFLOW_API_TOKEN" \
  --require-api-auth \
  --tls-cert /etc/xflow/tls/server.crt \
  --tls-key /etc/xflow/tls/server.key \
  --tls-client-ca /etc/xflow/tls/runner-ca.crt \
  --trace otlp \
  --trace-endpoint otel-collector.observability:4317 \
  --trace-insecure false \
  --metrics-addr :9090 \
  --metrics-path /metrics \
  --log-format json \
  --management
```

### HA flag 说明

| Flag | 默认值 | G1/G2 要求 | 说明 |
|---|---|---|---|
| `--redis-mode` | `single` | G2 必设 | Redis 部署模式：`single` / `sentinel` / `cluster`。空值与 `single` 等价，保持向后兼容 |
| `--redis-sentinel-master` | `""` | sentinel 必设 | Sentinel 监控的 master 名称（如 `mymaster`） |
| `--redis-sentinel-addrs` | `""` | sentinel 必设 | Sentinel 节点地址，逗号分隔 |
| `--redis-cluster-addrs` | `""` | cluster 必设 | Redis Cluster 节点地址，逗号分隔 |
| `--redis-username` | `""` | 按 ACL 配置 | Redis master/cluster 用户名（ACL） |
| `--redis-password` | `""` | 按 ACL 配置 | Redis master/cluster 密码 |
| `--redis-sentinel-username` | `""` | 可选 | Sentinel 用户名；空则回退到 `--redis-username` |
| `--redis-sentinel-password` | `""` | 可选 | Sentinel 密码；空则回退到 `--redis-password` |
| `--redis-db` | `0` | single/sentinel 可用 | Redis 逻辑库，cluster 模式忽略 |
| `--redis-tls` | `false` | 生产推荐 | 为 Redis 连接启用 TLS |

> 注意：cluster 模式使用 `xflow:ns:<namespace>:exec:{<id>}:... (tenant 前缀无花括号，hash tag 仍为 {<id>})` hash tag 保证同一 execution 的 key 共置同 slot；sentinel 模式仍使用单一 master 路由。配置错误时 `cmd/server` fail-closed 启动失败。

## 2. runners.yaml 示例

runner 协议鉴权策略（`--auth-policy`）。token 在 server 侧以 sha256 constant-time 比较；runner 端在注册时携带。

```yaml
# /etc/xflow/runners.yaml
runners:
  - id: runner-prod-1
    token: <high-entropy-bearer-token>   # 至少 128 bit 随机
    allowed_node_types: ["*"]            # 允许处理的节点类型；"*" 全放行
    # tls_subject: "runner-prod-1.internal"  # 可选：mTLS CN/SAN 必须匹配
```

字段语义见 `service/control/auth.go` 的 `FilePolicyStore`。token 不得硬编码在源码或镜像中；通过密钥管理注入。

## 3. 单副本部署拓扑声明

G1 是**单租户、允许维护窗口**的部署。单副本控制面：

- **不提供无中断 HA SLO**：控制面进程重启期间 API 不可用。
- 必须声明维护窗口（见 [maintenance-window-runbook](maintenance-window-runbook.md)）。
- Redis 必须是可恢复部署（持久化 + 备份/恢复演练）。
- runner 可横向扩缩容（多 runner 实例线性扩展执行吞吐），不影响控制面单副本限制。

多副本 control-plane HA 属于 G2（B2），未完成前不得宣称。

## 4. Prometheus 告警规则

基于 [dead-letter-runbook](dead-letter-runbook.md) 的指标。生产必须配置以下告警：

```yaml
groups:
  - name: xflow-dead-letter
    rules:
      # Dead-letter 积压：>5m 表示卡住，需排查根因后 replay 或 purge
      - alert: XflowDeadLetterBacklog
        expr: xflow_outbox_dead_letters > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "xflow dead-letter backlog"
          description: "Entries stuck in dead-letter storage. Inspect and replay after root-cause analysis."

      # Replay 拒绝率：对已完成/过期执行重放，可能自动化失控或用错 runbook
      - alert: XflowReplayRejectionRate
        expr: rate(xflow_outbox_dead_letters_replayed_total{outcome=~"rejected_terminal|rejected_inactive"}[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "xflow replay rejection rate"
          description: "Operators replaying entries for finished/expired executions."

  - name: xflow-outbox
    rules:
      # Ready 积压：待投递条目持续增长，队列/runner 不健康
      - alert: XflowOutboxPendingGrowth
        expr: deriv(xflow_outbox_pending[10m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "xflow outbox pending backlog growing"
          description: "Ready entries accumulating; queue or runner may be unhealthy."

      # Outbox 投递错误率持续 >0，dispatcher 无法投递
      - alert: XflowOutboxErrorRate
        expr: rate(xflow_outbox_errors_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "xflow outbox delivery errors"
```

### 进程与 leader 探针（HTTP）

控制面 leader 状态当前通过 management API 暴露，非 Prometheus 指标。用 blackbox exporter 探针：

| 探针 | 端点 | 含义 |
|---|---|---|
| liveness | `GET /healthz` | 进程存活，返回 `{"status":"ok"}` |
| readiness | `GET /readyz` | 就绪 + leader 字段；`leader:false` 持续应告警 |
| leader | `GET /v1/management/leader` | 显式 leader 状态，`is_leader:false` 持续表示无主 |

> management 端点由 `--management` flag 启用。启用后 `/v1/management/*` 由 `ManagementAuthMiddleware` 用 `--api-auth-token` 门控；`/healthz`、`/readyz` 保持开放供 Kubernetes 探针。未设 `--api-auth-token` 时 `/v1/management/*` 开放（仅 dev / 外部网关后）。多副本部署中持续 `is_leader:false` 表示 leader 选举异常，应触发 critical 告警。

> 指标名以 `observability/metrics/` 实际导出为准；上述是 G1 必备的最小告警集。

## 5. 生产启动前核对清单

启动 G1 生产前逐项确认：

- [ ] `--redis` 指向可恢复 Redis（持久化开启，RDB/AOF），非 `--memory`；若用 HA 模式，见下方额外检查项
- [ ] `--auth-policy` 配置 runners.yaml，token 为高熵随机值，未硬编码
- [ ] `--api-auth-token` 配置，`--require-api-auth` 启用（无 token 启动失败）
- [ ] `--tls-cert`/`--tls-key` 配置；runner 连接走 TLS，推荐 mTLS（`--tls-client-ca`）
- [ ] `--trace otlp` 指向 collector；`--trace-insecure=false`
- [ ] `--metrics-addr` 配置，Prometheus 抓取正常
- [ ] `--management` 启用；`/v1/management/*` 由 `--api-auth-token` 门控，`/healthz` `/readyz` 开放供探针
- [ ] `--log-format json`
- [ ] Redis 备份/恢复演练已完成并记录
- [ ] dead-letter 告警接入值班通知
- [ ] 维护窗口流程与值班团队同步
- [ ] 明确告知业务方：单副本、有维护窗口、at-least-once（业务幂等键）

**若使用 sentinel / cluster 模式，额外确认：**

- [ ] `--redis-mode` 显式设置为 `sentinel` 或 `cluster`
- [ ] sentinel 模式：`--redis-sentinel-master` 与 `--redis-sentinel-addrs` 已配置，且 master 名与 Sentinel 实际一致
- [ ] cluster 模式：`--redis-cluster-addrs` 已配置，地址为 cluster 初始节点
- [ ] `--redis-username` / `--redis-password` 与目标 Redis ACL 一致；sentinel 凭据通过 `--redis-sentinel-username` / `--redis-sentinel-password` 配置或正确回退到 master 凭据
- [ ] `--redis-tls` 启用时，证书与 Redis 服务端 TLS 互信已配置
- [ ] 已理解并记录：`--redis-mode=sentinel|cluster` 仅启用客户端 HA 连接，**不等于 control-plane HA 已验收**；多副本 soak 与 SLO 量化见 [ha-soak-plan](ha-soak-plan.md)，当前状态 `[ENV-GATED]`
