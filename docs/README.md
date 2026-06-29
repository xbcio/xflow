# XFlow - 分布式工作流引擎设计文档

## 概述

XFlow 是一个 Go 工作流引擎。当前生产版重点是通过 SDK 嵌入业务系统，
支撑长时间运行的审批流程：DAG 调度、Action 节点执行、挂起/Signal 恢复、
取消、检查执行状态，以及 Redis/Asynq 后端。

## 核心特性

- **高并发**: Worker Pool 动态扩缩容，支持海量任务并发执行
- **稳定性强**: 优雅降级、熔断、限流、健康检查
- **扩展性强**: 插件化架构，支持自定义任务类型和处理器
- **可追溯**: 通过 SDK `Inspect` 查询执行和节点状态，业务侧可基于节点输出形成审批审计
- **可监控**: Prometheus 指标、实时监控面板、告警通知
- **可控流程**: SDK 暴露 `Signal`、`RevokeSignal`、`Cancel`、`Inspect`
- **Context 管理**: 全局变量（`$vars`）、环境配置（`$config`）、凭证管理（`getCredential()`）

## SDK 入口

生产集成优先使用 `sdk/xflow`：

- `xflow.NewLocal()`：内存状态 + 本地 goroutine 队列，适合测试和单进程嵌入。
- `xflow.NewCluster(...)`：Redis/Asynq 后端，适合分布式执行。
- `Engine.Submit` / `Engine.Wait`：提交和等待工作流。
- `Engine.Signal` / `Engine.RevokeSignal`：审批/等待节点的外部信号控制。
- `Engine.Cancel` / `Engine.Inspect`：取消执行和查询执行详情。

审批节点可使用 `node.Approval(...).WithTimeout("48h", "reject")` 配置超时；
等待节点可使用 `node.Wait("signal").WithTimeout("30m")`。

## DSL 快速示例

```yaml
spec: "1.0"
name: "order-processing"
version: "1.0.0"

context:
  vars:
    max_retry: 3
  config:
    env: "production"

credentials:
  api_auth:
    name: "api_credentials"
    type: apiKey

nodes:
  - name: validate
    type: xflow.http
    parameters:
      method: POST
      url: "{{ $config.service_endpoints.order }}/validate"
      headers:
        Authorization: "Bearer {{ getCredential('api_auth').token }}"
      body:
        order_id: "${{ $params.order_id }}"

  - name: check_result
    type: xflow.if
    parameters:
      condition: "${{ $nodes['validate'].is_valid }}"

  - name: process
    type: xflow.http

connections:
  validate:
    main:
      - node: check_result
  check_result:
    true:
      - node: process
```

### 表达式语法

| 语法 | 模式 | 返回类型 | 示例 |
|------|------|---------|------|
| `${{ expr }}` | 表达式求值 | 保留原始类型 | `timeout: "${{ $vars.x * 1000 }}"` |
| `{{ expr }}` | 字符串插值 | 始终 string | `url: "{{ $config.base }}/api"` |

详见 [DSL 规范 - 表达式引擎](./DSL-SPECIFICATION.md#4-表达式引擎)。

## 项目结构

```
xflow/
├── engine/              # 纯调度内核
├── execution/           # Dispatcher / Runner / Registry
├── nodes/node/          # 内置 Action 节点
├── backend/             # memory、asynq 后端
├── store/               # 持久化接口和 SQL 实现
├── sdk/xflow/           # 公开 SDK
├── docs/                # 设计文档
└── sdk/examples/        # 可运行示例
```

## 文档导航

### 核心文档（必读）

1. **[DSL 规范](./DSL-SPECIFICATION.md)** - 完整的 DSL 语法规范
   - YAML DSL 结构与字段定义
   - 表达式引擎（`${{ }}`/`{{ }}`）
   - Connections 机制
   - 节点类型
   - 完整工作流示例

2. **核心组件**（按角色拆分）
   - **[Master 组件](./MASTER-COMPONENTS.md)** — 调度集群：WorkflowEngine、Scheduler、Executor、Reconciler、HA 方案、核心数据结构与接口
   - **[Gateway 组件](./GATEWAY-COMPONENTS.md)** — Edge Worker 接入层：注册流程、Long Poll API、System/User Worker 路由、断线恢复
   - **[Worker 组件](./WORKER-COMPONENTS.md)** — 执行层：Internal Worker（Asynq）、Edge Worker（HTTP Poll）、ActionHandler 接口、插件系统

### 示例

- [`examples/purchase-approval.yaml`](./examples/purchase-approval.yaml) - 采购审批工作流

## 技术栈

| 组件 | 技术选型 |
|------|---------|
| 任务调度 | Asynq (Redis-based) |
| 表达式引擎 | Expr (expr-lang/expr) |
| 状态存储 | Redis + PostgreSQL/MySQL |
| 监控 | Prometheus + Grafana |
| 链路追踪 | OpenTelemetry |
| API | gRPC + HTTP |

## 参考

- [Asynq](https://github.com/hibiken/asynq) - Go 分布式任务队列
- [Expr](https://github.com/expr-lang/expr) - Go 表达式引擎
- [n8n](https://n8n.io/) - 工作流自动化平台
