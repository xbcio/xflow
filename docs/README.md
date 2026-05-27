# XFlow - 分布式工作流引擎设计文档

## 概述

XFlow 是一个基于 Asynq 的分布式工作流引擎，采用 Master-Worker 架构，支持高并发、高可用、可扩展的工作流编排与调度。

## 核心特性

- **高并发**: Worker Pool 动态扩缩容，支持海量任务并发执行
- **稳定性强**: 优雅降级、熔断、限流、健康检查
- **扩展性强**: 插件化架构，支持自定义任务类型和处理器
- **可追溯**: 全链路 TraceID，详细执行日志和审计记录
- **可监控**: Prometheus 指标、实时监控面板、告警通知
- **可重试**: 多种重试策略（指数退避、固定间隔、自定义）
- **版本化**: 语义化版本管理，灰度发布，版本回滚
- **Context 管理**: 全局变量（`$vars`）、环境配置（`$config`）、凭证管理（`getCredential()`）

## 架构概览

```
Master Cluster (调度与编排)
    ├── Workflow Engine (工作流引擎)
    ├── Scheduler (任务调度器 - Asynq)
    ├── State Manager (状态管理器 - Redis)
    ├── API Server (:8080)
    ├── Gateway (:8081, Edge Worker 接入)
    └── Version Controller / Monitor
           ↓
    Asynq Queue (Redis)
           ↓
Worker Pool (任务执行)
    ├── Internal Worker (内网直连 Redis)
    ├── System Edge Worker (跨DC/跨网，via Gateway)
    └── User Edge Worker (用户本机/浏览器，via Gateway)
```

## DSL 快速示例

```yaml
spec: "1.0"
name: "order-processing"
version: "1.0.0"

triggers:
  - type: webhook
    parameters:
      path: "/webhook/order"
  - type: manual

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
├── cmd/
│   ├── server/          # Master 服务入口
│   └── worker/          # Worker 服务入口
├── pkg/
│   ├── types/           # 核心类型定义
│   ├── expression/      # 表达式引擎 (Expr)
│   ├── workflow/        # 工作流核心
│   │   ├── engine.go    # 工作流引擎
│   │   ├── parser.go    # DSL 解析器
│   │   ├── compiler.go  # 编译器
│   │   ├── executor.go  # 执行器
│   │   └── dag.go       # DAG 构建
│   ├── scheduler/       # 调度器 (Asynq)
│   ├── state/           # 状态管理
│   ├── task/            # 任务处理器
│   ├── monitor/         # 监控系统
│   └── api/             # API 服务
├── internal/
│   ├── config/          # 配置管理
│   ├── storage/         # 存储层
│   └── middleware/      # 中间件
├── docs/                # 设计文档
└── tests/               # 测试用例
```

## 文档导航

### 核心文档（必读）

1. **[DSL 规范](./DSL-SPECIFICATION.md)** - 完整的 DSL 语法规范
   - YAML DSL 结构与字段定义
   - 表达式引擎（`${{ }}`/`{{ }}`）
   - Connections 机制
   - 节点类型与触发器
   - 完整工作流示例

2. **核心组件**（按角色拆分）
   - **[Master 组件](./MASTER-COMPONENTS.md)** — 调度集群：WorkflowEngine、Scheduler、Executor、Reconciler、HA 方案、核心数据结构与接口
   - **[Gateway 组件](./GATEWAY-COMPONENTS.md)** — Edge Worker 接入层：注册流程、Long Poll API、System/User Worker 路由、断线恢复
   - **[Worker 组件](./WORKER-COMPONENTS.md)** — 执行层：Internal Worker（Asynq）、Edge Worker（HTTP Poll）、TaskHandler 接口、插件系统

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
