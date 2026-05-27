# XFlow DSL 完整规范

> 本文档包含 XFlow 的完整 DSL 定义，包括语法规范、Connections 机制和表达式引擎说明。

## 目录

1. [DSL 设计原则](#1-dsl-设计原则)
2. [核心概念](#2-核心概念)
3. [YAML DSL 规范](#3-yaml-dsl-规范)
4. [表达式引擎](#4-表达式引擎)
5. [Connections 机制](#5-connections-机制)
6. [节点类型](#6-节点类型)
7. [触发器](#7-触发器)
8. [Pin Data（测试数据钉住）](#8-pin-data测试数据钉住)
9. [完整示例](#9-完整示例)

---

## 1. DSL 设计原则

- **简洁性** - 易于阅读和编写
- **表达力** - 支持复杂工作流场景
- **可验证** - 支持静态检查和运行时验证
- **可扩展** - 支持自定义扩展
- **可视化友好** - 可直接映射到图形界面

## 2. 核心概念

### 2.1 Node（节点）

节点是工作流中的基本执行单元，代表一个独立的步骤或操作。

每个节点通过 Handler 描述符（`Descriptor()`）声明自身支持的输入/输出端口，用于编辑器渲染和编译期校验。节点可在 DSL 中通过 `inputs` 字段声明命名输入端口，实现与上游节点名的解耦。

### 2.2 Connections（连接）

显式定义节点间的数据流向，替代传统的 `depends_on`：

```yaml
connections:
  node_a:              # 源节点
    main:              # 输出端口
      - node: node_b   # 目标节点
```

**优势**：
- ✅ 显式表达数据流
- ✅ 支持多输出端口（main、error、success、failed等）
- ✅ 支持条件路由和分支合并
- ✅ 可视化编辑器友好

### 2.3 Triggers（触发器）

工作流支持多个触发器并存（如同时支持定时触发和手动触发）：
- **Manual** - 手动触发
- **Webhook** - HTTP Webhook
- **Cron** - 定时触发
- **Event** - 事件触发
- **Queue** - 队列触发

多个触发器任一激活即可启动工作流。每个触发器可通过 `enabled` 字段独立开关，无需删除配置。

### 2.4 Context（上下文）

Context 用于管理工作流的全局信息，包括：

**vars（全局变量，只读）**
- 可在所有节点中访问的共享变量，**运行时只读**
- 支持所有基本类型和复杂类型
- 可用于配置节点参数、条件判断等
- DSL key：`context.vars`，表达式中通过 `$vars.key` 访问
- 节点不可修改 `$vars`；如需在节点间传递可变数据，请通过节点输出 → connections → 下游 `$input` / `$inputs` 传递

**config（配置信息，只读）**
- 环境相关的配置项（如 API 地址、超时时间等），**运行时只读**
- 支持多环境配置（dev/staging/prod）
- 便于部署时动态替换
- DSL key：`context.config`，表达式中通过 `$config.key` 访问
- 节点不可修改 `$config`；环境差异通过部署时替换 `context.config` 值实现

**「放 vars 还是 config？」决策表**：

| 判断条件 | 放哪里 |
|---------|--------|
| 这个值随环境（dev/prod）变化？ | `config` → `$config` |
| 运维/部署时需要覆盖？ | `config` → `$config` |
| 业务逻辑常量、阈值、标签？ | `vars` → `$vars` |

> 简单记法：**随环境变的用 config，业务逻辑常量用 vars**。

**优势**：
- ✅ 集中管理全局数据，避免重复定义
- ✅ 支持环境隔离和动态配置
- ✅ 简化节点参数配置

### 2.5 表达式引擎

使用 **Expr** (expr-lang/expr) 作为表达式引擎：
- 类型安全
- 高性能（编译为字节码）
- 支持管道操作
- 沙箱隔离

## 3. YAML DSL 规范

### 3.1 基本结构

```yaml
# 元信息
spec: string              # DSL schema 版本（必填，如 "1.0"）
id: string                # 工作流 ID（可选，UUID v4；UI 创建时必带，YAML 手写时可省略由系统生成）
name: string              # 工作流名称（必填）
version: string           # 工作流业务版本号（必填，语义化版本）
description: string       # 描述（可选）

# 触发器（支持多个，任一激活即启动工作流）
triggers:
  - type: string          # manual/webhook/cron/event/queue
    parameters: object    # 触发器参数
    enabled: bool         # 是否启用（可选，默认 true）

# 上下文（全局变量、配置）
context:
  vars: object            # 全局变量，表达式中通过 $vars.key 访问（业务逻辑常量，只读）
  config: object          # 环境配置，表达式中通过 $config.key 访问（随环境变化的值，只读）

# 全局配置
settings:
  timeout: duration       # 超时时间
  concurrency: int        # 最大并发数
  timezone: string        # 时区
  on_error: string        # 全局默认错误策略（可选，stop|error_output|main_output，默认 stop）
  pin_data_mode: string   # pin_data 生效策略（可选，test_only|always|disabled，默认 test_only）
  retry:                  # 重试策略
    enabled: bool
    max_attempts: int
    strategy: string      # fixed/exponential

# 凭证引用
# 所有敏感信息（API Key、密码、Token、DB 连接等）统一在 credentials 中定义
# 在节点参数中通过名称引用：credential: db_conn 或 authentication: api_auth
credentials:
  credential_name:
    name: string          # 凭证名称（对应密钥管理系统中的凭证 ID）
    type: string          # 凭证类型：postgres|mysql|redis|oauth2|apiKey|basic

# 工作流参数定义（表达式中通过 $params 访问）
# 注：顶层 params 定义工作流的输入模式；节点级 inputs 是声明式端口，两者含义不同
params:
  param_name:
    type: string          # string/number/boolean/object/array
    required: bool
    display_name: string
    default: any
    validation: object

# 节点模板（可选，DSL 扩展提案）
# 定义可复用的节点配置模板，节点通过 template 字段引用
# 合并规则：
#   - 对象（map）：递归深度合并，节点级 key 覆盖模板同名 key
#   - 数组（list）：整体替换，不做 append 或元素级合并
#   - 标量（string/number/bool）：节点级覆盖模板值
#   - 显式删除：节点将模板中的 key 设为 null，编译展开后该 key 被移除
# 模板展开在编译阶段第一步完成
node_templates:
  template_name:
    type: string          # 节点类型
    parameters: object    # 模板参数（被引用节点的 parameters 深度合并覆盖）

# 节点定义
nodes:
  - id: string            # 节点 ID（可选，UUID v4；UI 创建时必带，YAML 手写时可省略由系统生成）
    name: string          # 节点名称
    type: string          # 节点类型（xflow.http/xflow.grpc/xflow.if等）
    template: string      # 引用 node_templates 中的模板名（可选，与 type 互斥：template 提供 type）
    position: [x, y]      # UI 位置坐标（可选）
    disabled: bool        # 是否禁用（可选，见下方「禁用行为」）
    on_error: string      # 错误处理策略（可选，stop|error_output|main_output，覆盖全局 settings.on_error）
    notes: string         # 节点备注（可选）
    inputs:               # 声明式输入端口（可选）
      - name: string      #   端口名称
        required: bool    #   是否必须连线
    output_schema: object # 输出数据 Schema（可选，JSON Schema 子集，用于编译期字段校验）
    ui: object            # UI 元数据（可选，运行时忽略，用于编辑器状态持久化）
                          #   collapsed: bool       - 节点折叠状态
                          #   color: string         - 节点颜色
                          #   group: string         - 分组标签
                          #   width: int            - 节点宽度
    parameters: object    # 节点参数

# 节点禁用行为
# disabled: true 的节点在编译后被标记为 skipped：
#   - 引擎不执行该节点，状态直接置为 skipped
#   - skipped 视为「已完成」：下游节点的依赖判定中，skipped 等同于 success
#   - 下游节点通过 $nodes['disabled_node'] 访问时返回 nil（与未执行节点一致）
#   - 连线保留（不重连）：disabled 节点的出边目标仍然生效，入边来源仍然计入上游
# 典型用途：调试时临时跳过某节点，不破坏工作流拓扑

# 连接定义
connections:
  source_node:
    output_port:
      - node: target_node
        input: input_port # 可选，默认 main

# 输出定义
outputs:
  output_name:
    value: expression
    display_name: string

# 测试数据钉住（可选，调试用途）
# 钉住的节点跳过实际执行，直接使用 mock 输出
# 下游节点通过 $nodes['xxx'] / $input 正常访问钉住数据
# 详见 §8「Pin Data」
pin_data:
  node_name: object       # key = 节点名，value = mock 输出数据
```

### 3.2 完整示例

```yaml
spec: "1.0"
name: "order-processing"
version: "1.0.0"
description: "订单处理工作流"

# 触发器（多触发器：webhook + 手动均可启动）
triggers:
  - type: webhook
    parameters:
      path: "/webhook/order"
      methods: ["POST"]
      auth:
        type: apiKey
        credential: webhook_auth
  - type: manual

# 上下文
context:
  # 全局变量（表达式中通过 $vars.key 访问；业务逻辑常量）
  vars:
    max_retry_count: 3
    default_timeout: 30
    notification_channels:
      - email
      - slack
    business_rules:
      min_order_amount: 1.0
      max_order_amount: 100000.0

  # 配置信息（表达式中通过 $config.key 访问，随环境变化的值）
  config:
    env: "production"
    region: "us-west-2"
    service_endpoints:
      order: "https://order.example.com"
      payment: "https://payment.example.com"
      inventory: "https://inventory.example.com"
      notification: "https://notification.example.com"
    feature_flags:
      enable_new_pricing: true
      enable_parallel_check: true

# 全局配置
settings:
  timeout: 3600s
  concurrency: 10
  timezone: "Asia/Shanghai"

  retry:
    enabled: true
    max_attempts: 3
    strategy: exponential
    initial_interval: 1s
    max_interval: 60s
    multiplier: 2.0

# 凭证（加密存储，表达式中通过 getCredential('name') 获取）
credentials:
  api_auth:
    name: "api_credentials"
    type: apiKey

  db_conn:
    name: "postgres_prod"
    type: postgres

# 工作流参数定义（表达式中通过 $params.key 访问）
params:
  order_id:
    type: string
    required: true
    display_name: "订单ID"

  user_id:
    type: string
    required: true
    display_name: "用户ID"

  amount:
    type: number
    required: true
    display_name: "订单金额"
    default: 0
    validation:
      min: 0.01
      max: 1000000

# 节点定义
nodes:
  # 1. 验证订单
  - name: validate_order
    type: xflow.http
    position: [250, 300]
    notes: "验证订单信息"

    parameters:
      method: POST
      url: "{{ $config.service_endpoints.order }}/validate"

      authentication: api_auth        # 声明式引用：Handler 自动设置 Authorization header
      headers:
        X-Environment: "{{ $config.env }}"
        X-Tenant-ID: "${{ getCredential('api_auth').tenant_id }}"  # 表达式引用：获取声明式无法自动注入的额外字段

      body:
        order_id: "${{ $params.order_id }}"
        user_id: "${{ $params.user_id }}"
        amount: "${{ $params.amount }}"
        timestamp: "${{ dateFormat(now(), '2006-01-02') }}"

      options:
        timeout: "${{ $vars.default_timeout * 1000 }}"
        retry:
          enabled: true
          max_attempts: "${{ $vars.max_retry_count }}"

  # 2. 验证结果分支（使用 xflow.if 二元判断）
  - name: check_validation
    type: xflow.if
    position: [450, 300]

    parameters:
      condition: "${{ $nodes['validate_order'].is_valid == true }}"

  # 3. 检查库存
  - name: check_inventory
    type: xflow.grpc
    position: [650, 200]

    parameters:
      service: inventory.InventoryService
      method: CheckStock
      host: inventory:50051

      request:
        order_id: "${{ $params.order_id }}"
        items: "${{ $nodes['validate_order'].items }}"

  # 4. 计算价格
  - name: calculate_price
    type: xflow.function
    position: [650, 300]

    parameters:
      code: "${{ $nodes['validate_order'].items | map(#.price * #.quantity * 0.9) | sum() }}"

  # 5. 合并并行结果（声明式输入端口）
  - name: merge_checks
    type: xflow.merge
    position: [850, 250]
    inputs:
      - name: inventory
        required: true
      - name: price
        required: true

    parameters:
      mode: wait_all

  # 6. 支付处理
  - name: process_payment
    type: xflow.http
    position: [1050, 250]

    parameters:
      method: POST
      url: "{{ $config.service_endpoints.payment }}/process"
      authentication: oauth2

      body:
        order_id: "${{ $params.order_id }}"
        amount: "${{ $inputs.price.total }}"

      options:
        timeout: 60000

  # 7. 支付结果分支（使用 xflow.switch 多路判断）
  - name: payment_result
    type: xflow.switch
    position: [1250, 250]

    parameters:
      outputs: [success, failed]
      rules:
        - condition: "${{ $nodes['process_payment'].status == 'success' }}"
          output: success
      default_output: failed

  # 8. 发送成功通知
  - name: send_success_email
    type: xflow.notification
    position: [1450, 150]

    parameters:
      channel: email
      to: "${{ $nodes['validate_order'].user_email }}"
      subject: "订单支付成功"
      template: payment_success

  # 9. 更新订单状态
  - name: update_order
    type: xflow.database
    position: [1650, 150]

    parameters:
      operation: update
      table: orders
      credential: db_conn

      where:
        id: "${{ $params.order_id }}"

      data:
        status: paid
        paid_at: "${{ now() }}"
        transaction_id: "${{ $nodes['process_payment'].transaction_id }}"

  # 10. 发送失败通知
  - name: send_failure_email
    type: xflow.notification
    position: [1450, 350]

    parameters:
      channel: email
      to: "${{ $nodes['validate_order'].user_email }}"
      subject: "订单支付失败"

  # 11. 无效订单通知
  - name: invalid_notification
    type: xflow.notification
    position: [650, 450]

    parameters:
      channel: email
      subject: "订单验证失败"

  # 12. 最终合并
  - name: final_merge
    type: xflow.merge
    position: [1850, 250]

    parameters:
      mode: wait_any

  # 13. 记录日志
  - name: log_result
    type: xflow.database
    position: [2050, 250]

    parameters:
      operation: insert
      table: workflow_logs
      data:
        workflow_id: "${{ $workflow.id }}"
        execution_id: "${{ $execution.id }}"
        order_id: "${{ $params.order_id }}"
        completed_at: "${{ now() }}"

# 连接定义
connections:
  # 验证流程
  validate_order:
    main:
      - node: check_validation

  # 验证分支（xflow.if 的 true/false 端口）
  check_validation:
    true:
      - node: check_inventory
      - node: calculate_price
    false:
      - node: invalid_notification

  # 合并结果（声明式端口输入）
  check_inventory:
    main:
      - node: merge_checks
        input: inventory

  calculate_price:
    main:
      - node: merge_checks
        input: price

  # 支付处理
  merge_checks:
    main:
      - node: process_payment

  process_payment:
    main:
      - node: payment_result

  # 支付结果分流（xflow.switch 的动态端口）
  payment_result:
    success:
      - node: send_success_email
      - node: update_order
    failed:
      - node: send_failure_email

  update_order:
    main:
      - node: final_merge

  send_failure_email:
    main:
      - node: final_merge

  send_success_email:
    main:
      - node: final_merge

  # 无效订单
  invalid_notification:
    main:
      - node: final_merge

  # 记录日志
  final_merge:
    main:
      - node: log_result

# 输出定义
outputs:
  status:
    value: "${{ $nodes['payment_result'].output }}"
    display_name: "最终状态"

  transaction_id:
    value: "${{ $nodes['process_payment'].transaction_id }}"
    display_name: "交易ID"
```

## 4. 表达式引擎

### 4.1 表达式语法

XFlow 使用 **Expr** (expr-lang/expr) 作为表达式引擎，提供两种动态值语法：

| 语法 | 模式 | 返回类型 | 场景 |
|------|------|---------|------|
| `${{ expr }}` | 表达式求值 | **保留原始类型**（number/bool/object/array/string） | 整个值是一个表达式 |
| `{{ expr }}` | 字符串插值 | **始终 string** | 文本中嵌入动态片段 |

**解析规则**：

1. 值以 `${{` 开头且以 `}}` 结尾 → **表达式模式**，整体交 Expr 求值，保留返回类型
2. 值包含 `{{ }}` 但不匹配规则 1 → **插值模式**，逐段求值后拼接为 string
3. 不含 `{{ }}` → 静态字面量

> **注意**：不支持 `={{ }}`、`${ }`、`#{ }` 等其他模板语法。引擎遇到非上述两种模式的花括号视为纯文本。

**表达式模式** `${{ expr }}`：

```yaml
# 访问工作流输入参数
field: "${{ $params.order_id }}"

# 访问节点数据
field: "${{ $nodes['validate_order'].is_valid }}"

# 函数调用
field: "${{ upper($params.name) }}"

# 三元表达式
field: "${{ $params.status == 'ok' ? 'success' : 'failed' }}"

# 空值合并
field: "${{ $nodes['fetch_user'].name ?? 'Anonymous' }}"

# 数值计算 → 返回 number
timeout: "${{ $vars.default_timeout * 1000 }}"

# 布尔判断 → 返回 bool
is_vip: "${{ $params.amount > 1000 }}"

# 数组操作 → 返回 array
items: "${{ $nodes['fetch'].data | filter(#.active) }}"
```

**插值模式** `{{ expr }}`：

```yaml
# 拼接 URL（比 ${{ $config.api_base_url + '/api/v1/users' }} 更清晰）
url: "{{ $config.api_base_url }}/api/v1/users"

# 拼接消息（比 sprintf 更直观）
subject: "订单 {{ $params.order_id }} 共 {{ $params.amount }} 元"

# 拼接 Header
auth: "Bearer {{ getCredential('api_auth').token }}"

# 拼接日志
log_msg: "[{{ $config.env }}] user={{ $params.user_id }} action={{ $params.action }}"
```

**边界规则**：

```yaml
# ✅ 合法 — 表达式模式（整个值是一个表达式）
timeout: "${{ $vars.x * 1000 }}"

# ✅ 合法 — 插值模式（文本中嵌入多段表达式）
msg: "Hello {{ $params.name }}, order {{ $params.id }}"

# ✅ 合法 — 插值模式（单段，结果为 string）
name: "{{ $params.name }}"

# ❌ 非法 — ${{ 必须包裹整个值，不允许前后有文本
bad: "${{ $params.x }} 后面有文本"    # 编译报错
bad: "前面有文本 ${{ $params.x }}"    # 编译报错

# ✅ 合法 — 纯文本，花括号不受影响
note: "格式参考 {name} 占位符"
```

### 4.2 数据访问

所有系统变量统一使用 `$` 前缀。分为两种访问模式：
- **属性访问**（`$xxx.field` / `$xxx['key']`）— 读取已有数据
- **方法调用**（`getXxx('name')`）— 运行时操作（涉及外部系统调用）

#### 变量全览

| 分类 | 变量 | 说明 | 示例 |
|------|------|------|------|
| **数据访问** | `$params` | 工作流参数（DSL 顶层 `params` 字段定义，触发时传入） | `$params.order_id` |
| | `$input` | 上游节点传入的数据 | `$input.amount` |
| | `$inputs.port_name` | 多条入边时按端口名区分 | `$inputs.order_data.id` |
| | `$nodes['name']` | 按节点名引用任意节点输出 | `$nodes['validate_order'].is_valid` |
| **上下文** | `$vars` | 全局变量（context.vars，只读） | `$vars.max_retry_count` |
| | `$config` | 环境配置（context.config，只读） | `$config.env` |
| **运行时获取** | `getCredential('name')` | 从凭证管理模块获取凭证（加密存储，运行时解密） | `getCredential('api_auth').token` |
| **运行时信息** | `$execution` | 执行上下文 | `$execution.id`、`$execution.mode` |
| | `$workflow` | 工作流信息 | `$workflow.name`、`$workflow.version` |
| | `$env` | 系统环境变量 | `$env.API_URL` |
| **循环** | `$item` | 循环当前项 | `$item.id` |
| | `$items` | 循环全量数据 | `$items` |
| | `$index` | 循环下标 | `$index` |

#### 命名规则

| 规则 | 示例 | 说明 |
|------|------|------|
| 集合 → 复数 | `$nodes`、`$inputs`、`$items` | 包含多个命名条目的对象 |
| 单一对象 → 单数 | `$input`、`$item`、`$config` | 单个实体 |
| 行业惯例 → 从俗 | `$env`（非 `$envs`） | 尊重开发者肌肉记忆 |
| 全称 → 无缩写 | `$vars`（对应 `context.vars`，非 `$ctx`）、`$params`（非 `$p`） | 可读性优先 |
| `$xxx` → 数据引用 | `$nodes['x'].field` | 读取已有数据 |
| `getXxx()` → 运行时操作 | `getCredential('name')` | 涉及外部系统调用 |

#### 数据访问路径

**`$input` — 访问上游传入的数据**
```yaml
# 编写节点逻辑时，通过 $input 获取上游节点传过来的值
amount: "${{ $input.amount }}"
is_valid: "${{ $input.is_valid }}"
```

**`$inputs.port_name` — 多条入边时按端口区分**
```yaml
# 节点有多条入边时，通过端口名区分各上游的数据
inventory: "${{ $inputs.inventory.in_stock }}"
price: "${{ $inputs.price.total }}"
```

**`$nodes['name']` — 按节点名引用（始终可用）**
```yaml
amount: "${{ $nodes['calculate_price'].total }}"
```

> **`$input` vs `$inputs`**：
> - `$input` 是 `$inputs.main` 的语法糖，用于访问上游传入的数据。
> - `$inputs.port_name` 用于节点有多条入边时，按端口名区分各上游的数据。
> - 未声明 `inputs` 的节点隐含 `main` 输入端口（connections 中未指定 `input:` 的连线默认进入 `main` 端口）。
> - 声明了 `inputs` 的节点仅包含所声明的端口；若未包含 `main`，则 `$input` 为 `nil`。
> - 未声明 `inputs` 且有多条入边：编译 **warning**，建议声明 `inputs` 端口。

#### 未执行节点的引用行为

`$nodes['x']` 引用一个**存在于工作流中但当前执行路径上未运行**的节点时，返回 `nil`（而非报错）。这在条件分支场景中很常见——switch 路由后只有一条路径激活，其他路径的节点不会执行。

**运行时规则**：

| 情况 | 行为 |
|------|------|
| 节点已执行且成功 | 返回节点输出数据 |
| 节点已执行但失败（`on_error` 为 `error_output` 或 `main_output`） | 按策略返回对应数据 |
| 节点存在但未执行 | 返回 `nil` |
| 节点名不存在于工作流定义 | **编译期报错** |

**编译期校验**：

编译器通过 DAG 拓扑分析判断 `$nodes['x']` 的引用安全性：

| 拓扑关系 | 编译器行为 |
|---------|-----------|
| `x` 是当前节点的确定性祖先（所有入边路径都经过 `x`） | 安全，无警告 |
| `x` 与当前节点在不同分支上（可能未执行） | **warning**：`$nodes['x'] 可能为 nil，建议使用 ?? 提供默认值` |
| `x` 是当前节点的下游 | **error**：禁止前向引用（数据尚未产生） |
| `x` 不存在于工作流定义 | **error**：节点不存在 |

**推荐用法**：

```yaml
# ✅ 跨分支引用：使用 ?? 提供默认值
trail: "${{ $nodes['l1_manager'] ?? null }}"
name:  "${{ $nodes['optional_step'].name ?? 'default' }}"

# ✅ 确定性祖先引用：无需 ??
amount: "${{ $nodes['validate_order'].amount }}"

# ⚠️ 跨分支引用未加 ??：编译器 warning（运行时不报错，返回 nil）
trail: "${{ $nodes['l1_manager'] }}"
```

#### 上下文访问示例

```yaml
# 访问全局变量（业务逻辑常量）
timeout: "${{ $vars.default_timeout * 1000 }}"

# 访问配置
endpoint: "${{ $config.service_endpoints.payment }}"
is_production: "${{ $config.env == 'production' }}"

# 获取凭证（运行时从加密存储解密）
auth_header: "Bearer {{ getCredential('api_auth').token }}"

# 组合使用
validation_rule: "${{ $params.amount >= $vars.business_rules.min_order_amount && $params.amount <= $vars.business_rules.max_order_amount }}"
```

#### 凭证引用

凭证有两种引用方式，适用场景不同：

**方式 1：声明式引用（节点参数）**

节点通过特定参数字段按名称引用凭证，Handler 内部负责读取凭证并正确应用（如设置 Authorization header、建立数据库连接等）。参数名因节点类型而异：

```yaml
# HTTP 节点 — authentication 参数
- name: call_api
  type: xflow.http
  parameters:
    authentication: api_auth     # Handler 读取凭证并自动设置请求认证

# Database 节点 — credential 参数
- name: save_data
  type: xflow.database
  parameters:
    credential: db_conn          # Handler 读取凭证并建立数据库连接
```

**方式 2：表达式引用（`getCredential()`）**

在表达式中显式获取凭证对象的特定字段，用于需要精细控制的场景：

```yaml
headers:
  X-Custom-Auth: "Bearer {{ getCredential('api_auth').token }}"
  X-API-Key: "${{ getCredential('api_auth').api_key }}"
```

**选择决策**：

| 判断条件 | 使用方式 |
|---------|---------|
| 节点类型已内置该凭证的标准用法（认证、连接） | 声明式（`authentication` / `credential`） |
| 需要在自定义 header、body 或表达式中使用凭证的特定字段 | 表达式（`getCredential('name').field`） |
| 同一节点中两种方式都需要 | 可共存——声明式用于主认证，表达式用于额外字段 |

> **约束**：两种方式都要求凭证名必须在顶层 `credentials` 块中预先声明，未声明的凭证名编译期报错。各节点类型支持的声明式参数名见 §6.2 节点配置。

### 4.3 内置函数

**日期时间**：
- `now()` - 当前时间
- `today()` - 今天日期
- `dateFormat(t, layout)` - 格式化时间

**字符串**：
- `upper(s)` - 转大写
- `lower(s)` - 转小写
- `trim(s)` - 去空格
- `replace(s, old, new)` - 替换
- `substr(s, start, length)` - 子串
- `split(s, sep)` - 分割
- `join(arr, sep)` - 连接
- `sprintf(format, args...)` - 格式化字符串（同 Go `fmt.Sprintf`）

**数组**：
- `len(v)` - 长度
- `first(arr)` - 首元素
- `last(arr)` - 尾元素

**JSON**：
- `parseJson(s)` - 解析JSON
- `toJson(v)` - 转JSON字符串

**编码**：
- `base64Encode(s)` - Base64编码
- `base64Decode(s)` - Base64解码
- `urlEncode(s)` - URL编码
- `md5(s)` - MD5哈希
- `sha256(s)` - SHA256哈希

**其他**：
- `uuid()` - 生成UUID
- `abs(x)` - 绝对值
- `ceil(x)` - 向上取整
- `floor(x)` - 向下取整
- `round(x)` - 四舍五入
- `if(cond, t, f)` - 三元表达式
- `isEmpty(v)` - 是否为空

### 4.4 管道操作

Expr 支持强大的管道操作：

```yaml
# 过滤并映射
"${{ $nodes['items'].data | filter(#.price > 100) | map(#.name) }}"

# 排序
"${{ $nodes['items'].data | sortBy('price') }}"

# 聚合
"${{ $nodes['items'].data | map(#.price) | sum() }}"

# 复杂处理
"${{ $nodes['items'].data
     | filter(#.active == true)
     | map(#{id: #.id, total: #.price * #.quantity})
     | sortBy('total')
}}"

# 字符串链式
"${{ $params.text | trim() | upper() | substr(0, 10) }}"
```

**管道操作符**：
- `|` - 管道符
- `#` - 当前项占位符
- `filter(#.price > 100)` - 过滤
- `map(#.name)` - 映射
- `sortBy('field')` - 排序
- `sum()` - 求和

### 4.5 表达式示例

```yaml
# 简单计算
total: "${{ $params.price * $params.quantity }}"

# 字符串插值
fullName: "{{ $params.firstName }} {{ $params.lastName }}"

# 条件判断
isVip: "${{ $params.amount > 1000 && $nodes['user'].level == 'vip' }}"

# 函数调用
userName: "${{ upper(trim($params.name)) }}"

# 日期格式化
createdAt: "${{ dateFormat(now(), '2006-01-02T15:04:05Z07:00') }}"

# 数据转换
items: "${{ $nodes['fetch'].items | map(#{
  id: #.id,
  name: upper(#.name),
  price: #.price * 0.9
}) }}"
```

## 5. Connections 机制

### 5.1 基本概念

Connections 显式定义节点间的数据流和连接关系。

**结构**：
```yaml
connections:
  source_node:        # 源节点名称
    output_port:      # 输出端口名称
      - node: target_node    # 目标节点名称
        input: input_port    # 目标输入端口（可选）
```

### 5.2 输出端口类型

| 端口名称 | 说明 | 使用场景 |
|---------|------|----------|
| `main` | 主输出 | 所有节点的默认输出 |
| `error` | 错误输出 | 节点执行失败时 |
| `success` | 成功输出 | Switch 节点的成功分支 |
| `failed` | 失败输出 | Switch 节点的失败分支 |
| 自定义 | 自定义输出 | Switch 节点可定义 |

### 5.3 连接模式

#### 顺序连接
```yaml
connections:
  step1:
    main:
      - node: step2
  step2:
    main:
      - node: step3
```

#### 并行分支
```yaml
connections:
  start:
    main:
      - node: branch_a
      - node: branch_b
      - node: branch_c
```

#### 条件路由（xflow.if）
```yaml
nodes:
  - name: is_valid
    type: xflow.if
    parameters:
      condition: "${{ $nodes['validate'].is_valid }}"

connections:
  is_valid:
    true:
      - node: process_handler
    false:
      - node: reject_handler
```

#### 多路分支（xflow.switch）
```yaml
nodes:
  - name: check_status
    type: xflow.switch
    parameters:
      outputs: [success, failed, pending]

connections:
  check_status:
    success:
      - node: success_handler
    failed:
      - node: failed_handler
    pending:
      - node: pending_handler
```

#### 分支合并
```yaml
connections:
  branch_a:
    main:
      - node: merge_node
        input: input_1

  branch_b:
    main:
      - node: merge_node
        input: input_2
```

#### 错误处理
```yaml
connections:
  risky_operation:
    main:
      - node: next_step
    error:
      - node: error_handler
```

### 5.4 错误处理机制

节点执行失败（Worker 回调 `ReportFailed`，且重试耗尽）时，引擎根据 `on_error` 策略决定行为。

**策略**：

| 值 | 行为 |
|---|------|
| `stop` (默认) | 工作流终止，状态置为 `failed` |
| `error_output` | 走 `error` 端口（未连线则 `stop`），工作流继续 |
| `main_output` | 走 `main` 端口，下游通过 `$input._error` 获取错误信息，工作流继续 |

**策略推导优先级**：

1. 节点显式 `on_error` → 使用它
2. `settings.on_error` → 使用全局策略
3. 均未配置 → `stop`

**`error_output` 下游数据**（通过 `$input` 接收）：

```yaml
# $input 结构
failed_node: string       # 失败节点名
error:
  code: string            # 错误码
  message: string         # 错误信息
  retryable: bool         # 是否可重试
execution_id: string
timestamp: time
```

**`main_output` 下游数据**（错误附加在 `$input._error`）：

```yaml
# 节点成功时：$input._error 不存在
# 节点失败时：$input._error 包含错误详情，其余输出字段为零值
_error:
  code: string
  message: string
  retryable: bool
```

**示例**：

```yaml
settings:
  on_error: error_output    # 全局：失败走 error 端口

nodes:
  - name: call_api
    type: xflow.http
    # 未设置 on_error → 使用全局 error_output
    # 未连 error 端口 → 降级为 stop
    parameters: ...

  - name: optional_notify
    type: xflow.http
    on_error: main_output   # 覆盖全局：通知失败不影响主流程
    parameters: ...

  - name: critical_step
    type: xflow.http
    on_error: stop          # 覆盖全局：必须成功，失败即终止
    parameters: ...
```

### 5.5 验证规则

- ✅ 源节点和目标节点必须存在
- ✅ 输出端口必须是节点支持的
- ✅ 不允许循环依赖
- ✅ 每个节点至少有一个输入或是起始节点
- ✅ 至少有一个终止节点
- ✅ cron 触发器：`params` 中 `required: true` 且无 `default` 的字段，必须在该触发器的 `parameters.static_input` 中提供（否则编译 **error**）

### 5.6 设计决策：连线不承载条件逻辑

XFlow 的 connections 仅描述拓扑关系（谁连到谁），条件逻辑由 `xflow.if` / `xflow.switch` 节点承担。这意味着即使最简单的二元判断也需要创建一个中间节点。

**权衡取舍**：

| 方案 | 优势 | 劣势 |
|------|------|------|
| ✅ 当前方案：条件在节点中 | 拓扑与逻辑分离，connections 结构统一，编辑器实现简单 | 简单条件也需额外节点 |
| ❌ 备选：连线上附加 filter | 减少节点数量，简单场景更简洁 | connections 结构变复杂，编辑器需渲染连线条件，调试时数据流更难追踪 |

当前方案优先保证 **拓扑与逻辑的正交分离**：connections 永远只回答"数据流向哪里"，节点回答"数据是否/如何流转"。

## 6. 节点类型

### 6.1 核心节点

每个节点通过 Handler 的 `Descriptor()` 方法声明支持的输入/输出端口。编译器和编辑器据此校验连接合法性和渲染端口连接点。

节点可声明可选的 `output_schema`（JSON Schema 子集），填写后编译器可静态校验其他节点通过 `$nodes['x'].field` 引用该节点输出字段的合法性。

| 节点类型 | 标识符 | 说明 | 输入端口 | 输出端口 | 动态端口 | output_schema |
|---------|--------|------|---------|---------|---------|--------------|
| HTTP请求 | xflow.http | HTTP/HTTPS 请求 | main | main, error | ❌ | 可选 |
| gRPC调用 | xflow.grpc | gRPC 服务调用 | main | main, error | ❌ | 可选 |
| 函数执行 | xflow.function | 执行 Go 函数或内联代码 | main | main, error | ❌ | 可选 |
| 数据库操作 | xflow.database | 数据库 CRUD 操作 | main | main, error | ❌ | 可选 |
| 通知 | xflow.notification | 发送通知（邮件/短信/Slack等） | main | main, error | ❌ | 可选 |
| 条件判断 | xflow.if | 二元条件分支（true/false） | main | true, false | ❌ | 不适用 |
| 多路分支 | xflow.switch | 根据条件多路分支 | main | _(无静态)_ | ✅ `parameters.outputs` | 不适用 |
| 循环 | xflow.loop | 循环处理数据 | main | main, error | ❌ | 可选 |
| 等待 | xflow.wait | 等待事件或时间 | main | main, timeout, error | ❌ | 可选 |
| 合并 | xflow.merge | 合并多个分支 | 动态（`input: xxx`） | main | ❌ | 可选 |
| 分割 | xflow.split | 数组扇出为并行路径 | main | main | ❌ | 可选 |

> **并行执行**：XFlow 不提供 `xflow.parallel` 节点。并行通过 connections 天然实现——一个输出端口连接多个目标节点即为并行分支，用 `xflow.merge` 汇合。如需控制并行度，使用 `xflow.loop`（`batch_size` + `max_concurrency`）。

### 6.2 节点配置

#### HTTP 节点
```yaml
- name: api_call
  type: xflow.http
  parameters:
    method: POST|GET|PUT|DELETE
    url: string
    authentication: apiKey|oauth2|basic
    headers: object
    query: object
    body: object
    options:
      timeout: int
      retry:
        enabled: bool
        max_attempts: int
```

#### gRPC 节点
```yaml
- name: grpc_call
  type: xflow.grpc
  parameters:
    service: string
    method: string
    host: string
    request: object
    options:
      timeout: int
      metadata: object
```

#### Function 节点

两种模式，`function_name` 与 `code` 互斥：

```yaml
# 模式一：调用预注册的 Go 函数（推荐，支持复杂逻辑）
- name: process
  type: xflow.function
  parameters:
    function_name: string    # Worker 中注册的 Go 函数名
    params: object           # 传给函数的参数，支持表达式

# 模式二：内联 Expr 表达式（仅适合单一返回值的简单计算）
- name: calc
  type: xflow.function
  parameters:
    code: string             # Expr 表达式，返回值即节点输出
                             # 可访问 $params、$nodes、$vars 等所有上下文
                             # 不支持多语句、循环、IO 操作
```

内联 `code` 示例：
```yaml
# 简单计算
code: "${{ $params.price * $params.quantity * 0.9 }}"

# 数组聚合（使用 Expr 管道）
code: "${{ $nodes['fetch'].items | map(#.price * #.qty) | sum() }}"
```

> **注意**：`code` 使用 Expr 表达式引擎执行，不支持 JavaScript/Python 语法。
> 复杂多步骤逻辑请使用 `function_name` 注册 Go 函数。

#### Database 节点
```yaml
- name: db_operation
  type: xflow.database
  parameters:
    operation: select|insert|update|delete
    table: string
    credential: string
    where: object
    data: object
```

#### IF 节点

二元条件判断，固定 `true` / `false` 两个输出端口：

```yaml
- name: is_valid
  type: xflow.if
  parameters:
    condition: expression    # 布尔表达式，true 走 true 端口，false 走 false 端口
```

示例：
```yaml
- name: check_amount
  type: xflow.if
  parameters:
    condition: "${{ $params.amount > 0 && $nodes['validate_order'].is_valid }}"

# connections 中使用具名端口
connections:
  check_amount:
    true:
      - node: process_order
    false:
      - node: reject_order
```

#### Switch 节点

多路分支，通过 `parameters.outputs` 动态声明输出端口：

```yaml
- name: condition
  type: xflow.switch
  parameters:
    mode: rules|expression
    outputs: [string, ...]   # 动态定义输出端口
    rules:
      - condition: expression
        output: string
    default_output: string   # 默认输出（所有 rules 均不匹配时走此端口）
```

> **注意**：`default_output` 是显式的 fallback 端口。禁止在 `rules` 中使用 `condition: true` 模拟默认分支，
> 编译器遇到 `condition: true` 会报警告。所有 rules 的 condition 应是有意义的布尔表达式。

**`expression` 模式**：

表达式直接返回输出端口名（字符串），适用于路由 key 与端口名直接对应的场景，比 rules 逐条匹配更简洁：

```yaml
- name: route_by_type
  type: xflow.switch
  parameters:
    mode: expression
    outputs: [email, sms, webhook]
    expression: "${{ $nodes['parse_message'].type }}"   # 返回值必须是 outputs 中的某个端口名
    default_output: webhook                              # 返回值不在 outputs 中时走此端口
```

> `expression` 模式下 `rules` 字段被忽略。表达式返回值必须为 string 类型，且应为 `outputs` 中声明的端口名之一；
> 返回值不匹配任何端口名时走 `default_output`（未配置 `default_output` 则编译报错）。

#### Loop 节点

循环体通过内嵌 `body` 子图定义，拥有独立的节点命名空间，语法与顶层 `nodes` + `connections` 完全一致，支持条件分支和错误路径：

```yaml
- name: process_items
  type: xflow.loop
  parameters:
    items: expression        # 数组表达式，每个元素依次绑定到 $item / $index
    batch_size: int          # 每批并发数量，默认 1（顺序执行）
    max_concurrency: int     # 最大并发批次数
    continue_on_error: bool  # 单项失败是否继续
    body:                    # 循环体子图（自包含，与外层节点命名空间隔离）
      nodes:                 # 子图节点，语法与顶层 nodes 相同
        - name: string
          type: string
          ...
      connections:           # 子图连接，语法与顶层 connections 相同
        source_node:
          output_port:
            - node: target_node
```

示例（循环体内含条件分支）：
```yaml
nodes:
  - name: batch_processor
    type: xflow.loop
    parameters:
      items: "${{ $nodes['fetch'].items }}"
      batch_size: 10
      max_concurrency: 3
      continue_on_error: true
      body:
        nodes:
          - name: validate_item
            type: xflow.function
            parameters:
              code: "${{ $item.amount > 0 }}"

          - name: call_api
            type: xflow.http
            parameters:
              method: POST
              url: "{{ $vars.api_base_url }}/process"
              body:
                id: "${{ $item.id }}"

          - name: skip_log
            type: xflow.function
            parameters:
              code: "${{ 'skip: ' + $item.id }}"

        connections:
          validate_item:
            true:
              - node: call_api    # 校验通过：发起请求
            false:
              - node: skip_log    # 校验不通过：记录跳过
```

> **作用域规则**：
> - `$item`、`$index`、`$items` 仅在 `body` 内可用
> - `body` 内节点可通过 `$nodes['外层节点名']` 读取外层节点输出；外层节点不能反向引用 `body` 内节点
> - `body.nodes` 中的节点名仅在循环体内有效，与顶层节点命名空间完全隔离（允许同名）
> - `$nodes['batch_processor'].result` 返回所有迭代结果的数组，每项对应一次迭代中最后执行节点的输出
> - **`continue_on_error: true` 时的结果结构**：
>   - 成功项：返回该迭代最后执行节点的正常输出
>   - 失败项：返回 `{ "_error": { "code": string, "message": string, "retryable": bool }, "_index": number }`，其余字段为零值
>   - 下游可通过表达式过滤成功项：`${{ $nodes['batch_processor'].result | filter(!has(#, '_error')) }}`
>
> **跨域引用编译规则**：
> - `body` 内 `$nodes['x']` 中 `x` 不在 `body.nodes` 中时，编译器视为**跨域引用**
> - 跨域引用仅允许读取 loop 节点的上游祖先节点（DAG 拓扑序中确定在 loop 之前完成的节点）
> - 引用不可达的外层节点（如 loop 的下游节点或无关分支节点）编译报错
> - 合法的跨域引用产生 **info 级提示**，帮助用户意识到隐式依赖

#### Merge 节点

```yaml
- name: merge
  type: xflow.merge
  parameters:
    mode: wait_all|wait_any  # wait_all：等待所有输入分支；wait_any：取最先到达的分支
    on_others: cancel        # 仅 wait_any 有效：cancel（尽力取消）| detach（继续运行但忽略输出）
                             # 默认 cancel；cancel 是 best-effort，已产生的副作用不会回滚
```

> ⚠️ **wait_any + cancel 使用限制**：此模式适用于**无副作用的竞速**（如多节点缓存查询取最快响应、超时兜底）。
> 若并行分支包含有副作用的操作（写库、调用外部 API、扣减库存等），`cancel` 只是尽力取消，
> **不提供补偿事务，已产生的副作用不会回滚**。此类场景应使用 `wait_all` 或在业务层实现 Saga 补偿。

**输出数据结构**：

`wait_all` 模式 — 以各输入端口名为 key，聚合所有分支结果：
```yaml
# connections 中定义了 input: inventory 和 input: price
# $nodes['merge'] 的结构为：
# {
#   "inventory": <check_inventory 的输出>,
#   "price":     <calculate_price 的输出>
# }

# 下游访问示例（按节点名）
stock_ok: "${{ $nodes['merge_checks'].inventory.in_stock }}"
total:    "${{ $nodes['merge_checks'].price.total }}"

# 下游访问示例（声明式端口，推荐）
# 如果下游节点声明了 inputs，可通过端口名访问，解耦节点名
stock_ok: "${{ $inputs.inventory.in_stock }}"
total:    "${{ $inputs.price.total }}"
```

`wait_any` 模式 — 直接返回最先完成的那个分支的输出（无 key 包装）：
```yaml
# $nodes['final_merge'] 即最先到达分支的原始输出
result: "${{ $nodes['final_merge'].status }}"
```

#### Split 节点

将数组拆分为独立数据项，每项沿下游 connections 路径独立执行。与 `xflow.loop` 的区别：loop 通过内嵌 `body` 子图定义迭代体，split 通过下游 connections 定义扇出路径，用 `xflow.merge` 汇合结果。

```yaml
- name: fan_out
  type: xflow.split
  parameters:
    items: expression            # 数组表达式（必填）
    batch_size: int              # 每批并发数量（可选，默认无限制，即全部并行）
    continue_on_error: bool      # 单项失败是否继续（可选，默认 false）
```

**执行行为**：

1. 对 `items` 数组的每个元素，引擎为下游路径创建一个独立的执行分支
2. 每个分支中，下游节点通过 `$input` 访问当前元素
3. 所有分支执行完毕后，split 节点的 `main` 端口输出结果数组（与 loop 的 `result` 格式相同）
4. 下游 merge 节点可汇合这些并行分支

**示例**：

```yaml
nodes:
  - name: split_orders
    type: xflow.split
    parameters:
      items: "${{ $nodes['fetch'].orders }}"
      batch_size: 5

  - name: process_order
    type: xflow.http
    parameters:
      method: POST
      url: "{{ $config.order_service }}/process"
      body:
        order_id: "${{ $input.id }}"

connections:
  split_orders:
    main:
      - node: process_order
```

> **何时用 split vs loop**：
> - 扇出路径是**现有 connections 中的多节点链路**（含分支、合并等复杂拓扑）→ 用 `split`
> - 扇出路径是**简单的几步操作**，不想污染顶层节点命名空间 → 用 `loop`（body 子图隔离）
>
> **`$input` vs `$item` 命名差异**：
> - `split` 的下游是顶层 connections 中的常规节点，每个并行分支将数组元素作为上游输出传递，因此 `$input`（标准上游访问器）自然指向当前元素。
> - `loop` 的 `body` 是隔离子图，子图内节点拥有自己的 connections 和 `$input`。若复用 `$input` 表示迭代元素会与子图内部连线数据冲突，因此使用独立的 `$item` 消除歧义。

#### Wait 节点

暂停工作流执行，等待外部信号、固定时长或到达指定时间点。常用于人工审批、定时延迟、等待外部系统回调等场景。

```yaml
- name: wait_approval
  type: xflow.wait
  parameters:
    mode: signal|timer       # 等待模式（必填）

    # signal 模式：等待外部信号唤醒（人工审批、Webhook 回调等）
    signal_name: string      # 信号名称，通过 API POST /executions/{id}/signal 触发
                             # body: { "name": "<signal_name>", "payload": {} }
    timeout: duration        # 超时时间（可选，超时走 timeout 输出端口）

    # timer 模式：等待固定时长或到达指定时间点（二选一）
    duration: duration       # 等待时长（如 "24h"、"30m"）
    until: expression        # 等待到指定时间（表达式，需返回 time.Time 类型）
```

输出端口：`main`（信号到达 / 计时结束）、`timeout`（超时，仅 signal 模式有效）、`error`

示例（人工审批）：
```yaml
- name: wait_manager_approval
  type: xflow.wait
  parameters:
    mode: signal
    signal_name: manager_approved
    timeout: 48h             # 48 小时内无人审批则走 timeout 端口

connections:
  wait_manager_approval:
    main:
      - node: process_order  # 审批通过
    timeout:
      - node: notify_timeout # 超时提醒
```

> **唤醒方式**：调用 `Engine.SendSignal(executionID, signal)` 或通过 HTTP API：
> `POST /api/v1/executions/{execution_id}/signal`
> `{"name": "manager_approved", "payload": {"approved_by": "alice"}}`
>
> `signal.Payload` 作为 wait 节点的输出，下游可通过 `$input.approved_by` 访问。

### 6.3 节点 Output Schema

`output_schema` 是节点的可选字段，使用 JSON Schema 子集描述节点输出数据结构。填写后编译器可在静态分析阶段校验下游节点对该节点输出字段的引用是否合法。

**语法**：

```yaml
- name: validate_order
  type: xflow.http
  output_schema:              # 可选；使用 JSON Schema 子集
    type: object
    properties:
      is_valid:   { type: boolean }
      user_email: { type: string }
      items:      { type: array }
    required: [is_valid]
  parameters:
    method: POST
    url: "{{ $config.service_endpoints.order }}/validate"
```

**编译器行为**：

| 情况 | 编译器行为 |
|------|-----------|
| 节点未声明 `output_schema` | 跳过字段校验，`$nodes['x'].any_field` 均视为合法（向后兼容） |
| 节点已声明 `output_schema` | 下游节点通过 `$nodes['x'].field` 引用时，校验 `field` 是否存在于 schema |
| 字段不在 schema 中 | 编译期报错，提示字段不存在 |

**支持的 JSON Schema 关键字**：

```yaml
output_schema:
  type: object                        # 支持：object|array|string|number|boolean
  properties:                         # 支持：对象属性定义
    field_name:
      type: string                    # 支持：类型声明
      description: string             # 支持：字段说明（可选）
      enum: [value, ...]              # 支持：枚举值约束（可选）
  required: [field_name, ...]         # 支持：必填字段列表（可选）
  items:                              # 支持：数组元素 schema（type 为 array 时使用）
    type: object
    properties: ...
```

**不支持的 JSON Schema 关键字**（编译器忽略，不报错）：

`$ref`、`allOf`、`anyOf`、`oneOf`、`not`、`if/then/else`、`additionalProperties`、`patternProperties`、`minItems`、`maxItems`、`minLength`、`maxLength`、`pattern`、`format`

> 设计意图：`output_schema` 仅用于编译期字段存在性校验和编辑器自动补全，不做运行时数据验证。
> 因此只需描述"有哪些字段、什么类型"，不需要复杂的组合/约束语义。

**使用示例**：

```yaml
nodes:
  - name: fetch_user
    type: xflow.http
    output_schema:
      type: object
      properties:
        user_id:  { type: string }
        name:     { type: string }
        vip_level: { type: number }
      required: [user_id, name]
    parameters:
      method: GET
      url: "{{ $config.service_endpoints.user }}/info"

  - name: check_vip
    type: xflow.if
    parameters:
      # 编译器校验 vip_level 是否在 fetch_user 的 output_schema.properties 中 → 合法
      condition: "${{ $nodes['fetch_user'].vip_level > 3 }}"

  - name: send_notify
    type: xflow.notification
    parameters:
      # 编译器校验 email 是否在 fetch_user 的 output_schema.properties 中 → 报错
      to: "${{ $nodes['fetch_user'].email }}"
```

> **注意**：`output_schema` 是可选的，不填写不影响运行时行为。仅用于提升开发体验，建议在复杂工作流中为关键节点填写。

### 6.4 计划扩展：子工作流

当前 `node_templates` 解决节点级复用，但不支持**流程级复用**（将一段节点 + connections 子图封装为可引用的单元）。对于大型工作流，计划引入 `xflow.subworkflow` 节点：

```yaml
- name: payment_flow
  type: xflow.subworkflow
  parameters:
    workflow: "payment-processing"   # 引用已注册的工作流名称
    version: "1.x"                   # 版本约束（语义化版本范围）
    input:                           # 映射当前上下文到子工作流 $params
      order_id: "${{ $params.order_id }}"
      amount: "${{ $nodes['calculate'].total }}"
    timeout: 300s                    # 子工作流级超时
```

> 子工作流作为独立 Execution 运行，拥有独立的状态和生命周期。父工作流通过 `$nodes['payment_flow']` 访问子工作流的 `outputs`。
> 此特性为计划扩展，当前版本未实现。

## 7. 触发器

### 7.1 手动触发

```yaml
triggers:
  - type: manual
```

### 7.2 Webhook 触发

```yaml
triggers:
  - type: webhook
    parameters:
      path: string             # Webhook 路径
      methods: [string, ...]   # HTTP 方法
      auth:
        type: basic|apiKey|bearer
        credential: string
      validation:
        headers: object
        body: object
      response:
        mode: sync|async
        timeout: duration
```

### 7.3 定时触发

```yaml
triggers:
  - type: cron
    parameters:
      schedule: string         # Cron 表达式
      timezone: string
    enabled: bool              # 可选，默认 true
```

### 7.4 事件触发

```yaml
triggers:
  - type: event
    parameters:
      source: redis|kafka|rabbitmq
      event: string
      filter: expression       # 过滤条件
```

### 7.5 队列触发

```yaml
triggers:
  - type: queue
    parameters:
      queue: string
      prefetch: int
```

### 7.6 多触发器组合

一个工作流可同时配置多个触发器，任一激活即启动工作流：

```yaml
triggers:
  - type: webhook
    parameters:
      path: "/webhook/order"
      methods: [POST]
  - type: manual
  - type: cron
    parameters:
      schedule: "0 2 * * *"   # 每天凌晨 2 点
    enabled: false             # 暂时关闭定时，但保留配置
```

每个触发器的 `enabled` 字段（可选，默认 `true`）可独立控制是否启用，无需删除配置即可暂停某个触发方式。

### 7.7 触发器与 `$params` 映射规则

不同触发器类型以不同方式填充 `$params`。所有触发器填充的 `$params` 必须满足顶层 `params` 中 `required: true` 字段的约束，否则执行拒绝启动。

| 触发器类型 | `$params` 来源 | 说明 |
|-----------|---------------|------|
| `manual` | 用户提交的输入参数（匹配 `params` 定义） | 通过 API 或 UI 提交 |
| `webhook` | HTTP request body（经 `validation` 校验后） | Content-Type 为 JSON |
| `cron` | `{ trigger_time: <time.Time> }` | 包含本次触发的计划时间 |
| `event` | 事件 payload（经 `filter` 过滤后） | 如 Kafka message value |
| `queue` | 消息体 | 队列 message body |

> **注意**：`cron` 触发器的 `$params.trigger_time` 是本次计划触发时间（非实际执行时间），类型为 `time.Time`，
> 可用于日期计算，如 `${{ dateFormat($params.trigger_time, '2006-01-02') }}`。
> 如果 cron 触发的工作流需要额外输入参数，可在触发器 `parameters` 中配置 `static_input` 合并到 `$params`：
>
> ```yaml
> triggers:
>   - type: cron
>     parameters:
>       schedule: "0 2 * * *"
>       static_input:            # 合并到 $params（trigger_time 自动注入，无需手动声明）
>         report_type: "daily"
> ```
>
> **编译期校验**：`cron` 触发器仅注入 `trigger_time`。若工作流顶层 `params` 中存在 `required: true` 且无 `default` 值的字段，
> 且该 cron 触发器未通过 `static_input` 提供该字段，编译器报 **error**：
> `cron 触发器无法满足 params.{field} 的 required 约束，请在 parameters.static_input 中提供`。

## 8. Pin Data（测试数据钉住）

### 8.1 概述

Pin Data 允许在工作流级别为指定节点提供静态模拟输出数据。钉住的节点跳过实际执行（不入队 Asynq），直接使用 mock 数据作为节点输出，下游节点通过 `$nodes['xxx']` / `$input` 正常访问。

**典型用途**：
- 调试时跳过慢节点（HTTP/gRPC 调用），加速工作流验证
- 前端开发时无需真实后端服务即可测试完整流程
- 复现特定场景（如支付失败、库存不足），无需模拟真实外部状态

### 8.2 DSL 语法

在工作流顶层声明 `pin_data` 块，与 `nodes`、`connections` 同级：

```yaml
pin_data:
  node_name:                    # key = 节点名（必须存在于 nodes 中）
    field_a: value              # mock 输出数据（任意结构）
    field_b: value
```

**执行模式控制**：

```yaml
settings:
  pin_data_mode: test_only      # 可选，默认 test_only
```

| 值 | 行为 |
|---|------|
| `test_only`（默认） | 仅 test/debug 模式生效，production 执行忽略 pin_data |
| `always` | 所有执行模式都生效（慎用，调试专用场景） |
| `disabled` | 完全忽略 pin_data（等同于没写） |

### 8.3 与 `disabled` 的区别

| 维度 | `disabled: true` | `pin_data` |
|------|-----------------|------------|
| 执行 | 跳过，状态 `skipped` | 跳过实际执行，状态 `pinned`（视为 `success`） |
| 输出 | `$nodes['x']` → `nil` | `$nodes['x']` → pin_data 中的 mock 数据 |
| 下游影响 | 下游可调度但拿不到数据 | 下游正常运行，数据完整 |
| 用途 | 临时移除节点 | 跳过慢节点，加速调试 |
| 优先级 | — | `disabled: true` 的节点如果在 pin_data 中也有数据，`disabled` 优先（状态为 `skipped`，pin_data 被忽略） |

### 8.4 运行时行为

```
Executor 调度节点前检查：
  │
  ├─ 节点 disabled? → 状态置 skipped，输出 nil，推进下游
  │
  ├─ 节点在 pin_data 中？
  │    ├─ pin_data_mode == disabled → 正常执行
  │    ├─ pin_data_mode == test_only && execution.mode != test → 正常执行
  │    └─ 生效 → 状态置 pinned，输出 = pin_data[node_name]，推进下游
  │
  └─ 正常执行 → 入队 Asynq → Worker 执行
```

**`pinned` 状态语义**：
- 在依赖判定中等同 `success`
- 下游节点通过 `$nodes['xxx']`、`$input`、`$inputs.port` 正常访问 mock 数据
- 执行日志中标记为 `pinned`，便于区分真实执行结果

### 8.5 编译器校验规则

| 规则 | 级别 | 说明 |
|------|------|------|
| pin_data 中的节点名不存在于 nodes | **warning** | 可能是拼写错误或节点已被移除 |
| pin_data 数据不满足节点 output_schema 的 required 字段 | **warning** | mock 数据不完整，下游可能拿到 nil |
| production 工作流配置 pin_data_mode: always | **warning** | 生产环境使用 pin_data 存在风险 |

### 8.6 示例

```yaml
spec: "1.0"
name: "order-processing"
version: "1.0.0"

settings:
  timeout: 3600s
  pin_data_mode: test_only

triggers:
  - type: manual

params:
  order_id:
    type: string
    required: true

nodes:
  - name: fetch_order
    type: xflow.http
    output_schema:
      type: object
      properties:
        order_id: { type: string }
        amount: { type: number }
        status: { type: string }
      required: [order_id, amount]
    parameters:
      method: GET
      url: "{{ $config.order_service }}/orders/{{ $params.order_id }}"

  - name: check_payment
    type: xflow.http
    parameters:
      method: GET
      url: "{{ $config.payment_service }}/status/{{ $nodes['fetch_order'].order_id }}"

  - name: process_result
    type: xflow.function
    parameters:
      code: "${{ $nodes['fetch_order'].amount > 100 ? 'large' : 'small' }}"

connections:
  fetch_order:
    main:
      - node: check_payment
      - node: process_result

# 测试数据钉住：跳过 HTTP 调用，直接使用 mock 数据
pin_data:
  fetch_order:
    order_id: "ORD-001"
    amount: 250.00
    status: "pending"

  check_payment:
    paid: true
    transaction_id: "TXN-MOCK-001"
```

---

## 9. 完整示例

### 9.1 ETL 数据处理

```yaml
spec: "1.0"
name: "etl-pipeline"
version: "1.0.0"

triggers:
  - type: cron
    parameters:
      schedule: "0 2 * * *"
      timezone: "Asia/Shanghai"
  - type: manual

nodes:
  # Extract
  - name: fetch_source_a
    type: xflow.database
    parameters:
      operation: select
      table: source_a
      credential: db_source

  - name: fetch_source_b
    type: xflow.http
    parameters:
      method: GET
      url: "https://api.example.com/data"

  # Transform
  - name: merge_data
    type: xflow.merge
    parameters:
      mode: wait_all

  - name: transform
    type: xflow.function
    parameters:
      function_name: processData         # 预注册的 Go 函数，处理合并数据
      params:
        source_a: "${{ $nodes['merge_data'].source_a }}"
        source_b: "${{ $nodes['merge_data'].source_b }}"

  - name: validate
    type: xflow.function
    parameters:
      function_name: validateData        # 预注册的 Go 函数，校验转换结果
      params:
        data: "${{ $nodes['transform'] }}"

  - name: check_valid
    type: xflow.if
    parameters:
      condition: "${{ $nodes['validate'].is_valid }}"

  # Load
  - name: load_to_warehouse
    type: xflow.database
    parameters:
      operation: insert_many
      table: warehouse
      credential: db_warehouse
      data: "${{ $nodes['transform'] }}"

  - name: log_error
    type: xflow.database
    parameters:
      operation: insert
      table: error_logs
      data:
        error: "${{ $nodes['validate'].error }}"
        timestamp: "${{ now() }}"

connections:
  fetch_source_a:
    main:
      - node: merge_data
        input: source_a

  fetch_source_b:
    main:
      - node: merge_data
        input: source_b

  merge_data:
    main:
      - node: transform

  transform:
    main:
      - node: validate

  validate:
    main:
      - node: check_valid

  check_valid:
    true:
      - node: load_to_warehouse
    false:
      - node: log_error
```

### 9.2 异步任务处理

```yaml
spec: "1.0"
name: "async-task-processor"
version: "1.0.0"

triggers:
  - type: queue
    parameters:
      queue: high_priority
      prefetch: 10

nodes:
  - name: parse_message
    type: xflow.function
    parameters:
      code: "${{ parseJson($params.message) }}"

  - name: validate_task
    type: xflow.function
    parameters:
      function_name: validateTask        # 预注册的 Go 函数
      params:
        task: "${{ $nodes['parse_message'] }}"

  - name: task_switch
    type: xflow.switch
    parameters:
      outputs: [email, sms, webhook]
      rules:
        - condition: "${{ $nodes['parse_message'].type == 'email' }}"
          output: email
        - condition: "${{ $nodes['parse_message'].type == 'sms' }}"
          output: sms
      default_output: webhook

  - name: send_email
    type: xflow.notification
    parameters:
      channel: email
      to: "${{ $nodes['parse_message'].recipient }}"
      subject: "${{ $nodes['parse_message'].subject }}"
      body: "${{ $nodes['parse_message'].body }}"

  - name: send_sms
    type: xflow.notification
    parameters:
      channel: sms
      to: "${{ $nodes['parse_message'].phone }}"
      message: "${{ $nodes['parse_message'].content }}"

  - name: send_webhook
    type: xflow.http
    parameters:
      method: POST
      url: "${{ $nodes['parse_message'].webhook_url }}"
      body: "${{ $nodes['parse_message'].payload }}"

  - name: log_result
    type: xflow.database
    parameters:
      operation: insert
      table: task_logs
      data:
        task_id: "${{ $nodes['parse_message'].id }}"
        status: success
        completed_at: "${{ now() }}"

connections:
  parse_message:
    main:
      - node: validate_task

  validate_task:
    main:
      - node: task_switch

  task_switch:
    email:
      - node: send_email
    sms:
      - node: send_sms
    webhook:
      - node: send_webhook

  send_email:
    main:
      - node: log_result

  send_sms:
    main:
      - node: log_result

  send_webhook:
    main:
      - node: log_result
```

---

## 最佳实践

### 命名规范
- 工作流名称：小写字母、数字、连字符（`order-processing`）
- 节点名称：小写字母、数字、下划线（`validate_order`）
- 使用描述性名称，避免 `node1`、`step2` 等

### 表达式使用
- 保持表达式简洁，复杂逻辑使用函数节点
- 使用内置函数而非手动实现
- 利用管道操作提高可读性
- **优先使用 `$inputs.port_name` 而非 `$nodes['node_name']`**。声明式端口解耦了上游节点名，重命名节点时只需改 connections，表达式无需修改

### 连接组织
- 为关键节点配置错误处理路径
- 合理使用合并节点汇总并行分支
- 保持连接结构清晰，避免过度复杂

### 性能优化
- 合理设置超时时间
- 使用并行执行提高效率
- 避免不必要的数据转换

## 参考资源

- [Expr 官方文档](https://expr-lang.org/) - Expr 表达式语言
- [Asynq](https://github.com/hibiken/asynq) - 分布式任务队列
- [n8n](https://n8n.io/) - 工作流自动化平台
