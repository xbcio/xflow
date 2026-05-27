# n8n 架构设计

## 整体架构

n8n 采用**分层架构设计**，将应用分为多个独立但协作的层次：

```
┌─────────────────────────────────────────────────────┐
│                    前端层 (Vue.js)                   │
│  - 工作流编辑器  - 执行历史  - 凭证管理  - 设置    │
└────────────────────┬────────────────────────────────┘
                     │ REST API / WebSocket
┌────────────────────▼────────────────────────────────┐
│                   API 层 (Express)                   │
│  - 工作流 CRUD  - 执行控制  - 认证授权  - Webhook  │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│                 核心业务层 (n8n-core)                │
│  - 工作流解析  - 执行编排  - 凭证加密  - 数据转换  │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│             执行引擎层 (workflow-engine)             │
│  - 节点执行器  - 数据流控制  - 错误处理  - 重试    │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│               节点层 (n8n-nodes-base)                │
│  - HTTP Request  - Database  - Code  - 400+ 集成   │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│                  基础设施层                          │
│  - Database  - Queue (Bull/Redis)  - File Storage   │
└─────────────────────────────────────────────────────┘
```

## 核心模块

### 1. 前端模块 (n8n-editor-ui)

**技术栈**: Vue.js 3 + TypeScript + Pinia

**核心功能**:
- 可视化工作流编辑器（基于 JSPlumb）
- 节点配置面板
- 表达式编辑器
- 执行历史查看器
- 凭证管理界面

**关键组件**:
```typescript
// 工作流画布组件
NodeView.vue
  ├── Node.vue              // 单个节点组件
  ├── NodeConnection.vue    // 节点连接线
  └── ContextMenu.vue       // 右键菜单

// 节点设置面板
NodeSettings.vue
  ├── ParameterInput.vue    // 参数输入组件
  ├── ExpressionEditor.vue  // 表达式编辑器
  └── CredentialPicker.vue  // 凭证选择器
```

### 2. API 服务层 (n8n)

**技术栈**: Express.js + TypeScript

**核心端点**:
```typescript
// 工作流管理
POST   /api/v1/workflows          // 创建工作流
GET    /api/v1/workflows/:id      // 获取工作流
PUT    /api/v1/workflows/:id      // 更新工作流
DELETE /api/v1/workflows/:id      // 删除工作流
GET    /api/v1/workflows          // 列出工作流

// 执行管理
POST   /api/v1/workflows/:id/execute      // 执行工作流
GET    /api/v1/executions/:id             // 获取执行详情
GET    /api/v1/executions                 // 列出执行历史
DELETE /api/v1/executions/:id             // 删除执行记录

// Webhook 接收
POST   /webhook/:path                      // 接收 webhook
GET    /webhook-test/:path                 // 测试 webhook

// 凭证管理
POST   /api/v1/credentials                 // 创建凭证
GET    /api/v1/credentials                 // 列出凭证
PUT    /api/v1/credentials/:id             // 更新凭证
DELETE /api/v1/credentials/:id             // 删除凭证
```

### 3. 核心库 (n8n-core)

**主要功能**:
- 工作流解析和验证
- 凭证加密和解密
- 执行上下文管理
- 数据类型转换
- 表达式求值引擎

**关键类**:
```typescript
class Workflow {
  id: string;
  name: string;
  nodes: INode[];
  connections: IConnections;
  settings: IWorkflowSettings;

  // 核心方法
  execute(mode: WorkflowExecuteMode, runData?: IRunExecutionData): Promise<IWorkflowExecuteResult>;
  getNode(nodeName: string): INode | undefined;
  getParentNodes(nodeName: string, type?: string): string[];
  getChildNodes(nodeName: string): string[];
}

class Expression {
  // 表达式解析和求值
  resolve(expression: string, data: IDataObject): any;
  getParameterValue(value: any, runIndex: number, itemIndex: number): any;
}

class BinaryDataManager {
  // 二进制数据管理
  store(data: Buffer, mimeType: string): Promise<IBinaryData>;
  retrieve(dataId: string): Promise<Buffer>;
}
```

### 4. 工作流执行引擎 (n8n-workflow)

**执行流程**:

```
开始执行
    ↓
1. 解析工作流定义
    ↓
2. 初始化执行上下文
    ↓
3. 确定起始节点
    ↓
4. ┌─────────────────┐
   │  节点执行循环   │
   │  ┌──────────┐   │
   │  │ 准备数据 │   │
   │  ├──────────┤   │
   │  │ 执行节点 │   │
   │  ├──────────┤   │
   │  │ 处理输出 │   │
   │  ├──────────┤   │
   │  │ 错误处理 │   │
   │  └──────────┘   │
   └────┬────────────┘
        │ 是否有下一节点？
        ├─ 是 ──→ 继续循环
        └─ 否
    ↓
5. 生成执行结果
    ↓
6. 保存执行记录
    ↓
结束
```

**核心执行器**:
```typescript
class WorkflowExecute {
  async run(
    workflow: Workflow,
    startNode?: INode,
    destinationNode?: string
  ): Promise<IRun> {
    const executionData = await this.processRunExecutionData(workflow);
    return {
      data: executionData,
      finished: true,
      mode: this.mode,
      startedAt: new Date(),
      stoppedAt: new Date(),
    };
  }

  // 执行单个节点
  async executeNode(
    node: INode,
    inputData: INodeExecutionData[][],
    runIndex: number
  ): Promise<INodeExecutionData[][] | null> {
    // 节点执行逻辑
  }
}
```

### 5. 节点系统 (n8n-nodes-base)

**节点类型分类**:

```typescript
// 节点基类
interface INodeType {
  description: INodeTypeDescription;
  execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;

  // 可选的生命周期钩子
  webhookMethods?: IWebhookFunctions;
  triggerMethods?: ITriggerFunctions;
  pollMethods?: IPollFunctions;
}

// 节点描述
interface INodeTypeDescription {
  displayName: string;
  name: string;
  icon: string;
  group: string[];
  version: number;
  description: string;
  defaults: INodeParameters;
  inputs: string[];
  outputs: string[];
  credentials?: INodeCredentialDescription[];
  properties: INodeProperties[];
}
```

## 数据模型

### 工作流定义

```typescript
interface IWorkflowDb {
  id: string;
  name: string;
  active: boolean;
  nodes: INode[];
  connections: IConnections;
  settings?: IWorkflowSettings;
  staticData?: IDataObject;
  tags?: ITag[];
  createdAt: Date;
  updatedAt: Date;
}

interface INode {
  id: string;
  name: string;
  type: string;
  typeVersion: number;
  position: [number, number];
  parameters: INodeParameters;
  credentials?: INodeCredentials;
  disabled?: boolean;
  notes?: string;
  notesInFlow?: boolean;
}

interface IConnections {
  [nodeName: string]: {
    [outputType: string]: Array<Array<{
      node: string;
      type: string;
      index: number;
    }>>;
  };
}
```

### 执行记录

```typescript
interface IExecutionDb {
  id: string;
  workflowId: string;
  finished: boolean;
  mode: WorkflowExecuteMode;
  retryOf?: string;
  retrySuccessId?: string;
  startedAt: Date;
  stoppedAt?: Date;
  workflowData: IWorkflowDb;
  data: IRun;
  waitTill?: Date;
}

interface IRun {
  data: IRunExecutionData;
  finished?: boolean;
  mode: WorkflowExecuteMode;
  startedAt: Date;
  stoppedAt?: Date;
  error?: ExecutionError;
}
```

## 部署架构

### 单机模式

```
┌────────────────────────────────┐
│         n8n Instance           │
│  ┌──────────────────────────┐  │
│  │  Web Server (Express)    │  │
│  ├──────────────────────────┤  │
│  │  Execution Engine        │  │
│  ├──────────────────────────┤  │
│  │  Queue Worker (Bull)     │  │
│  └──────────────────────────┘  │
│          │         │            │
│          ▼         ▼            │
│    ┌────────┐  ┌───────┐       │
│    │ SQLite │  │ Redis │       │
│    └────────┘  └───────┘       │
└────────────────────────────────┘
```

### 队列模式（高可用）

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ n8n Instance │  │ n8n Instance │  │ n8n Instance │
│   (Main)     │  │  (Worker 1)  │  │  (Worker 2)  │
│              │  │              │  │              │
│  Web Server  │  │   Executor   │  │   Executor   │
│  API Layer   │  │    Only      │  │    Only      │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                  │
       └─────────────────┼──────────────────┘
                         │
                    ┌────▼────┐
                    │  Redis  │
                    │  Queue  │
                    └────┬────┘
                         │
       ┌─────────────────┼──────────────────┐
       │                 │                  │
  ┌────▼────┐      ┌─────▼────┐      ┌─────▼────┐
  │PostgreSQL│      │  Redis   │      │   S3     │
  │ (Primary)│      │  (Cache) │      │ (Binary) │
  └──────────┘      └──────────┘      └──────────┘
```

## 技术决策

### 为什么选择 TypeScript
- 类型安全，减少运行时错误
- 更好的 IDE 支持和自动补全
- 大型项目的可维护性

### 为什么选择 Bull Queue
- 基于 Redis，性能优秀
- 支持任务优先级和延迟执行
- 内置重试机制
- 任务进度追踪

### 为什么支持多数据库
- SQLite: 轻量级部署，适合个人和小团队
- PostgreSQL: 企业级功能，适合大规模部署
- MySQL: 广泛支持，易于维护

### 为什么采用插件化节点
- 社区可扩展
- 核心代码与业务集成分离
- 版本独立更新
- 按需加载，减少内存占用

## 性能优化

### 1. 执行优化
- 并行执行无依赖节点
- 流式处理大数据集
- 二进制数据引用而非复制
- 执行结果分页加载

### 2. 数据库优化
- 执行数据分表存储
- 定期清理历史记录
- 索引优化查询
- 使用连接池

### 3. 内存优化
- 大数据集分批处理
- 二进制数据外部存储
- 执行完成后释放内存
- 限制并发执行数

## 安全设计

### 1. 凭证加密
- 使用 AES-256-GCM 加密存储
- 密钥派生使用 PBKDF2
- 凭证仅在执行时解密
- 执行日志中凭证脱敏

### 2. 代码执行隔离
- JavaScript 节点使用 VM2 沙箱
- 限制可访问的 Node.js 模块
- 超时控制防止无限循环
- 资源使用限制

### 3. API 安全
- JWT 身份认证
- CSRF 保护
- Rate Limiting
- Webhook 签名验证

---

**下一章**: [核心组件详解](./core-components.md)
