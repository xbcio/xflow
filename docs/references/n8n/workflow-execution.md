# n8n 工作流执行机制

## 执行生命周期

n8n 工作流的执行遵循清晰的生命周期，从触发到完成经历以下阶段：

```
1. 触发阶段 (Trigger)
    ↓
2. 初始化阶段 (Initialization)
    ↓
3. 执行阶段 (Execution)
    ↓
4. 完成阶段 (Completion)
    ↓
5. 清理阶段 (Cleanup)
```

---

## 1. 触发阶段

### 触发方式

n8n 支持多种工作流触发方式：

#### 1.1 手动触发 (Manual Trigger)
用户在 UI 中点击"执行"按钮。

```typescript
// API 端点
POST /api/v1/workflows/:id/execute

{
  "data": {
    "node": "Start",  // 起始节点（可选）
    "runData": {}     // 初始数据（可选）
  }
}
```

#### 1.2 Webhook 触发
外部系统通过 HTTP 请求触发。

```typescript
// Webhook URL 格式
POST https://n8n.example.com/webhook/:path

// 示例
POST https://n8n.example.com/webhook/github-events
Content-Type: application/json

{
  "event": "push",
  "repository": "myrepo",
  "commit": "abc123"
}
```

#### 1.3 定时触发 (Cron Trigger)
基于时间表达式自动执行。

```javascript
// Cron 表达式示例
"0 */15 * * * *"   // 每 15 分钟
"0 0 9 * * 1-5"    // 工作日上午 9 点
"0 0 0 1 * *"      // 每月 1 号午夜
```

#### 1.4 事件触发 (Event Trigger)
监听外部事件（如新邮件、文件变化）。

```typescript
// 轮询检查
class PollingTrigger implements INodeType {
  async poll(this: IPollFunctions): Promise<INodeExecutionData[][] | null> {
    const lastCheck = this.getWorkflowStaticData('lastCheck');
    const newItems = await this.checkForNewItems(lastCheck);

    if (newItems.length > 0) {
      this.setWorkflowStaticData('lastCheck', Date.now());
      return [[{ json: newItems }]];
    }

    return null;
  }
}
```

---

## 2. 初始化阶段

### 2.1 执行上下文创建

```typescript
interface IWorkflowExecuteAdditionalData {
  // 凭证
  credentialsHelper: CredentialsHelper;

  // 数据库访问
  executeWorkflow: (
    workflowInfo: IExecuteWorkflowInfo,
    inputData?: INodeExecutionData[]
  ) => Promise<Array<INodeExecutionData[] | null>>;

  // 钩子函数
  hooks?: WorkflowHooks;

  // 二进制数据管理器
  binaryDataManager: BinaryDataManager;

  // 当前执行 ID
  executionId?: string;

  // 用户 ID
  userId?: string;

  // 时区
  timezone: string;
}
```

### 2.2 工作流验证

```typescript
class WorkflowValidator {
  // 验证工作流配置
  validate(workflow: IWorkflowBase): ValidationResult {
    const errors: string[] = [];

    // 检查是否有节点
    if (!workflow.nodes || workflow.nodes.length === 0) {
      errors.push('Workflow must have at least one node');
    }

    // 检查起始节点
    const startNode = this.findStartNode(workflow);
    if (!startNode) {
      errors.push('Workflow must have a trigger or start node');
    }

    // 检查节点连接
    for (const node of workflow.nodes) {
      if (!node.disabled) {
        const hasInput = this.hasIncomingConnection(workflow, node.name);
        const hasOutput = this.hasOutgoingConnection(workflow, node.name);

        if (!hasInput && !this.isTriggerNode(node)) {
          errors.push(`Node "${node.name}" has no incoming connections`);
        }

        if (!hasOutput && !this.isEndNode(node)) {
          errors.push(`Node "${node.name}" has no outgoing connections`);
        }
      }
    }

    // 检查循环依赖
    if (this.hasCircularDependency(workflow)) {
      errors.push('Workflow has circular dependencies');
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }

  // 检测循环依赖
  private hasCircularDependency(workflow: IWorkflowBase): boolean {
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    const dfs = (nodeName: string): boolean => {
      visited.add(nodeName);
      recursionStack.add(nodeName);

      const children = this.getChildNodes(workflow, nodeName);
      for (const child of children) {
        if (!visited.has(child)) {
          if (dfs(child)) return true;
        } else if (recursionStack.has(child)) {
          return true;
        }
      }

      recursionStack.delete(nodeName);
      return false;
    };

    const startNode = this.findStartNode(workflow);
    if (startNode) {
      return dfs(startNode.name);
    }

    return false;
  }
}
```

---

## 3. 执行阶段

### 3.1 执行策略

n8n 采用**拓扑排序 + 数据驱动**的执行策略：

```typescript
class ExecutionStrategy {
  async execute(
    workflow: Workflow,
    inputData: INodeExecutionData[]
  ): Promise<IRunExecutionData> {
    const runData: IRunExecutionData = {
      resultData: {
        runData: {},
      },
      executionData: {
        contextData: {},
        nodeExecutionStack: [],
        waitingExecution: {},
        waitingExecutionSource: {},
      },
    };

    // 1. 确定执行顺序
    const executionOrder = this.calculateExecutionOrder(workflow);

    // 2. 按顺序执行节点
    for (const nodeName of executionOrder) {
      const node = workflow.getNode(nodeName);

      if (!node || node.disabled) {
        continue;
      }

      // 3. 准备输入数据
      const nodeInputData = this.getNodeInputData(
        workflow,
        runData,
        nodeName
      );

      // 4. 执行节点
      try {
        const nodeOutputData = await this.executeNode(
          node,
          nodeInputData,
          runData
        );

        // 5. 存储输出数据
        runData.resultData.runData[nodeName] = [
          {
            startTime: Date.now(),
            executionTime: 0,
            data: {
              main: nodeOutputData,
            },
            source: [],
          },
        ];
      } catch (error) {
        // 6. 错误处理
        await this.handleNodeError(node, error, runData);

        // 如果设置了 continueOnFail，继续执行
        if (!node.continueOnFail) {
          throw error;
        }
      }
    }

    return runData;
  }

  // 计算执行顺序（拓扑排序）
  private calculateExecutionOrder(workflow: Workflow): string[] {
    const order: string[] = [];
    const visited = new Set<string>();

    const visit = (nodeName: string) => {
      if (visited.has(nodeName)) return;
      visited.add(nodeName);

      // 先访问所有父节点
      const parents = workflow.getParentNodes(nodeName);
      for (const parent of parents) {
        visit(parent);
      }

      order.push(nodeName);
    };

    // 从起始节点开始
    const startNode = workflow.getStartNode();
    if (startNode) {
      visit(startNode.name);
    }

    return order;
  }
}
```

### 3.2 节点输入数据准备

```typescript
class NodeInputDataPreparer {
  // 获取节点的输入数据
  getInputData(
    workflow: Workflow,
    runData: IRunExecutionData,
    nodeName: string,
    inputIndex: number = 0
  ): INodeExecutionData[] {
    const connections = workflow.connectionsByDestinationNode[nodeName];

    if (!connections || !connections.main || !connections.main[inputIndex]) {
      return [];
    }

    const inputData: INodeExecutionData[] = [];

    // 遍历所有输入连接
    for (const connection of connections.main[inputIndex]) {
      const sourceNodeName = connection.node;
      const sourceOutputIndex = connection.index;

      // 获取源节点的输出数据
      const sourceNodeRunData = runData.resultData.runData[sourceNodeName];

      if (!sourceNodeRunData || !sourceNodeRunData[0]) {
        continue;
      }

      const sourceOutputData =
        sourceNodeRunData[0].data?.main?.[sourceOutputIndex] || [];

      // 合并数据
      inputData.push(...sourceOutputData);
    }

    return inputData;
  }
}
```

### 3.3 数据流转

n8n 支持多种数据流转模式：

#### 模式 1: 一对一 (One-to-One)
```
Node A [item1, item2, item3]
    ↓
Node B 处理每个 item
    ↓
[result1, result2, result3]
```

#### 模式 2: 多对一 (Many-to-One)
```
Node A [item1, item2]    Node B [item3, item4]
         ↓                       ↓
         └──────── Merge ────────┘
                    ↓
         [item1, item2, item3, item4]
```

#### 模式 3: 一对多 (One-to-Many)
```
Node A [item1]
    ↓
Split (条件分支)
    ├─→ Branch 1: [item1] (if condition1)
    └─→ Branch 2: [item1] (if condition2)
```

### 3.4 并行执行

n8n 支持节点的并行执行：

```typescript
class ParallelExecutor {
  async executeParallel(
    nodes: INode[],
    inputData: Map<string, INodeExecutionData[]>
  ): Promise<Map<string, INodeExecutionData[]>> {
    const results = new Map<string, INodeExecutionData[]>();

    // 创建执行 Promise
    const promises = nodes.map(async (node) => {
      const nodeInputData = inputData.get(node.name) || [];
      const output = await this.executeNode(node, nodeInputData);
      return { nodeName: node.name, output };
    });

    // 并行等待所有节点完成
    const executionResults = await Promise.allSettled(promises);

    // 处理结果
    for (const result of executionResults) {
      if (result.status === 'fulfilled') {
        results.set(result.value.nodeName, result.value.output);
      } else {
        // 处理失败的节点
        throw result.reason;
      }
    }

    return results;
  }

  // 识别可并行执行的节点
  findParallelNodes(workflow: Workflow): string[][] {
    const levels: string[][] = [];
    const visited = new Set<string>();

    const assignLevel = (nodeName: string, level: number) => {
      if (visited.has(nodeName)) return;
      visited.add(nodeName);

      if (!levels[level]) {
        levels[level] = [];
      }

      levels[level].push(nodeName);

      // 递归处理子节点
      const children = workflow.getChildNodes(nodeName);
      for (const child of children) {
        assignLevel(child, level + 1);
      }
    };

    const startNode = workflow.getStartNode();
    if (startNode) {
      assignLevel(startNode.name, 0);
    }

    return levels;
  }
}
```

---

## 4. 错误处理

### 4.1 错误类型

```typescript
enum ErrorType {
  NodeExecutionError = 'NodeExecutionError',
  ConnectionError = 'ConnectionError',
  AuthenticationError = 'AuthenticationError',
  ValidationError = 'ValidationError',
  TimeoutError = 'TimeoutError',
}

class WorkflowOperationError extends Error {
  node?: INode;
  type: ErrorType;
  httpStatusCode?: number;
  description?: string;

  constructor(
    message: string,
    node?: INode,
    type: ErrorType = ErrorType.NodeExecutionError
  ) {
    super(message);
    this.node = node;
    this.type = type;
    this.name = 'WorkflowOperationError';
  }
}
```

### 4.2 错误处理策略

```typescript
class ErrorHandler {
  async handleError(
    error: Error,
    node: INode,
    runData: IRunExecutionData
  ): Promise<void> {
    // 记录错误
    console.error(`Error in node "${node.name}":`, error);

    // 存储错误信息
    if (!runData.resultData.runData[node.name]) {
      runData.resultData.runData[node.name] = [];
    }

    runData.resultData.runData[node.name].push({
      startTime: Date.now(),
      executionTime: 0,
      data: {
        main: [[]],
      },
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name,
      },
      source: [],
    });

    // 检查是否有错误处理节点
    const errorWorkflow = this.findErrorWorkflow(node);
    if (errorWorkflow) {
      await this.executeErrorWorkflow(errorWorkflow, error, node);
    }

    // 检查 continueOnFail 设置
    if (node.continueOnFail) {
      // 继续执行后续节点
      return;
    }

    // 否则停止工作流执行
    throw error;
  }

  // 查找错误处理工作流
  private findErrorWorkflow(node: INode): IWorkflowBase | null {
    // 检查节点是否配置了错误处理
    if (node.onError) {
      return node.onError;
    }
    return null;
  }
}
```

### 4.3 重试机制

```typescript
class RetryHandler {
  async executeWithRetry(
    node: INode,
    inputData: INodeExecutionData[],
    options: {
      maxRetries?: number;
      retryDelay?: number;
      backoffMultiplier?: number;
    } = {}
  ): Promise<INodeExecutionData[][]> {
    const maxRetries = options.maxRetries || node.retryOnFail || 0;
    const retryDelay = options.retryDelay || 1000;
    const backoffMultiplier = options.backoffMultiplier || 2;

    let lastError: Error | null = null;
    let delay = retryDelay;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await this.executeNode(node, inputData);
      } catch (error) {
        lastError = error;

        if (attempt < maxRetries) {
          console.log(
            `Node "${node.name}" failed (attempt ${attempt + 1}/${maxRetries + 1}). ` +
            `Retrying in ${delay}ms...`
          );

          await this.sleep(delay);
          delay *= backoffMultiplier;
        }
      }
    }

    // 所有重试都失败
    throw lastError;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

---

## 5. 执行模式

### 5.1 同步执行 (Synchronous Execution)

适用于：
- 手动触发的工作流
- Webhook 需要立即返回结果
- 快速执行的工作流

```typescript
async function executeSynchronously(
  workflowData: IWorkflowBase
): Promise<IRun> {
  const workflowExecute = new WorkflowExecute(additionalData, 'manual');

  // 直接执行并等待结果
  const result = await workflowExecute.run(workflowData);

  // 保存执行记录
  await saveExecution(result);

  return result;
}
```

### 5.2 异步执行 (Asynchronous Execution)

适用于：
- 长时间运行的工作流
- 需要队列管理的场景
- 高并发场景

```typescript
async function executeAsynchronously(
  workflowData: IWorkflowBase
): Promise<string> {
  // 添加到队列
  const jobId = await jobQueue.add({
    workflowId: workflowData.id,
    workflowData,
    mode: 'trigger',
  });

  // 立即返回执行 ID
  return jobId;
}

// Worker 处理任务
jobQueue.process(async (job) => {
  const { workflowData } = job.data;

  const workflowExecute = new WorkflowExecute(additionalData, 'trigger');
  const result = await workflowExecute.run(workflowData);

  await saveExecution(result);

  return result;
});
```

### 5.3 部分执行 (Partial Execution)

仅执行工作流的部分节点：

```typescript
async function executePartially(
  workflowData: IWorkflowBase,
  startNode: string,
  destinationNode?: string
): Promise<IRun> {
  const workflowExecute = new WorkflowExecute(additionalData, 'manual');

  // 从指定节点开始执行
  const result = await workflowExecute.run(
    workflowData,
    null,
    undefined,
    destinationNode
  );

  return result;
}
```

---

## 6. 执行优化

### 6.1 数据流优化

```typescript
class DataFlowOptimizer {
  // 减少不必要的数据复制
  optimizeDataFlow(
    sourceData: INodeExecutionData[],
    targetNode: INode
  ): INodeExecutionData[] {
    // 如果目标节点不修改数据，使用引用而非复制
    if (this.isReadOnlyNode(targetNode)) {
      return sourceData;
    }

    // 否则深拷贝数据
    return JSON.parse(JSON.stringify(sourceData));
  }

  // 判断节点是否为只读
  private isReadOnlyNode(node: INode): boolean {
    const readOnlyTypes = ['If', 'Switch', 'Merge'];
    return readOnlyTypes.includes(node.type);
  }
}
```

### 6.2 大数据集处理

```typescript
class LargeDatasetHandler {
  // 流式处理大数据集
  async *processInBatches(
    items: INodeExecutionData[],
    batchSize: number = 100
  ): AsyncGenerator<INodeExecutionData[]> {
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      yield batch;

      // 允许事件循环处理其他任务
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  // 使用流式处理执行节点
  async executeWithBatching(
    node: INode,
    inputData: INodeExecutionData[]
  ): Promise<INodeExecutionData[][]> {
    const results: INodeExecutionData[] = [];

    for await (const batch of this.processInBatches(inputData)) {
      const batchResults = await this.executeNode(node, batch);
      results.push(...batchResults[0]);
    }

    return [results];
  }
}
```

### 6.3 并发控制

```typescript
class ConcurrencyController {
  private maxConcurrent: number = 10;
  private currentlyRunning: number = 0;
  private queue: Array<() => Promise<any>> = [];

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    while (this.currentlyRunning >= this.maxConcurrent) {
      await this.waitForSlot();
    }

    this.currentlyRunning++;

    try {
      return await fn();
    } finally {
      this.currentlyRunning--;
      this.processQueue();
    }
  }

  private async waitForSlot(): Promise<void> {
    return new Promise((resolve) => {
      this.queue.push(async () => {
        resolve();
        return Promise.resolve();
      });
    });
  }

  private processQueue(): void {
    if (this.queue.length > 0 && this.currentlyRunning < this.maxConcurrent) {
      const next = this.queue.shift();
      if (next) next();
    }
  }
}
```

---

## 7. 执行监控

### 7.1 执行钩子 (Execution Hooks)

```typescript
class WorkflowHooks {
  // 工作流开始执行
  async onWorkflowStart(workflow: IWorkflowBase): Promise<void> {
    console.log(`Workflow "${workflow.name}" started`);

    // 发送通知、记录指标等
  }

  // 节点开始执行
  async onNodeExecuteStart(nodeName: string): Promise<void> {
    console.log(`Node "${nodeName}" started`);
  }

  // 节点执行完成
  async onNodeExecuteComplete(
    nodeName: string,
    data: INodeExecutionData[][]
  ): Promise<void> {
    console.log(`Node "${nodeName}" completed with ${data[0]?.length || 0} items`);
  }

  // 节点执行错误
  async onNodeExecuteError(nodeName: string, error: Error): Promise<void> {
    console.error(`Node "${nodeName}" failed:`, error);
  }

  // 工作流执行完成
  async onWorkflowComplete(result: IRun): Promise<void> {
    console.log(`Workflow completed in ${result.executionTime}ms`);
  }

  // 工作流执行错误
  async onWorkflowError(error: Error): Promise<void> {
    console.error('Workflow failed:', error);
  }
}
```

### 7.2 性能指标收集

```typescript
interface IExecutionMetrics {
  workflowId: string;
  executionId: string;
  startTime: number;
  endTime: number;
  duration: number;
  nodeMetrics: Map<string, INodeMetrics>;
}

interface INodeMetrics {
  nodeName: string;
  startTime: number;
  endTime: number;
  duration: number;
  inputItems: number;
  outputItems: number;
  memoryUsage: number;
}

class MetricsCollector {
  private metrics: IExecutionMetrics;

  startWorkflow(workflowId: string, executionId: string): void {
    this.metrics = {
      workflowId,
      executionId,
      startTime: Date.now(),
      endTime: 0,
      duration: 0,
      nodeMetrics: new Map(),
    };
  }

  startNode(nodeName: string, inputItems: number): void {
    this.metrics.nodeMetrics.set(nodeName, {
      nodeName,
      startTime: Date.now(),
      endTime: 0,
      duration: 0,
      inputItems,
      outputItems: 0,
      memoryUsage: process.memoryUsage().heapUsed,
    });
  }

  endNode(nodeName: string, outputItems: number): void {
    const nodeMetrics = this.metrics.nodeMetrics.get(nodeName);
    if (nodeMetrics) {
      nodeMetrics.endTime = Date.now();
      nodeMetrics.duration = nodeMetrics.endTime - nodeMetrics.startTime;
      nodeMetrics.outputItems = outputItems;
      nodeMetrics.memoryUsage = process.memoryUsage().heapUsed - nodeMetrics.memoryUsage;
    }
  }

  endWorkflow(): IExecutionMetrics {
    this.metrics.endTime = Date.now();
    this.metrics.duration = this.metrics.endTime - this.metrics.startTime;
    return this.metrics;
  }
}
```

---

## 8. 等待和延迟

### 8.1 等待节点 (Wait Node)

```typescript
class WaitNode implements INodeType {
  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const waitTime = this.getNodeParameter('waitTime', 0) as number;
    const unit = this.getNodeParameter('unit', 0) as string;

    let milliseconds: number;
    switch (unit) {
      case 'seconds':
        milliseconds = waitTime * 1000;
        break;
      case 'minutes':
        milliseconds = waitTime * 60 * 1000;
        break;
      case 'hours':
        milliseconds = waitTime * 60 * 60 * 1000;
        break;
      default:
        milliseconds = waitTime;
    }

    // 设置工作流等待
    await this.putExecutionToWait(new Date(Date.now() + milliseconds));

    return this.prepareOutputData(items);
  }
}
```

### 8.2 等待 Webhook (Wait for Webhook)

```typescript
class WaitForWebhookHandler {
  async waitForWebhook(
    executionId: string,
    webhookPath: string,
    timeout: number
  ): Promise<INodeExecutionData[]> {
    // 注册临时 webhook
    this.registerTempWebhook(executionId, webhookPath);

    // 设置超时
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        this.unregisterTempWebhook(executionId);
        reject(new Error('Webhook timeout'));
      }, timeout);
    });

    // 等待 webhook 数据或超时
    try {
      const webhookData = await Promise.race([
        this.waitForWebhookData(executionId),
        timeoutPromise,
      ]);

      return webhookData;
    } finally {
      this.unregisterTempWebhook(executionId);
    }
  }
}
```

---

## 执行流程总结

```
触发 → 验证 → 初始化 → 执行 → 监控 → 完成 → 清理
  ↓      ↓       ↓        ↓      ↓      ↓      ↓
手动   配置    上下文   节点链  指标   保存   释放
定时   连接    凭证    数据流  日志   通知   资源
Webhook 节点   静态    并行   错误   响应
事件   权限    数据    重试   钩子
```

---

**下一章**: [节点系统](./nodes-system.md)
