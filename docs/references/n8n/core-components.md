# n8n 核心组件详解

## 组件概览

n8n 的核心组件按功能可分为以下几类：

```
n8n 核心组件
├── 1. 工作流引擎 (Workflow Engine)
├── 2. 节点执行器 (Node Executor)
├── 3. 表达式引擎 (Expression Engine)
├── 4. 凭证管理器 (Credentials Manager)
├── 5. 队列系统 (Queue System)
├── 6. 数据管理器 (Data Manager)
├── 7. Webhook 处理器 (Webhook Handler)
└── 8. 触发器系统 (Trigger System)
```

---

## 1. 工作流引擎 (Workflow Engine)

### 核心职责
- 解析工作流定义
- 编排节点执行顺序
- 管理执行上下文
- 处理节点间数据流转

### 关键类和接口

```typescript
// 工作流引擎核心类
class WorkflowExecute {
  private workflow: Workflow;
  private mode: WorkflowExecuteMode;
  private executionId?: string;
  private additionalData: IWorkflowExecuteAdditionalData;

  constructor(
    additionalData: IWorkflowExecuteAdditionalData,
    mode: WorkflowExecuteMode,
    runExecutionData?: IRunExecutionData
  ) {
    this.mode = mode;
    this.additionalData = additionalData;
  }

  // 运行工作流
  async run(
    workflowData: IWorkflowBase,
    inputData?: INodeExecutionData[] | null,
    parentExecutionId?: string,
    destinationNode?: string
  ): Promise<IRun> {
    const workflow = new Workflow({
      id: workflowData.id,
      name: workflowData.name,
      nodes: workflowData.nodes,
      connections: workflowData.connections,
      active: workflowData.active,
      nodeTypes: this.additionalData.nodeTypes,
      staticData: workflowData.staticData,
      settings: workflowData.settings,
    });

    return this.runExecutionData(workflow, inputData, destinationNode);
  }

  // 处理运行执行数据
  private async runExecutionData(
    workflow: Workflow,
    inputData?: INodeExecutionData[] | null,
    destinationNode?: string
  ): Promise<IRun> {
    const runData = await this.processRunExecutionData(
      workflow,
      inputData,
      destinationNode
    );

    return {
      data: runData,
      finished: true,
      mode: this.mode,
      startedAt: new Date(),
      stoppedAt: new Date(),
    };
  }
}
```

### 工作流对象

```typescript
class Workflow {
  id?: string;
  name: string;
  nodes: INode[];
  connections: IConnections;
  active: boolean;
  settings?: IWorkflowSettings;
  staticData?: IDataObject;

  // 获取节点的父节点
  getParentNodes(nodeName: string, type?: string, depth?: number): string[] {
    const parents: string[] = [];
    // 递归查找父节点逻辑
    return parents;
  }

  // 获取节点的子节点
  getChildNodes(nodeName: string): string[] {
    const children: string[] = [];
    // 查找子节点逻辑
    return children;
  }

  // 获取起始节点
  getStartNode(destinationNode?: string): INode | undefined {
    // 查找触发节点或指定的起始节点
  }

  // 检查工作流是否有效
  checkReadyForExecution(workflow: Workflow): void {
    // 验证工作流配置
  }
}
```

### 执行模式

```typescript
enum WorkflowExecuteMode {
  manual = 'manual',         // 手动执行
  trigger = 'trigger',       // 触发器执行
  webhook = 'webhook',       // Webhook 执行
  retry = 'retry',          // 重试执行
  integrated = 'integrated', // 集成执行
  cli = 'cli',              // CLI 执行
}
```

---

## 2. 节点执行器 (Node Executor)

### 核心职责
- 执行单个节点逻辑
- 处理节点输入输出
- 管理节点生命周期
- 错误处理和重试

### 节点执行函数

```typescript
interface INodeType {
  description: INodeTypeDescription;

  // 主要执行方法
  execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;

  // Webhook 方法
  webhook?(this: IWebhookFunctions): Promise<IWebhookResponseData>;

  // 触发器方法
  trigger?(this: ITriggerFunctions): Promise<ITriggerResponse>;

  // 轮询方法
  poll?(this: IPollFunctions): Promise<INodeExecutionData[][] | null>;

  // 钩子函数
  hooks?: {
    activate?(this: IHookFunctions): Promise<void>;
    deactivate?(this: IHookFunctions): Promise<void>;
  };
}
```

### 执行上下文 (IExecuteFunctions)

```typescript
interface IExecuteFunctions extends IExecuteFunctionsBase {
  // 获取输入数据
  getInputData(inputIndex?: number, inputName?: string): INodeExecutionData[];

  // 获取节点参数
  getNodeParameter(
    parameterName: string,
    itemIndex: number,
    fallbackValue?: any
  ): any;

  // 获取工作流静态数据
  getWorkflowStaticData(type: string): IDataObject;

  // 发起 HTTP 请求
  helpers: {
    request(options: IHttpRequestOptions): Promise<any>;
    requestOAuth1(
      credentialsType: string,
      options: IHttpRequestOptions
    ): Promise<any>;
    requestOAuth2(
      credentialsType: string,
      options: IHttpRequestOptions
    ): Promise<any>;
  };

  // 准备输出数据
  prepareOutputData(outputData: INodeExecutionData[]): Promise<INodeExecutionData[][]>;

  // 获取凭证
  getCredentials(type: string): Promise<ICredentialDataDecryptedObject>;
}
```

### 节点执行示例

```typescript
// HTTP Request 节点实现示例
class HttpRequest implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'HTTP Request',
    name: 'httpRequest',
    icon: 'fa:exchange-alt',
    group: ['output'],
    version: 1,
    description: 'Makes an HTTP request',
    defaults: {
      name: 'HTTP Request',
    },
    inputs: ['main'],
    outputs: ['main'],
    properties: [
      {
        displayName: 'Method',
        name: 'method',
        type: 'options',
        options: [
          { name: 'GET', value: 'GET' },
          { name: 'POST', value: 'POST' },
          { name: 'PUT', value: 'PUT' },
          { name: 'DELETE', value: 'DELETE' },
        ],
        default: 'GET',
      },
      {
        displayName: 'URL',
        name: 'url',
        type: 'string',
        default: '',
        required: true,
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];

    for (let i = 0; i < items.length; i++) {
      const method = this.getNodeParameter('method', i) as string;
      const url = this.getNodeParameter('url', i) as string;

      try {
        const response = await this.helpers.request({
          method,
          url,
        });

        returnData.push({
          json: response,
        });
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: { error: error.message },
          });
          continue;
        }
        throw error;
      }
    }

    return this.prepareOutputData(returnData);
  }
}
```

---

## 3. 表达式引擎 (Expression Engine)

### 核心职责
- 解析和计算表达式
- 访问工作流数据
- 提供内置函数
- 支持 JavaScript 代码片段

### 表达式语法

```javascript
// 基本表达式
{{ $json.field }}                    // 访问当前节点数据
{{ $node["Node Name"].json.field }}  // 访问其他节点数据
{{ $parameter["parameterName"] }}    // 访问参数值

// 内置变量
{{ $now }}                           // 当前时间戳
{{ $today }}                         // 今天日期
{{ $workflow.id }}                   // 工作流 ID
{{ $workflow.name }}                 // 工作流名称
{{ $execution.id }}                  // 执行 ID
{{ $execution.mode }}                // 执行模式

// 内置函数
{{ $jmespath(data, "query") }}       // JMESPath 查询
{{ $toDateTime(timestamp) }}         // 转换为日期时间
{{ $uuid() }}                        // 生成 UUID
```

### 表达式解析器实现

```typescript
class Expression {
  // 解析表达式
  resolve(
    expression: string,
    data: IRunExecutionData,
    runIndex: number,
    itemIndex: number,
    nodeName: string,
    returnObjectAsString = false
  ): any {
    if (!expression.startsWith('{{') || !expression.endsWith('}}')) {
      return expression;
    }

    const expressionCode = expression.slice(2, -2).trim();

    // 创建求值上下文
    const context = this.createContext(data, runIndex, itemIndex, nodeName);

    // 执行表达式
    return this.evaluate(expressionCode, context);
  }

  // 创建表达式上下文
  private createContext(
    data: IRunExecutionData,
    runIndex: number,
    itemIndex: number,
    nodeName: string
  ): IDataObject {
    return {
      $json: data.resultData.runData[nodeName]?.[runIndex]?.data?.main?.[0]?.[itemIndex]?.json,
      $node: this.createNodeProxy(data, runIndex),
      $parameter: this.parameters,
      $workflow: {
        id: this.workflow.id,
        name: this.workflow.name,
        active: this.workflow.active,
      },
      $execution: {
        id: this.executionId,
        mode: this.mode,
      },
      $now: Date.now(),
      $today: new Date().toISOString().split('T')[0],
      // 内置函数
      $jmespath: jmespath.search,
      $toDateTime: this.toDateTime,
      $uuid: () => uuid.v4(),
    };
  }

  // 执行表达式代码
  private evaluate(code: string, context: IDataObject): any {
    // 使用 Function 构造器安全执行
    const keys = Object.keys(context);
    const values = Object.values(context);

    try {
      const fn = new Function(...keys, `return ${code}`);
      return fn(...values);
    } catch (error) {
      throw new Error(`Expression evaluation failed: ${error.message}`);
    }
  }
}
```

---

## 4. 凭证管理器 (Credentials Manager)

### 核心职责
- 安全存储凭证
- 加密和解密凭证
- 凭证类型管理
- OAuth 流程处理

### 凭证数据结构

```typescript
interface ICredentialsDb {
  id: string;
  name: string;
  type: string;
  data: string;  // 加密后的凭证数据
  nodesAccess: Array<{
    nodeType: string;
  }>;
  createdAt: Date;
  updatedAt: Date;
}

interface ICredentialType {
  name: string;
  displayName: string;
  documentationUrl?: string;
  properties: INodeProperties[];
  authenticate?: IAuthenticate;
}
```

### 凭证加密实现

```typescript
class Credentials {
  private encryptionKey: string;

  constructor(nodeCredentials: INodeCredentials, type: string, data?: ICredentialDataDecryptedObject) {
    this.type = type;
    this.data = data || {};
  }

  // 加密凭证数据
  encrypt(): string {
    const dataString = JSON.stringify(this.data);

    // 生成随机 IV
    const iv = crypto.randomBytes(16);

    // 使用 AES-256-GCM 加密
    const cipher = crypto.createCipheriv(
      'aes-256-gcm',
      Buffer.from(this.encryptionKey, 'hex'),
      iv
    );

    let encrypted = cipher.update(dataString, 'utf8', 'base64');
    encrypted += cipher.final('base64');

    const authTag = cipher.getAuthTag();

    // 返回格式: iv:authTag:encryptedData
    return `${iv.toString('base64')}:${authTag.toString('base64')}:${encrypted}`;
  }

  // 解密凭证数据
  decrypt(encryptedData: string): ICredentialDataDecryptedObject {
    const [ivBase64, authTagBase64, encrypted] = encryptedData.split(':');

    const iv = Buffer.from(ivBase64, 'base64');
    const authTag = Buffer.from(authTagBase64, 'base64');

    // 使用 AES-256-GCM 解密
    const decipher = crypto.createDecipheriv(
      'aes-256-gcm',
      Buffer.from(this.encryptionKey, 'hex'),
      iv
    );

    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encrypted, 'base64', 'utf8');
    decrypted += decipher.final('utf8');

    return JSON.parse(decrypted);
  }

  // 获取凭证用于 HTTP 请求
  getCredentials(): ICredentialDataDecryptedObject {
    if (!this.data || Object.keys(this.data).length === 0) {
      throw new Error(`Credentials "${this.name}" not found`);
    }
    return this.data;
  }
}
```

### OAuth 凭证处理

```typescript
interface IOAuth2Credentials extends ICredentialDataDecryptedObject {
  clientId: string;
  clientSecret: string;
  accessToken?: string;
  refreshToken?: string;
  expiresIn?: number;
  tokenType?: string;
}

class OAuth2CredentialHelper {
  // 获取访问令牌
  async getAccessToken(
    credentials: IOAuth2Credentials,
    oAuthConfig: IOAuth2Options
  ): Promise<string> {
    // 检查令牌是否过期
    if (this.isTokenExpired(credentials)) {
      return this.refreshAccessToken(credentials, oAuthConfig);
    }

    return credentials.accessToken!;
  }

  // 刷新访问令牌
  private async refreshAccessToken(
    credentials: IOAuth2Credentials,
    oAuthConfig: IOAuth2Options
  ): Promise<string> {
    const response = await axios.post(oAuthConfig.tokenUrl, {
      grant_type: 'refresh_token',
      refresh_token: credentials.refreshToken,
      client_id: credentials.clientId,
      client_secret: credentials.clientSecret,
    });

    // 更新凭证
    credentials.accessToken = response.data.access_token;
    credentials.expiresIn = response.data.expires_in;

    // 保存更新后的凭证
    await this.saveCredentials(credentials);

    return credentials.accessToken;
  }
}
```

---

## 5. 队列系统 (Queue System)

### 核心职责
- 异步任务处理
- 执行调度管理
- 并发控制
- 任务重试机制

### Bull Queue 集成

```typescript
import Queue from 'bull';

class WorkflowRunner {
  private jobQueue: Queue.Queue;

  constructor() {
    // 初始化队列
    this.jobQueue = new Queue('n8n-jobs', {
      redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379'),
      },
      settings: {
        maxStalledCount: 3,
        stalledInterval: 30000,
      },
    });

    // 注册任务处理器
    this.jobQueue.process(this.processJob.bind(this));
  }

  // 添加执行任务到队列
  async add(
    workflowData: IWorkflowBase,
    mode: WorkflowExecuteMode,
    options?: {
      priority?: number;
      delay?: number;
      attempts?: number;
    }
  ): Promise<string> {
    const job = await this.jobQueue.add(
      {
        workflowData,
        mode,
      },
      {
        priority: options?.priority || 0,
        delay: options?.delay || 0,
        attempts: options?.attempts || 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        removeOnComplete: {
          age: 3600, // 保留 1 小时
        },
        removeOnFail: {
          age: 24 * 3600, // 保留 24 小时
        },
      }
    );

    return job.id.toString();
  }

  // 处理队列任务
  private async processJob(job: Queue.Job): Promise<IRun> {
    const { workflowData, mode } = job.data;

    // 更新任务进度
    await job.progress(10);

    try {
      // 执行工作流
      const workflowExecute = new WorkflowExecute(
        this.additionalData,
        mode
      );

      await job.progress(50);

      const result = await workflowExecute.run(workflowData);

      await job.progress(100);

      return result;
    } catch (error) {
      // 错误将触发重试机制
      throw error;
    }
  }

  // 获取任务状态
  async getJobStatus(jobId: string): Promise<Queue.Job | null> {
    return this.jobQueue.getJob(jobId);
  }
}
```

---

## 6. 数据管理器 (Data Manager)

### 核心职责
- 二进制数据处理
- 大数据集管理
- 数据流优化
- 内存管理

### 二进制数据管理

```typescript
interface IBinaryData {
  data: string;          // Base64 或文件路径
  mimeType: string;
  fileExtension?: string;
  fileName?: string;
  fileSize?: number;
  id?: string;
}

class BinaryDataManager {
  private storagePath: string;
  private mode: 'default' | 'filesystem';

  constructor(config: IBinaryDataConfig) {
    this.storagePath = config.storagePath;
    this.mode = config.mode;
  }

  // 存储二进制数据
  async store(
    workflowId: string,
    executionId: string,
    buffer: Buffer,
    mimeType: string
  ): Promise<IBinaryData> {
    const fileId = uuid.v4();

    if (this.mode === 'filesystem') {
      // 存储到文件系统
      const filePath = path.join(
        this.storagePath,
        workflowId,
        executionId,
        fileId
      );

      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
      await fs.promises.writeFile(filePath, buffer);

      return {
        data: filePath,
        mimeType,
        fileSize: buffer.length,
        id: fileId,
      };
    } else {
      // 存储为 Base64
      return {
        data: buffer.toString('base64'),
        mimeType,
        fileSize: buffer.length,
        id: fileId,
      };
    }
  }

  // 检索二进制数据
  async retrieve(binaryData: IBinaryData): Promise<Buffer> {
    if (this.mode === 'filesystem') {
      return fs.promises.readFile(binaryData.data);
    } else {
      return Buffer.from(binaryData.data, 'base64');
    }
  }

  // 清理执行相关的二进制数据
  async cleanup(workflowId: string, executionId: string): Promise<void> {
    if (this.mode === 'filesystem') {
      const dirPath = path.join(this.storagePath, workflowId, executionId);
      await fs.promises.rm(dirPath, { recursive: true, force: true });
    }
  }
}
```

---

## 7. Webhook 处理器 (Webhook Handler)

### 核心职责
- 接收外部 HTTP 请求
- 触发工作流执行
- 返回响应数据
- 签名验证

### Webhook 实现

```typescript
class WebhookServer {
  private app: Express;
  private activeWebhooks: Map<string, IWebhookData>;

  constructor() {
    this.app = express();
    this.activeWebhooks = new Map();
    this.setupRoutes();
  }

  // 设置路由
  private setupRoutes(): void {
    // Webhook 生产路由
    this.app.all('/webhook/:path(*)', async (req, res) => {
      await this.handleWebhook(req, res, 'production');
    });

    // Webhook 测试路由
    this.app.all('/webhook-test/:path(*)', async (req, res) => {
      await this.handleWebhook(req, res, 'test');
    });
  }

  // 处理 Webhook 请求
  private async handleWebhook(
    req: Request,
    res: Response,
    mode: 'production' | 'test'
  ): Promise<void> {
    const path = req.params.path;
    const webhookKey = `${req.method}-${path}`;

    const webhookData = this.activeWebhooks.get(webhookKey);

    if (!webhookData) {
      res.status(404).json({ error: 'Webhook not found' });
      return;
    }

    try {
      // 准备 Webhook 数据
      const workflowData = webhookData.workflow;
      const webhookNode = workflowData.nodes.find(
        (node) => node.name === webhookData.node
      );

      if (!webhookNode) {
        throw new Error('Webhook node not found');
      }

      // 创建输入数据
      const inputData: INodeExecutionData = {
        json: {
          headers: req.headers,
          params: req.params,
          query: req.query,
          body: req.body,
        },
      };

      // 执行工作流
      const workflowExecute = new WorkflowExecute(
        this.additionalData,
        'webhook'
      );

      const executionResult = await workflowExecute.run(
        workflowData,
        [inputData],
        undefined,
        webhookData.node
      );

      // 返回响应
      const responseData = this.getWebhookResponse(executionResult, webhookNode);

      res
        .status(responseData.statusCode || 200)
        .set(responseData.headers || {})
        .send(responseData.body);

    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  // 注册 Webhook
  registerWebhook(
    workflowId: string,
    workflow: IWorkflowBase,
    node: string,
    path: string,
    method: string
  ): void {
    const webhookKey = `${method}-${path}`;

    this.activeWebhooks.set(webhookKey, {
      workflowId,
      workflow,
      node,
      path,
      method,
    });
  }

  // 注销 Webhook
  unregisterWebhook(path: string, method: string): void {
    const webhookKey = `${method}-${path}`;
    this.activeWebhooks.delete(webhookKey);
  }
}
```

---

## 8. 触发器系统 (Trigger System)

### 核心职责
- 定时触发
- 事件监听
- 轮询检查
- 触发器生命周期管理

### 触发器实现

```typescript
class TriggerManager {
  private activeTriggers: Map<string, ITriggerData>;

  constructor() {
    this.activeTriggers = new Map();
  }

  // 激活触发器
  async activate(
    workflow: IWorkflowBase,
    triggerNode: INode
  ): Promise<void> {
    const nodeType = this.nodeTypes.getByName(triggerNode.type);

    if (!nodeType.trigger) {
      throw new Error(`Node "${triggerNode.type}" is not a trigger node`);
    }

    // 创建触发器上下文
    const triggerFunctions = this.createTriggerFunctions(workflow, triggerNode);

    // 调用触发器的 activate 方法
    const triggerResponse = await nodeType.trigger.call(triggerFunctions);

    // 存储触发器信息
    const triggerId = `${workflow.id}-${triggerNode.name}`;
    this.activeTriggers.set(triggerId, {
      workflow,
      node: triggerNode,
      closeFunction: triggerResponse.closeFunction,
      manualTriggerFunction: triggerResponse.manualTriggerFunction,
    });
  }

  // 停用触发器
  async deactivate(workflowId: string, nodeName: string): Promise<void> {
    const triggerId = `${workflowId}-${nodeName}`;
    const triggerData = this.activeTriggers.get(triggerId);

    if (triggerData && triggerData.closeFunction) {
      await triggerData.closeFunction();
    }

    this.activeTriggers.delete(triggerId);
  }

  // 创建触发器函数上下文
  private createTriggerFunctions(
    workflow: IWorkflowBase,
    node: INode
  ): ITriggerFunctions {
    return {
      emit: async (data: INodeExecutionData[][]): Promise<void> => {
        // 触发工作流执行
        await this.executeWorkflow(workflow, node.name, data);
      },
      getNode: () => node,
      getWorkflow: () => workflow,
      getWorkflowStaticData: (type: string) => {
        return workflow.staticData?.[type] || {};
      },
      getNodeParameter: (parameterName: string, fallbackValue?: any) => {
        return node.parameters[parameterName] ?? fallbackValue;
      },
      helpers: {
        // 辅助函数
      },
    };
  }

  // 执行工作流
  private async executeWorkflow(
    workflow: IWorkflowBase,
    triggerNodeName: string,
    data: INodeExecutionData[][]
  ): Promise<void> {
    const workflowExecute = new WorkflowExecute(
      this.additionalData,
      'trigger'
    );

    await workflowExecute.run(
      workflow,
      data[0],
      undefined,
      triggerNodeName
    );
  }
}
```

### Cron 触发器示例

```typescript
class CronTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cron',
    name: 'cron',
    icon: 'fa:clock',
    group: ['trigger'],
    version: 1,
    description: 'Triggers the workflow at a specified interval',
    defaults: {
      name: 'Cron',
    },
    inputs: [],
    outputs: ['main'],
    properties: [
      {
        displayName: 'Trigger Times',
        name: 'triggerTimes',
        type: 'fixedCollection',
        typeOptions: {
          multipleValues: true,
        },
        default: {},
        options: [
          {
            name: 'item',
            displayName: 'Item',
            values: [
              {
                displayName: 'Mode',
                name: 'mode',
                type: 'options',
                options: [
                  {
                    name: 'Every Minute',
                    value: 'everyMinute',
                  },
                  {
                    name: 'Every Hour',
                    value: 'everyHour',
                  },
                  {
                    name: 'Custom Cron',
                    value: 'custom',
                  },
                ],
                default: 'everyMinute',
              },
              {
                displayName: 'Cron Expression',
                name: 'cronExpression',
                type: 'string',
                displayOptions: {
                  show: {
                    mode: ['custom'],
                  },
                },
                default: '0 * * * *',
              },
            ],
          },
        ],
      },
    ],
  };

  async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
    const triggerTimes = this.getNodeParameter('triggerTimes') as IDataObject;
    const cronJobs: CronJob[] = [];

    for (const item of (triggerTimes.item as IDataObject[]) || []) {
      let cronExpression: string;

      switch (item.mode) {
        case 'everyMinute':
          cronExpression = '* * * * *';
          break;
        case 'everyHour':
          cronExpression = '0 * * * *';
          break;
        case 'custom':
          cronExpression = item.cronExpression as string;
          break;
        default:
          cronExpression = '* * * * *';
      }

      const cronJob = new CronJob(cronExpression, async () => {
        // 触发工作流
        this.emit([
          [
            {
              json: {
                timestamp: new Date().toISOString(),
              },
            },
          ],
        ]);
      });

      cronJob.start();
      cronJobs.push(cronJob);
    }

    // 返回关闭函数
    return {
      closeFunction: async () => {
        for (const cronJob of cronJobs) {
          cronJob.stop();
        }
      },
    };
  }
}
```

---

## 组件交互流程

```
用户触发执行
    ↓
Webhook/Trigger System
    ↓
Queue System (异步处理)
    ↓
Workflow Engine (解析工作流)
    ↓
Node Executor (循环执行节点)
    ↓
├─→ Expression Engine (计算表达式)
├─→ Credentials Manager (获取凭证)
└─→ Data Manager (处理数据)
    ↓
返回执行结果
    ↓
存储到数据库
```

---

**下一章**: [工作流执行机制](./workflow-execution.md)
