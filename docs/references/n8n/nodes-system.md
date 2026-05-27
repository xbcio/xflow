# n8n 节点系统

## 节点系统概述

节点是 n8n 工作流的基本构建块。每个节点执行特定的任务，从发送 HTTP 请求到处理数据、调用 AI 服务等。

---

## 节点分类

### 1. 按功能分类

```
n8n 节点
├── 触发器节点 (Trigger Nodes)
│   ├── Webhook Trigger
│   ├── Cron Trigger
│   ├── Manual Trigger
│   └── Event Trigger
│
├── 操作节点 (Action Nodes)
│   ├── HTTP Request
│   ├── Database Query
│   ├── Code (JavaScript/Python)
│   └── 第三方集成 (400+)
│
├── 流程控制节点 (Flow Control Nodes)
│   ├── IF (条件判断)
│   ├── Switch (多路分支)
│   ├── Merge (合并数据)
│   ├── Split In Batches (批处理)
│   └── Loop Over Items (循环)
│
└── 数据处理节点 (Data Processing Nodes)
    ├── Set (设置数据)
    ├── Filter (过滤数据)
    ├── Sort (排序)
    ├── Aggregate (聚合)
    └── Transform (转换)
```

### 2. 按输入输出分类

```typescript
interface INodeTypeDescription {
  // 输入类型
  inputs: string[] | INodeInputConfiguration[];
  // 输出类型
  outputs: string[] | INodeOutputConfiguration[];
}

// 示例
{
  inputs: ['main'],           // 单一主输入
  outputs: ['main']           // 单一主输出
}

{
  inputs: ['main', 'main'],   // 两个主输入
  outputs: ['main', 'main']   // 两个主输出（如 IF 节点）
}
```

---

## 节点定义

### 核心接口

```typescript
interface INodeType {
  // 节点描述
  description: INodeTypeDescription;

  // 执行方法
  execute?(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;

  // Webhook 方法
  webhook?(this: IWebhookFunctions): Promise<IWebhookResponseData>;

  // 触发器方法
  trigger?(this: ITriggerFunctions): Promise<ITriggerResponse>;

  // 轮询方法
  poll?(this: IPollFunctions): Promise<INodeExecutionData[][] | null>;

  // 生命周期钩子
  hooks?: {
    activate?(this: IHookFunctions): Promise<void>;
    deactivate?(this: IHookFunctions): Promise<void>;
  };

  // 方法定义
  methods?: {
    loadOptions?: {
      [key: string]: (this: ILoadOptionsFunctions) => Promise<INodePropertyOptions[]>;
    };
    credentialTest?: {
      [key: string]: (this: ICredentialTestFunctions) => Promise<INodeCredentialTestResult>;
    };
  };
}
```

### 节点描述结构

```typescript
interface INodeTypeDescription {
  // 基本信息
  displayName: string;              // 显示名称
  name: string;                     // 唯一标识符
  icon: string;                     // 图标 (fa:icon-name 或 file:icon.svg)
  group: string[];                  // 分组 ['trigger', 'transform', 'output']
  version: number | number[];       // 版本号
  description: string;              // 描述
  subtitle?: string;                // 副标题

  // 默认配置
  defaults: {
    name: string;                   // 默认节点名称
    color?: string;                 // 节点颜色
  };

  // 输入输出
  inputs: string[] | INodeInputConfiguration[];
  outputs: string[] | INodeOutputConfiguration[];

  // 凭证
  credentials?: INodeCredentialDescription[];

  // Webhook 配置
  webhooks?: IWebhookDescription[];

  // 轮询配置
  polling?: boolean;

  // 参数定义
  properties: INodeProperties[];

  // 限制和要求
  maxNodes?: number;                // 工作流中最大节点数
  requestDefaults?: IHttpRequestOptions; // 默认 HTTP 请求配置
}
```

---

## 节点参数 (Properties)

### 参数类型

```typescript
type NodePropertyTypes =
  | 'string'           // 字符串
  | 'number'           // 数字
  | 'boolean'          // 布尔值
  | 'color'            // 颜色选择器
  | 'dateTime'         // 日期时间
  | 'json'             // JSON 编辑器
  | 'options'          // 下拉选择
  | 'multiOptions'     // 多选下拉
  | 'collection'       // 对象集合
  | 'fixedCollection'  // 固定字段集合
  | 'credentialsSelect'; // 凭证选择

interface INodeProperties {
  displayName: string;           // 显示名称
  name: string;                  // 参数名称
  type: NodePropertyTypes;       // 参数类型
  default: any;                  // 默认值
  required?: boolean;            // 是否必填
  description?: string;          // 描述
  placeholder?: string;          // 占位符

  // 选项配置（用于 options/multiOptions 类型）
  options?: INodePropertyOptions[];

  // 显示条件
  displayOptions?: {
    show?: { [key: string]: any[] };
    hide?: { [key: string]: any[] };
  };

  // 类型配置
  typeOptions?: INodePropertyTypeOptions;

  // 提取值配置
  extractValue?: {
    type: string;
    regex: string;
  };
}
```

### 参数示例

#### 1. 字符串参数

```typescript
{
  displayName: 'URL',
  name: 'url',
  type: 'string',
  default: '',
  placeholder: 'https://example.com/api',
  description: 'The URL to make the request to',
  required: true,
}
```

#### 2. 选项参数

```typescript
{
  displayName: 'Method',
  name: 'method',
  type: 'options',
  options: [
    {
      name: 'GET',
      value: 'GET',
      description: 'Get data from a URL',
    },
    {
      name: 'POST',
      value: 'POST',
      description: 'Post data to a URL',
    },
    {
      name: 'PUT',
      value: 'PUT',
      description: 'Update data at a URL',
    },
    {
      name: 'DELETE',
      value: 'DELETE',
      description: 'Delete data from a URL',
    },
  ],
  default: 'GET',
}
```

#### 3. 集合参数

```typescript
{
  displayName: 'Headers',
  name: 'headers',
  type: 'fixedCollection',
  typeOptions: {
    multipleValues: true,
  },
  default: {},
  options: [
    {
      name: 'parameter',
      displayName: 'Parameter',
      values: [
        {
          displayName: 'Name',
          name: 'name',
          type: 'string',
          default: '',
          description: 'Name of the header',
        },
        {
          displayName: 'Value',
          name: 'value',
          type: 'string',
          default: '',
          description: 'Value of the header',
        },
      ],
    },
  ],
}
```

#### 4. 条件显示参数

```typescript
{
  displayName: 'Body Parameters',
  name: 'bodyParameters',
  type: 'json',
  default: '{}',
  displayOptions: {
    show: {
      method: ['POST', 'PUT', 'PATCH'],
    },
  },
  description: 'Body parameters as JSON',
}
```

---

## 节点开发示例

### 示例 1: HTTP Request 节点

```typescript
import { IExecuteFunctions } from 'n8n-core';
import {
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeOperationError,
} from 'n8n-workflow';

export class HttpRequest implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'HTTP Request',
    name: 'httpRequest',
    icon: 'fa:exchange-alt',
    group: ['output'],
    version: 1,
    description: 'Makes an HTTP request and returns the response',
    defaults: {
      name: 'HTTP Request',
      color: '#2C63D1',
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
        description: 'The HTTP method to use',
      },
      {
        displayName: 'URL',
        name: 'url',
        type: 'string',
        default: '',
        placeholder: 'https://example.com/api',
        required: true,
        description: 'The URL to make the request to',
      },
      {
        displayName: 'Headers',
        name: 'headers',
        type: 'fixedCollection',
        typeOptions: {
          multipleValues: true,
        },
        default: {},
        options: [
          {
            name: 'parameter',
            displayName: 'Header',
            values: [
              {
                displayName: 'Name',
                name: 'name',
                type: 'string',
                default: '',
              },
              {
                displayName: 'Value',
                name: 'value',
                type: 'string',
                default: '',
              },
            ],
          },
        ],
      },
      {
        displayName: 'Body',
        name: 'body',
        type: 'json',
        default: '{}',
        displayOptions: {
          show: {
            method: ['POST', 'PUT', 'PATCH'],
          },
        },
        description: 'Body parameters as JSON',
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];

    // 处理每个输入项
    for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
      try {
        // 获取参数
        const method = this.getNodeParameter('method', itemIndex) as string;
        const url = this.getNodeParameter('url', itemIndex) as string;
        const headersParams = this.getNodeParameter('headers', itemIndex, {}) as any;

        // 构建请求头
        const headers: { [key: string]: string } = {};
        if (headersParams.parameter) {
          for (const header of headersParams.parameter) {
            headers[header.name] = header.value;
          }
        }

        // 构建请求选项
        const options: any = {
          method,
          url,
          headers,
        };

        // 如果有 body 参数
        if (['POST', 'PUT', 'PATCH'].includes(method)) {
          const body = this.getNodeParameter('body', itemIndex, '{}') as string;
          try {
            options.body = JSON.parse(body);
            options.json = true;
          } catch (error) {
            throw new NodeOperationError(
              this.getNode(),
              `Invalid JSON in body parameter: ${error.message}`,
              { itemIndex }
            );
          }
        }

        // 发起请求
        const response = await this.helpers.request(options);

        // 添加到返回数据
        returnData.push({
          json: typeof response === 'string' ? { response } : response,
          pairedItem: { item: itemIndex },
        });

      } catch (error) {
        // 错误处理
        if (this.continueOnFail()) {
          returnData.push({
            json: {
              error: error.message,
            },
            pairedItem: { item: itemIndex },
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

### 示例 2: 数据转换节点

```typescript
export class DataTransform implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Data Transform',
    name: 'dataTransform',
    icon: 'fa:exchange',
    group: ['transform'],
    version: 1,
    description: 'Transform data structure',
    defaults: {
      name: 'Data Transform',
    },
    inputs: ['main'],
    outputs: ['main'],
    properties: [
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        options: [
          {
            name: 'Rename Fields',
            value: 'rename',
            description: 'Rename fields in the data',
          },
          {
            name: 'Remove Fields',
            value: 'remove',
            description: 'Remove fields from the data',
          },
          {
            name: 'Add Fields',
            value: 'add',
            description: 'Add new fields to the data',
          },
        ],
        default: 'rename',
      },
      {
        displayName: 'Field Mappings',
        name: 'mappings',
        type: 'fixedCollection',
        typeOptions: {
          multipleValues: true,
        },
        displayOptions: {
          show: {
            operation: ['rename'],
          },
        },
        default: {},
        options: [
          {
            name: 'mapping',
            displayName: 'Mapping',
            values: [
              {
                displayName: 'Old Name',
                name: 'oldName',
                type: 'string',
                default: '',
              },
              {
                displayName: 'New Name',
                name: 'newName',
                type: 'string',
                default: '',
              },
            ],
          },
        ],
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const operation = this.getNodeParameter('operation', 0) as string;

    for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
      const item = items[itemIndex];
      let newItem: INodeExecutionData;

      switch (operation) {
        case 'rename':
          newItem = this.renameFields(item, itemIndex);
          break;
        case 'remove':
          newItem = this.removeFields(item, itemIndex);
          break;
        case 'add':
          newItem = this.addFields(item, itemIndex);
          break;
        default:
          newItem = item;
      }

      returnData.push(newItem);
    }

    return this.prepareOutputData(returnData);
  }

  private renameFields(
    item: INodeExecutionData,
    itemIndex: number
  ): INodeExecutionData {
    const mappings = this.getNodeParameter('mappings', itemIndex, {}) as any;
    const newJson = { ...item.json };

    if (mappings.mapping) {
      for (const mapping of mappings.mapping) {
        const { oldName, newName } = mapping;
        if (oldName in newJson) {
          newJson[newName] = newJson[oldName];
          delete newJson[oldName];
        }
      }
    }

    return {
      json: newJson,
      binary: item.binary,
      pairedItem: { item: itemIndex },
    };
  }

  private removeFields(
    item: INodeExecutionData,
    itemIndex: number
  ): INodeExecutionData {
    // 实现移除字段逻辑
    return item;
  }

  private addFields(
    item: INodeExecutionData,
    itemIndex: number
  ): INodeExecutionData {
    // 实现添加字段逻辑
    return item;
  }
}
```

### 示例 3: Webhook 触发器

```typescript
export class WebhookTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Webhook',
    name: 'webhook',
    icon: 'fa:plug',
    group: ['trigger'],
    version: 1,
    description: 'Starts the workflow when a webhook is called',
    defaults: {
      name: 'Webhook',
    },
    inputs: [],
    outputs: ['main'],
    webhooks: [
      {
        name: 'default',
        httpMethod: '={{$parameter["httpMethod"] || "POST"}}',
        responseMode: '={{$parameter["responseMode"] || "onReceived"}}',
        path: '={{$parameter["path"]}}',
      },
    ],
    properties: [
      {
        displayName: 'HTTP Method',
        name: 'httpMethod',
        type: 'options',
        options: [
          { name: 'GET', value: 'GET' },
          { name: 'POST', value: 'POST' },
          { name: 'PUT', value: 'PUT' },
          { name: 'DELETE', value: 'DELETE' },
        ],
        default: 'POST',
      },
      {
        displayName: 'Path',
        name: 'path',
        type: 'string',
        default: '',
        placeholder: 'webhook-path',
        required: true,
        description: 'The path to listen for requests on',
      },
      {
        displayName: 'Response Mode',
        name: 'responseMode',
        type: 'options',
        options: [
          {
            name: 'On Received',
            value: 'onReceived',
            description: 'Returns data immediately',
          },
          {
            name: 'Last Node',
            value: 'lastNode',
            description: 'Returns data from the last node',
          },
        ],
        default: 'onReceived',
      },
    ],
  };

  async webhook(this: IWebhookFunctions): Promise<IWebhookResponseData> {
    const req = this.getRequestObject();
    const resp = this.getResponseObject();
    const responseMode = this.getNodeParameter('responseMode') as string;

    // 构建输出数据
    const returnData: INodeExecutionData[] = [
      {
        json: {
          headers: req.headers,
          params: req.params,
          query: req.query,
          body: req.body,
        },
      },
    ];

    if (responseMode === 'onReceived') {
      // 立即返回响应
      return {
        workflowData: [returnData],
        webhookResponse: {
          statusCode: 200,
          body: { received: true },
        },
      };
    } else {
      // 等待工作流完成后返回
      return {
        workflowData: [returnData],
      };
    }
  }
}
```

### 示例 4: Cron 触发器

```typescript
import { CronJob } from 'cron';

export class CronTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cron',
    name: 'cron',
    icon: 'fa:clock',
    group: ['trigger'],
    version: 1,
    description: 'Triggers the workflow on a schedule',
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
                  { name: 'Every Minute', value: 'everyMinute' },
                  { name: 'Every Hour', value: 'everyHour' },
                  { name: 'Every Day', value: 'everyDay' },
                  { name: 'Custom', value: 'custom' },
                ],
                default: 'everyHour',
              },
              {
                displayName: 'Hour',
                name: 'hour',
                type: 'number',
                displayOptions: {
                  show: {
                    mode: ['everyDay'],
                  },
                },
                typeOptions: {
                  minValue: 0,
                  maxValue: 23,
                },
                default: 0,
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
                description: 'Custom cron expression',
              },
            ],
          },
        ],
      },
    ],
  };

  async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
    const triggerTimes = this.getNodeParameter('triggerTimes') as any;
    const staticData = this.getWorkflowStaticData('node');
    const timezone = this.getTimezone();

    const cronJobs: CronJob[] = [];

    for (const item of triggerTimes.item || []) {
      let cronExpression: string;

      switch (item.mode) {
        case 'everyMinute':
          cronExpression = '* * * * *';
          break;
        case 'everyHour':
          cronExpression = '0 * * * *';
          break;
        case 'everyDay':
          cronExpression = `0 ${item.hour || 0} * * *`;
          break;
        case 'custom':
          cronExpression = item.cronExpression;
          break;
        default:
          continue;
      }

      const cronJob = new CronJob(
        cronExpression,
        () => {
          this.emit([
            [
              {
                json: {
                  timestamp: new Date().toISOString(),
                  mode: item.mode,
                  timezone,
                },
              },
            ],
          ]);
        },
        undefined,
        true,
        timezone
      );

      cronJobs.push(cronJob);
    }

    // 返回关闭函数
    async function closeFunction() {
      for (const cronJob of cronJobs) {
        cronJob.stop();
      }
    }

    return {
      closeFunction,
    };
  }
}
```

---

## 节点最佳实践

### 1. 参数验证

```typescript
// 验证 URL
const url = this.getNodeParameter('url', itemIndex) as string;
if (!url.startsWith('http://') && !url.startsWith('https://')) {
  throw new NodeOperationError(
    this.getNode(),
    'URL must start with http:// or https://',
    { itemIndex }
  );
}

// 验证 JSON
const jsonString = this.getNodeParameter('json', itemIndex) as string;
try {
  JSON.parse(jsonString);
} catch (error) {
  throw new NodeOperationError(
    this.getNode(),
    `Invalid JSON: ${error.message}`,
    { itemIndex }
  );
}
```

### 2. 错误处理

```typescript
try {
  const result = await someAsyncOperation();
  returnData.push({ json: result });
} catch (error) {
  if (this.continueOnFail()) {
    // 继续执行，返回错误信息
    returnData.push({
      json: { error: error.message },
      pairedItem: { item: itemIndex },
    });
  } else {
    // 抛出错误，停止工作流
    throw new NodeOperationError(
      this.getNode(),
      error.message,
      { itemIndex }
    );
  }
}
```

### 3. 性能优化

```typescript
// 批量处理而非逐个处理
const allUrls = items.map((_, i) => this.getNodeParameter('url', i));
const results = await Promise.all(allUrls.map(url => fetch(url)));

// 使用流式处理大数据
async function* processInBatches(items: any[], batchSize: number) {
  for (let i = 0; i < items.length; i += batchSize) {
    yield items.slice(i, i + batchSize);
  }
}
```

### 4. 凭证使用

```typescript
// 获取凭证
const credentials = await this.getCredentials('apiKeyAuth');
const apiKey = credentials.apiKey as string;

// 在请求中使用
const options = {
  url: 'https://api.example.com/data',
  headers: {
    'Authorization': `Bearer ${apiKey}`,
  },
};
```

---

## 节点测试

```typescript
// 节点单元测试示例
import { WorkflowTestData } from 'n8n-workflow';

describe('HTTP Request Node', () => {
  const testData: WorkflowTestData = {
    description: 'Test HTTP GET request',
    input: {
      workflowData: {
        nodes: [
          {
            name: 'Start',
            type: 'n8n-nodes-base.start',
            position: [250, 300],
            parameters: {},
          },
          {
            name: 'HTTP Request',
            type: 'n8n-nodes-base.httpRequest',
            position: [450, 300],
            parameters: {
              url: 'https://api.example.com/data',
              method: 'GET',
            },
          },
        ],
        connections: {
          Start: {
            main: [[{ node: 'HTTP Request', type: 'main', index: 0 }]],
          },
        },
      },
    },
    output: {
      nodeData: {
        'HTTP Request': [
          [
            {
              json: {
                id: 1,
                name: 'Test',
              },
            },
          ],
        ],
      },
    },
  };

  test('should make GET request', async () => {
    const result = await executeWorkflow(testData);
    expect(result.finished).toBe(true);
    expect(result.data.resultData.runData['HTTP Request']).toBeDefined();
  });
});
```

---

**下一章**: [实现指南](./implementation-guide.md)
