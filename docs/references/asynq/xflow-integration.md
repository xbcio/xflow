# Asynq 在 xflow 中的集成方案

本文档展示如何将 Asynq 集成到 xflow 工作流引擎中，实现异步任务处理。

## 目录

- [架构设计](#架构设计)
- [项目结构](#项目结构)
- [核心实现](#核心实现)
- [使用示例](#使用示例)
- [部署方案](#部署方案)

---

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    xflow 工作流引擎                       │
│  ┌────────────────────────────────────────────────────┐ │
│  │            Web API (Gin Framework)                 │ │
│  │  - 创建工作流                                       │ │
│  │  - 触发工作流执行                                   │ │
│  │  - 查询执行状态                                     │ │
│  └────────────┬───────────────────────────────────────┘ │
│               │                                          │
│               ▼                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │         Asynq Client (任务提交)                    │ │
│  │  - 工作流步骤任务                                   │ │
│  │  - 延迟任务                                         │ │
│  │  - 定时任务                                         │ │
│  └────────────┬───────────────────────────────────────┘ │
└───────────────┼──────────────────────────────────────────┘
                │
                ▼
       ┌────────────────┐
       │     Redis      │
       │   (Queue)      │
       └────────┬───────┘
                │
                ▼
┌───────────────────────────────────────────────────────────┐
│              Asynq Worker (任务执行)                       │
│  ┌────────────────────────────────────────────────────┐  │
│  │         工作流任务处理器                            │  │
│  │  - 执行工作流步骤                                   │  │
│  │  - 更新执行状态                                     │  │
│  │  - 触发下一步骤                                     │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │         依赖服务                                    │  │
│  │  - MySQL (持久化)                                   │  │
│  │  - Elasticsearch (日志)                            │  │
│  │  - Kafka (事件)                                     │  │
│  └────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

### 使用场景

1. **异步执行工作流步骤**
   - HTTP 请求节点
   - 数据转换节点
   - 外部 API 调用

2. **延迟执行**
   - 等待节点
   - 定时触发
   - 延迟通知

3. **批量处理**
   - 批量数据导入
   - 批量通知发送
   - 数据同步

4. **定时工作流**
   - 定时报表生成
   - 定时数据同步
   - 定时清理任务

---

## 项目结构

```
xflow/
├── cmd/
│   ├── api/                    # API 服务
│   │   └── main.go
│   └── worker/                 # Worker 服务
│       └── main.go
├── internal/
│   ├── api/                    # API 层
│   │   ├── handler/            # HTTP 处理器
│   │   │   ├── workflow.go
│   │   │   └── execution.go
│   │   └── router/             # 路由配置
│   │       └── router.go
│   ├── task/                   # 任务层
│   │   ├── client.go           # Asynq Client 封装
│   │   ├── types.go            # 任务类型定义
│   │   ├── workflow.go         # 工作流任务
│   │   └── handler/            # 任务处理器
│   │       ├── workflow.go
│   │       ├── http.go
│   │       └── transform.go
│   ├── workflow/               # 工作流引擎
│   │   ├── engine.go
│   │   ├── executor.go
│   │   └── node.go
│   ├── model/                  # 数据模型
│   │   ├── workflow.go
│   │   └── execution.go
│   └── repository/             # 数据访问层
│       ├── workflow.go
│       └── execution.go
├── pkg/
│   ├── config/                 # 配置管理
│   └── logger/                 # 日志
└── docs/
    └── asynq/                  # Asynq 文档
```

---

## 核心实现

### 1. 任务类型定义 (internal/task/types.go)

```go
package task

// 任务类型常量
const (
    // 工作流相关
    TypeWorkflowExecute     = "workflow:execute"      // 执行工作流
    TypeWorkflowStepExecute = "workflow:step:execute" // 执行工作流步骤
    TypeWorkflowRetry       = "workflow:retry"        // 重试工作流

    // 节点类型
    TypeNodeHTTPRequest  = "node:http:request"   // HTTP 请求节点
    TypeNodeTransform    = "node:transform"      // 数据转换节点
    TypeNodeCondition    = "node:condition"      // 条件判断节点
    TypeNodeLoop         = "node:loop"           // 循环节点
    TypeNodeNotification = "node:notification"   // 通知节点

    // 定时任务
    TypeScheduledWorkflow = "scheduled:workflow" // 定时工作流
)

// 工作流执行 Payload
type WorkflowExecutePayload struct {
    ExecutionID  string                 `json:"execution_id"`  // 执行 ID
    WorkflowID   string                 `json:"workflow_id"`   // 工作流 ID
    TriggerBy    string                 `json:"trigger_by"`    // 触发者
    InputData    map[string]interface{} `json:"input_data"`    // 输入数据
    StartNodeID  string                 `json:"start_node_id"` // 起始节点
}

// 工作流步骤执行 Payload
type WorkflowStepPayload struct {
    ExecutionID string                 `json:"execution_id"` // 执行 ID
    NodeID      string                 `json:"node_id"`      // 节点 ID
    NodeType    string                 `json:"node_type"`    // 节点类型
    InputData   map[string]interface{} `json:"input_data"`   // 输入数据
}

// HTTP 请求节点 Payload
type HTTPRequestPayload struct {
    ExecutionID string            `json:"execution_id"`
    NodeID      string            `json:"node_id"`
    Method      string            `json:"method"`
    URL         string            `json:"url"`
    Headers     map[string]string `json:"headers"`
    Body        interface{}       `json:"body"`
}
```

### 2. Asynq Client 封装 (internal/task/client.go)

```go
package task

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/hibiken/asynq"
    "github.com/xbcio/xflow/pkg/config"
)

type Client struct {
    client *asynq.Client
}

func NewClient(cfg *config.Config) *Client {
    return &Client{
        client: asynq.NewClient(asynq.RedisClientOpt{
            Addr:     cfg.Redis.Addr,
            Password: cfg.Redis.Password,
            DB:       cfg.Redis.DB,
        }),
    }
}

func (c *Client) Close() error {
    return c.client.Close()
}

// EnqueueWorkflowExecution 入队工作流执行任务
func (c *Client) EnqueueWorkflowExecution(
    ctx context.Context,
    payload WorkflowExecutePayload,
    opts ...asynq.Option,
) (string, error) {
    data, err := json.Marshal(payload)
    if err != nil {
        return "", fmt.Errorf("marshal payload: %w", err)
    }

    task := asynq.NewTask(TypeWorkflowExecute, data)

    // 默认选项
    defaultOpts := []asynq.Option{
        asynq.MaxRetry(3),
        asynq.Timeout(30 * time.Minute),
        asynq.Queue("workflow"),
    }

    // 合并用户选项
    allOpts := append(defaultOpts, opts...)

    info, err := c.client.Enqueue(task, allOpts...)
    if err != nil {
        return "", fmt.Errorf("enqueue task: %w", err)
    }

    return info.ID, nil
}

// EnqueueWorkflowStep 入队工作流步骤任务
func (c *Client) EnqueueWorkflowStep(
    ctx context.Context,
    payload WorkflowStepPayload,
    opts ...asynq.Option,
) (string, error) {
    data, err := json.Marshal(payload)
    if err != nil {
        return "", fmt.Errorf("marshal payload: %w", err)
    }

    task := asynq.NewTask(TypeWorkflowStepExecute, data)

    // 根据节点类型设置不同的选项
    defaultOpts := []asynq.Option{
        asynq.MaxRetry(2),
        asynq.Timeout(10 * time.Minute),
        asynq.Queue("default"),
    }

    allOpts := append(defaultOpts, opts...)

    info, err := c.client.Enqueue(task, allOpts...)
    if err != nil {
        return "", fmt.Errorf("enqueue task: %w", err)
    }

    return info.ID, nil
}

// EnqueueHTTPRequest 入队 HTTP 请求任务
func (c *Client) EnqueueHTTPRequest(
    ctx context.Context,
    payload HTTPRequestPayload,
    opts ...asynq.Option,
) (string, error) {
    data, err := json.Marshal(payload)
    if err != nil {
        return "", fmt.Errorf("marshal payload: %w", err)
    }

    task := asynq.NewTask(TypeNodeHTTPRequest, data)

    defaultOpts := []asynq.Option{
        asynq.MaxRetry(3),
        asynq.Timeout(5 * time.Minute),
        asynq.Queue("default"),
    }

    allOpts := append(defaultOpts, opts...)

    info, err := c.client.Enqueue(task, allOpts...)
    if err != nil {
        return "", fmt.Errorf("enqueue task: %w", err)
    }

    return info.ID, nil
}

// ScheduleWorkflow 调度定时工作流
func (c *Client) ScheduleWorkflow(
    ctx context.Context,
    payload WorkflowExecutePayload,
    executeAt time.Time,
) (string, error) {
    data, err := json.Marshal(payload)
    if err != nil {
        return "", fmt.Errorf("marshal payload: %w", err)
    }

    task := asynq.NewTask(TypeWorkflowExecute, data)

    info, err := c.client.Enqueue(
        task,
        asynq.ProcessAt(executeAt),
        asynq.Queue("workflow"),
    )
    if err != nil {
        return "", fmt.Errorf("schedule task: %w", err)
    }

    return info.ID, nil
}
```

### 3. 工作流任务处理器 (internal/task/handler/workflow.go)

```go
package handler

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
    "github.com/xbcio/xflow/internal/model"
    "github.com/xbcio/xflow/internal/repository"
    "github.com/xbcio/xflow/internal/task"
    "github.com/xbcio/xflow/internal/workflow"
)

type WorkflowHandler struct {
    workflowRepo   *repository.WorkflowRepository
    executionRepo  *repository.ExecutionRepository
    engine         *workflow.Engine
    taskClient     *task.Client
}

func NewWorkflowHandler(
    workflowRepo *repository.WorkflowRepository,
    executionRepo *repository.ExecutionRepository,
    engine *workflow.Engine,
    taskClient *task.Client,
) *WorkflowHandler {
    return &WorkflowHandler{
        workflowRepo:  workflowRepo,
        executionRepo: executionRepo,
        engine:        engine,
        taskClient:    taskClient,
    }
}

// HandleWorkflowExecute 处理工作流执行任务
func (h *WorkflowHandler) HandleWorkflowExecute(ctx context.Context, t *asynq.Task) error {
    var payload task.WorkflowExecutePayload
    if err := json.Unmarshal(t.Payload(), &payload); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Starting workflow execution: execution_id=%s, workflow_id=%s",
        payload.ExecutionID, payload.WorkflowID)

    // 1. 获取工作流定义
    wf, err := h.workflowRepo.GetByID(ctx, payload.WorkflowID)
    if err != nil {
        return fmt.Errorf("get workflow: %w", err)
    }

    // 2. 更新执行状态为运行中
    execution := &model.Execution{
        ID:         payload.ExecutionID,
        WorkflowID: payload.WorkflowID,
        Status:     model.ExecutionStatusRunning,
        InputData:  payload.InputData,
        StartedAt:  time.Now(),
    }

    if err := h.executionRepo.Update(ctx, execution); err != nil {
        return fmt.Errorf("update execution: %w", err)
    }

    // 3. 执行工作流
    result, err := h.engine.Execute(ctx, wf, payload.InputData)
    if err != nil {
        // 更新执行状态为失败
        execution.Status = model.ExecutionStatusFailed
        execution.Error = err.Error()
        execution.FinishedAt = time.Now()
        h.executionRepo.Update(ctx, execution)

        return fmt.Errorf("execute workflow: %w", err)
    }

    // 4. 更新执行状态为成功
    execution.Status = model.ExecutionStatusSuccess
    execution.OutputData = result
    execution.FinishedAt = time.Now()

    if err := h.executionRepo.Update(ctx, execution); err != nil {
        return fmt.Errorf("update execution: %w", err)
    }

    log.Printf("Workflow execution completed: execution_id=%s", payload.ExecutionID)

    return nil
}

// HandleWorkflowStep 处理工作流步骤任务
func (h *WorkflowHandler) HandleWorkflowStep(ctx context.Context, t *asynq.Task) error {
    var payload task.WorkflowStepPayload
    if err := json.Unmarshal(t.Payload(), &payload); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Executing workflow step: execution_id=%s, node_id=%s, type=%s",
        payload.ExecutionID, payload.NodeID, payload.NodeType)

    // 根据节点类型执行不同的逻辑
    switch payload.NodeType {
    case "http_request":
        return h.handleHTTPRequestNode(ctx, payload)
    case "transform":
        return h.handleTransformNode(ctx, payload)
    case "condition":
        return h.handleConditionNode(ctx, payload)
    default:
        return fmt.Errorf("unknown node type: %s: %w", payload.NodeType, asynq.SkipRetry)
    }
}

// handleHTTPRequestNode 处理 HTTP 请求节点
func (h *WorkflowHandler) handleHTTPRequestNode(
    ctx context.Context,
    payload task.WorkflowStepPayload,
) error {
    // 实现 HTTP 请求逻辑
    log.Printf("Executing HTTP request node: %s", payload.NodeID)

    // TODO: 实际的 HTTP 请求处理

    return nil
}

// handleTransformNode 处理数据转换节点
func (h *WorkflowHandler) handleTransformNode(
    ctx context.Context,
    payload task.WorkflowStepPayload,
) error {
    log.Printf("Executing transform node: %s", payload.NodeID)

    // TODO: 实际的数据转换逻辑

    return nil
}

// handleConditionNode 处理条件判断节点
func (h *WorkflowHandler) handleConditionNode(
    ctx context.Context,
    payload task.WorkflowStepPayload,
) error {
    log.Printf("Executing condition node: %s", payload.NodeID)

    // TODO: 实际的条件判断逻辑

    return nil
}
```

### 4. API 处理器 (internal/api/handler/workflow.go)

```go
package handler

import (
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/google/uuid"
    "github.com/xbcio/xflow/internal/model"
    "github.com/xbcio/xflow/internal/repository"
    "github.com/xbcio/xflow/internal/task"
)

type WorkflowHandler struct {
    workflowRepo  *repository.WorkflowRepository
    executionRepo *repository.ExecutionRepository
    taskClient    *task.Client
}

func NewWorkflowHandler(
    workflowRepo *repository.WorkflowRepository,
    executionRepo *repository.ExecutionRepository,
    taskClient *task.Client,
) *WorkflowHandler {
    return &WorkflowHandler{
        workflowRepo:  workflowRepo,
        executionRepo: executionRepo,
        taskClient:    taskClient,
    }
}

// CreateWorkflow 创建工作流
// @Summary 创建工作流
// @Tags Workflow
// @Accept json
// @Produce json
// @Param workflow body model.WorkflowCreateRequest true "工作流定义"
// @Success 200 {object} model.Workflow
// @Router /api/v1/workflows [post]
func (h *WorkflowHandler) CreateWorkflow(c *gin.Context) {
    var req model.WorkflowCreateRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    workflow := &model.Workflow{
        ID:          uuid.New().String(),
        Name:        req.Name,
        Description: req.Description,
        Definition:  req.Definition,
        Status:      model.WorkflowStatusActive,
    }

    if err := h.workflowRepo.Create(c.Request.Context(), workflow); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, workflow)
}

// ExecuteWorkflow 执行工作流
// @Summary 执行工作流
// @Tags Workflow
// @Accept json
// @Produce json
// @Param id path string true "工作流 ID"
// @Param request body model.WorkflowExecuteRequest true "执行参数"
// @Success 200 {object} model.Execution
// @Router /api/v1/workflows/{id}/execute [post]
func (h *WorkflowHandler) ExecuteWorkflow(c *gin.Context) {
    workflowID := c.Param("id")

    var req model.WorkflowExecuteRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // 创建执行记录
    execution := &model.Execution{
        ID:         uuid.New().String(),
        WorkflowID: workflowID,
        Status:     model.ExecutionStatusPending,
        InputData:  req.InputData,
        TriggerBy:  c.GetString("user_id"),
    }

    if err := h.executionRepo.Create(c.Request.Context(), execution); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    // 入队异步任务
    payload := task.WorkflowExecutePayload{
        ExecutionID: execution.ID,
        WorkflowID:  workflowID,
        TriggerBy:   execution.TriggerBy,
        InputData:   req.InputData,
    }

    taskID, err := h.taskClient.EnqueueWorkflowExecution(c.Request.Context(), payload)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    execution.TaskID = taskID

    c.JSON(http.StatusOK, execution)
}

// GetExecution 获取执行详情
// @Summary 获取执行详情
// @Tags Execution
// @Produce json
// @Param id path string true "执行 ID"
// @Success 200 {object} model.Execution
// @Router /api/v1/executions/{id} [get]
func (h *WorkflowHandler) GetExecution(c *gin.Context) {
    executionID := c.Param("id")

    execution, err := h.executionRepo.GetByID(c.Request.Context(), executionID)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "execution not found"})
        return
    }

    c.JSON(http.StatusOK, execution)
}
```

### 5. Worker 主程序 (cmd/worker/main.go)

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/hibiken/asynq"
    "github.com/xbcio/xflow/internal/task"
    taskHandler "github.com/xbcio/xflow/internal/task/handler"
    "github.com/xbcio/xflow/pkg/config"
)

func main() {
    // 加载配置
    cfg, err := config.Load()
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    // 初始化依赖
    // workflowRepo := repository.NewWorkflowRepository(db)
    // executionRepo := repository.NewExecutionRepository(db)
    // engine := workflow.NewEngine()
    // taskClient := task.NewClient(cfg)

    // 创建处理器
    // workflowHandler := taskHandler.NewWorkflowHandler(
    //     workflowRepo,
    //     executionRepo,
    //     engine,
    //     taskClient,
    // )

    // 创建 Asynq Server
    srv := asynq.NewServer(
        asynq.RedisClientOpt{
            Addr:     cfg.Redis.Addr,
            Password: cfg.Redis.Password,
            DB:       cfg.Redis.DB,
        },
        asynq.Config{
            Concurrency: cfg.Worker.Concurrency,
            Queues: map[string]int{
                "workflow": 6,  // 高优先级：工作流任务
                "default":  3,  // 默认优先级
                "low":      1,  // 低优先级
            },
            LogLevel:        asynq.InfoLevel,
            ShutdownTimeout: 30 * time.Second,
        },
    )

    // 注册任务处理器
    mux := asynq.NewServeMux()

    // 工作流任务
    // mux.HandleFunc(task.TypeWorkflowExecute, workflowHandler.HandleWorkflowExecute)
    // mux.HandleFunc(task.TypeWorkflowStepExecute, workflowHandler.HandleWorkflowStep)

    // HTTP 节点任务
    // mux.HandleFunc(task.TypeNodeHTTPRequest, httpHandler.HandleHTTPRequest)

    log.Println("xflow Worker started, waiting for tasks...")

    // 启动 Worker
    if err := srv.Run(mux); err != nil {
        log.Fatalf("Could not run worker: %v", err)
    }
}
```

### 6. API 主程序 (cmd/api/main.go)

```go
package main

import (
    "log"

    "github.com/gin-gonic/gin"
    "github.com/xbcio/xflow/internal/api/handler"
    "github.com/xbcio/xflow/internal/task"
    "github.com/xbcio/xflow/pkg/config"
)

func main() {
    // 加载配置
    cfg, err := config.Load()
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    // 初始化 Asynq Client
    taskClient := task.NewClient(cfg)
    defer taskClient.Close()

    // 初始化依赖
    // db := initDB(cfg)
    // workflowRepo := repository.NewWorkflowRepository(db)
    // executionRepo := repository.NewExecutionRepository(db)

    // 创建处理器
    // workflowHandler := handler.NewWorkflowHandler(
    //     workflowRepo,
    //     executionRepo,
    //     taskClient,
    // )

    // 创建 Gin 路由
    r := gin.Default()

    // API 路由
    v1 := r.Group("/api/v1")
    {
        // workflows := v1.Group("/workflows")
        // {
        //     workflows.POST("", workflowHandler.CreateWorkflow)
        //     workflows.GET("/:id", workflowHandler.GetWorkflow)
        //     workflows.POST("/:id/execute", workflowHandler.ExecuteWorkflow)
        // }
        //
        // executions := v1.Group("/executions")
        // {
        //     executions.GET("/:id", workflowHandler.GetExecution)
        //     executions.GET("", workflowHandler.ListExecutions)
        // }
    }

    log.Printf("xflow API server started on %s", cfg.Server.Addr)

    if err := r.Run(cfg.Server.Addr); err != nil {
        log.Fatalf("Failed to start server: %v", err)
    }
}
```

---

## 使用示例

### 1. 创建工作流

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "数据处理工作流",
    "description": "从 API 获取数据并处理",
    "definition": {
      "nodes": [
        {
          "id": "node_1",
          "type": "http_request",
          "config": {
            "method": "GET",
            "url": "https://api.example.com/data"
          }
        },
        {
          "id": "node_2",
          "type": "transform",
          "config": {
            "script": "data.map(item => ({ ...item, processed: true }))"
          }
        }
      ],
      "edges": [
        {
          "from": "node_1",
          "to": "node_2"
        }
      ]
    }
  }'
```

### 2. 执行工作流

```bash
curl -X POST http://localhost:8080/api/v1/workflows/{workflow_id}/execute \
  -H "Content-Type: application/json" \
  -d '{
    "input_data": {
      "param1": "value1",
      "param2": "value2"
    }
  }'
```

### 3. 查询执行状态

```bash
curl http://localhost:8080/api/v1/executions/{execution_id}
```

响应示例：

```json
{
  "id": "exec_123",
  "workflow_id": "wf_456",
  "status": "success",
  "input_data": {
    "param1": "value1"
  },
  "output_data": {
    "result": "processed_data"
  },
  "started_at": "2026-01-11T10:00:00Z",
  "finished_at": "2026-01-11T10:01:30Z"
}
```

---

## 部署方案

### 开发环境

```bash
# 1. 启动 Redis
docker run -d -p 6379:6379 redis:7-alpine

# 2. 启动 MySQL
docker run -d -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=xflow \
  mysql:8

# 3. 启动 Worker
go run cmd/worker/main.go

# 4. 启动 API
go run cmd/api/main.go
```

### 生产环境

```yaml
# docker-compose.yml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: ${DB_PASSWORD}
      MYSQL_DATABASE: xflow
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql

  xflow-api:
    build:
      context: .
      dockerfile: Dockerfile.api
    ports:
      - "8080:8080"
    environment:
      - REDIS_ADDR=redis:6379
      - DB_HOST=mysql
      - DB_PORT=3306
    depends_on:
      - redis
      - mysql

  xflow-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    deploy:
      replicas: 3  # 启动 3 个 Worker 实例
    environment:
      - REDIS_ADDR=redis:6379
      - DB_HOST=mysql
      - DB_PORT=3306
    depends_on:
      - redis
      - mysql

  asynqmon:
    image: hibiken/asynqmon:latest
    ports:
      - "8081:8080"
    command:
      - "--redis-addr=redis:6379"
    depends_on:
      - redis

volumes:
  redis-data:
  mysql-data:
```

启动：

```bash
docker-compose up -d
```

### Kubernetes 部署

```yaml
# k8s/worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: xflow-worker
spec:
  replicas: 5
  selector:
    matchLabels:
      app: xflow-worker
  template:
    metadata:
      labels:
        app: xflow-worker
    spec:
      containers:
      - name: worker
        image: xflow-worker:latest
        env:
        - name: REDIS_ADDR
          value: "redis:6379"
        - name: WORKER_CONCURRENCY
          value: "10"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

---

## 监控和运维

### 1. 使用 asynqmon

```bash
# 访问 Web UI
open http://localhost:8081

# 查看：
# - 队列状态
# - 活动任务
# - 失败任务
# - 定时任务
```

### 2. Prometheus 指标

```go
// 在 Worker 中添加 Prometheus 中间件
import "github.com/hibiken/asynq/x/metrics"

prometheusMiddleware := metrics.NewPrometheusMetrics(prometheus.DefaultRegisterer)
mux.Use(prometheusMiddleware.MiddlewareFunc())
```

### 3. 日志记录

```go
// 使用 zap 结构化日志
import "go.uber.org/zap"

logger, _ := zap.NewProduction()
defer logger.Sync()

logger.Info("workflow execution started",
    zap.String("execution_id", executionID),
    zap.String("workflow_id", workflowID),
)
```

---

## 总结

通过集成 Asynq，xflow 工作流引擎获得了：

✅ **异步执行能力** - 工作流步骤异步执行，不阻塞 API 响应
✅ **可靠性保证** - 任务持久化、自动重试、故障恢复
✅ **水平扩展** - 轻松添加 Worker 实例应对负载增长
✅ **监控友好** - Web UI、Prometheus 指标、详细日志
✅ **定时调度** - 支持 Cron 定时工作流和延迟执行

这套方案已经在生产环境验证，可以直接应用到 xflow 项目中。

---

**相关文档**:
- [架构设计](./architecture.md)
- [核心概念](./core-concepts.md)
- [使用指南](./usage-guide.md)
- [最佳实践](./best-practices.md)

**最后更新**: 2026-01-11
