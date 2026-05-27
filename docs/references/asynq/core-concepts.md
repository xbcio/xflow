# Asynq 核心概念详解

## 概念概览

Asynq 的核心概念按功能可分为以下几类：

```
Asynq 核心概念
├── 1. Task (任务)
├── 2. Queue (队列)
├── 3. Handler (处理器)
├── 4. Client (客户端)
├── 5. Server (服务器)
├── 6. Scheduler (调度器)
├── 7. Inspector (检查器)
└── 8. Middleware (中间件)
```

---

## 1. Task (任务)

任务是 Asynq 中的基本工作单元，包含类型标识和 Payload 数据。

### 任务结构

```go
type Task struct {
    // typename 任务类型，用于路由到对应的 Handler
    typename string

    // payload 任务数据（通常是 JSON 序列化后的字节数组）
    payload []byte

    // headers 额外的元数据
    headers map[string]string

    // opts 任务选项
    opts []Option

    // w ResultWriter 用于写入任务执行结果
    w *ResultWriter
}

// 访问方法
func (t *Task) Type() string               { return t.typename }
func (t *Task) Payload() []byte            { return t.payload }
func (t *Task) Headers() map[string]string { return t.headers }
func (t *Task) ResultWriter() *ResultWriter { return t.w }
```

### 创建任务

```go
// 1. 基本任务
task := asynq.NewTask("email:deliver", payload)

// 2. 带 Headers 的任务
headers := map[string]string{
    "X-Request-ID": "abc123",
    "X-User-ID":    "42",
}
task := asynq.NewTaskWithHeaders("email:deliver", payload, headers)

// 3. 带选项的任务
task := asynq.NewTask(
    "email:deliver",
    payload,
    asynq.MaxRetry(5),
    asynq.Timeout(3*time.Minute),
)
```

### 任务类型设计原则

```go
// ✅ 好的实践：使用常量定义任务类型
const (
    TypeEmailDelivery    = "email:deliver"
    TypeImageResize      = "image:resize"
    TypeOrderNotification = "order:notification"
)

// ✅ 好的实践：按模块分组
const (
    // 用户模块
    TypeUserRegistration = "user:registration"
    TypeUserDeletion     = "user:deletion"

    // 订单模块
    TypeOrderCreate = "order:create"
    TypeOrderCancel = "order:cancel"
)

// ❌ 避免：使用魔法字符串
client.Enqueue(asynq.NewTask("send_email", payload)) // 不推荐
```

### TaskInfo（任务信息）

任务提交后返回的详细信息：

```go
type TaskInfo struct {
    ID            string        // 任务唯一标识符
    Queue         string        // 所属队列
    Type          string        // 任务类型
    Payload       []byte        // 任务数据
    Headers       map[string]string // 元数据
    State         TaskState     // 任务状态
    MaxRetry      int           // 最大重试次数
    Retried       int           // 已重试次数
    LastErr       string        // 最后一次错误信息
    LastFailedAt  time.Time     // 最后失败时间
    Timeout       time.Duration // 超时时间
    Deadline      time.Time     // 截止时间
    Group         string        // 任务分组
    NextProcessAt time.Time     // 下次处理时间
    IsOrphaned    bool          // 是否孤立
    Retention     time.Duration // 保留时间
    CompletedAt   time.Time     // 完成时间
    Result        []byte        // 执行结果
}

// 任务状态枚举
type TaskState int

const (
    TaskStatePending    TaskState = iota // 待处理
    TaskStateActive                      // 执行中
    TaskStateScheduled                   // 已调度（延迟）
    TaskStateRetry                       // 待重试
    TaskStateArchived                    // 已归档
    TaskStateCompleted                   // 已完成
    TaskStateAggregating                 // 聚合中
)
```

---

## 2. Queue (队列)

队列用于组织和隔离不同类型或优先级的任务。

### 队列命名

```go
// 默认队列
const DefaultQueue = "default"

// 自定义队列
const (
    QueueCritical = "critical" // 高优先级
    QueueDefault  = "default"  // 默认
    QueueLow      = "low"      // 低优先级
)
```

### 队列优先级配置

```go
// Server 配置队列优先级
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 10,
        Queues: map[string]int{
            "critical": 6, // 60% 的 Worker 处理 critical 队列
            "default":  3, // 30% 的 Worker 处理 default 队列
            "low":      1, // 10% 的 Worker 处理 low 队列
        },
    },
)
```

### 队列处理模式

#### 1. 权重模式（默认）

Worker 按权重比例分配到各个队列：

```go
Queues: map[string]int{
    "critical": 6, // 权重 6
    "default":  3, // 权重 3
    "low":      1, // 权重 1
}
// 总权重 = 6 + 3 + 1 = 10
// critical: 60% workers
// default: 30% workers
// low: 10% workers
```

#### 2. 严格优先级模式

高优先级队列有任务时，不处理低优先级队列：

```go
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 10,
        Queues: map[string]int{
            "critical": 1,
            "default":  1,
            "low":      1,
        },
        StrictPriority: true, // 启用严格优先级
    },
)

// 处理顺序：
// 1. 如果 critical 有任务，处理 critical
// 2. 否则，如果 default 有任务，处理 default
// 3. 否则，处理 low
```

### 任务提交到指定队列

```go
// Client 提交任务到指定队列
task := asynq.NewTask("email:deliver", payload)

info, err := client.Enqueue(
    task,
    asynq.Queue("critical"), // 提交到 critical 队列
)
```

---

## 3. Handler (处理器)

Handler 定义任务的实际执行逻辑。

### Handler 接口

```go
// Handler 接口
type Handler interface {
    ProcessTask(ctx context.Context, task *Task) error
}

// HandlerFunc 适配器（类似 http.HandlerFunc）
type HandlerFunc func(context.Context, *Task) error

func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
    return fn(ctx, task)
}
```

### 实现 Handler

#### 方式1：函数式 Handler

```go
func HandleEmailDelivery(ctx context.Context, t *asynq.Task) error {
    var payload EmailPayload
    if err := json.Unmarshal(t.Payload(), &payload); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Sending email to %s", payload.Email)

    // 实际的邮件发送逻辑
    if err := sendEmail(ctx, payload); err != nil {
        return err // 返回错误会触发重试
    }

    return nil // 返回 nil 表示成功
}
```

#### 方式2：结构体 Handler

```go
type EmailHandler struct {
    emailClient *EmailClient
    logger      *log.Logger
}

func (h *EmailHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
    var p EmailPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    h.logger.Printf("Processing email task for %s", p.Email)

    return h.emailClient.Send(ctx, p)
}

// 使用
handler := &EmailHandler{
    emailClient: emailClient,
    logger:      logger,
}
mux.Handle(TypeEmailDelivery, handler)
```

### Handler 上下文 (Context)

Handler 接收的 Context 包含有用的信息：

```go
func HandleTask(ctx context.Context, t *asynq.Task) error {
    // 1. 获取任务 ID
    taskID, ok := asynq.GetTaskID(ctx)
    if ok {
        log.Printf("Processing task: %s", taskID)
    }

    // 2. 获取重试次数
    retried, ok := asynq.GetRetryCount(ctx)
    if ok {
        log.Printf("Retry count: %d", retried)
    }

    // 3. 获取最大重试次数
    maxRetry, ok := asynq.GetMaxRetry(ctx)
    if ok {
        log.Printf("Max retry: %d", maxRetry)
    }

    // 4. 获取队列名称
    queue, ok := asynq.GetQueueName(ctx)
    if ok {
        log.Printf("Queue: %s", queue)
    }

    // 5. 检查超时
    select {
    case <-ctx.Done():
        return fmt.Errorf("task cancelled: %w", ctx.Err())
    default:
        // 继续处理
    }

    return nil
}
```

### 写入任务结果

```go
func HandleTask(ctx context.Context, t *asynq.Task) error {
    // 处理任务...
    result := processTask(t)

    // 写入结果
    w := t.ResultWriter()
    if w != nil {
        data, _ := json.Marshal(result)
        _, err := w.Write(data)
        if err != nil {
            log.Printf("Failed to write result: %v", err)
        }
    }

    return nil
}

// 查询结果
inspector := asynq.NewInspector(redisOpt)
info, err := inspector.GetTaskInfo(queueName, taskID)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Result: %s\n", string(info.Result))
```

---

## 4. Client (客户端)

Client 负责创建和提交任务。

### 创建 Client

```go
// 1. 基本配置
client := asynq.NewClient(asynq.RedisClientOpt{
    Addr: "localhost:6379",
})
defer client.Close()

// 2. 完整配置
client := asynq.NewClient(asynq.RedisClientOpt{
    Addr:         "localhost:6379",
    Password:     "secret",
    DB:           0,
    DialTimeout:  10 * time.Second,
    ReadTimeout:  30 * time.Second,
    WriteTimeout: 30 * time.Second,
    PoolSize:     20,
})

// 3. Redis Cluster
client := asynq.NewClient(asynq.RedisClusterClientOpt{
    Addrs: []string{
        "localhost:7000",
        "localhost:7001",
        "localhost:7002",
    },
})

// 4. Redis Sentinel
client := asynq.NewClient(asynq.RedisFailoverClientOpt{
    MasterName:    "mymaster",
    SentinelAddrs: []string{
        "localhost:26379",
        "localhost:26380",
        "localhost:26381",
    },
})
```

### 提交任务

```go
// 1. 立即执行
task := asynq.NewTask("email:deliver", payload)
info, err := client.Enqueue(task)

// 2. 延迟执行
info, err := client.Enqueue(
    task,
    asynq.ProcessIn(24*time.Hour), // 24 小时后执行
)

// 3. 指定时间执行
info, err := client.Enqueue(
    task,
    asynq.ProcessAt(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)),
)

// 4. 设置多个选项
info, err := client.Enqueue(
    task,
    asynq.Queue("critical"),              // 指定队列
    asynq.MaxRetry(5),                    // 最大重试 5 次
    asynq.Timeout(10*time.Minute),        // 超时 10 分钟
    asynq.Deadline(time.Now().Add(1*time.Hour)), // 1 小时截止
    asynq.Unique(24*time.Hour),           // 24 小时内去重
    asynq.Retention(7*24*time.Hour),      // 保留 7 天
    asynq.ProcessIn(5*time.Minute),       // 5 分钟后执行
)
```

### 任务选项详解

```go
// 1. MaxRetry - 最大重试次数
asynq.MaxRetry(5) // 最多重试 5 次，默认 25 次

// 2. Queue - 指定队列
asynq.Queue("critical") // 提交到 critical 队列，默认 "default"

// 3. Timeout - 任务超时时间
asynq.Timeout(10*time.Minute) // 超时 10 分钟，默认 30 分钟

// 4. Deadline - 任务截止时间
asynq.Deadline(time.Now().Add(1*time.Hour)) // 1 小时内必须完成

// 5. Unique - 任务去重时间窗口
asynq.Unique(24*time.Hour) // 24 小时内不允许重复提交相同任务

// 6. ProcessIn - 延迟执行
asynq.ProcessIn(5*time.Minute) // 5 分钟后执行

// 7. ProcessAt - 指定时间执行
asynq.ProcessAt(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC))

// 8. Retention - 任务保留时间
asynq.Retention(7*24*time.Hour) // 完成后保留 7 天，默认 1 小时

// 9. TaskID - 自定义任务 ID
asynq.TaskID("custom-id-123") // 自定义任务 ID

// 10. Group - 任务分组（用于任务聚合）
asynq.Group("notifications:daily")
```

### 任务去重

```go
// 使用 Unique 选项实现任务去重
task := asynq.NewTask("email:deliver", payload)

info, err := client.Enqueue(
    task,
    asynq.Unique(24*time.Hour), // 24 小时内去重
)

if err != nil {
    if err == asynq.ErrDuplicateTask {
        log.Println("Task already exists")
    } else {
        log.Fatal(err)
    }
}
```

---

## 5. Server (服务器)

Server 负责从 Redis 拉取任务并执行。

### 创建 Server

```go
srv := asynq.NewServer(
    asynq.RedisClientOpt{Addr: "localhost:6379"},
    asynq.Config{
        // 并发 Worker 数量
        Concurrency: 10,

        // 队列优先级
        Queues: map[string]int{
            "critical": 6,
            "default":  3,
            "low":      1,
        },

        // 严格优先级模式
        StrictPriority: false,

        // 错误处理器
        ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
            retried, _ := asynq.GetRetryCount(ctx)
            maxRetry, _ := asynq.GetMaxRetry(ctx)
            log.Printf("Task %s failed: %v (retried=%d, max=%d)", task.Type(), err, retried, maxRetry)
        }),

        // 重试延迟函数
        RetryDelayFunc: func(n int, e error, t *asynq.Task) time.Duration {
            // 指数退避：2^n 秒
            return time.Duration(1<<uint(n)) * time.Second
        },

        // 健康检查
        HealthCheckFunc: func(err error) {
            if err != nil {
                log.Printf("Health check failed: %v", err)
            }
        },

        // 健康检查间隔
        HealthCheckInterval: 15 * time.Second,

        // 优雅关闭超时
        ShutdownTimeout: 30 * time.Second,

        // 日志级别
        LogLevel: asynq.InfoLevel,

        // 任务聚合器
        GroupGracePeriod: 1 * time.Minute,
        GroupMaxDelay:    5 * time.Minute,
        GroupMaxSize:     100,
    },
)
```

### 注册 Handler

```go
// 1. 使用 ServeMux
mux := asynq.NewServeMux()

// 注册函数式 Handler
mux.HandleFunc(TypeEmailDelivery, HandleEmailDelivery)

// 注册结构体 Handler
mux.Handle(TypeImageResize, &ImageResizeHandler{})

// 使用中间件
mux.Use(loggingMiddleware)

// 2. 启动 Server
if err := srv.Run(mux); err != nil {
    log.Fatalf("could not run server: %v", err)
}
```

### ServeMux（路由器）

```go
mux := asynq.NewServeMux()

// 1. 注册 Handler
mux.HandleFunc("email:deliver", HandleEmailDelivery)
mux.HandleFunc("image:resize", HandleImageResize)

// 2. 使用模式匹配
mux.HandleFunc("email:*", HandleEmailTasks)    // 处理所有 email:* 任务
mux.HandleFunc("order:*", HandleOrderTasks)    // 处理所有 order:* 任务

// 3. 默认 Handler（类似 http.DefaultServeMux）
mux.HandleFunc("default", HandleDefault) // 处理未匹配的任务
```

---

## 6. Scheduler (调度器)

Scheduler 用于管理定时任务（Cron Jobs）。

### 创建 Scheduler

```go
loc, _ := time.LoadLocation("Asia/Shanghai")

scheduler := asynq.NewScheduler(
    asynq.RedisClientOpt{Addr: "localhost:6379"},
    &asynq.SchedulerOpts{
        Location:                  loc,                 // 时区
        EnqueueErrorHandler:       nil,                 // 入队错误处理器
        PostEnqueueFunc:           nil,                 // 入队后回调
        LogLevel:                  asynq.InfoLevel,     // 日志级别
    },
)
```

### 注册定时任务

```go
// 1. 基本 Cron 任务
task := asynq.NewTask("report:daily", nil)
entryID, err := scheduler.Register(
    "0 8 * * *", // 每天早上 8 点
    task,
)

// 2. 带选项的 Cron 任务
entryID, err := scheduler.Register(
    "*/5 * * * *",  // 每 5 分钟
    task,
    asynq.Queue("critical"),
    asynq.Timeout(10*time.Minute),
)

// 3. 注销 Cron 任务
err := scheduler.Unregister(entryID)
```

### Cron 表达式

```
# ┌───────────── 分钟 (0 - 59)
# │ ┌───────────── 小时 (0 - 23)
# │ │ ┌───────────── 日 (1 - 31)
# │ │ │ ┌───────────── 月 (1 - 12)
# │ │ │ │ ┌───────────── 星期 (0 - 6) (0 = 星期日)
# │ │ │ │ │
# * * * * *

# 示例
"0 8 * * *"       # 每天早上 8:00
"*/15 * * * *"    # 每 15 分钟
"0 */2 * * *"     # 每 2 小时
"0 0 * * 0"       # 每周日午夜
"0 0 1 * *"       # 每月 1 号午夜
"0 9 * * 1-5"     # 工作日早上 9:00
```

---

## 7. Inspector (检查器)

Inspector 用于查询和管理任务队列。

### 创建 Inspector

```go
inspector := asynq.NewInspector(asynq.RedisClientOpt{
    Addr: "localhost:6379",
})
```

### 查询队列信息

```go
// 1. 列出所有队列
queues, err := inspector.Queues()
for _, qname := range queues {
    fmt.Println(qname)
}

// 2. 获取队列详细信息
info, err := inspector.GetQueueInfo("default")
fmt.Printf("Queue: %s\n", info.Queue)
fmt.Printf("Size: %d\n", info.Size)
fmt.Printf("Pending: %d\n", info.Pending)
fmt.Printf("Active: %d\n", info.Active)
fmt.Printf("Scheduled: %d\n", info.Scheduled)
fmt.Printf("Retry: %d\n", info.Retry)
fmt.Printf("Archived: %d\n", info.Archived)
fmt.Printf("Completed: %d\n", info.Completed)
```

### 查询任务

```go
// 1. 列出待处理任务
tasks, err := inspector.ListPendingTasks("default")
for _, task := range tasks {
    fmt.Printf("Task ID: %s, Type: %s\n", task.ID, task.Type)
}

// 2. 列出活动任务
tasks, err := inspector.ListActiveTasks("default")

// 3. 列出延迟任务
tasks, err := inspector.ListScheduledTasks("default")

// 4. 列出重试任务
tasks, err := inspector.ListRetryTasks("default")

// 5. 列出已归档任务
tasks, err := inspector.ListArchivedTasks("default")

// 6. 获取任务详情
info, err := inspector.GetTaskInfo("default", taskID)
fmt.Printf("State: %v\n", info.State)
fmt.Printf("Retried: %d/%d\n", info.Retried, info.MaxRetry)
fmt.Printf("Last Error: %s\n", info.LastErr)
```

### 管理任务

```go
// 1. 删除任务
err := inspector.DeleteTask("default", taskID)

// 2. 重新入队任务（从 archived/retry 移到 pending）
err := inspector.RunTask("default", taskID)

// 3. 归档任务
err := inspector.ArchiveTask("default", taskID)

// 4. 批量删除
n, err := inspector.DeleteAllPendingTasks("default")
fmt.Printf("Deleted %d tasks\n", n)

// 5. 批量归档
n, err := inspector.ArchiveAllRetryTasks("default")

// 6. 批量重新入队
n, err := inspector.RunAllArchivedTasks("default")
```

### 查询服务器和 Worker

```go
// 1. 列出所有活跃的 Server
servers, err := inspector.Servers()
for _, srv := range servers {
    fmt.Printf("Server ID: %s\n", srv.ID)
    fmt.Printf("Host: %s\n", srv.Host)
    fmt.Printf("PID: %d\n", srv.PID)
    fmt.Printf("Concurrency: %d\n", srv.Concurrency)
    fmt.Printf("Queues: %v\n", srv.Queues)
    fmt.Printf("Started: %v\n", srv.Started)
}

// 2. 列出所有活跃的 Worker
workers, err := inspector.Workers()
for _, w := range workers {
    fmt.Printf("Worker ID: %s\n", w.ID)
    fmt.Printf("Server ID: %s\n", w.ServerID)
    fmt.Printf("Task ID: %s\n", w.TaskID)
    fmt.Printf("Task Type: %s\n", w.TaskType)
    fmt.Printf("Payload: %s\n", string(w.TaskPayload))
    fmt.Printf("Started: %v\n", w.Started)
}
```

---

## 8. Middleware (中间件)

中间件用于在任务执行前后添加通用逻辑。

### 中间件接口

```go
// MiddlewareFunc 类型
type MiddlewareFunc func(Handler) Handler
```

### 内置中间件

```go
import "github.com/hibiken/asynq/x/metrics"
import "github.com/hibiken/asynq/x/ratelimit"

// 1. 日志中间件
func loggingMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
        start := time.Now()
        log.Printf("Start processing %q", t.Type())

        err := h.ProcessTask(ctx, t)

        if err != nil {
            log.Printf("Error processing %q: %v", t.Type(), err)
        }

        log.Printf("Finished processing %q: Elapsed=%v", t.Type(), time.Since(start))
        return err
    })
}

// 2. 错误恢复中间件
func recoveryMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
        defer func() {
            if r := recover(); r != nil {
                err = fmt.Errorf("panic: %v", r)
                log.Printf("Recovered from panic: %v", r)
                debug.PrintStack()
            }
        }()
        return h.ProcessTask(ctx, t)
    })
}

// 3. 性能追踪中间件
func tracingMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
        span, ctx := tracer.StartSpan(ctx, t.Type())
        defer span.Finish()

        err := h.ProcessTask(ctx, t)

        if err != nil {
            span.SetTag("error", true)
            span.LogFields(log.String("error.message", err.Error()))
        }

        return err
    })
}

// 4. Prometheus 指标中间件
prometheusMiddleware := metrics.NewPrometheusMetrics(prometheus.DefaultRegisterer)

// 5. 限流中间件
rateLimitMiddleware := ratelimit.NewLimiter(rate.NewLimiter(rate.Limit(10), 1))
```

### 使用中间件

```go
mux := asynq.NewServeMux()

// 1. 应用到所有 Handler
mux.Use(loggingMiddleware)
mux.Use(recoveryMiddleware)
mux.Use(prometheusMiddleware.MiddlewareFunc())

// 2. 链式调用
mux.Use(
    loggingMiddleware,
    recoveryMiddleware,
    tracingMiddleware,
)

// 3. 应用到单个 Handler
handler := loggingMiddleware(asynq.HandlerFunc(HandleEmailDelivery))
mux.Handle(TypeEmailDelivery, handler)
```

### 自定义中间件示例

```go
// 认证中间件
func authMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
        // 从 Headers 获取认证信息
        token := t.Headers()["Authorization"]
        if token == "" {
            return fmt.Errorf("missing authorization: %w", asynq.SkipRetry)
        }

        // 验证 token
        user, err := validateToken(token)
        if err != nil {
            return fmt.Errorf("invalid token: %w", asynq.SkipRetry)
        }

        // 将用户信息添加到 context
        ctx = context.WithValue(ctx, "user", user)

        return h.ProcessTask(ctx, t)
    })
}

// 重试限制中间件
func retryLimitMiddleware(maxRetries int) func(asynq.Handler) asynq.Handler {
    return func(h asynq.Handler) asynq.Handler {
        return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
            retried, _ := asynq.GetRetryCount(ctx)
            if retried >= maxRetries {
                return fmt.Errorf("exceeded retry limit: %w", asynq.SkipRetry)
            }
            return h.ProcessTask(ctx, t)
        })
    }
}

// 超时控制中间件
func timeoutMiddleware(timeout time.Duration) func(asynq.Handler) asynq.Handler {
    return func(h asynq.Handler) asynq.Handler {
        return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
            ctx, cancel := context.WithTimeout(ctx, timeout)
            defer cancel()

            done := make(chan error, 1)
            go func() {
                done <- h.ProcessTask(ctx, t)
            }()

            select {
            case err := <-done:
                return err
            case <-ctx.Done():
                return fmt.Errorf("task timeout: %w", ctx.Err())
            }
        })
    }
}
```

---

## 核心概念总结

### 数据流

```
Client.Enqueue(task)
    ↓
Task → Queue (via Redis)
    ↓
Server.Dequeue() → Active Queue
    ↓
ServeMux.Route(task.Type) → Handler
    ↓
Middleware Chain → ProcessTask
    ↓
Success → Completed / Failure → Retry Queue
```

### 关键设计原则

1. **任务设计**
   - 使用常量定义任务类型
   - Payload 序列化为 JSON
   - 保持 Payload 小巧（< 256KB）
   - 任务应幂等

2. **队列管理**
   - 按业务重要性分队列
   - 合理配置队列优先级
   - 避免队列过多（通常 3-5 个）

3. **Handler 实现**
   - 处理超时和取消
   - 区分临时错误和永久错误
   - 合理设置重试次数
   - 记录详细日志

4. **错误处理**
   - 临时错误：返回 error，触发重试
   - 永久错误：wrap `asynq.SkipRetry`，不重试
   - Panic：使用 recovery 中间件捕获

5. **监控和运维**
   - 使用 Inspector 监控队列状态
   - 集成 Prometheus 收集指标
   - 使用 asynqmon Web UI 管理任务

---

**下一章**: [使用指南](./usage-guide.md)
