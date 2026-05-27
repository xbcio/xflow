# Asynq 使用指南

本文档提供 Asynq 的完整使用教程，包含丰富的代码示例和最佳实践。

## 目录

- [快速开始](#快速开始)
- [基础用法](#基础用法)
- [进阶用法](#进阶用法)
- [实战案例](#实战案例)
- [故障排查](#故障排查)

---

## 快速开始

### 安装依赖

```bash
# 安装 Asynq
go get -u github.com/hibiken/asynq

# 启动 Redis
docker run -d -p 6379:6379 redis:7-alpine

# 安装 Web UI (可选)
go install github.com/hibiken/asynq/tools/asynqmon@latest
```

### 最小化示例

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
)

// 1. 定义任务类型
const TypeWelcomeEmail = "email:welcome"

// 2. 定义 Payload
type WelcomeEmailPayload struct {
    UserID int    `json:"user_id"`
    Email  string `json:"email"`
}

// 3. 创建任务工厂函数
func NewWelcomeEmailTask(userID int, email string) (*asynq.Task, error) {
    payload, err := json.Marshal(WelcomeEmailPayload{
        UserID: userID,
        Email:  email,
    })
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeWelcomeEmail, payload), nil
}

// 4. 实现任务处理器
func HandleWelcomeEmailTask(ctx context.Context, t *asynq.Task) error {
    var p WelcomeEmailPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Sending welcome email to %s (user_id=%d)", p.Email, p.UserID)

    // 模拟邮件发送
    time.Sleep(2 * time.Second)

    return nil
}

func main() {
    // 5. 创建 Redis 连接选项
    redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}

    // 6. 提交任务（Client）
    client := asynq.NewClient(redisOpt)
    defer client.Close()

    task, err := NewWelcomeEmailTask(42, "user@example.com")
    if err != nil {
        log.Fatal(err)
    }

    info, err := client.Enqueue(task)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Enqueued task: id=%s queue=%s", info.ID, info.Queue)

    // 7. 处理任务（Server）
    srv := asynq.NewServer(
        redisOpt,
        asynq.Config{Concurrency: 10},
    )

    mux := asynq.NewServeMux()
    mux.HandleFunc(TypeWelcomeEmail, HandleWelcomeEmailTask)

    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}
```

---

## 基础用法

### 1. 项目结构

推荐的项目结构：

```
myapp/
├── cmd/
│   ├── server/          # Worker 服务器
│   │   └── main.go
│   ├── scheduler/       # 定时任务调度器
│   │   └── main.go
│   └── web/             # Web 应用（提交任务）
│       └── main.go
├── internal/
│   └── tasks/           # 任务定义和处理器
│       ├── tasks.go     # 任务类型常量
│       ├── email.go     # 邮件相关任务
│       ├── image.go     # 图片处理任务
│       └── order.go     # 订单相关任务
├── config/
│   └── config.go        # 配置管理
└── go.mod
```

### 2. 任务定义 (internal/tasks/tasks.go)

```go
package tasks

// 任务类型常量
const (
    // 邮件任务
    TypeEmailWelcome      = "email:welcome"
    TypeEmailVerification = "email:verification"
    TypeEmailPasswordReset = "email:password_reset"

    // 图片处理任务
    TypeImageResize   = "image:resize"
    TypeImageThumbnail = "image:thumbnail"

    // 订单任务
    TypeOrderCreate = "order:create"
    TypeOrderCancel = "order:cancel"
    TypeOrderNotify = "order:notify"

    // 数据任务
    TypeDataExport = "data:export"
    TypeDataImport = "data:import"
)
```

### 3. 邮件任务实现 (internal/tasks/email.go)

```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/hibiken/asynq"
)

// ============================================
// Welcome Email Task
// ============================================

type WelcomeEmailPayload struct {
    UserID int    `json:"user_id"`
    Email  string `json:"email"`
    Name   string `json:"name"`
}

func NewWelcomeEmailTask(userID int, email, name string) (*asynq.Task, error) {
    payload, err := json.Marshal(WelcomeEmailPayload{
        UserID: userID,
        Email:  email,
        Name:   name,
    })
    if err != nil {
        return nil, err
    }

    return asynq.NewTask(TypeEmailWelcome, payload), nil
}

func HandleWelcomeEmailTask(ctx context.Context, t *asynq.Task) error {
    var p WelcomeEmailPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Sending welcome email to %s (user_id=%d)", p.Email, p.UserID)

    // 实际的邮件发送逻辑
    if err := sendWelcomeEmail(ctx, p.Email, p.Name); err != nil {
        return fmt.Errorf("sendWelcomeEmail failed: %w", err)
    }

    return nil
}

func sendWelcomeEmail(ctx context.Context, email, name string) error {
    // 实际实现...
    return nil
}

// ============================================
// Email Verification Task
// ============================================

type EmailVerificationPayload struct {
    UserID         int    `json:"user_id"`
    Email          string `json:"email"`
    VerificationURL string `json:"verification_url"`
}

func NewEmailVerificationTask(userID int, email, verificationURL string) (*asynq.Task, error) {
    payload, err := json.Marshal(EmailVerificationPayload{
        UserID:         userID,
        Email:          email,
        VerificationURL: verificationURL,
    })
    if err != nil {
        return nil, err
    }

    // 设置任务选项
    return asynq.NewTask(
        TypeEmailVerification,
        payload,
        asynq.MaxRetry(5),                // 最多重试 5 次
        asynq.Timeout(30*time.Second),    // 30 秒超时
        asynq.Queue("critical"),          // 关键队列
    ), nil
}

func HandleEmailVerificationTask(ctx context.Context, t *asynq.Task) error {
    var p EmailVerificationPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Sending verification email to %s", p.Email)

    if err := sendVerificationEmail(ctx, p.Email, p.VerificationURL); err != nil {
        return fmt.Errorf("sendVerificationEmail failed: %w", err)
    }

    return nil
}

func sendVerificationEmail(ctx context.Context, email, url string) error {
    // 实际实现...
    return nil
}
```

### 4. 图片处理任务 (internal/tasks/image.go)

```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/hibiken/asynq"
)

type ImageResizePayload struct {
    ImageID  string `json:"image_id"`
    SourceURL string `json:"source_url"`
    Width     int    `json:"width"`
    Height    int    `json:"height"`
}

func NewImageResizeTask(imageID, sourceURL string, width, height int) (*asynq.Task, error) {
    payload, err := json.Marshal(ImageResizePayload{
        ImageID:  imageID,
        SourceURL: sourceURL,
        Width:     width,
        Height:    height,
    })
    if err != nil {
        return nil, err
    }

    return asynq.NewTask(
        TypeImageResize,
        payload,
        asynq.MaxRetry(3),
        asynq.Timeout(5*time.Minute), // 图片处理可能较慢
        asynq.Queue("default"),
    ), nil
}

func HandleImageResizeTask(ctx context.Context, t *asynq.Task) error {
    var p ImageResizePayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Resizing image %s to %dx%d", p.ImageID, p.Width, p.Height)

    // 检查上下文是否已取消
    select {
    case <-ctx.Done():
        return fmt.Errorf("task cancelled: %w", ctx.Err())
    default:
    }

    // 执行图片处理
    if err := resizeImage(ctx, p.SourceURL, p.Width, p.Height); err != nil {
        return fmt.Errorf("resizeImage failed: %w", err)
    }

    // 写入处理结果
    result := map[string]interface{}{
        "image_id": p.ImageID,
        "width":    p.Width,
        "height":   p.Height,
        "success":  true,
    }

    if w := t.ResultWriter(); w != nil {
        data, _ := json.Marshal(result)
        w.Write(data)
    }

    return nil
}

func resizeImage(ctx context.Context, sourceURL string, width, height int) error {
    // 实际实现...
    return nil
}
```

### 5. Worker 服务器 (cmd/server/main.go)

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/hibiken/asynq"
    "myapp/internal/tasks"
)

func main() {
    // 1. 创建 Redis 连接选项
    redisOpt := asynq.RedisClientOpt{
        Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
        Password: getEnv("REDIS_PASSWORD", ""),
        DB:       getEnvAsInt("REDIS_DB", 0),
    }

    // 2. 创建 Server
    srv := asynq.NewServer(
        redisOpt,
        asynq.Config{
            // 并发数
            Concurrency: getEnvAsInt("WORKER_CONCURRENCY", 10),

            // 队列配置
            Queues: map[string]int{
                "critical": 6,
                "default":  3,
                "low":      1,
            },

            // 严格优先级
            StrictPriority: false,

            // 错误处理器
            ErrorHandler: asynq.ErrorHandlerFunc(logError),

            // 重试延迟函数
            RetryDelayFunc: exponentialBackoff,

            // 日志级别
            LogLevel: asynq.InfoLevel,

            // 优雅关闭超时
            ShutdownTimeout: 30 * time.Second,
        },
    )

    // 3. 创建 ServeMux 并注册 Handler
    mux := asynq.NewServeMux()

    // 使用中间件
    mux.Use(loggingMiddleware)
    mux.Use(recoveryMiddleware)

    // 注册邮件任务
    mux.HandleFunc(tasks.TypeEmailWelcome, tasks.HandleWelcomeEmailTask)
    mux.HandleFunc(tasks.TypeEmailVerification, tasks.HandleEmailVerificationTask)

    // 注册图片任务
    mux.HandleFunc(tasks.TypeImageResize, tasks.HandleImageResizeTask)
    mux.HandleFunc(tasks.TypeImageThumbnail, tasks.HandleImageThumbnailTask)

    // 注册订单任务
    mux.HandleFunc(tasks.TypeOrderCreate, tasks.HandleOrderCreateTask)
    mux.HandleFunc(tasks.TypeOrderCancel, tasks.HandleOrderCancelTask)

    // 4. 启动 Server
    log.Println("Starting worker server...")
    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}

// 日志中间件
func loggingMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
        start := time.Now()
        taskID, _ := asynq.GetTaskID(ctx)

        log.Printf("[%s] Start processing %q", taskID, t.Type())

        err := h.ProcessTask(ctx, t)

        if err != nil {
            log.Printf("[%s] Error processing %q: %v (elapsed=%v)",
                taskID, t.Type(), err, time.Since(start))
        } else {
            log.Printf("[%s] Finished processing %q (elapsed=%v)",
                taskID, t.Type(), time.Since(start))
        }

        return err
    })
}

// 错误恢复中间件
func recoveryMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
        defer func() {
            if r := recover(); r != nil {
                log.Printf("Recovered from panic: %v", r)
                err = fmt.Errorf("panic: %v", r)
            }
        }()
        return h.ProcessTask(ctx, t)
    })
}

// 错误处理器
func logError(ctx context.Context, task *asynq.Task, err error) {
    retried, _ := asynq.GetRetryCount(ctx)
    maxRetry, _ := asynq.GetMaxRetry(ctx)
    taskID, _ := asynq.GetTaskID(ctx)

    log.Printf("[%s] Task %s failed: %v (retried=%d, max=%d)",
        taskID, task.Type(), err, retried, maxRetry)
}

// 指数退避重试延迟
func exponentialBackoff(n int, e error, t *asynq.Task) time.Duration {
    // n 是第几次重试 (从 0 开始)
    // 2^n 秒，最大 1 小时
    delay := time.Duration(1<<uint(n)) * time.Second
    if delay > time.Hour {
        delay = time.Hour
    }
    return delay
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        if intValue, err := strconv.Atoi(value); err == nil {
            return intValue
        }
    }
    return defaultValue
}
```

### 6. Web 应用 (cmd/web/main.go)

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"

    "github.com/hibiken/asynq"
    "myapp/internal/tasks"
)

var asynqClient *asynq.Client

func main() {
    // 初始化 Asynq Client
    asynqClient = asynq.NewClient(asynq.RedisClientOpt{
        Addr: "localhost:6379",
    })
    defer asynqClient.Close()

    // 注册路由
    http.HandleFunc("/api/users/register", handleUserRegistration)
    http.HandleFunc("/api/images/upload", handleImageUpload)
    http.HandleFunc("/api/orders/create", handleOrderCreate)

    log.Println("Starting web server on :8080...")
    log.Fatal(http.ListenAndServe(":8080", nil))
}

// 用户注册处理器
func handleUserRegistration(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    // 解析请求
    var req struct {
        Email string `json:"email"`
        Name  string `json:"name"`
    }

    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 保存用户到数据库...
    userID := 123 // 假设这是新创建的用户 ID

    // 提交欢迎邮件任务
    task, err := tasks.NewWelcomeEmailTask(userID, req.Email, req.Name)
    if err != nil {
        log.Printf("Failed to create task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    info, err := asynqClient.Enqueue(task)
    if err != nil {
        log.Printf("Failed to enqueue task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    log.Printf("Enqueued welcome email task: id=%s", info.ID)

    // 返回响应
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "success": true,
        "user_id": userID,
        "task_id": info.ID,
    })
}

// 图片上传处理器
func handleImageUpload(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    // 解析上传的文件...
    imageID := "img_123"
    sourceURL := "https://example.com/uploads/original.jpg"

    // 提交图片处理任务
    task, err := tasks.NewImageResizeTask(imageID, sourceURL, 800, 600)
    if err != nil {
        log.Printf("Failed to create task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    // 立即执行
    info, err := asynqClient.Enqueue(task)
    if err != nil {
        log.Printf("Failed to enqueue task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    log.Printf("Enqueued image resize task: id=%s", info.ID)

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "success":  true,
        "image_id": imageID,
        "task_id":  info.ID,
    })
}

// 订单创建处理器
func handleOrderCreate(w http.ResponseWriter, r *http.Request) {
    // 创建订单...
    orderID := "order_123"

    // 提交订单取消任务（30 分钟后执行）
    task, err := tasks.NewOrderCancelTask(orderID)
    if err != nil {
        log.Printf("Failed to create task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    // 30 分钟后执行
    info, err := asynqClient.Enqueue(
        task,
        asynq.ProcessIn(30*time.Minute),
    )
    if err != nil {
        log.Printf("Failed to enqueue task: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }

    log.Printf("Enqueued order cancel task: id=%s (scheduled)", info.ID)

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "success":  true,
        "order_id": orderID,
        "task_id":  info.ID,
    })
}
```

---

## 进阶用法

### 1. 定时任务 (Cron Jobs)

```go
// cmd/scheduler/main.go
package main

import (
    "log"
    "time"

    "github.com/hibiken/asynq"
    "myapp/internal/tasks"
)

func main() {
    loc, err := time.LoadLocation("Asia/Shanghai")
    if err != nil {
        log.Fatal(err)
    }

    scheduler := asynq.NewScheduler(
        asynq.RedisClientOpt{Addr: "localhost:6379"},
        &asynq.SchedulerOpts{
            Location: loc,
            LogLevel: asynq.InfoLevel,
        },
    )

    // 每天早上 8 点发送日报
    task, _ := tasks.NewDailyReportTask()
    entryID, err := scheduler.Register(
        "0 8 * * *",
        task,
        asynq.Queue("critical"),
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Registered daily report: %s", entryID)

    // 每小时清理过期数据
    task, _ = tasks.NewDataCleanupTask()
    entryID, err = scheduler.Register(
        "0 * * * *",
        task,
        asynq.Queue("low"),
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Registered data cleanup: %s", entryID)

    // 每 5 分钟同步数据
    task, _ = tasks.NewDataSyncTask()
    entryID, err = scheduler.Register(
        "*/5 * * * *",
        task,
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Registered data sync: %s", entryID)

    log.Println("Starting scheduler...")
    if err := scheduler.Run(); err != nil {
        log.Fatal(err)
    }
}
```

### 2. 任务聚合 (Task Aggregation)

批量处理相似任务，提高效率：

```go
// Server 配置任务聚合
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 10,

        // 任务聚合配置
        GroupGracePeriod: 1 * time.Minute,  // 等待期：收集任务的时间窗口
        GroupMaxDelay:    5 * time.Minute,  // 最大延迟：最长等待时间
        GroupMaxSize:     100,               // 最大批量：一次最多聚合多少任务
    },
)

// 提交带分组的任务
task := asynq.NewTask("notification:send", payload)
client.Enqueue(
    task,
    asynq.Group("notifications:daily"), // 设置分组
)

// 处理聚合任务
func HandleAggregatedNotifications(ctx context.Context, tasks []*asynq.Task) error {
    log.Printf("Processing %d notifications in batch", len(tasks))

    var notifications []Notification
    for _, task := range tasks {
        var p NotificationPayload
        if err := json.Unmarshal(task.Payload(), &p); err != nil {
            continue
        }
        notifications = append(notifications, p)
    }

    // 批量发送通知
    return sendBatchNotifications(ctx, notifications)
}

// 注册聚合 Handler
mux.HandleFunc("notification:send", func(ctx context.Context, t *asynq.Task) error {
    // 单个任务处理
    return HandleSingleNotification(ctx, t)
})

// 如果配置了聚合，Asynq 会自动将相同 Group 的任务聚合
```

### 3. 任务依赖和工作流

使用任务结果和条件执行实现工作流：

```go
// 步骤 1: 数据导入
func HandleDataImportTask(ctx context.Context, t *asynq.Task) error {
    var p DataImportPayload
    json.Unmarshal(t.Payload(), &p)

    // 执行导入
    importID, err := importData(ctx, p.FileURL)
    if err != nil {
        return err
    }

    // 写入结果
    result := map[string]interface{}{
        "import_id": importID,
        "records":   1000,
    }

    if w := t.ResultWriter(); w != nil {
        data, _ := json.Marshal(result)
        w.Write(data)
    }

    // 提交下一步任务
    client := asynq.NewClient(redisOpt)
    defer client.Close()

    nextTask, _ := tasks.NewDataValidationTask(importID)
    client.Enqueue(nextTask, asynq.ProcessIn(1*time.Minute))

    return nil
}

// 步骤 2: 数据验证
func HandleDataValidationTask(ctx context.Context, t *asynq.Task) error {
    var p DataValidationPayload
    json.Unmarshal(t.Payload(), &p)

    // 执行验证
    valid, err := validateData(ctx, p.ImportID)
    if err != nil {
        return err
    }

    if !valid {
        return fmt.Errorf("data validation failed: %w", asynq.SkipRetry)
    }

    // 提交最后一步任务
    client := asynq.NewClient(redisOpt)
    defer client.Close()

    nextTask, _ := tasks.NewDataPublishTask(p.ImportID)
    client.Enqueue(nextTask)

    return nil
}

// 步骤 3: 数据发布
func HandleDataPublishTask(ctx context.Context, t *asynq.Task) error {
    var p DataPublishPayload
    json.Unmarshal(t.Payload(), &p)

    return publishData(ctx, p.ImportID)
}
```

### 4. 监控和告警

```go
// 集成 Prometheus
import (
    "github.com/hibiken/asynq/x/metrics"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
    // 创建 Prometheus 指标中间件
    prometheusMiddleware := metrics.NewPrometheusMetrics(prometheus.DefaultRegisterer)

    // 使用中间件
    mux := asynq.NewServeMux()
    mux.Use(prometheusMiddleware.MiddlewareFunc())

    // 注册 Handler...

    // 启动 metrics 服务器
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        log.Fatal(http.ListenAndServe(":9090", nil))
    }()

    // 启动 Worker
    srv := asynq.NewServer(redisOpt, cfg)
    srv.Run(mux)
}
```

### 5. 使用 Inspector 监控

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
)

func main() {
    inspector := asynq.NewInspector(asynq.RedisClientOpt{
        Addr: "localhost:6379",
    })

    // 定期检查队列状态
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        checkQueueStats(inspector)
        checkFailedTasks(inspector)
    }
}

func checkQueueStats(inspector *asynq.Inspector) {
    queues, err := inspector.Queues()
    if err != nil {
        log.Printf("Failed to list queues: %v", err)
        return
    }

    for _, qname := range queues {
        info, err := inspector.GetQueueInfo(qname)
        if err != nil {
            continue
        }

        fmt.Printf("Queue: %s\n", qname)
        fmt.Printf("  Pending: %d\n", info.Pending)
        fmt.Printf("  Active: %d\n", info.Active)
        fmt.Printf("  Retry: %d\n", info.Retry)
        fmt.Printf("  Archived: %d\n", info.Archived)

        // 告警：待处理任务过多
        if info.Pending > 1000 {
            log.Printf("ALERT: Queue %s has %d pending tasks", qname, info.Pending)
            // 发送告警...
        }

        // 告警：失败任务过多
        if info.Archived > 100 {
            log.Printf("ALERT: Queue %s has %d archived tasks", qname, info.Archived)
            // 发送告警...
        }
    }
}

func checkFailedTasks(inspector *asynq.Inspector) {
    // 检查最近失败的任务
    tasks, err := inspector.ListArchivedTasks("default", asynq.PageSize(10))
    if err != nil {
        log.Printf("Failed to list archived tasks: %v", err)
        return
    }

    for _, task := range tasks {
        log.Printf("Failed task: id=%s type=%s error=%s",
            task.ID, task.Type, task.LastErr)

        // 根据错误类型决定是否重新入队
        if shouldRetry(task.LastErr) {
            if err := inspector.RunTask("default", task.ID); err != nil {
                log.Printf("Failed to requeue task: %v", err)
            }
        }
    }
}

func shouldRetry(errMsg string) bool {
    // 判断是否应该重试
    // 例如：网络错误应该重试，数据错误不重试
    return strings.Contains(errMsg, "connection refused") ||
           strings.Contains(errMsg, "timeout")
}
```

---

## 实战案例

### 案例 1: 订单超时自动取消

```go
// 1. 创建订单时提交取消任务
func CreateOrder(userID int, items []Item) (*Order, error) {
    // 创建订单
    order := &Order{
        ID:     generateOrderID(),
        UserID: userID,
        Items:  items,
        Status: "pending",
    }

    // 保存到数据库
    if err := db.Create(order); err != nil {
        return nil, err
    }

    // 提交 30 分钟后的取消任务
    task, _ := tasks.NewOrderCancelTask(order.ID)
    client.Enqueue(
        task,
        asynq.ProcessIn(30*time.Minute),
        asynq.TaskID(fmt.Sprintf("order:cancel:%s", order.ID)), // 使用订单 ID 作为任务 ID
    )

    return order, nil
}

// 2. 处理订单支付
func HandleOrderPayment(orderID string) error {
    // 更新订单状态
    if err := db.UpdateOrderStatus(orderID, "paid"); err != nil {
        return err
    }

    // 取消自动取消任务
    inspector := asynq.NewInspector(redisOpt)
    taskID := fmt.Sprintf("order:cancel:%s", orderID)

    // 删除取消任务
    if err := inspector.DeleteTask("default", taskID); err != nil {
        log.Printf("Failed to delete cancel task: %v", err)
    }

    return nil
}

// 3. 订单取消任务处理器
func HandleOrderCancelTask(ctx context.Context, t *asynq.Task) error {
    var p OrderCancelPayload
    json.Unmarshal(t.Payload(), &p)

    // 检查订单状态
    order, err := db.GetOrder(p.OrderID)
    if err != nil {
        return err
    }

    // 如果订单已支付，不取消
    if order.Status == "paid" {
        log.Printf("Order %s is already paid, skipping cancel", p.OrderID)
        return nil
    }

    // 取消订单
    if err := db.UpdateOrderStatus(p.OrderID, "cancelled"); err != nil {
        return err
    }

    log.Printf("Order %s cancelled due to timeout", p.OrderID)

    // 发送取消通知
    task, _ := tasks.NewOrderCancelNotificationTask(p.OrderID)
    client.Enqueue(task)

    return nil
}
```

### 案例 2: 批量数据导出

```go
// 1. 提交导出任务
func ExportUserData(ctx context.Context, userID int) (string, error) {
    task, err := tasks.NewDataExportTask(userID)
    if err != nil {
        return "", err
    }

    info, err := client.Enqueue(
        task,
        asynq.Timeout(30*time.Minute), // 长时间任务
        asynq.Retention(24*time.Hour), // 保留 24 小时
    )
    if err != nil {
        return "", err
    }

    return info.ID, nil
}

// 2. 处理导出任务
func HandleDataExportTask(ctx context.Context, t *asynq.Task) error {
    var p DataExportPayload
    json.Unmarshal(t.Payload(), &p)

    // 分批导出数据
    var allData []UserData
    offset := 0
    limit := 1000

    for {
        // 检查上下文
        select {
        case <-ctx.Done():
            return fmt.Errorf("export cancelled: %w", ctx.Err())
        default:
        }

        // 查询数据
        data, err := db.GetUserData(p.UserID, offset, limit)
        if err != nil {
            return err
        }

        if len(data) == 0 {
            break
        }

        allData = append(allData, data...)
        offset += limit

        // 报告进度
        log.Printf("Exported %d records for user %d", len(allData), p.UserID)
    }

    // 生成文件
    fileURL, err := generateExportFile(allData)
    if err != nil {
        return err
    }

    // 写入结果
    result := map[string]interface{}{
        "user_id":  p.UserID,
        "file_url": fileURL,
        "records":  len(allData),
    }

    if w := t.ResultWriter(); w != nil {
        data, _ := json.Marshal(result)
        w.Write(data)
    }

    // 发送完成通知
    task, _ := tasks.NewExportCompleteNotificationTask(p.UserID, fileURL)
    client.Enqueue(task)

    return nil
}

// 3. 查询导出状态
func GetExportStatus(taskID string) (*ExportStatus, error) {
    inspector := asynq.NewInspector(redisOpt)

    info, err := inspector.GetTaskInfo("default", taskID)
    if err != nil {
        return nil, err
    }

    status := &ExportStatus{
        TaskID: taskID,
        State:  info.State.String(),
    }

    if info.State == asynq.TaskStateCompleted {
        var result map[string]interface{}
        json.Unmarshal(info.Result, &result)
        status.FileURL = result["file_url"].(string)
        status.Records = int(result["records"].(float64))
    }

    return status, nil
}
```

### 案例 3: 定时数据同步

```go
// 1. 注册定时任务
func SetupDataSync() error {
    scheduler := asynq.NewScheduler(redisOpt, &asynq.SchedulerOpts{})

    // 每小时同步一次
    task, _ := tasks.NewDataSyncTask()
    _, err := scheduler.Register(
        "0 * * * *",
        task,
        asynq.Queue("low"),
    )

    return err
}

// 2. 同步任务处理器
func HandleDataSyncTask(ctx context.Context, t *asynq.Task) error {
    var p DataSyncPayload
    json.Unmarshal(t.Payload(), &p)

    log.Printf("Starting data sync...")

    // 获取上次同步时间
    lastSync, err := db.GetLastSyncTime()
    if err != nil {
        return err
    }

    // 从外部 API 获取数据
    data, err := fetchDataFromAPI(lastSync)
    if err != nil {
        return fmt.Errorf("fetch data failed: %w", err)
    }

    // 批量保存数据
    if err := db.BulkInsert(data); err != nil {
        return fmt.Errorf("bulk insert failed: %w", err)
    }

    // 更新同步时间
    if err := db.UpdateLastSyncTime(time.Now()); err != nil {
        return err
    }

    log.Printf("Data sync completed: %d records synced", len(data))

    return nil
}
```

---

## 故障排查

### 1. 任务未执行

**症状**: 任务已提交但一直处于 pending 状态

**可能原因**:
1. Worker 未启动
2. Worker 配置的队列与任务队列不匹配
3. Redis 连接问题

**排查步骤**:

```go
// 检查队列状态
inspector := asynq.NewInspector(redisOpt)

queues, _ := inspector.Queues()
fmt.Printf("Available queues: %v\n", queues)

info, _ := inspector.GetQueueInfo("default")
fmt.Printf("Pending: %d, Active: %d\n", info.Pending, info.Active)

// 检查 Worker 状态
servers, _ := inspector.Servers()
fmt.Printf("Active servers: %d\n", len(servers))

for _, srv := range servers {
    fmt.Printf("Server: %s, Queues: %v\n", srv.Host, srv.Queues)
}

workers, _ := inspector.Workers()
fmt.Printf("Active workers: %d\n", len(workers))
```

### 2. 任务重复执行

**症状**: 相同的任务被执行多次

**可能原因**:
1. 未使用任务去重
2. 任务处理器不幂等
3. 任务超时导致重试

**解决方案**:

```go
// 1. 使用任务去重
task := asynq.NewTask("email:send", payload)
client.Enqueue(
    task,
    asynq.Unique(24*time.Hour), // 24 小时内去重
)

// 2. 实现幂等性
func HandlePaymentTask(ctx context.Context, t *asynq.Task) error {
    var p PaymentPayload
    json.Unmarshal(t.Payload(), &p)

    // 检查是否已处理
    processed, _ := db.IsPaymentProcessed(p.PaymentID)
    if processed {
        log.Printf("Payment %s already processed, skipping", p.PaymentID)
        return nil
    }

    // 处理支付
    return processPayment(ctx, p)
}

// 3. 设置合理的超时时间
client.Enqueue(
    task,
    asynq.Timeout(5*time.Minute), // 根据实际情况设置
)
```

### 3. 任务堆积

**症状**: pending 队列任务数量持续增长

**可能原因**:
1. Worker 并发数不足
2. 任务处理速度慢
3. Redis 性能问题

**解决方案**:

```go
// 1. 增加 Worker 并发数
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 50, // 增加并发数
    },
)

// 2. 增加 Worker 实例
// 启动多个 Worker 进程并行处理

// 3. 优化任务处理器
func HandleSlowTask(ctx context.Context, t *asynq.Task) error {
    // 使用连接池
    // 批量处理
    // 异步 I/O
    // 缓存结果
}

// 4. 监控和告警
go func() {
    ticker := time.NewTicker(1 * time.Minute)
    for range ticker.C {
        info, _ := inspector.GetQueueInfo("default")
        if info.Pending > 10000 {
            sendAlert("High pending tasks: %d", info.Pending)
        }
    }
}()
```

### 4. 内存泄漏

**症状**: Worker 内存持续增长

**可能原因**:
1. 任务处理器未释放资源
2. 大 Payload 数据
3. goroutine 泄漏

**解决方案**:

```go
// 1. 正确释放资源
func HandleTask(ctx context.Context, t *asynq.Task) error {
    conn, err := db.GetConnection()
    if err != nil {
        return err
    }
    defer conn.Close() // 确保关闭连接

    // 处理任务...
    return nil
}

// 2. 限制 Payload 大小
const MaxPayloadSize = 256 * 1024 // 256KB

func NewTask(typename string, payload []byte) (*asynq.Task, error) {
    if len(payload) > MaxPayloadSize {
        return nil, fmt.Errorf("payload too large: %d bytes", len(payload))
    }
    return asynq.NewTask(typename, payload), nil
}

// 3. 使用 pprof 分析内存
import _ "net/http/pprof"

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()

// 访问 http://localhost:6060/debug/pprof/heap
```

---

## 总结

本使用指南涵盖了 Asynq 的完整使用流程，从基础用法到进阶特性，再到实战案例和故障排查。

### 关键要点

1. **项目结构**: 合理组织代码，分离任务定义和处理逻辑
2. **任务设计**: 使用常量定义类型，保持 Payload 小巧，实现幂等性
3. **队列管理**: 按优先级分队列，合理配置并发数
4. **错误处理**: 区分临时错误和永久错误，合理设置重试策略
5. **监控告警**: 使用 Inspector 和 Prometheus 监控任务状态
6. **性能优化**: 批量处理、连接池、缓存

---

**下一章**: [最佳实践](./best-practices.md)
