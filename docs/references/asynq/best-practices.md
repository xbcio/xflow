# Asynq 最佳实践

本文档总结了在生产环境使用 Asynq 的最佳实践、性能优化技巧和常见陷阱。

## 目录

- [任务设计](#任务设计)
- [性能优化](#性能优化)
- [可靠性保证](#可靠性保证)
- [监控和运维](#监控和运维)
- [安全性](#安全性)
- [常见陷阱](#常见陷阱)

---

## 任务设计

### 1. 任务幂等性

任务可能被重试，必须确保多次执行产生相同结果。

```go
// ✅ 好的实践：幂等性设计
func HandlePaymentTask(ctx context.Context, t *asynq.Task) error {
    var p PaymentPayload
    json.Unmarshal(t.Payload(), &p)

    // 1. 检查是否已处理（使用唯一标识）
    status, err := db.GetPaymentStatus(p.PaymentID)
    if err != nil {
        return err
    }

    if status == "completed" {
        log.Printf("Payment %s already processed", p.PaymentID)
        return nil // 已处理，直接返回成功
    }

    // 2. 使用数据库事务确保原子性
    tx, err := db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 3. 更新状态（使用乐观锁或悲观锁）
    affected, err := tx.Exec(`
        UPDATE payments
        SET status = 'processing'
        WHERE id = ? AND status = 'pending'
    `, p.PaymentID)

    if affected == 0 {
        log.Printf("Payment %s already being processed", p.PaymentID)
        return nil
    }

    // 4. 执行业务逻辑
    if err := processPayment(ctx, p); err != nil {
        return err
    }

    // 5. 提交事务
    if err := tx.Commit(); err != nil {
        return err
    }

    return nil
}

// ❌ 不好的实践：非幂等性设计
func HandlePaymentTask(ctx context.Context, t *asynq.Task) error {
    var p PaymentPayload
    json.Unmarshal(t.Payload(), &p)

    // 直接扣款，可能导致重复扣款
    if err := deductBalance(p.UserID, p.Amount); err != nil {
        return err
    }

    return nil
}
```

### 2. Payload 设计

保持 Payload 精简，只包含必要信息。

```go
// ✅ 好的实践：只包含 ID 和必要参数
type EmailTaskPayload struct {
    UserID     int    `json:"user_id"`     // 用户 ID（在 Handler 中查询详情）
    TemplateID string `json:"template_id"` // 模板 ID
    Locale     string `json:"locale"`      // 语言
}

// 在 Handler 中按需加载数据
func HandleEmailTask(ctx context.Context, t *asynq.Task) error {
    var p EmailTaskPayload
    json.Unmarshal(t.Payload(), &p)

    // 从数据库加载用户数据
    user, err := userRepo.GetByID(ctx, p.UserID)
    if err != nil {
        return err
    }

    // 加载模板
    template, err := templateRepo.GetByID(ctx, p.TemplateID)
    if err != nil {
        return err
    }

    return sendEmail(ctx, user, template)
}

// ❌ 不好的实践：包含完整对象
type EmailTaskPayload struct {
    User     User     `json:"user"`     // 完整用户对象
    Template Template `json:"template"` // 完整模板对象
    Metadata Metadata `json:"metadata"` // 大量元数据
}
// 问题：
// 1. Payload 过大，影响 Redis 性能
// 2. 数据可能过期（提交时的快照）
// 3. 序列化/反序列化开销大
```

### 3. 任务类型命名

使用清晰、层次化的命名规范。

```go
// ✅ 好的实践：模块:操作 格式
const (
    // 用户模块
    TypeUserCreate       = "user:create"
    TypeUserUpdate       = "user:update"
    TypeUserDelete       = "user:delete"
    TypeUserVerifyEmail  = "user:verify_email"

    // 订单模块
    TypeOrderCreate      = "order:create"
    TypeOrderCancel      = "order:cancel"
    TypeOrderRefund      = "order:refund"
    TypeOrderNotify      = "order:notify"

    // 通知模块
    TypeNotifyEmail      = "notify:email"
    TypeNotifySMS        = "notify:sms"
    TypeNotifyPush       = "notify:push"
)

// ❌ 不好的实践：无规则命名
const (
    TypeSendEmail        = "send_email"
    TypeEmailSending     = "email_sending"
    TypeEmail            = "email"
    TypeNotification     = "notification"
)
```

### 4. 错误处理

区分临时错误（可重试）和永久错误（不可重试）。

```go
func HandleTask(ctx context.Context, t *asynq.Task) error {
    var p Payload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        // 序列化错误：永久错误，不重试
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    // 参数验证
    if err := validate(p); err != nil {
        // 验证错误：永久错误，不重试
        return fmt.Errorf("validation failed: %v: %w", err, asynq.SkipRetry)
    }

    // 调用外部服务
    if err := callExternalAPI(ctx, p); err != nil {
        // 检查错误类型
        if isClientError(err) {
            // 4xx 客户端错误：永久错误
            return fmt.Errorf("client error: %v: %w", err, asynq.SkipRetry)
        } else if isServerError(err) {
            // 5xx 服务器错误：临时错误，可重试
            return fmt.Errorf("server error: %v", err)
        } else if isNetworkError(err) {
            // 网络错误：临时错误，可重试
            return fmt.Errorf("network error: %v", err)
        }
    }

    return nil
}

func isClientError(err error) bool {
    // 检查是否为 4xx 错误
    return strings.Contains(err.Error(), "400") ||
           strings.Contains(err.Error(), "404") ||
           strings.Contains(err.Error(), "422")
}

func isServerError(err error) bool {
    // 检查是否为 5xx 错误
    return strings.Contains(err.Error(), "500") ||
           strings.Contains(err.Error(), "502") ||
           strings.Contains(err.Error(), "503")
}

func isNetworkError(err error) bool {
    // 检查是否为网络错误
    return strings.Contains(err.Error(), "connection refused") ||
           strings.Contains(err.Error(), "timeout") ||
           strings.Contains(err.Error(), "dial tcp")
}
```

---

## 性能优化

### 1. 并发调优

根据任务类型调整并发数。

```go
// CPU 密集型任务（图片处理、数据计算等）
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: runtime.NumCPU(), // 等于 CPU 核心数
    },
)

// I/O 密集型任务（网络请求、数据库查询等）
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: runtime.NumCPU() * 4, // CPU 核心数的 2-4 倍
    },
)

// 混合型任务
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: runtime.NumCPU() * 2,
        Queues: map[string]int{
            "cpu_intensive": 2, // CPU 密集型：少量 Worker
            "io_intensive":  8, // I/O 密集型：大量 Worker
        },
    },
)
```

### 2. 批量操作

批量提交任务减少网络往返。

```go
// ❌ 不好的实践：逐个提交
for _, user := range users {
    task, _ := NewEmailTask(user.ID, user.Email)
    client.Enqueue(task)
}

// ✅ 好的实践：使用 Pipeline 批量提交
tasks := make([]*asynq.Task, 0, len(users))
for _, user := range users {
    task, _ := NewEmailTask(user.ID, user.Email)
    tasks = append(tasks, task)
}

// 批量提交
for i := 0; i < len(tasks); i += 100 {
    end := i + 100
    if end > len(tasks) {
        end = len(tasks)
    }
    batch := tasks[i:end]

    // 使用 Pipeline
    pipe := client.Pipeline()
    for _, task := range batch {
        pipe.Enqueue(task)
    }
    pipe.Exec()
}
```

### 3. 连接池优化

优化 Redis 连接池配置。

```go
client := asynq.NewClient(asynq.RedisClientOpt{
    Addr:     "localhost:6379",

    // 连接池配置
    PoolSize:     50,              // 连接池大小（根据并发数调整）
    MinIdleConns: 10,              // 最小空闲连接
    MaxRetries:   3,               // 最大重试次数

    // 超时配置
    DialTimeout:  5 * time.Second,  // 连接超时
    ReadTimeout:  3 * time.Second,  // 读超时
    WriteTimeout: 3 * time.Second,  // 写超时

    // 连接存活配置
    PoolTimeout:  4 * time.Second,  // 从池中获取连接的超时时间
    IdleTimeout:  5 * time.Minute,  // 空闲连接超时时间
})

// 连接池大小建议：
// PoolSize = 并发 Worker 数 + 10（buffer）
```

### 4. 任务分组

使用任务分组批量处理。

```go
// Server 配置
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 10,

        // 任务聚合配置
        GroupGracePeriod: 30 * time.Second, // 收集窗口
        GroupMaxDelay:    2 * time.Minute,  // 最大延迟
        GroupMaxSize:     100,               // 最大批量
    },
)

// 提交任务
for _, user := range users {
    task, _ := NewNotificationTask(user.ID)
    client.Enqueue(
        task,
        asynq.Group("notifications:batch"), // 设置分组
    )
}

// 批量处理 Handler
func HandleBatchNotifications(ctx context.Context, tasks []*asynq.Task) error {
    userIDs := make([]int, 0, len(tasks))

    for _, task := range tasks {
        var p NotificationPayload
        json.Unmarshal(task.Payload(), &p)
        userIDs = append(userIDs, p.UserID)
    }

    // 批量查询用户
    users, err := userRepo.GetByIDs(ctx, userIDs)
    if err != nil {
        return err
    }

    // 批量发送通知
    return sendBatchNotifications(ctx, users)
}
```

### 5. 避免大 Payload

对于大数据，使用引用而非直接传递。

```go
// ❌ 不好的实践：传递大数据
type ReportTaskPayload struct {
    Data []ReportData `json:"data"` // 可能有几 MB
}

// ✅ 好的实践：传递引用
type ReportTaskPayload struct {
    DataID string `json:"data_id"` // 数据 ID 或文件路径
}

func HandleReportTask(ctx context.Context, t *asynq.Task) error {
    var p ReportTaskPayload
    json.Unmarshal(t.Payload(), &p)

    // 从存储加载数据
    data, err := loadDataFromStorage(ctx, p.DataID)
    if err != nil {
        return err
    }

    return generateReport(ctx, data)
}

// 或者使用临时文件
type ImportTaskPayload struct {
    FileURL string `json:"file_url"` // S3/OSS 文件 URL
}
```

---

## 可靠性保证

### 1. 重试策略

合理配置重试次数和延迟。

```go
// 指数退避重试
func exponentialBackoff(n int, e error, t *asynq.Task) time.Duration {
    // n: 重试次数 (0, 1, 2, ...)
    // 延迟: 2^n 秒，最大 1 小时

    delay := time.Duration(1<<uint(n)) * time.Second
    if delay > time.Hour {
        delay = time.Hour
    }

    // 添加随机抖动，避免重试风暴
    jitter := time.Duration(rand.Int63n(int64(delay) / 10))
    return delay + jitter
}

// 固定延迟重试
func fixedDelay(n int, e error, t *asynq.Task) time.Duration {
    return 1 * time.Minute
}

// 线性增长重试
func linearBackoff(n int, e error, t *asynq.Task) time.Duration {
    return time.Duration(n+1) * time.Minute
}

// Server 配置
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        RetryDelayFunc: exponentialBackoff,
    },
)

// 任务级别配置
task, _ := NewTask(typename, payload)
client.Enqueue(
    task,
    asynq.MaxRetry(5), // 最多重试 5 次
)
```

### 2. 超时控制

设置合理的超时时间。

```go
// 全局超时配置
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        Concurrency: 10,
        // 默认超时 30 分钟
    },
)

// 任务级别超时
client.Enqueue(
    task,
    asynq.Timeout(5*time.Minute), // 5 分钟超时
)

// Handler 中响应超时
func HandleTask(ctx context.Context, t *asynq.Task) error {
    // 使用带超时的 context
    ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
    defer cancel()

    // 使用 select 监听超时
    done := make(chan error, 1)
    go func() {
        done <- doWork(ctx)
    }()

    select {
    case err := <-done:
        return err
    case <-ctx.Done():
        return fmt.Errorf("task timeout: %w", ctx.Err())
    }
}

// 分段超时控制
func HandleComplexTask(ctx context.Context, t *asynq.Task) error {
    // 步骤 1：2 分钟超时
    ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Minute)
    defer cancel1()
    if err := step1(ctx1); err != nil {
        return err
    }

    // 步骤 2：3 分钟超时
    ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Minute)
    defer cancel2()
    if err := step2(ctx2); err != nil {
        return err
    }

    return nil
}
```

### 3. 死信队列处理

监控和处理失败任务。

```go
// 定期检查死信队列
func monitorDeadQueue() {
    inspector := asynq.NewInspector(redisOpt)
    ticker := time.NewTicker(5 * time.Minute)

    for range ticker.C {
        queues, _ := inspector.Queues()

        for _, qname := range queues {
            info, _ := inspector.GetQueueInfo(qname)

            if info.Archived > 0 {
                log.Printf("Queue %s has %d archived tasks", qname, info.Archived)

                // 获取失败任务
                tasks, _ := inspector.ListArchivedTasks(qname, asynq.PageSize(100))

                for _, task := range tasks {
                    log.Printf("Failed task: %s, Error: %s", task.Type, task.LastErr)

                    // 根据错误类型决定处理方式
                    if shouldRequeue(task) {
                        // 重新入队
                        inspector.RunTask(qname, task.ID)
                    } else if shouldAlert(task) {
                        // 发送告警
                        sendAlert(task)
                    }
                }
            }
        }
    }
}

func shouldRequeue(task *asynq.TaskInfo) bool {
    // 网络错误可以重新入队
    return strings.Contains(task.LastErr, "network") ||
           strings.Contains(task.LastErr, "timeout")
}

func shouldAlert(task *asynq.TaskInfo) bool {
    // 重要任务失败需要告警
    criticalTypes := []string{
        "payment:process",
        "order:create",
    }

    for _, t := range criticalTypes {
        if task.Type == t {
            return true
        }
    }

    return false
}
```

### 4. 优雅关闭

确保任务完整执行。

```go
func main() {
    srv := asynq.NewServer(
        redisOpt,
        asynq.Config{
            Concurrency:     10,
            ShutdownTimeout: 30 * time.Second, // 优雅关闭超时
        },
    )

    mux := asynq.NewServeMux()
    // 注册 Handler...

    // 捕获信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    // 启动 Server
    go func() {
        if err := srv.Run(mux); err != nil {
            log.Fatalf("could not run server: %v", err)
        }
    }()

    // 等待信号
    <-sigChan
    log.Println("Received shutdown signal, shutting down gracefully...")

    // 优雅关闭（会等待正在执行的任务完成）
    srv.Shutdown()
}

// Handler 中检查关闭信号
func HandleTask(ctx context.Context, t *asynq.Task) error {
    for i := 0; i < 100; i++ {
        // 检查 context 是否已取消
        select {
        case <-ctx.Done():
            log.Println("Task cancelled, cleaning up...")
            // 清理资源
            return fmt.Errorf("task cancelled: %w", ctx.Err())
        default:
        }

        // 处理任务...
        processChunk(i)
    }

    return nil
}
```

---

## 监控和运维

### 1. Prometheus 集成

```go
import (
    "github.com/hibiken/asynq/x/metrics"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
    // 创建 Prometheus 中间件
    prometheusMiddleware := metrics.NewPrometheusMetrics(
        prometheus.DefaultRegisterer,
    )

    // 应用中间件
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

// Grafana 告警规则
// 1. 待处理任务过多
// alert: HighPendingTasks
// expr: asynq_queue_size{state="pending"} > 1000
// for: 5m

// 2. 失败率过高
// alert: HighFailureRate
// expr: rate(asynq_tasks_failed_total[5m]) > 0.1
// for: 5m

// 3. 任务处理延迟过高
// alert: HighTaskLatency
// expr: histogram_quantile(0.95, asynq_task_duration_seconds) > 60
// for: 5m
```

### 2. 日志记录

```go
// 结构化日志中间件
func structuredLoggingMiddleware(logger *zap.Logger) func(asynq.Handler) asynq.Handler {
    return func(h asynq.Handler) asynq.Handler {
        return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
            start := time.Now()

            taskID, _ := asynq.GetTaskID(ctx)
            queueName, _ := asynq.GetQueueName(ctx)
            retryCount, _ := asynq.GetRetryCount(ctx)

            logger.Info("task started",
                zap.String("task_id", taskID),
                zap.String("task_type", t.Type()),
                zap.String("queue", queueName),
                zap.Int("retry", retryCount),
            )

            err := h.ProcessTask(ctx, t)

            elapsed := time.Since(start)

            if err != nil {
                logger.Error("task failed",
                    zap.String("task_id", taskID),
                    zap.String("task_type", t.Type()),
                    zap.Error(err),
                    zap.Duration("elapsed", elapsed),
                )
            } else {
                logger.Info("task completed",
                    zap.String("task_id", taskID),
                    zap.String("task_type", t.Type()),
                    zap.Duration("elapsed", elapsed),
                )
            }

            return err
        })
    }
}
```

### 3. 健康检查

```go
// HTTP 健康检查端点
func setupHealthCheck(srv *asynq.Server, inspector *asynq.Inspector) {
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        // 检查 Redis 连接
        queues, err := inspector.Queues()
        if err != nil {
            w.WriteHeader(http.StatusServiceUnavailable)
            json.NewEncoder(w).Encode(map[string]interface{}{
                "status": "unhealthy",
                "error":  "redis connection failed",
            })
            return
        }

        // 检查队列状态
        var totalPending int
        for _, qname := range queues {
            info, _ := inspector.GetQueueInfo(qname)
            totalPending += info.Pending
        }

        // 检查活跃 Worker
        servers, _ := inspector.Servers()

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "status":        "healthy",
            "queues":        len(queues),
            "pending_tasks": totalPending,
            "active_servers": len(servers),
        })
    })

    // 就绪检查
    http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
        // 检查 Server 是否正在运行
        servers, err := inspector.Servers()
        if err != nil || len(servers) == 0 {
            w.WriteHeader(http.StatusServiceUnavailable)
            json.NewEncoder(w).Encode(map[string]string{
                "status": "not ready",
            })
            return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]string{
            "status": "ready",
        })
    })

    go http.ListenAndServe(":8080", nil)
}
```

### 4. asynqmon Web UI

```bash
# 安装
go install github.com/hibiken/asynq/tools/asynqmon@latest

# 运行（单机模式）
asynqmon --redis-addr=localhost:6379

# 运行（集群模式）
asynqmon --redis-cluster-nodes=node1:7000,node2:7001,node3:7002

# 自定义端口
asynqmon --port=8081

# 启用认证
asynqmon --redis-password=secret

# 访问
# http://localhost:8080
```

---

## 安全性

### 1. Redis 认证

```go
client := asynq.NewClient(asynq.RedisClientOpt{
    Addr:     "localhost:6379",
    Password: os.Getenv("REDIS_PASSWORD"), // 从环境变量读取
    DB:       0,

    // TLS 配置（用于生产环境）
    TLSConfig: &tls.Config{
        MinVersion: tls.VersionTLS12,
    },
})
```

### 2. 敏感数据处理

```go
// ❌ 不好的实践：Payload 包含敏感数据
type PaymentPayload struct {
    CardNumber string `json:"card_number"` // 信用卡号
    CVV        string `json:"cvv"`         // CVV
}

// ✅ 好的实践：使用加密或引用
type PaymentPayload struct {
    PaymentMethodID string `json:"payment_method_id"` // 支付方式 ID
    Amount          int64  `json:"amount"`
}

// 在 Handler 中从安全存储获取敏感数据
func HandlePaymentTask(ctx context.Context, t *asynq.Task) error {
    var p PaymentPayload
    json.Unmarshal(t.Payload(), &p)

    // 从安全存储（如 Vault）获取支付方式详情
    paymentMethod, err := secureStore.GetPaymentMethod(p.PaymentMethodID)
    if err != nil {
        return err
    }

    return processPayment(ctx, paymentMethod, p.Amount)
}

// 加密 Payload（如果必须存储敏感数据）
func encryptPayload(data []byte, key []byte) ([]byte, error) {
    block, err := aes.NewCipher(key)
    if err != nil {
        return nil, err
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return nil, err
    }

    nonce := make([]byte, gcm.NonceSize())
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        return nil, err
    }

    return gcm.Seal(nonce, nonce, data, nil), nil
}
```

### 3. 访问控制

```go
// 中间件验证任务权限
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

        // 检查权限
        if !hasPermission(user, t.Type()) {
            return fmt.Errorf("permission denied: %w", asynq.SkipRetry)
        }

        // 将用户信息添加到 context
        ctx = context.WithValue(ctx, "user", user)

        return h.ProcessTask(ctx, t)
    })
}

// 提交任务时添加认证信息
func EnqueueTask(task *asynq.Task, userToken string) error {
    headers := map[string]string{
        "Authorization": userToken,
        "X-Request-ID":  uuid.New().String(),
    }

    taskWithHeaders := asynq.NewTaskWithHeaders(
        task.Type(),
        task.Payload(),
        headers,
    )

    _, err := client.Enqueue(taskWithHeaders)
    return err
}
```

---

## 常见陷阱

### 1. 任务不幂等

**问题**: 任务被重试时产生副作用。

**解决方案**: 使用唯一标识检查重复执行。

```go
func HandleTask(ctx context.Context, t *asynq.Task) error {
    var p Payload
    json.Unmarshal(t.Payload(), &p)

    // 检查是否已执行
    if db.IsProcessed(p.ID) {
        return nil
    }

    // 执行任务
    if err := process(p); err != nil {
        return err
    }

    // 标记为已执行
    db.MarkProcessed(p.ID)
    return nil
}
```

### 2. Goroutine 泄漏

**问题**: Handler 中启动的 goroutine 未正确关闭。

**解决方案**: 使用 context 控制 goroutine 生命周期。

```go
// ❌ 不好的实践：goroutine 泄漏
func HandleTask(ctx context.Context, t *asynq.Task) error {
    go func() {
        // 这个 goroutine 可能永远不会退出
        for {
            doSomething()
            time.Sleep(1 * time.Second)
        }
    }()

    return nil
}

// ✅ 好的实践：使用 context 控制
func HandleTask(ctx context.Context, t *asynq.Task) error {
    done := make(chan struct{})

    go func() {
        defer close(done)

        ticker := time.NewTicker(1 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                doSomething()
            }
        }
    }()

    // 等待 goroutine 完成或 context 取消
    <-done
    return nil
}
```

### 3. 内存泄漏

**问题**: 连接、文件等资源未释放。

**解决方案**: 使用 defer 确保资源释放。

```go
// ✅ 好的实践：正确释放资源
func HandleTask(ctx context.Context, t *asynq.Task) error {
    // 数据库连接
    db, err := openDB()
    if err != nil {
        return err
    }
    defer db.Close()

    // HTTP 客户端
    client := &http.Client{
        Timeout: 30 * time.Second,
    }
    defer client.CloseIdleConnections()

    // 文件操作
    file, err := os.Open("data.txt")
    if err != nil {
        return err
    }
    defer file.Close()

    // 处理任务...
    return nil
}
```

### 4. Panic 未恢复

**问题**: Panic 导致 Worker 崩溃。

**解决方案**: 使用 recovery 中间件。

```go
func recoveryMiddleware(h asynq.Handler) asynq.Handler {
    return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
        defer func() {
            if r := recover(); r != nil {
                err = fmt.Errorf("panic recovered: %v", r)
                log.Printf("Panic in task %s: %v\n%s", t.Type(), r, debug.Stack())
            }
        }()
        return h.ProcessTask(ctx, t)
    })
}

// 使用中间件
mux := asynq.NewServeMux()
mux.Use(recoveryMiddleware)
```

### 5. 死锁

**问题**: 任务相互依赖导致死锁。

**解决方案**: 避免任务间循环依赖，使用超时。

```go
// ❌ 不好的实践：任务 A 等待任务 B，任务 B 等待任务 A

// ✅ 好的实践：避免循环依赖，使用工作流
func HandleTaskA(ctx context.Context, t *asynq.Task) error {
    // 执行 A
    resultA := doA()

    // 提交后续任务
    taskB, _ := NewTaskB(resultA)
    client.Enqueue(taskB)

    return nil
}

func HandleTaskB(ctx context.Context, t *asynq.Task) error {
    // 执行 B（不依赖 A 的执行状态）
    return doB()
}
```

---

## 总结

### 黄金法则

1. **幂等性第一**: 所有任务必须幂等
2. **小 Payload**: 只传递必要数据
3. **快速失败**: 永久错误不重试
4. **监控告警**: 监控队列和任务状态
5. **优雅关闭**: 确保任务完整执行

### 性能清单

- [ ] 根据任务类型调整并发数
- [ ] 使用批量操作减少网络往返
- [ ] 优化 Redis 连接池配置
- [ ] 使用任务聚合批量处理
- [ ] 避免大 Payload，使用引用

### 可靠性清单

- [ ] 实现任务幂等性
- [ ] 区分临时错误和永久错误
- [ ] 配置合理的重试策略
- [ ] 设置任务超时时间
- [ ] 处理死信队列任务
- [ ] 实现优雅关闭

### 监控清单

- [ ] 集成 Prometheus 指标
- [ ] 配置 Grafana 告警
- [ ] 记录结构化日志
- [ ] 实现健康检查端点
- [ ] 使用 asynqmon Web UI
- [ ] 监控队列积压情况

### 安全清单

- [ ] 使用 Redis 认证和 TLS
- [ ] 不在 Payload 存储敏感数据
- [ ] 实现访问控制
- [ ] 加密敏感配置
- [ ] 定期审计日志

---

**相关文档**:
- [架构设计](./architecture.md)
- [核心概念](./core-concepts.md)
- [使用指南](./usage-guide.md)

**最后更新**: 2026-01-11
