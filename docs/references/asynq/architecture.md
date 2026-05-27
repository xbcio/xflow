# Asynq 架构设计

## 整体架构

Asynq 采用**生产者-消费者模式**，基于 Redis 实现分布式任务队列：

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层 (Application)                    │
│  ┌──────────────┐                    ┌──────────────────┐   │
│  │   Client     │                    │     Server       │   │
│  │ (Producer)   │                    │   (Consumer)     │   │
│  │              │                    │                  │   │
│  │ - 创建任务    │                    │ - 拉取任务       │   │
│  │ - 设置选项    │                    │ - 执行处理器     │   │
│  │ - 提交队列    │                    │ - 管理重试       │   │
│  └──────┬───────┘                    └────────┬─────────┘   │
└─────────┼──────────────────────────────────────┼─────────────┘
          │                                      │
          │            Redis Protocol            │
          └──────────────────┬───────────────────┘
                             │
         ┌───────────────────▼────────────────────┐
         │           Redis (Broker)               │
         │  ┌──────────────────────────────────┐  │
         │  │  Pending Queue (待处理队列)      │  │
         │  ├──────────────────────────────────┤  │
         │  │  Active Queue (活动队列)         │  │
         │  ├──────────────────────────────────┤  │
         │  │  Scheduled Queue (延迟队列)      │  │
         │  ├──────────────────────────────────┤  │
         │  │  Retry Queue (重试队列)          │  │
         │  ├──────────────────────────────────┤  │
         │  │  Dead Queue (死信队列)           │  │
         │  ├──────────────────────────────────┤  │
         │  │  Completed Queue (完成队列)      │  │
         │  └──────────────────────────────────┘  │
         └─────────────────────────────────────────┘

                    ┌──────────────────┐
                    │    Scheduler     │
                    │  (调度器进程)    │
                    │                  │
                    │ - 扫描延迟任务   │
                    │ - 移动到待处理   │
                    └────────┬─────────┘
                             │
                             ▼
                        定时扫描 Redis
```

## 核心模块

### 1. Client（任务生产者）

**技术实现**: Go + go-redis

**核心职责**:
- 创建任务并序列化 Payload
- 设置任务选项（队列、优先级、延迟、重试等）
- 提交任务到 Redis
- 支持批量操作

**关键数据结构**:

```go
// Client 结构
type Client struct {
    broker broker // Redis 连接管理器
}

// 任务定义
type Task struct {
    Type    string // 任务类型标识
    Payload []byte // 任务数据（JSON 序列化）
}

// 任务选项
type Option interface {
    Type() string
    Value() interface{}
}

// 常见选项
func MaxRetry(n int) Option           // 最大重试次数
func Queue(name string) Option        // 指定队列
func Timeout(d time.Duration) Option  // 超时时间
func Deadline(t time.Time) Option     // 截止时间
func Unique(d time.Duration) Option   // 去重时间窗口
func ProcessIn(d time.Duration) Option // 延迟执行
func ProcessAt(t time.Time) Option    // 指定时间执行
```

**Client 实现原理**:

```go
type Client struct {
    broker broker
}

func NewClient(r RedisConnOpt) *Client {
    c := redis.NewClient(&redis.Options{
        Addr: r.Addr,
        Password: r.Password,
        DB: r.DB,
    })
    return &Client{
        broker: newRedisBroker(c),
    }
}

// 提交任务
func (c *Client) Enqueue(task *Task, opts ...Option) (*TaskInfo, error) {
    // 1. 生成任务 ID
    taskID := uuid.New().String()

    // 2. 应用选项
    opt := composeOptions(opts...)

    // 3. 构建任务消息
    msg := &TaskMessage{
        ID:        taskID,
        Type:      task.Type,
        Payload:   task.Payload,
        Queue:     opt.queue,
        Retry:     opt.maxRetry,
        Timeout:   int(opt.timeout.Seconds()),
        Deadline:  opt.deadline.Unix(),
    }

    // 4. 根据选项决定存储位置
    if opt.processAt.IsZero() {
        // 立即执行：存入 pending 队列
        return c.broker.Enqueue(msg)
    } else {
        // 延迟执行：存入 scheduled 队列
        return c.broker.Schedule(msg, opt.processAt)
    }
}

// 批量提交
func (c *Client) EnqueueBatch(tasks ...*Task) ([]*TaskInfo, error) {
    // 使用 Redis Pipeline 批量操作
    pipe := c.broker.Pipeline()
    for _, task := range tasks {
        pipe.Enqueue(task)
    }
    return pipe.Exec()
}
```

### 2. Server（任务消费者）

**技术实现**: Go + Worker Pool

**核心职责**:
- 从 Redis 拉取任务
- 管理 Worker 协程池
- 执行任务处理器（Handler）
- 处理任务结果（成功/失败/重试）
- 心跳保活和优雅关闭

**关键配置**:

```go
type Config struct {
    // 并发 Worker 数量
    Concurrency int

    // 队列优先级配置
    // map[队列名称]权重
    // 权重越高，从该队列拉取的频率越高
    Queues map[string]int

    // 严格优先级模式
    // 如果为 true，高优先级队列有任务时，不会处理低优先级队列
    StrictPriority bool

    // 重试延迟函数
    RetryDelayFunc RetryDelayFunc

    // 错误处理器
    ErrorHandler ErrorHandler

    // 健康检查函数
    HealthCheckFunc func(error)

    // 优雅关闭超时时间
    ShutdownTimeout time.Duration
}
```

**Server 实现原理**:

```go
type Server struct {
    broker    broker
    scheduler *scheduler
    processor *processor

    // Worker 管理
    workers []*worker

    // 状态管理
    state serverState
    mu    sync.Mutex
}

func NewServer(r RedisConnOpt, cfg Config) *Server {
    broker := newRedisBroker(r)

    return &Server{
        broker:    broker,
        scheduler: newScheduler(broker),
        processor: newProcessor(broker, cfg),
    }
}

// 启动服务器
func (srv *Server) Run(handler Handler) error {
    // 1. 启动调度器（处理延迟任务）
    srv.scheduler.start()

    // 2. 启动处理器（拉取和执行任务）
    srv.processor.start(handler)

    // 3. 启动心跳
    srv.startHeartbeat()

    // 4. 等待关闭信号
    srv.waitForSignals()

    // 5. 优雅关闭
    return srv.shutdown()
}

// 处理器：核心执行逻辑
type processor struct {
    broker broker
    config Config

    done chan struct{}

    // 待处理任务通道
    taskQueue chan *TaskMessage
}

func (p *processor) start(handler Handler) {
    // 1. 启动任务拉取协程
    go p.pullTasks()

    // 2. 启动 Worker 协程池
    for i := 0; i < p.config.Concurrency; i++ {
        go p.worker(handler)
    }
}

// 拉取任务
func (p *processor) pullTasks() {
    for {
        select {
        case <-p.done:
            return
        default:
            // 从 Redis 拉取任务
            msg, err := p.broker.Dequeue(p.config.Queues)
            if err != nil {
                time.Sleep(time.Second)
                continue
            }

            // 发送到 Worker
            p.taskQueue <- msg
        }
    }
}

// Worker 执行任务
func (p *processor) worker(handler Handler) {
    for msg := range p.taskQueue {
        // 1. 创建执行上下文
        ctx := context.Background()
        ctx = context.WithValue(ctx, taskIDKey, msg.ID)

        // 2. 设置超时
        if msg.Timeout > 0 {
            var cancel context.CancelFunc
            ctx, cancel = context.WithTimeout(ctx, time.Duration(msg.Timeout)*time.Second)
            defer cancel()
        }

        // 3. 执行处理器
        task := &Task{
            Type:    msg.Type,
            Payload: msg.Payload,
        }

        err := handler.ProcessTask(ctx, task)

        // 4. 处理结果
        if err != nil {
            p.handleFailure(msg, err)
        } else {
            p.handleSuccess(msg)
        }
    }
}

// 处理失败
func (p *processor) handleFailure(msg *TaskMessage, err error) {
    msg.Retried++

    // 检查是否应该重试
    if msg.Retried < msg.Retry {
        // 计算重试延迟
        delay := p.config.RetryDelayFunc(msg.Retried, err, msg)
        retryAt := time.Now().Add(delay)

        // 移动到 retry 队列
        p.broker.Retry(msg, retryAt)
    } else {
        // 超过重试次数，移动到 dead 队列
        p.broker.Kill(msg, err.Error())
    }
}

// 处理成功
func (p *processor) handleSuccess(msg *TaskMessage) {
    // 标记任务完成
    p.broker.Done(msg)
}
```

### 3. Scheduler（调度器）

**核心职责**:
- 扫描 scheduled 队列（延迟任务）
- 扫描 retry 队列（重试任务）
- 将到期任务移动到 pending 队列
- 管理 Cron 定时任务

**实现原理**:

```go
type scheduler struct {
    broker broker

    // 扫描间隔
    interval time.Duration

    done chan struct{}
}

func (s *scheduler) start() {
    go s.runScheduledTasksPoller()
    go s.runRetryTasksPoller()
    go s.runCronScheduler()
}

// 扫描延迟任务
func (s *scheduler) runScheduledTasksPoller() {
    ticker := time.NewTicker(s.interval)
    defer ticker.Stop()

    for {
        select {
        case <-s.done:
            return
        case <-ticker.C:
            // 查询到期的延迟任务
            tasks, err := s.broker.ListScheduledTasks(time.Now())
            if err != nil {
                continue
            }

            // 移动到 pending 队列
            for _, task := range tasks {
                s.broker.EnqueueScheduledTask(task)
            }
        }
    }
}

// Cron 任务调度器
type CronScheduler struct {
    scheduler *scheduler
    entries   map[string]*cronEntry
}

type cronEntry struct {
    spec     string           // Cron 表达式
    task     *Task            // 任务模板
    opts     []Option         // 任务选项
    location *time.Location   // 时区
}

// 注册定时任务
func (cs *CronScheduler) Register(
    spec string,
    task *Task,
    opts ...Option,
) (entryID string, err error) {
    // 1. 解析 Cron 表达式
    schedule, err := cron.Parse(spec)
    if err != nil {
        return "", err
    }

    // 2. 生成 Entry ID
    entryID = uuid.New().String()

    // 3. 保存到 Redis
    entry := &cronEntry{
        spec:     spec,
        task:     task,
        opts:     opts,
        location: time.UTC,
    }

    cs.entries[entryID] = entry

    // 4. 启动定时器
    go cs.runCronEntry(entryID, schedule, entry)

    return entryID, nil
}

func (cs *CronScheduler) runCronEntry(
    entryID string,
    schedule cron.Schedule,
    entry *cronEntry,
) {
    for {
        // 计算下次执行时间
        next := schedule.Next(time.Now())

        // 等待到执行时间
        time.Sleep(time.Until(next))

        // 提交任务
        client := NewClient(cs.redisOpt)
        _, err := client.Enqueue(entry.task, entry.opts...)
        if err != nil {
            log.Printf("Failed to enqueue cron task: %v", err)
        }
    }
}
```

### 4. Broker（Redis 抽象层）

**核心职责**:
- 封装所有 Redis 操作
- 管理 Redis 连接
- 实现队列操作原语
- 保证操作原子性（使用 Lua 脚本）

**Redis 数据结构**:

```
# 待处理队列（List）
asynq:{<qname>}:pending
├── task_id_1
├── task_id_2
└── task_id_3

# 活动队列（Sorted Set）- score 为 deadline
asynq:{<qname>}:active
├── task_id_4 (score: 1640000000)
└── task_id_5 (score: 1640000100)

# 延迟队列（Sorted Set）- score 为执行时间
asynq:{<qname>}:scheduled
├── task_id_6 (score: 1640001000)
└── task_id_7 (score: 1640002000)

# 重试队列（Sorted Set）- score 为重试时间
asynq:{<qname>}:retry
├── task_id_8 (score: 1640003000)
└── task_id_9 (score: 1640004000)

# 死信队列（Sorted Set）- score 为失败时间
asynq:{<qname>}:dead
└── task_id_10 (score: 1640005000)

# 任务详情（Hash）
asynq:{<qname>}:t:<task_id>
├── type: "email:deliver"
├── payload: "{\"user_id\":42}"
├── state: "active"
├── retry: 3
├── retried: 1
├── timeout: 300
└── deadline: 1640000000

# 去重集合（Set）- TTL 控制去重窗口
asynq:{<qname>}:unique:<task_type>:<unique_key>
└── task_id_11

# Servers（Hash）- 记录活跃的 Server
asynq:servers
├── server_id_1: {"host":"app-1","pid":1234,"started_at":1640000000}
└── server_id_2: {"host":"app-2","pid":5678,"started_at":1640000100}

# Workers（Hash）- 记录活跃的 Worker
asynq:workers
├── worker_id_1: {"server_id":"server_id_1","task_id":"task_id_4"}
└── worker_id_2: {"server_id":"server_id_1","task_id":"task_id_5"}
```

**关键 Lua 脚本**:

```go
// Dequeue 脚本：原子性地从 pending 移动到 active
var dequeueScript = redis.NewScript(`
local queue = KEYS[1]
local active = KEYS[2]
local deadline = ARGV[1]

-- 从 pending 队列弹出任务
local task_id = redis.call("RPOPLPUSH", queue, active)
if not task_id then
    return nil
end

-- 设置 deadline（用于超时检测）
redis.call("ZADD", active, deadline, task_id)

return task_id
`)

// Done 脚本：标记任务完成
var doneScript = redis.NewScript(`
local active = KEYS[1]
local completed = KEYS[2]
local task_key = KEYS[3]
local task_id = ARGV[1]
local completed_at = ARGV[2]

-- 从 active 移除
redis.call("ZREM", active, task_id)

-- 添加到 completed（可选，用于保留历史）
redis.call("ZADD", completed, completed_at, task_id)

-- 更新任务状态
redis.call("HSET", task_key, "state", "completed", "completed_at", completed_at)

return redis.status_reply("OK")
`)

// Retry 脚本：将任务移动到重试队列
var retryScript = redis.NewScript(`
local active = KEYS[1]
local retry_queue = KEYS[2]
local task_key = KEYS[3]
local task_id = ARGV[1]
local retry_at = ARGV[2]
local error_msg = ARGV[3]

-- 从 active 移除
redis.call("ZREM", active, task_id)

-- 添加到 retry 队列
redis.call("ZADD", retry_queue, retry_at, task_id)

-- 更新任务信息
redis.call("HINCRBY", task_key, "retried", 1)
redis.call("HSET", task_key, "error", error_msg, "state", "retry")

return redis.status_reply("OK")
`)
```

### 5. Inspector（检查器）

**核心职责**:
- 查询队列状态
- 查询任务详情
- 管理任务（删除、重新入队、归档）
- 管理定时任务
- 查询 Server/Worker 状态

**API 示例**:

```go
type Inspector struct {
    broker broker
}

func NewInspector(r RedisConnOpt) *Inspector {
    return &Inspector{
        broker: newRedisBroker(r),
    }
}

// 获取队列信息
func (i *Inspector) GetQueueInfo(qname string) (*QueueInfo, error) {
    return &QueueInfo{
        Queue:      qname,
        Size:       i.broker.GetQueueSize(qname),
        Pending:    i.broker.GetPendingCount(qname),
        Active:     i.broker.GetActiveCount(qname),
        Scheduled:  i.broker.GetScheduledCount(qname),
        Retry:      i.broker.GetRetryCount(qname),
        Dead:       i.broker.GetDeadCount(qname),
        Processed:  i.broker.GetProcessedCount(qname),
        Failed:     i.broker.GetFailedCount(qname),
    }, nil
}

// 获取任务详情
func (i *Inspector) GetTaskInfo(qname, taskID string) (*TaskInfo, error) {
    msg, err := i.broker.GetTaskMessage(qname, taskID)
    if err != nil {
        return nil, err
    }

    return &TaskInfo{
        ID:          msg.ID,
        Queue:       msg.Queue,
        Type:        msg.Type,
        Payload:     msg.Payload,
        State:       msg.State,
        MaxRetry:    msg.Retry,
        Retried:     msg.Retried,
        LastError:   msg.ErrorMsg,
        NextProcessAt: msg.NextProcessAt,
    }, nil
}

// 删除任务
func (i *Inspector) DeleteTask(qname, taskID string) error {
    return i.broker.DeleteTask(qname, taskID)
}

// 重新入队任务
func (i *Inspector) RunTask(qname, taskID string) error {
    return i.broker.EnqueueTaskByID(qname, taskID)
}

// 归档任务
func (i *Inspector) ArchiveTask(qname, taskID string) error {
    return i.broker.ArchiveTask(qname, taskID)
}

// 列出所有 Servers
func (i *Inspector) Servers() ([]*ServerInfo, error) {
    return i.broker.ListServers()
}

// 列出所有 Workers
func (i *Inspector) Workers() ([]*WorkerInfo, error) {
    return i.broker.ListWorkers()
}
```

## 任务生命周期

```
任务创建 (Client.Enqueue)
    ↓
┌───▼──────────────────┐
│   Pending Queue      │ ← 等待处理
└───┬──────────────────┘
    │ Server.Dequeue
    ↓
┌───▼──────────────────┐
│   Active Queue       │ ← 正在执行
└───┬──────────────────┘
    │
    ├─→ 成功 ──→ Done ──→ [Completed Queue]
    │
    ├─→ 失败 ──→ 检查重试次数
    │            │
    │            ├─→ 未超限 ──→ [Retry Queue] ──→ 等待重试时间 ──→ [Pending Queue]
    │            │
    │            └─→ 已超限 ──→ [Dead Queue] (死信)
    │
    └─→ 超时 ──→ [Retry Queue] 或 [Dead Queue]
```

## 数据流图

```
┌──────────────┐
│  Application │
│   (Client)   │
└──────┬───────┘
       │ 1. Enqueue Task
       ▼
┌──────────────────────────────────┐
│         Redis Broker             │
│                                  │
│  2. Store in Pending Queue       │
│     SET task:<id> {...}          │
│     LPUSH queue:pending <id>     │
└──────┬───────────────────────────┘
       │
       │ 3. Server pulls task
       ▼
┌──────────────────────────────────┐
│        Server (Worker Pool)      │
│                                  │
│  4. RPOPLPUSH pending → active   │
│  5. Execute Handler              │
│     - ctx with timeout           │
│     - call ProcessTask()         │
│  6. Handle result                │
└──────┬───────────────────────────┘
       │
       ├─→ Success
       │   │
       │   ▼
       │   ┌─────────────────────┐
       │   │ Mark as Done        │
       │   │ ZREM active <id>    │
       │   │ ZADD completed <id> │
       │   └─────────────────────┘
       │
       └─→ Failure
           │
           ▼
           ┌─────────────────────┐
           │ Check Retry Count   │
           ├─────────────────────┤
           │ < max: Retry Queue  │
           │ >= max: Dead Queue  │
           └─────────────────────┘
```

## 部署架构

### 单机模式

```
┌────────────────────────────┐
│      Application           │
│  ┌──────────────────────┐  │
│  │   Client + Server    │  │
│  │   (同一进程)          │  │
│  └──────────┬───────────┘  │
└─────────────┼───────────────┘
              │
              ▼
     ┌────────────────┐
     │  Redis         │
     │  (Standalone)  │
     └────────────────┘
```

### 分布式模式

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Web App 1  │  │  Web App 2  │  │  Web App 3  │
│   (Client)  │  │   (Client)  │  │   (Client)  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │  Redis Cluster  │
              │   (HA Setup)    │
              └────────┬────────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
       ▼               ▼               ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│  Worker 1   │ │  Worker 2   │ │  Worker 3   │
│  (Server)   │ │  (Server)   │ │  (Server)   │
│             │ │             │ │             │
│ Concurrency │ │ Concurrency │ │ Concurrency │
│     10      │ │     10      │ │     10      │
└─────────────┘ └─────────────┘ └─────────────┘
```

### 队列隔离模式

```
┌─────────────────────────────────┐
│       Application               │
│  ┌──────────────────────────┐   │
│  │   Client (Producer)      │   │
│  └────────┬─────────────────┘   │
└───────────┼──────────────────────┘
            │
            ▼
   ┌────────────────┐
   │  Redis         │
   └────┬───────────┘
        │
        ├─→ Queue: critical (高优先级)
        ├─→ Queue: default  (默认)
        └─→ Queue: low      (低优先级)
        │
        ├──────────────┬──────────────┐
        │              │              │
        ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│  Worker 1   │ │  Worker 2   │ │  Worker 3   │
│             │ │             │ │             │
│ Queues:     │ │ Queues:     │ │ Queues:     │
│ critical: 6 │ │ default: 8  │ │ low: 10     │
│ default: 3  │ │ low: 2      │ │             │
│ low: 1      │ │             │ │             │
└─────────────┘ └─────────────┘ └─────────────┘
```

## 技术决策

### 为什么选择 Redis

1. **性能优异**: 内存操作，低延迟高吞吐
2. **数据结构丰富**: List、Sorted Set 等天然适合队列场景
3. **原子操作**: Lua 脚本保证操作原子性
4. **持久化**: AOF/RDB 保证数据可靠性
5. **部署简单**: 单机或集群都易于维护

### 为什么不用消息队列（RabbitMQ/Kafka）

1. **复杂度**: RabbitMQ/Kafka 部署和维护成本高
2. **功能过剩**: Asynq 的场景不需要复杂的路由和分区
3. **依赖**: 很多项目已经在使用 Redis，复用基础设施
4. **性能**: Redis 对于任务队列场景性能足够

### 任务持久化策略

```bash
# Redis 持久化配置
# AOF 模式：每秒同步
appendonly yes
appendfsync everysec

# RDB 模式：定期快照
save 900 1
save 300 10
save 60 10000
```

### 任务去重实现

```go
// 使用 Redis SET + TTL 实现
func (c *Client) EnqueueUnique(task *Task, ttl time.Duration) (*TaskInfo, error) {
    // 生成去重 key
    uniqueKey := fmt.Sprintf("asynq:unique:%s:%s", task.Type, computeHash(task.Payload))

    // 尝试设置（SETNX）
    ok, err := c.broker.SetNX(uniqueKey, task.ID, ttl)
    if err != nil {
        return nil, err
    }

    if !ok {
        // 已存在相同任务
        return nil, ErrDuplicateTask
    }

    // 提交任务
    return c.Enqueue(task)
}
```

## 性能优化

### 1. 连接池优化

```go
// Redis 连接池配置
redis.Options{
    PoolSize:     20,              // 连接池大小
    MinIdleConns: 5,               // 最小空闲连接
    MaxRetries:   3,               // 最大重试次数
    DialTimeout:  5 * time.Second, // 连接超时
    ReadTimeout:  3 * time.Second, // 读超时
    WriteTimeout: 3 * time.Second, // 写超时
}
```

### 2. 批量操作

```go
// 使用 Pipeline 批量提交任务
pipe := client.Pipeline()
for _, task := range tasks {
    pipe.Enqueue(task)
}
results, err := pipe.Exec()
```

### 3. 并发控制

```go
// 根据任务类型调整并发数
config := asynq.Config{
    Concurrency: runtime.NumCPU() * 2, // CPU 密集型: NumCPU
                                        // IO 密集型: NumCPU * 2-4
    Queues: map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    },
}
```

### 4. 内存优化

- 大 Payload 使用引用而非复制
- 限制 Payload 大小（建议 < 256KB）
- 及时清理已完成任务

---

**下一章**: [核心概念详解](./core-concepts.md)
