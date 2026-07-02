# Asynq 分布式任务队列实现方案

## 目录

- [架构设计](./architecture.md) - Asynq 整体架构和技术栈
- [核心概念](./core-concepts.md) - 详细的核心概念说明
- [使用指南](./usage-guide.md) - 完整的使用教程和代码示例
- [最佳实践](./best-practices.md) - 生产环境最佳实践和性能优化
- **[xflow 集成方案](./xflow-integration.md) - 在 xflow 工作流引擎中集成 Asynq** ⭐

## 什么是 Asynq

Asynq 是一个基于 **Redis** 的 Go 语言**分布式任务队列库**，提供简单、可靠且高效的异步任务处理能力。

### 核心特点

- **简单易用** - API 设计优雅，上手快速
- **可靠性强** - 任务持久化、自动重试、超时控制
- **功能完善** - 支持延迟任务、定时任务、任务优先级、任务去重
- **监控友好** - 提供 Web UI、Inspector API、Prometheus 指标
- **高性能** - 基于 Redis，支持高并发任务处理
- **生产就绪** - 被众多公司在生产环境使用

### GitHub 仓库

- 主仓库: [hibiken/asynq](https://github.com/hibiken/asynq)
- Web UI: [hibiken/asynqmon](https://github.com/hibiken/asynqmon)
- 官方文档: [GitHub Wiki](https://github.com/hibiken/asynq/wiki)

## 技术栈

### 核心依赖
- **Go 1.16+** - 编程语言
- **Redis 4.0+** - 消息代理和存储
- **go-redis/redis** - Redis 客户端
- **cron** - Cron 表达式解析

### 可选组件
- **Prometheus** - 指标监控
- **Grafana** - 可视化监控
- **asynqmon** - Web 管理界面

### 架构特点
- **Client-Server 架构** - 生产者和消费者分离
- **无侵入式设计** - 不需要修改现有业务代码
- **插件化扩展** - 支持中间件机制

## 快速开始

```bash
# 安装 Asynq
go get -u github.com/hibiken/asynq

# 启动 Redis
docker run -d -p 6379:6379 redis:7-alpine

# 安装 Web UI (可选)
go install github.com/hibiken/asynq/tools/asynqmon@latest
asynqmon --redis-addr=localhost:6379
```

### Master-Worker 架构示例

Asynq 采用 Master-Worker 架构，Master 负责提交任务，Worker 负责执行任务。

#### 1. 共享任务定义 (tasks/email.go)

```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
)

// 定义任务类型
const TypeEmailDelivery = "email:deliver"

// 任务 Payload
type EmailPayload struct {
    UserID int    `json:"user_id"`
    Email  string `json:"email"`
}

// 创建任务（Master 使用）
func NewEmailTask(userID int, email string) (*asynq.Task, error) {
    payload, err := json.Marshal(EmailPayload{
        UserID: userID,
        Email:  email,
    })
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeEmailDelivery, payload), nil
}

// 处理任务（Worker 使用）
func HandleEmailTask(ctx context.Context, t *asynq.Task) error {
    var p EmailPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }

    log.Printf("Sending email to %s (user_id=%d)", p.Email, p.UserID)

    // 模拟邮件发送
    time.Sleep(2 * time.Second)

    log.Printf("Email sent successfully to %s", p.Email)
    return nil
}
```

#### 2. Master 程序 (cmd/master/main.go)

Master 负责提交任务到队列。

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
    "yourapp/tasks"
)

func main() {
    // 创建 Redis 连接
    redisOpt := asynq.RedisClientOpt{
        Addr: "localhost:6379",
    }

    // 创建 Client
    client := asynq.NewClient(redisOpt)
    defer client.Close()

    log.Println("Master started, enqueueing tasks...")

    // 入队任务
    for i := 1; i <= 5; i++ {
        task, err := tasks.NewEmailTask(
            100+i,
            fmt.Sprintf("user%d@example.com", i),
        )
        if err != nil {
            log.Printf("Failed to create task: %v", err)
            continue
        }

        // 入队任务
        info, err := client.Enqueue(task)
        if err != nil {
            log.Printf("Failed to enqueue task: %v", err)
            continue
        }

        log.Printf("✓ Enqueued task: id=%s, queue=%s", info.ID, info.Queue)

        // 间隔 1 秒
        time.Sleep(1 * time.Second)
    }

    log.Println("All tasks enqueued")
}
```

#### 3. Worker 程序 (cmd/worker/main.go)

Worker 负责从队列拉取并执行任务。

```go
package main

import (
    "log"

    "github.com/hibiken/asynq"
    "yourapp/tasks"
)

func main() {
    // 创建 Redis 连接
    redisOpt := asynq.RedisClientOpt{
        Addr: "localhost:6379",
    }

    // 创建 Server（Worker）
    srv := asynq.NewServer(
        redisOpt,
        asynq.Config{
            // 并发 Worker 数量
            Concurrency: 10,

            // 队列优先级
            Queues: map[string]int{
                "critical": 6,
                "default":  3,
                "low":      1,
            },

            // 日志级别
            LogLevel: asynq.InfoLevel,
        },
    )

    // 创建任务路由器
    mux := asynq.NewServeMux()

    // 注册任务处理器
    mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailTask)
    // 可以注册更多任务类型...
    // mux.HandleFunc(tasks.TypeImageResize, tasks.HandleImageTask)

    log.Println("Worker started, waiting for tasks...")

    // 启动 Worker（会阻塞）
    if err := srv.Run(mux); err != nil {
        log.Fatalf("Could not run worker: %v", err)
    }
}
```

#### 4. 运行示例

```bash
# 终端 1: 启动 Redis
docker run -d -p 6379:6379 redis:7-alpine

# 终端 2: 启动 Worker（先启动，持续运行）
go run cmd/worker/main.go

# 终端 3: 运行 Master（提交任务）
go run cmd/master/main.go
```

#### 5. 预期输出

**Worker 输出**:
```
2026/01/11 10:00:00 Worker started, waiting for tasks...
2026/01/11 10:00:01 Sending email to user1@example.com (user_id=101)
2026/01/11 10:00:03 Email sent successfully to user1@example.com
2026/01/11 10:00:03 Sending email to user2@example.com (user_id=102)
2026/01/11 10:00:05 Email sent successfully to user2@example.com
...
```

**Master 输出**:
```
2026/01/11 10:00:00 Master started, enqueueing tasks...
2026/01/11 10:00:00 ✓ Enqueued task: id=abc123, queue=default
2026/01/11 10:00:01 ✓ Enqueued task: id=def456, queue=default
2026/01/11 10:00:02 ✓ Enqueued task: id=ghi789, queue=default
...
2026/01/11 10:00:05 All tasks enqueued
```

#### 6. 生产环境部署

```bash
# 编译 Master
go build -o master cmd/master/main.go

# 编译 Worker
go build -o worker cmd/worker/main.go

# 部署多个 Worker 实例实现负载均衡
./worker &  # Worker 1
./worker &  # Worker 2
./worker &  # Worker 3

# Master 可以集成到 Web 应用或定时任务中
./master
```

## 在 xflow 项目中的应用

本文档集旨在帮助理解 Asynq 的核心理念和实现方式，为 xflow 项目提供参考：

1. **任务队列系统设计** - 如何设计可靠的异步任务处理系统
2. **任务调度机制** - 延迟任务、定时任务的实现原理
3. **可靠性保证** - 任务重试、超时控制、死信队列的设计
4. **监控和管理** - 任务状态追踪、性能指标收集
5. **分布式架构** - 多 Worker 协作、负载均衡的实现

## 主要使用场景

### 1. 异步任务处理
- 邮件发送
- 短信通知
- 消息推送
- 文件上传/下载
- 图片/视频处理

### 2. 延迟任务
- 订单超时自动取消
- 定时提醒
- 延迟通知
- 定时数据清理

### 3. 定时任务
- 每日数据统计
- 定时报表生成
- 定时数据同步
- 定时备份

### 4. 批量处理
- 批量数据导入
- 批量用户通知
- 数据批量更新

## 核心概念预览

### Client（任务生产者）
负责创建和提交任务到 Redis 队列。

### Server（任务消费者）
从 Redis 拉取任务并执行。

### Task（任务）
包含任务类型和 Payload 数据。

### Queue（队列）
任务按队列分组，每个队列可设置不同优先级。

### Scheduler（调度器）
管理延迟任务和定时任务的触发。

### Inspector（检查器）
提供查询和管理任务队列的能力。

## 与其他方案对比

| 特性 | Asynq | Celery | RabbitMQ | AWS SQS |
|------|-------|--------|----------|---------|
| 语言 | Go | Python | 多语言 | 多语言 |
| 消息代理 | Redis | RabbitMQ/Redis | 自身 | 云服务 |
| 部署复杂度 | 低 | 中 | 中 | 低（托管）|
| 定时任务 | 原生支持 | 需要 Beat | 需要插件 | 不支持 |
| 任务去重 | 原生支持 | 需要自行实现 | 需要自行实现 | 不支持 |
| Web UI | 官方提供 | Flower（第三方）| 官方提供 | AWS Console |
| 适用规模 | 中小型 | 大型 | 大型 | 任意规模 |

## 性能特点

- **高吞吐量**: 单个 Server 实例可处理数千 tasks/秒
- **低延迟**: 毫秒级任务分发延迟
- **内存友好**: Worker 数量可控，内存占用稳定
- **水平扩展**: 支持多 Server 实例并行处理

## 文档约定

- 代码示例使用 Go 语言
- 架构图使用 ASCII 或 Mermaid 格式
- 配置示例基于 Redis 单机模式
- 生产环境建议使用 Redis 集群

## 贡献

如有问题或建议，请联系 xflow 项目团队。

---

**最后更新**: 2026-01-11
