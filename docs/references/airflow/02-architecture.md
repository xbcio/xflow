# Airflow 架构深入解析

## 1. 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                        Airflow 系统                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌────────────┐         ┌────────────┐      ┌─────────────┐ │
│  │ Web Server │◄────────┤  Scheduler │      │   Workers   │ │
│  │   (Flask)  │         │            │      │             │ │
│  └─────┬──────┘         └──────┬─────┘      └──────┬──────┘ │
│        │                       │                   │        │
│        │                       │                   │        │
│        └───────────┬───────────┴───────────────────┘        │
│                    │                                         │
│             ┌──────▼──────┐                                  │
│             │  Metadata   │                                  │
│             │  Database   │                                  │
│             │(PostgreSQL/ │                                  │
│             │   MySQL)    │                                  │
│             └─────────────┘                                  │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           DAG Files Directory                        │   │
│  │  /dags/dag1.py  /dags/dag2.py  /dags/dag3.py       │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## 2. 核心组件详解

### 2.1 Web Server

**功能**
- 提供用户界面（UI）
- 展示 DAG、任务状态
- 允许用户手动触发、暂停、重试任务
- 查看日志和执行历史
- 管理连接和变量

**技术栈**
- Flask Web 框架
- Gunicorn WSGI 服务器
- JavaScript 前端（React）

**启动命令**
```bash
airflow webserver --port 8080 --workers 4
```

**配置**
```ini
# airflow.cfg
[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
workers = 4
worker_class = sync
```

**主要页面**
- **DAGs 视图**: 所有 DAG 列表及其状态
- **Graph 视图**: DAG 的图形化依赖关系
- **Tree 视图**: 历史运行的树状视图
- **Gantt 视图**: 任务执行时间线
- **Task Duration**: 任务执行时长统计
- **Code**: DAG 源代码查看

---

### 2.2 Scheduler

**功能**
- 监控 DAG 文件变化
- 解析 DAG 定义
- 触发需要运行的任务
- 将任务提交给 Executor
- 更新任务状态

**工作流程**
```
1. 扫描 DAG 目录
   ↓
2. 解析 DAG 文件（Python 代码）
   ↓
3. 将 DAG 信息存储到元数据库
   ↓
4. 检查 schedule_interval
   ↓
5. 创建 DagRun 实例
   ↓
6. 检查任务依赖关系
   ↓
7. 将就绪任务排队
   ↓
8. 提交给 Executor 执行
```

**启动命令**
```bash
airflow scheduler
```

**配置**
```ini
# airflow.cfg
[scheduler]
# DAG 文件扫描间隔
dag_dir_list_interval = 300

# 每个循环处理的 DAG 文件数
max_dagruns_per_loop_to_schedule = 20

# 每次调度循环创建的 DagRun 数量
max_dagruns_to_create_per_loop = 10

# 并行解析的 DAG 文件数
parsing_processes = 2

# Scheduler 心跳间隔
scheduler_heartbeat_sec = 5
```

**调度逻辑**
```python
# 伪代码
while True:
    # 1. 扫描 DAG 目录
    dags = scan_dag_directory()

    # 2. 解析 DAG
    for dag_file in new_or_modified_files:
        parse_dag(dag_file)

    # 3. 创建 DagRun
    for dag in dags:
        if should_create_dagrun(dag):
            create_dagrun(dag)

    # 4. 调度任务
    for dagrun in active_dagruns:
        for task in dagrun.get_ready_tasks():
            executor.queue_task(task)

    # 5. 更新状态
    executor.heartbeat()
    update_task_states()

    # 6. 等待下一个周期
    sleep(scheduler_heartbeat_sec)
```

---

### 2.3 Executor

Executor 决定如何执行任务。Airflow 支持多种 Executor。

#### 2.3.1 SequentialExecutor

**特点**
- 单进程顺序执行
- 使用 SQLite 数据库
- 仅用于开发测试
- 不支持并行

**配置**
```ini
[core]
executor = SequentialExecutor
sql_alchemy_conn = sqlite:////path/to/airflow.db
```

**适用场景**
- 本地开发
- 功能测试
- 学习 Airflow

---

#### 2.3.2 LocalExecutor

**特点**
- 本地多进程并行执行
- 需要 PostgreSQL 或 MySQL
- 适合单机部署
- 支持并发控制

**配置**
```ini
[core]
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://user:password@localhost/airflow
parallelism = 32  # 全局最大并行任务数

[core]
max_active_tasks_per_dag = 16  # 单个 DAG 最大并行任务数
```

**工作原理**
```
Scheduler
    ↓
LocalExecutor (主进程)
    ↓
    ├──> Worker Process 1 → 执行 Task A
    ├──> Worker Process 2 → 执行 Task B
    ├──> Worker Process 3 → 执行 Task C
    └──> Worker Process 4 → 执行 Task D
```

**适用场景**
- 中小型单机部署
- 任务量不大的生产环境
- 资源受限的环境

---

#### 2.3.3 CeleryExecutor

**特点**
- 分布式执行
- 使用 Celery 作为任务队列
- 支持多个 Worker 节点
- 高可用和水平扩展

**架构**
```
                    ┌─────────────┐
                    │  Scheduler  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────────┐
                    │ CeleryExecutor  │
                    └──────┬──────────┘
                           │
                ┌──────────┴──────────┐
                │   Message Broker    │
                │  (Redis/RabbitMQ)   │
                └──────────┬──────────┘
                           │
        ┏━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━┓
        ┃                                      ┃
   ┌────▼─────┐                         ┌────▼─────┐
   │ Worker 1 │                         │ Worker 2 │
   │  Node 1  │                         │  Node 2  │
   └──────────┘                         └──────────┘
```

**配置**
```ini
# airflow.cfg
[core]
executor = CeleryExecutor

[celery]
# 消息代理
broker_url = redis://localhost:6379/0

# 结果后端
result_backend = db+postgresql://user:password@localhost/airflow

# Worker 并发数
worker_concurrency = 16

# 任务超时时间
task_time_limit = 600
```

**启动 Worker**
```bash
# 在每个 Worker 节点上运行
airflow celery worker --queues default,high_priority

# 启动 Flower (Celery 监控工具)
airflow celery flower
```

**队列管理**
```python
# 定义任务队列
task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Default queue"',
    queue='default'
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "High priority"',
    queue='high_priority'
)

# Worker 启动时指定队列
# airflow celery worker --queues high_priority
```

**适用场景**
- 大规模分布式部署
- 需要任务队列管理
- 高可用要求
- 需要根据负载动态扩容

---

#### 2.3.4 KubernetesExecutor

**特点**
- 每个任务在独立的 Kubernetes Pod 中运行
- 动态资源分配
- 任务隔离性强
- 自动伸缩

**架构**
```
┌─────────────┐
│  Scheduler  │
│  (K8s Pod)  │
└──────┬──────┘
       │
┌──────▼───────────┐
│ KubernetesExecutor│
└──────┬───────────┘
       │
┌──────▼──────────────────────────────┐
│      Kubernetes Cluster             │
│                                      │
│  ┌──────┐  ┌──────┐  ┌──────┐      │
│  │Task 1│  │Task 2│  │Task 3│      │
│  │ Pod  │  │ Pod  │  │ Pod  │      │
│  └──────┘  └──────┘  └──────┘      │
│                                      │
└──────────────────────────────────────┘
```

**配置**
```python
# 在 DAG 中配置 Pod
from kubernetes.client import models as k8s

pod_override = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"app": "airflow-task"}
    ),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"memory": "512Mi", "cpu": "500m"},
                    limits={"memory": "1Gi", "cpu": "1000m"}
                )
            )
        ]
    )
)

task = BashOperator(
    task_id='k8s_task',
    bash_command='echo "Running in K8s"',
    executor_config={"pod_override": pod_override}
)
```

**优势**
- 资源隔离：每个任务独立 Pod
- 弹性伸缩：根据负载自动创建/销毁 Pod
- 资源控制：可为每个任务指定 CPU/内存
- 成本优化：任务完成即释放资源

**适用场景**
- 云原生部署
- 任务资源需求差异大
- 需要强隔离的环境
- 使用 Kubernetes 的组织

---

#### 2.3.5 CeleryKubernetesExecutor

**特点**
- 结合 CeleryExecutor 和 KubernetesExecutor
- 可以根据任务类型选择执行器
- 灵活的资源分配策略

**配置**
```ini
[core]
executor = CeleryKubernetesExecutor
```

**使用**
```python
# 使用 Celery 执行
celery_task = BashOperator(
    task_id='celery_task',
    bash_command='echo "On Celery"',
    queue='celery'
)

# 使用 Kubernetes 执行
k8s_task = BashOperator(
    task_id='k8s_task',
    bash_command='echo "On K8s"',
    queue='kubernetes'
)
```

---

### 2.4 Metadata Database

**功能**
- 存储 DAG 定义和元数据
- 记录任务执行历史
- 保存连接信息和变量
- 存储用户和权限信息
- 存储 XCom 数据

**支持的数据库**
- **PostgreSQL** (推荐)
- **MySQL** (推荐)
- SQLite (仅开发)

**数据库结构**
主要表：
```
dag                  - DAG 定义
dag_run             - DAG 运行实例
task_instance       - 任务实例
task_fail           - 任务失败记录
xcom                - XCom 数据
connection          - 连接配置
variable            - 变量
log                 - 日志
job                 - 作业信息
sla_miss            - SLA 违规记录
```

**配置**
```ini
[core]
# PostgreSQL
sql_alchemy_conn = postgresql+psycopg2://user:password@localhost:5432/airflow

# MySQL
sql_alchemy_conn = mysql://user:password@localhost:3306/airflow

# 连接池配置
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 1800
sql_alchemy_max_overflow = 10
```

**初始化数据库**
```bash
# 初始化
airflow db init

# 升级
airflow db upgrade

# 重置（危险！会删除所有数据）
airflow db reset
```

**性能优化**
```ini
[core]
# 保留历史运行记录的天数
max_db_retries = 3

[scheduler]
# 清理作业
schedule_after_task_execution = True

# 每日清理
job_heartbeat_sec = 5
```

---

### 2.5 Worker

**功能**
- 实际执行任务
- 从 Executor 接收任务
- 更新任务状态
- 生成日志

**Worker 配置**
```bash
# CeleryExecutor Worker
airflow celery worker \
    --queues default,high_priority \
    --concurrency 8 \
    --hostname worker1@%h

# 查看活跃的 Worker
airflow celery workers
```

**监控 Worker**
```bash
# 使用 Flower
airflow celery flower --port 5555

# 访问 http://localhost:5555
```

---

## 3. 执行流程

### 3.1 完整的任务执行流程

```
1. Scheduler 扫描 DAG 目录
   └─> 解析 DAG Python 文件
   └─> 更新元数据库

2. Scheduler 检查调度时间
   └─> 创建 DagRun 实例
   └─> 状态: RUNNING

3. Scheduler 评估任务依赖
   └─> 检查上游任务状态
   └─> 检查 depends_on_past
   └─> 检查 trigger_rule

4. 任务进入队列
   └─> 状态: QUEUED
   └─> Executor 接收任务

5. Executor 分配 Worker
   └─> LocalExecutor: 创建子进程
   └─> CeleryExecutor: 发送到消息队列
   └─> KubernetesExecutor: 创建 Pod

6. Worker 执行任务
   └─> 状态: RUNNING
   └─> 执行 Operator 逻辑
   └─> 生成日志

7. 任务完成
   └─> 状态: SUCCESS / FAILED
   └─> 更新元数据库
   └─> 触发下游任务

8. DagRun 完成
   └─> 所有任务完成
   └─> 状态: SUCCESS / FAILED
```

### 3.2 调度决策流程

```python
# 伪代码
def should_task_execute(task, dagrun):
    # 1. 检查任务是否已执行
    if task.state in [SUCCESS, FAILED]:
        return False

    # 2. 检查依赖
    if not all_upstream_success(task):
        return False

    # 3. 检查 depends_on_past
    if task.depends_on_past:
        prev_dagrun = get_previous_dagrun(dagrun)
        if prev_dagrun and prev_dagrun.get_task(task).state != SUCCESS:
            return False

    # 4. 检查资源池
    if not pool_has_capacity(task.pool):
        return False

    # 5. 检查并发限制
    if dag_max_active_tasks_reached(dagrun.dag):
        return False

    return True
```

---

## 4. 高可用架构

### 4.1 单点故障预防

**多 Scheduler 部署** (Airflow 2.0+)
```ini
[scheduler]
# 启用多 Scheduler
max_threads = 2

# 多个 Scheduler 实例可以同时运行
# 它们会通过数据库锁协调工作
```

```bash
# 在不同节点启动多个 Scheduler
# Node 1
airflow scheduler

# Node 2
airflow scheduler

# Node 3
airflow scheduler
```

**Web Server 负载均衡**
```
        ┌─────────────┐
        │ Load Balancer│
        └──────┬──────┘
               │
      ┌────────┴────────┐
      │                 │
┌─────▼─────┐     ┌─────▼─────┐
│WebServer 1│     │WebServer 2│
└───────────┘     └───────────┘
      │                 │
      └────────┬────────┘
               │
        ┌──────▼──────┐
        │  Database   │
        └─────────────┘
```

### 4.2 数据库高可用

**PostgreSQL 主从复制**
```
┌─────────────┐
│  Primary DB │ ─┐
└─────────────┘  │
                 │ Replication
┌─────────────┐  │
│ Standby DB  │◄─┘
└─────────────┘
```

**连接池和故障转移**
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    'postgresql://user:pass@host/db',
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True  # 检测连接有效性
)
```

### 4.3 消息队列高可用（CeleryExecutor）

**Redis Sentinel**
```ini
[celery]
broker_url = sentinel://sentinel-host:26379/0
result_backend = db+postgresql://user:password@localhost/airflow
```

**RabbitMQ 集群**
```ini
[celery]
broker_url = amqp://guest:guest@rabbitmq-cluster:5672//
```

---

## 5. 性能优化

### 5.1 Scheduler 优化

```ini
[scheduler]
# 增加解析进程
parsing_processes = 4

# 调整调度频率
scheduler_heartbeat_sec = 5

# 批量处理
max_dagruns_to_create_per_loop = 20
max_tis_per_query = 512
```

### 5.2 数据库优化

```ini
[core]
# 连接池大小
sql_alchemy_pool_size = 10
sql_alchemy_max_overflow = 20

# 连接回收时间
sql_alchemy_pool_recycle = 3600

# 预检查连接
sql_alchemy_pool_pre_ping = True
```

**定期清理历史数据**
```bash
# 清理30天前的任务实例
airflow db clean --clean-before-timestamp "$(date -d '30 days ago' '+%Y-%m-%d')"
```

### 5.3 Worker 优化

**资源分配**
```ini
[celery]
# Worker 并发数
worker_concurrency = 16

# 任务预取
worker_prefetch_multiplier = 1

# 最大内存限制
worker_max_memory_per_child = 8000000  # 8GB
```

---

## 6. 监控和日志

### 6.1 健康检查

```bash
# Scheduler 健康检查
airflow jobs check --job-type SchedulerJob --hostname $(hostname)

# 数据库连接检查
airflow db check
```

### 6.2 指标收集

**StatsD 集成**
```ini
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

**Prometheus 集成**
```bash
pip install airflow-prometheus-exporter
```

### 6.3 日志管理

**远程日志存储**
```ini
[logging]
# S3
remote_logging = True
remote_base_log_folder = s3://my-bucket/airflow/logs
remote_log_conn_id = aws_default

# GCS
remote_base_log_folder = gs://my-bucket/airflow/logs
```

---

## 总结

Airflow 的架构设计具有以下特点：

1. **模块化**: 各组件职责清晰，易于扩展
2. **可扩展**: 支持多种 Executor，适应不同规模
3. **高可用**: 支持多 Scheduler、数据库复制、消息队列集群
4. **灵活性**: 可根据需求选择合适的部署方式
5. **可观测**: 完善的监控和日志系统

理解 Airflow 架构是构建可靠、高效数据管道的基础。
