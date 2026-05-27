# Apache Airflow 详细介绍

## 📚 文档导航

本文档集合提供了 Airflow 的全面介绍，从基础概念到生产部署。

### 文档列表

1. **[核心概念详解](./01-core-concepts.md)** - DAG、Operator、Task、XCom、Sensor 等
2. **[架构深入解析](./02-architecture.md)** - Web Server、Scheduler、Executor、数据库
3. **[实战示例](./03-practical-examples.md)** - ETL、机器学习、多数据源整合等完整示例
4. **[最佳实践](./04-best-practices.md)** - DAG 设计、性能优化、错误处理、安全
5. **[部署指南](./05-deployment-guide.md)** - 本地开发、生产部署、Kubernetes、监控
6. **[调度和执行机制](./06-scheduling-execution.md)** - 调度原理、依赖管理、并发控制

### 快速开始路线

**🔰 新手入门**
1. 阅读本文档的"什么是 Airflow"部分
2. 查看[核心概念详解](./01-core-concepts.md)
3. 参考[实战示例](./03-practical-examples.md)中的 ETL 示例
4. 按照[部署指南](./05-deployment-guide.md)搭建本地环境

**⚡ 快速实践**
1. 使用 Docker Compose 快速启动（见[部署指南](./05-deployment-guide.md)）
2. 创建第一个 DAG
3. 在 UI 中触发和监控
4. 查看[最佳实践](./04-best-practices.md)优化代码

**🏢 生产部署**
1. 理解[架构深入解析](./02-architecture.md)
2. 选择合适的 Executor
3. 按照[部署指南](./05-deployment-guide.md)进行生产部署
4. 配置监控和告警
5. 遵循[最佳实践](./04-best-practices.md)

**📖 深入学习**
1. 研究[调度和执行机制](./06-scheduling-execution.md)
2. 学习高级特性（Sensor、Branch、SubDAG 等）
3. 探索源码（可以帮你克隆 Airflow 仓库）

---

## 什么是 Airflow？

Apache Airflow 是一个开源的工作流编排平台，用于以编程方式创建、调度和监控数据管道（workflows）。它最初由 Airbnb 开发，2016 年加入 Apache 孵化器，现在是 Apache 顶级项目。

## 核心概念

### 1. **DAG (Directed Acyclic Graph)**
- 有向无环图，是 Airflow 中工作流的核心表示
- 定义任务之间的依赖关系和执行顺序
- 不允许循环依赖

### 2. **Operator**
- 定义单个任务要执行的工作
- 常见类型：
  - `BashOperator`: 执行 bash 命令
  - `PythonOperator`: 执行 Python 函数
  - `SqlOperator`: 执行 SQL 查询
  - `DockerOperator`: 在 Docker 容器中执行任务
  - `KubernetesPodOperator`: 在 K8s Pod 中执行任务

### 3. **Task**
- Operator 的实例化
- DAG 中的一个执行单元

### 4. **Scheduler**
- 监控所有 DAG 和任务
- 触发满足条件的任务执行
- 将任务提交给 Executor

### 5. **Executor**
- 决定如何执行任务
- 类型：
  - `SequentialExecutor`: 单机顺序执行（默认，仅开发用）
  - `LocalExecutor`: 本地并行执行
  - `CeleryExecutor`: 分布式执行
  - `KubernetesExecutor`: K8s 集群执行

### 6. **Worker**
- 实际执行任务的进程

## 架构组件

```
┌─────────────┐
│  Web Server │ ← 用户界面，监控和管理
└──────┬──────┘
       │
┌──────┴──────┐
│  Scheduler  │ ← 调度和触发任务
└──────┬──────┘
       │
┌──────┴──────┐
│   Executor  │ ← 决定如何执行
└──────┬──────┘
       │
┌──────┴──────┐
│   Workers   │ ← 执行任务
└──────┬──────┘
       │
┌──────┴──────┐
│  Metadata   │ ← 存储状态（PostgreSQL/MySQL）
│   Database  │
└─────────────┘
```

## 使用场景

1. **ETL 管道**: 数据提取、转换和加载
2. **机器学习工作流**: 模型训练、评估和部署
3. **数据仓库维护**: 定期更新、清理和优化
4. **报表生成**: 定时生成和发送报表
5. **批处理任务**: 大规模数据处理
6. **监控和告警**: 系统健康检查

## 核心优势

### 1. **代码即配置 (Configuration as Code)**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'example_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    task2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    task1 >> task2  # 定义依赖关系
```

### 2. **丰富的 UI**
- 可视化 DAG 结构
- 实时监控任务状态
- 查看日志和错误
- 手动触发和重试
- Gantt 图和树状视图

### 3. **扩展性强**
- 插件系统
- 自定义 Operator
- Hook（连接外部系统）
- Sensor（等待条件满足）

### 4. **可靠性**
- 自动重试机制
- 任务超时控制
- SLA 监控
- 告警通知

## 基本示例

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_data():
    print("Extracting data...")
    return {"data": [1, 2, 3]}

def transform_data(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    print(f"Transforming {data}")
    return {"transformed": [x * 2 for x in data['data']]}

def load_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform')
    print(f"Loading {data}")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Simple ETL pipeline',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    # 定义任务流
    extract >> transform >> load
```

## 关键特性

- **动态管道生成**: 可以根据配置动态生成任务
- **参数化**: 支持变量和模板
- **XCom**: 任务间数据共享
- **分支**: 条件执行
- **子 DAG**: 模块化复用
- **触发规则**: all_success, one_success, all_failed 等
- **连接池**: 限制并发连接数
- **任务组**: 逻辑分组

## 安装和快速开始

```bash
# 安装
pip install apache-airflow

# 初始化数据库
airflow db init

# 创建用户
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# 启动 Web 服务器
airflow webserver --port 8080

# 启动调度器（新终端）
airflow scheduler
```

## 适合团队

- 数据工程师
- 数据科学家
- ML 工程师
- DevOps 工程师

## 下一步

- 阅读[官方文档](https://airflow.apache.org/docs/)
- 探索 [Airflow 源码](https://github.com/apache/airflow)
- 尝试构建第一个 DAG
- 了解生产环境部署最佳实践
