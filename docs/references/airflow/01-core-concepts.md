# Airflow 核心概念详解

## 1. DAG (Directed Acyclic Graph) - 有向无环图

### 什么是 DAG？

DAG 是 Airflow 中的核心概念，代表一个完整的工作流。它定义了任务之间的依赖关系和执行顺序。

### DAG 的关键特性

**有向 (Directed)**
- 任务之间有明确的执行方向
- 通过 `>>` 或 `<<` 操作符定义依赖关系
- 数据流向是单向的

**无环 (Acyclic)**
- 不允许循环依赖
- 保证任务执行的确定性
- 防止无限循环

**图 (Graph)**
- 由节点（任务）和边（依赖关系）组成
- 可以有多个起点和终点
- 支持复杂的依赖关系

### DAG 定义示例

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 方式1: 使用 with 语句（推荐）
with DAG(
    dag_id='example_dag',
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email': ['team@example.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='Example DAG with detailed configuration',
    schedule_interval='@daily',  # 或 '0 0 * * *'
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'tutorial'],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Task 1"'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Task 2"'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "Task 3"'
    )

    # 定义依赖关系
    task1 >> [task2, task3]  # task1 完成后，task2 和 task3 并行执行

# 方式2: 传统方式
dag = DAG(
    'example_dag_2',
    default_args=default_args,
    schedule_interval='@daily',
)

task_a = BashOperator(
    task_id='task_a',
    bash_command='echo "A"',
    dag=dag
)
```

### DAG 参数详解

#### dag_id
- DAG 的唯一标识符
- 必须在 Airflow 实例中唯一
- 建议使用描述性名称，如 `etl_user_data_daily`

#### schedule_interval
调度间隔，支持多种格式：

**Cron 表达式**
```python
'0 0 * * *'      # 每天午夜
'*/15 * * * *'   # 每15分钟
'0 9 * * 1-5'    # 工作日早上9点
```

**预设值**
```python
'@once'       # 只运行一次
'@hourly'     # 每小时
'@daily'      # 每天午夜
'@weekly'     # 每周日午夜
'@monthly'    # 每月1日午夜
'@yearly'     # 每年1月1日午夜
```

**Timedelta**
```python
from datetime import timedelta
schedule_interval=timedelta(hours=2)  # 每2小时
```

**None**
```python
schedule_interval=None  # 手动触发
```

#### start_date
- DAG 开始执行的日期
- 必须是过去的日期
- Airflow 会从这个日期开始调度

```python
from datetime import datetime
start_date=datetime(2024, 1, 1)
```

#### end_date (可选)
- DAG 停止执行的日期
- 到达此日期后不再调度新的运行

```python
end_date=datetime(2024, 12, 31)
```

#### catchup
- 是否回填历史运行
- `True`: 从 start_date 到当前时间的所有缺失运行都会被执行
- `False`: 只执行最新的一次

```python
# 如果 start_date 是 2024-01-01，今天是 2024-01-10
catchup=True   # 会执行 1月1日到1月9日的所有运行
catchup=False  # 只执行当前的一次运行
```

#### default_args
- 应用于所有任务的默认参数
- 任务级别的参数会覆盖这些默认值

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,     # 是否依赖上一次运行成功
    'email': ['admin@example.com'],
    'email_on_failure': True,     # 失败时发送邮件
    'email_on_retry': False,      # 重试时发送邮件
    'retries': 3,                 # 重试次数
    'retry_delay': timedelta(minutes=5),     # 重试间隔
    'execution_timeout': timedelta(hours=1), # 执行超时
    'on_failure_callback': notify_failure,   # 失败回调函数
    'on_success_callback': notify_success,   # 成功回调函数
}
```

#### max_active_runs
- 同时运行的 DAG 实例数量限制
- 防止资源过度使用

```python
max_active_runs=1  # 同一时间只能有一个实例运行
```

#### concurrency (已弃用，使用 max_active_tasks)
- 同时运行的任务数量限制

```python
max_active_tasks=16  # 最多同时运行16个任务
```

#### tags
- DAG 的标签，用于分类和过滤
- 在 UI 中可以按标签搜索

```python
tags=['production', 'etl', 'daily']
```

---

## 2. Operator - 任务操作符

Operator 定义了任务要执行的具体操作。每个 Operator 实例就是一个 Task。

### 常用 Operator 类型

#### 2.1 BashOperator
执行 Bash 命令或脚本。

```python
from airflow.operators.bash import BashOperator

# 简单命令
simple_task = BashOperator(
    task_id='simple_bash',
    bash_command='echo "Hello Airflow"'
)

# 多行命令
multi_line_task = BashOperator(
    task_id='multi_line',
    bash_command="""
        cd /tmp
        ls -la
        echo "Current directory: $(pwd)"
    """
)

# 执行脚本
script_task = BashOperator(
    task_id='run_script',
    bash_command='/path/to/script.sh',
    env={'ENV_VAR': 'value'}  # 设置环境变量
)

# 使用模板
template_task = BashOperator(
    task_id='template_example',
    bash_command='echo "Execution date: {{ ds }}"'
)
```

#### 2.2 PythonOperator
执行 Python 函数。

```python
from airflow.operators.python import PythonOperator

def my_function(param1, param2, **context):
    """
    context 包含 Airflow 提供的上下文变量
    """
    execution_date = context['execution_date']
    print(f"Param1: {param1}, Param2: {param2}")
    print(f"Execution Date: {execution_date}")
    return {"result": "success"}

python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_function,
    op_args=['value1'],           # 位置参数
    op_kwargs={'param2': 'value2'} # 关键字参数
)

# 使用 lambda
lambda_task = PythonOperator(
    task_id='lambda_task',
    python_callable=lambda: print("Hello from lambda")
)
```

#### 2.3 PythonVirtualenvOperator
在独立的虚拟环境中执行 Python 代码。

```python
from airflow.operators.python import PythonVirtualenvOperator

def complex_function():
    import pandas as pd
    import numpy as np

    df = pd.DataFrame({'A': [1, 2, 3]})
    return df.sum().to_dict()

venv_task = PythonVirtualenvOperator(
    task_id='virtualenv_task',
    python_callable=complex_function,
    requirements=['pandas==1.3.0', 'numpy==1.21.0'],
    system_site_packages=False
)
```

#### 2.4 BranchPythonOperator
根据条件选择执行分支。

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    execution_date = context['execution_date']
    if execution_date.day % 2 == 0:
        return 'even_day_task'
    else:
        return 'odd_day_task'

branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_branch
)

even_task = BashOperator(
    task_id='even_day_task',
    bash_command='echo "Even day"'
)

odd_task = BashOperator(
    task_id='odd_day_task',
    bash_command='echo "Odd day"'
)

branch >> [even_task, odd_task]
```

#### 2.5 EmailOperator
发送邮件通知。

```python
from airflow.operators.email import EmailOperator

email_task = EmailOperator(
    task_id='send_email',
    to='recipient@example.com',
    subject='Airflow Alert: {{ dag.dag_id }}',
    html_content="""
        <h3>DAG Run Summary</h3>
        <p>Execution Date: {{ ds }}</p>
        <p>Status: Success</p>
    """
)
```

#### 2.6 SqlOperators
执行数据库操作。

```python
# PostgreSQL
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_task = PostgresOperator(
    task_id='postgres_query',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO users (name, email)
        VALUES ('John', 'john@example.com')
    """
)

# MySQL
from airflow.providers.mysql.operators.mysql import MySqlOperator

mysql_task = MySqlOperator(
    task_id='mysql_query',
    mysql_conn_id='mysql_default',
    sql='SELECT * FROM users WHERE created_at > {{ ds }}'
)
```

#### 2.7 HttpOperator
发送 HTTP 请求。

```python
from airflow.providers.http.operators.http import SimpleHttpOperator

http_task = SimpleHttpOperator(
    task_id='http_request',
    http_conn_id='http_default',
    endpoint='api/v1/users',
    method='POST',
    data='{"name": "John"}',
    headers={'Content-Type': 'application/json'}
)
```

#### 2.8 DockerOperator
在 Docker 容器中执行任务。

```python
from airflow.providers.docker.operators.docker import DockerOperator

docker_task = DockerOperator(
    task_id='docker_task',
    image='python:3.9',
    command='python -c "print(\'Hello from Docker\')"',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)
```

#### 2.9 KubernetesPodOperator
在 Kubernetes Pod 中执行任务。

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

k8s_task = KubernetesPodOperator(
    task_id='k8s_task',
    name='airflow-pod',
    namespace='default',
    image='python:3.9',
    cmds=['python', '-c'],
    arguments=['print("Hello from K8s")'],
    get_logs=True,
    is_delete_operator_pod=True
)
```

### Operator 通用参数

所有 Operator 都支持以下参数：

```python
task = SomeOperator(
    task_id='unique_task_id',        # 必需，唯一标识符
    owner='data_team',                # 任务负责人
    email=['alert@example.com'],      # 通知邮箱
    retries=3,                        # 重试次数
    retry_delay=timedelta(minutes=5), # 重试间隔
    execution_timeout=timedelta(hours=1), # 执行超时
    depends_on_past=False,            # 是否依赖上一次运行
    wait_for_downstream=False,        # 是否等待下游任务
    priority_weight=1,                # 优先级权重
    pool='default_pool',              # 资源池
    queue='default',                  # 队列
    trigger_rule='all_success',       # 触发规则
    on_failure_callback=None,         # 失败回调
    on_success_callback=None,         # 成功回调
    on_retry_callback=None,           # 重试回调
)
```

---

## 3. Task - 任务

Task 是 Operator 的实例化，是 DAG 中的执行单元。

### Task 依赖关系

#### 3.1 基本依赖

```python
# 方式1: 使用 >> 操作符（推荐）
task1 >> task2  # task2 在 task1 之后执行

# 方式2: 使用 << 操作符
task2 << task1  # 等同于 task1 >> task2

# 方式3: 使用 set_downstream
task1.set_downstream(task2)

# 方式4: 使用 set_upstream
task2.set_upstream(task1)
```

#### 3.2 多个依赖

```python
# 线性依赖
task1 >> task2 >> task3 >> task4

# 并行任务
task1 >> [task2, task3, task4]  # task2, task3, task4 并行执行

# 汇聚点
[task1, task2, task3] >> task4  # task4 等待所有前置任务完成

# 复杂依赖
task1 >> task2
task1 >> task3
[task2, task3] >> task4
```

#### 3.3 交叉依赖

```python
from airflow.models.baseoperator import cross_downstream, chain

# cross_downstream: 笛卡尔积依赖
cross_downstream([task1, task2], [task3, task4])
# 等同于:
# task1 >> task3, task1 >> task4
# task2 >> task3, task2 >> task4

# chain: 链式依赖
chain(task1, task2, task3, task4)
# 等同于: task1 >> task2 >> task3 >> task4

# 复杂链
chain(task1, [task2, task3], task4)
# 等同于:
# task1 >> task2 >> task4
# task1 >> task3 >> task4
```

### Task 状态

Airflow 中的 Task 有以下状态：

- **none**: 任务尚未排队
- **scheduled**: 任务已计划执行
- **queued**: 任务在队列中等待
- **running**: 任务正在执行
- **success**: 任务成功完成
- **failed**: 任务执行失败
- **skipped**: 任务被跳过（通常是分支逻辑）
- **upstream_failed**: 上游任务失败
- **up_for_retry**: 任务将重试
- **up_for_reschedule**: 任务将重新调度
- **removed**: 任务已被移除
- **restarting**: 任务正在重启

### Trigger Rules - 触发规则

控制任务在什么条件下执行：

```python
from airflow.utils.trigger_rule import TriggerRule

task = SomeOperator(
    task_id='example',
    trigger_rule='all_success'  # 默认值
)
```

可用的触发规则：

- **all_success** (默认): 所有上游任务成功
- **all_failed**: 所有上游任务失败
- **all_done**: 所有上游任务完成（不管成功失败）
- **one_success**: 至少一个上游任务成功
- **one_failed**: 至少一个上游任务失败
- **none_failed**: 没有上游任务失败（成功或跳过）
- **none_skipped**: 没有上游任务被跳过
- **dummy**: 无条件执行

```python
# 示例：清理任务，无论前面任务成功失败都要执行
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='rm -rf /tmp/data',
    trigger_rule=TriggerRule.ALL_DONE
)

[task1, task2, task3] >> cleanup_task
```

---

## 4. TaskGroup - 任务组

TaskGroup 用于在 UI 中逻辑性地组织任务，使复杂 DAG 更易读。

```python
from airflow.utils.task_group import TaskGroup

with DAG('example_task_group', start_date=datetime(2024, 1, 1)) as dag:

    start = BashOperator(task_id='start', bash_command='echo start')

    # 创建任务组
    with TaskGroup('processing_group', tooltip="Data Processing") as processing:
        task1 = BashOperator(task_id='task1', bash_command='echo 1')
        task2 = BashOperator(task_id='task2', bash_command='echo 2')
        task3 = BashOperator(task_id='task3', bash_command='echo 3')

        task1 >> [task2, task3]

    with TaskGroup('validation_group', tooltip="Data Validation") as validation:
        validate1 = BashOperator(task_id='validate1', bash_command='echo v1')
        validate2 = BashOperator(task_id='validate2', bash_command='echo v2')

        validate1 >> validate2

    end = BashOperator(task_id='end', bash_command='echo end')

    # 任务组之间的依赖
    start >> processing >> validation >> end
```

TaskGroup 的好处：
- UI 中可以折叠/展开
- 逻辑分组，提高可读性
- 可以对整个组设置依赖
- 任务 ID 自动添加前缀（如 `processing_group.task1`）

---

## 5. XCom - 任务间通信

XCom (Cross-Communication) 允许任务之间传递小量数据。

### 基本用法

```python
# 推送数据
def push_function(**context):
    context['ti'].xcom_push(key='my_key', value='my_value')
    # 或者直接 return（会自动推送到 'return_value' key）
    return {'result': 42}

# 拉取数据
def pull_function(**context):
    ti = context['ti']
    # 拉取特定 key
    value = ti.xcom_pull(key='my_key', task_ids='push_task')
    # 拉取 return value
    result = ti.xcom_pull(task_ids='push_task')
    print(f"Value: {value}, Result: {result}")

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_function
)

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function
)

push_task >> pull_task
```

### XCom 高级用法

```python
# 从多个任务拉取
def pull_multiple(**context):
    ti = context['ti']
    values = ti.xcom_pull(task_ids=['task1', 'task2', 'task3'])
    print(values)  # 列表形式返回

# 拉取所有 keys
def pull_all_keys(**context):
    ti = context['ti']
    all_values = ti.xcom_pull(task_ids='push_task', key=None)
    print(all_values)  # 字典形式返回所有 key-value

# 在模板中使用 XCom
bash_task = BashOperator(
    task_id='bash_with_xcom',
    bash_command='echo "{{ ti.xcom_pull(task_ids="push_task") }}"'
)
```

### XCom 注意事项

- 只适合传递小量数据（KB 级别）
- 数据存储在元数据库中
- 大数据应使用外部存储（S3, GCS 等）并传递路径
- XCom 数据会被序列化（pickle）

```python
# 推荐：传递文件路径而不是大数据
def process_large_data(**context):
    # 处理数据并保存
    output_path = '/tmp/large_data.csv'
    # ... 保存数据 ...
    return output_path  # 只传递路径

def use_large_data(**context):
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='process_task')
    # 从文件读取数据
    # ...
```

---

## 6. Sensors - 传感器

Sensor 是一种特殊的 Operator，用于等待某个条件满足。

### 常用 Sensor

#### 6.1 FileSensor
等待文件出现。

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/path/to/file.csv',
    poke_interval=30,  # 每30秒检查一次
    timeout=600,       # 10分钟超时
    mode='poke'        # 'poke' 或 'reschedule'
)
```

#### 6.2 DateTimeSensor
等待特定时间。

```python
from airflow.sensors.date_time import DateTimeSensor

wait_for_time = DateTimeSensor(
    task_id='wait_for_midnight',
    target_time='{{ macros.datetime.now().replace(hour=0, minute=0) }}'
)
```

#### 6.3 ExternalTaskSensor
等待另一个 DAG 的任务完成。

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_external = ExternalTaskSensor(
    task_id='wait_for_other_dag',
    external_dag_id='other_dag',
    external_task_id='other_task',
    timeout=3600
)
```

#### 6.4 SqlSensor
等待 SQL 查询返回行。

```python
from airflow.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id='wait_for_data',
    conn_id='postgres_default',
    sql="SELECT COUNT(*) FROM users WHERE created_at > '{{ ds }}'"
)
```

#### 6.5 HttpSensor
等待 HTTP 端点返回成功。

```python
from airflow.providers.http.sensors.http import HttpSensor

wait_for_api = HttpSensor(
    task_id='wait_for_api',
    http_conn_id='http_default',
    endpoint='api/health',
    response_check=lambda response: 'status' in response.json() and response.json()['status'] == 'healthy'
)
```

### Sensor 模式

**Poke 模式** (默认)
- Sensor 占用一个 worker slot
- 持续检查直到条件满足
- 适合短时间等待

```python
sensor = FileSensor(
    task_id='sensor',
    filepath='/path/to/file',
    mode='poke',
    poke_interval=60  # 每60秒检查一次
)
```

**Reschedule 模式**
- Sensor 释放 worker slot
- 定期重新调度检查
- 适合长时间等待

```python
sensor = FileSensor(
    task_id='sensor',
    filepath='/path/to/file',
    mode='reschedule',
    poke_interval=300  # 每5分钟检查一次
)
```

---

## 7. Hooks - 连接器

Hook 是与外部系统交互的接口，封装了连接逻辑。

### 使用 Hook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def query_database():
    # 创建 Hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # 获取连接
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # 执行查询
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()

    # 或使用简化方法
    records = pg_hook.get_records("SELECT * FROM users")

    return len(records)

query_task = PythonOperator(
    task_id='query_db',
    python_callable=query_database
)
```

### 常用 Hooks

- `PostgresHook`: PostgreSQL
- `MySqlHook`: MySQL
- `HttpHook`: HTTP/REST API
- `S3Hook`: AWS S3
- `SlackHook`: Slack
- `EmailHook`: Email

### 配置连接

在 Airflow UI 中：Admin -> Connections

或使用环境变量：
```bash
AIRFLOW_CONN_MY_POSTGRES='postgresql://user:password@host:5432/database'
```

---

## 总结

Airflow 的核心概念形成了完整的工作流编排体系：

1. **DAG**: 定义工作流结构和调度规则
2. **Operator**: 定义具体的任务操作
3. **Task**: Operator 的实例，实际执行单元
4. **TaskGroup**: 任务的逻辑分组
5. **XCom**: 任务间的数据传递
6. **Sensor**: 等待特定条件的特殊任务
7. **Hook**: 与外部系统交互的接口

这些概念相互配合，使得 Airflow 能够构建复杂、可靠、可维护的数据管道。
