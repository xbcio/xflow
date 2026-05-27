# Airflow 调度和执行机制

## 1. 调度基础

### 1.1 Execution Date vs Logical Date

Airflow 的时间概念可能与直觉相反。

```python
# DAG 配置
with DAG(
    'time_example',
    start_date=datetime(2024, 1, 1, 0, 0),
    schedule_interval='@daily',  # 每天午夜
) as dag:
    task = BashOperator(
        task_id='print_date',
        bash_command='echo "Execution Date: {{ ds }}"'
    )
```

**关键概念**

- **Logical Date (execution_date)**: 数据的逻辑时间（数据窗口的开始）
- **Start Date**: 实际开始执行的时间（数据窗口的结束）

**示例时间线**
```
DAG: start_date=2024-01-01, schedule_interval='@daily'

┌────────────────────────────────────────────────────────┐
│  Logical Date      │  Actual Run Time                  │
├────────────────────────────────────────────────────────┤
│  2024-01-01 00:00  │  2024-01-02 00:00 (次日午夜)      │
│  2024-01-02 00:00  │  2024-01-03 00:00                │
│  2024-01-03 00:00  │  2024-01-04 00:00                │
└────────────────────────────────────────────────────────┘

理解：
- 2024-01-01 的 DAG Run 处理 2024-01-01 这一天的数据
- 但实际执行时间是 2024-01-02 00:00（数据收集完毕后）
```

**为什么这样设计？**
- 批处理通常处理已完成时段的数据
- 例如：每天凌晨处理昨天的数据

**在代码中使用**
```python
def process_data(**context):
    # Logical Date（数据日期）
    logical_date = context['ds']  # "2024-01-01"
    execution_date = context['execution_date']  # datetime(2024, 1, 1)

    # 实际运行时间
    run_start = context['data_interval_start']  # 数据窗口开始
    run_end = context['data_interval_end']      # 数据窗口结束

    print(f"Processing data from {run_start} to {run_end}")
    print(f"Logical date: {logical_date}")

    # 查询数据库
    query = f"""
        SELECT * FROM events
        WHERE event_time >= '{run_start}'
        AND event_time < '{run_end}'
    """
```

---

### 1.2 Schedule Interval 详解

#### Cron 表达式

```python
# 基本格式: 分 时 日 月 周
# *  *  *  *  *
# │  │  │  │  │
# │  │  │  │  └─── 周几 (0-7, 0 和 7 都是周日)
# │  │  │  └────── 月份 (1-12)
# │  │  └───────── 日期 (1-31)
# │  └──────────── 小时 (0-23)
# └─────────────── 分钟 (0-59)

# 常见示例
'0 0 * * *'        # 每天午夜
'0 9 * * *'        # 每天早上9点
'0 */2 * * *'      # 每2小时
'*/15 * * * *'     # 每15分钟
'0 0 * * 0'        # 每周日午夜
'0 0 1 * *'        # 每月1日午夜
'0 9 * * 1-5'      # 工作日早上9点
'30 8,14 * * *'    # 每天8:30和14:30
'0 0 1 1 *'        # 每年1月1日午夜
```

**复杂示例**
```python
# 工作日每2小时（9点到17点）
schedule_interval='0 9-17/2 * * 1-5'

# 每月最后一天
# 需要使用 timetable（Airflow 2.2+）
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    'monthly_last_day',
    timetable=CronDataIntervalTimetable('0 0 L * *', timezone='UTC'),
) as dag:
    task = BashOperator(...)
```

#### Timedelta

```python
from datetime import timedelta

# 每30分钟
schedule_interval=timedelta(minutes=30)

# 每2小时
schedule_interval=timedelta(hours=2)

# 每3天
schedule_interval=timedelta(days=3)

# 组合
schedule_interval=timedelta(days=1, hours=12)  # 每36小时
```

#### 预设值

```python
'@once'      # 只运行一次
'@hourly'    # 每小时 = '0 * * * *'
'@daily'     # 每天午夜 = '0 0 * * *'
'@weekly'    # 每周日午夜 = '0 0 * * 0'
'@monthly'   # 每月1日午夜 = '0 0 1 * *'
'@yearly'    # 每年1月1日午夜 = '0 0 1 1 *'

None         # 手动触发，无自动调度
```

#### 自定义 Timetable (Airflow 2.2+)

```python
from typing import Optional
from datetime import datetime
from airflow.timetables.base import Timetable, DataInterval
from airflow.plugins_manager import AirflowPlugin

class BusinessDayTimetable(Timetable):
    """只在工作日运行"""

    def next_dagrun_info(self, last_automated_data_interval, restriction):
        if last_automated_data_interval is None:
            # 第一次运行
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end

        # 跳过周末
        while next_start.weekday() >= 5:  # 5=周六, 6=周日
            next_start += timedelta(days=1)

        next_end = next_start + timedelta(days=1)

        return DataInterval(start=next_start, end=next_end)

class CustomTimetablePlugin(AirflowPlugin):
    name = "custom_timetable"
    timetables = [BusinessDayTimetable]

# 使用自定义 Timetable
with DAG(
    'business_day_dag',
    timetable=BusinessDayTimetable(),
    start_date=datetime(2024, 1, 1),
) as dag:
    task = BashOperator(...)
```

---

## 2. 任务执行流程

### 2.1 任务生命周期

```
1. NONE (未创建)
   │
   ├─> 2. SCHEDULED (已调度)
   │      │
   │      ├─> 3. QUEUED (已入队)
   │      │      │
   │      │      ├─> 4. RUNNING (运行中)
   │      │      │      │
   │      │      │      ├─> 5. SUCCESS (成功) ✓
   │      │      │      │
   │      │      │      ├─> 6. FAILED (失败) ✗
   │      │      │      │      │
   │      │      │      │      └─> 7. UP_FOR_RETRY (等待重试)
   │      │      │      │             │
   │      │      │      │             └─> (返回 QUEUED)
   │      │      │      │
   │      │      │      └─> 8. SKIPPED (跳过)
   │      │      │
   │      │      └─> 9. UPSTREAM_FAILED (上游失败)
   │      │
   │      └─> 10. REMOVED (已移除)
   │
   └─> 11. DEFERRED (延迟) [Airflow 2.2+]
```

### 2.2 任务执行详细流程

```python
# 伪代码展示 Scheduler 的决策过程

def schedule_dag_run(dag, execution_date):
    """创建 DAG Run"""
    # 1. 检查是否应该创建 DAG Run
    if not should_create_dagrun(dag, execution_date):
        return

    # 2. 创建 DAG Run
    dagrun = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        state=State.RUNNING
    )
    session.add(dagrun)
    session.commit()

    # 3. 调度任务
    schedule_tasks(dagrun)

def schedule_tasks(dagrun):
    """调度 DAG Run 中的任务"""
    for task in dagrun.dag.tasks:
        if should_schedule_task(task, dagrun):
            task_instance = TaskInstance(
                task=task,
                execution_date=dagrun.execution_date,
                state=State.SCHEDULED
            )
            session.add(task_instance)

    session.commit()

def should_schedule_task(task, dagrun):
    """判断任务是否应该被调度"""
    # 1. 检查任务是否已经执行
    ti = get_task_instance(task, dagrun.execution_date)
    if ti and ti.state in [State.SUCCESS, State.RUNNING]:
        return False

    # 2. 检查上游依赖
    if not check_upstream_dependencies(task, dagrun):
        return False

    # 3. 检查 depends_on_past
    if task.depends_on_past:
        prev_ti = get_previous_task_instance(task, dagrun.execution_date)
        if not prev_ti or prev_ti.state != State.SUCCESS:
            return False

    # 4. 检查资源池
    if not check_pool_availability(task.pool):
        return False

    # 5. 检查并发限制
    if not check_concurrency_limits(task, dagrun.dag):
        return False

    # 6. 检查 trigger_rule
    if not evaluate_trigger_rule(task, dagrun):
        return False

    return True

def check_upstream_dependencies(task, dagrun):
    """检查上游任务状态"""
    for upstream_task in task.upstream_list:
        upstream_ti = get_task_instance(upstream_task, dagrun.execution_date)

        if not upstream_ti:
            return False

        if upstream_ti.state != State.SUCCESS:
            # 根据 trigger_rule 判断
            if task.trigger_rule == TriggerRule.ALL_SUCCESS:
                return False

    return True

def execute_task(task_instance):
    """执行任务"""
    try:
        # 1. 设置状态为 RUNNING
        task_instance.state = State.RUNNING
        task_instance.start_date = datetime.now()
        session.commit()

        # 2. 执行任务
        context = build_context(task_instance)
        result = task_instance.task.execute(context)

        # 3. 处理 XCom
        if result is not None:
            xcom_push(task_instance, result)

        # 4. 标记成功
        task_instance.state = State.SUCCESS
        task_instance.end_date = datetime.now()

    except Exception as e:
        # 处理失败
        handle_failure(task_instance, e)

    finally:
        session.commit()

def handle_failure(task_instance, exception):
    """处理任务失败"""
    # 1. 记录错误
    log_error(task_instance, exception)

    # 2. 检查重试
    if task_instance.try_number < task_instance.max_tries:
        # 安排重试
        task_instance.state = State.UP_FOR_RETRY
        task_instance.end_date = datetime.now()
        task_instance.next_retry = datetime.now() + task_instance.retry_delay
    else:
        # 标记失败
        task_instance.state = State.FAILED
        task_instance.end_date = datetime.now()

        # 调用失败回调
        if task_instance.task.on_failure_callback:
            task_instance.task.on_failure_callback(build_context(task_instance))
```

---

## 3. 依赖管理

### 3.1 任务依赖类型

**线性依赖**
```python
task1 >> task2 >> task3 >> task4
# task1 -> task2 -> task3 -> task4
```

**并行分支**
```python
start >> [task1, task2, task3] >> end
#        /     |     \
# start -+-----+-----+- end
```

**复杂依赖**
```python
# 钻石依赖
start >> [task1, task2]
[task1, task2] >> task3
task3 >> end

#      task1
#     /     \
# start     task3 -> end
#     \     /
#      task2
```

**交叉依赖**
```python
from airflow.models.baseoperator import cross_downstream

cross_downstream([task1, task2], [task3, task4])
# task1 -> task3, task1 -> task4
# task2 -> task3, task2 -> task4
```

### 3.2 Trigger Rules

控制任务在何种条件下执行。

```python
from airflow.utils.trigger_rule import TriggerRule

# ALL_SUCCESS (默认): 所有上游任务成功
task = PythonOperator(
    task_id='default_task',
    python_callable=my_func,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# ALL_FAILED: 所有上游任务失败
cleanup = PythonOperator(
    task_id='cleanup_on_all_failed',
    python_callable=cleanup_func,
    trigger_rule=TriggerRule.ALL_FAILED
)

# ONE_SUCCESS: 至少一个上游任务成功
notify = PythonOperator(
    task_id='notify_on_any_success',
    python_callable=send_notification,
    trigger_rule=TriggerRule.ONE_SUCCESS
)

# ONE_FAILED: 至少一个上游任务失败
alert = PythonOperator(
    task_id='alert_on_any_failure',
    python_callable=send_alert,
    trigger_rule=TriggerRule.ONE_FAILED
)

# ALL_DONE: 所有上游任务完成（无论成功失败）
final = PythonOperator(
    task_id='always_run',
    python_callable=finalize,
    trigger_rule=TriggerRule.ALL_DONE
)

# NONE_FAILED: 没有上游任务失败（成功或跳过）
continue_task = PythonOperator(
    task_id='continue_if_none_failed',
    python_callable=continue_func,
    trigger_rule=TriggerRule.NONE_FAILED
)

# NONE_SKIPPED: 没有上游任务被跳过
validate = PythonOperator(
    task_id='validate_all_ran',
    python_callable=validate_func,
    trigger_rule=TriggerRule.NONE_SKIPPED
)

# DUMMY: 总是执行
always = PythonOperator(
    task_id='always_execute',
    python_callable=always_func,
    trigger_rule=TriggerRule.DUMMY
)
```

**实战示例：错误处理模式**
```python
with DAG('error_handling_dag') as dag:
    try_task1 = PythonOperator(
        task_id='try_task1',
        python_callable=risky_operation1
    )

    try_task2 = PythonOperator(
        task_id='try_task2',
        python_callable=risky_operation2
    )

    # 至少一个成功就继续
    on_any_success = PythonOperator(
        task_id='on_any_success',
        python_callable=handle_success,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # 全部失败才执行
    on_all_failed = PythonOperator(
        task_id='on_all_failed',
        python_callable=handle_all_failed,
        trigger_rule=TriggerRule.ALL_FAILED
    )

    # 无论如何都清理
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup_resources,
        trigger_rule=TriggerRule.ALL_DONE
    )

    [try_task1, try_task2] >> on_any_success
    [try_task1, try_task2] >> on_all_failed
    [on_any_success, on_all_failed] >> cleanup
```

### 3.3 depends_on_past

控制任务是否依赖上一次运行的结果。

```python
# 不依赖历史（默认）
task1 = PythonOperator(
    task_id='independent_task',
    python_callable=my_func,
    depends_on_past=False
)

# 依赖上一次成功
task2 = PythonOperator(
    task_id='sequential_task',
    python_callable=my_func,
    depends_on_past=True  # 必须等待前一次运行成功
)

# wait_for_downstream: 等待下游也完成
task3 = PythonOperator(
    task_id='wait_downstream_task',
    python_callable=my_func,
    depends_on_past=True,
    wait_for_downstream=True  # 等待前一次运行及其下游都成功
)
```

**使用场景**
```python
# 增量ETL: 必须按顺序处理
with DAG(
    'incremental_etl',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
) as dag:

    extract = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental_data,
        depends_on_past=True  # 确保按时间顺序处理
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        depends_on_past=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_to_warehouse,
        depends_on_past=True
    )

    extract >> transform >> load
```

---

## 4. 并发控制

### 4.1 全局并发设置

```ini
# airflow.cfg

[core]
# 整个 Airflow 实例的最大并行任务数
parallelism = 32

# 单个 DAG 的最大并行任务数
max_active_tasks_per_dag = 16

# 单个 DAG 的最大活跃运行数
max_active_runs_per_dag = 16
```

### 4.2 DAG 级别并发

```python
with DAG(
    'concurrent_dag',
    # 该 DAG 最多同时运行的实例数
    max_active_runs=3,

    # 该 DAG 中最多同时运行的任务数
    max_active_tasks=10,

    # DAG 级别的并发（旧配置，已弃用）
    concurrency=10,  # 使用 max_active_tasks 替代
) as dag:
    pass
```

### 4.3 任务级别并发

```python
# 限制特定任务的并发实例数
task = PythonOperator(
    task_id='limited_task',
    python_callable=my_func,
    max_active_tis_per_dag=3  # 最多3个该任务的实例同时运行
)
```

### 4.4 Pool - 资源池

Pool 用于限制对共享资源的访问。

**创建 Pool**
```bash
# CLI 创建
airflow pools set db_pool 5 "Database connection pool"
airflow pools set api_pool 10 "API rate limit pool"

# 查看 Pool
airflow pools list
airflow pools get db_pool

# 删除 Pool
airflow pools delete db_pool
```

**在 UI 中创建**
- Admin -> Pools -> Add
- 设置 Pool 名称、Slots 数量和描述

**使用 Pool**
```python
# 任务使用 Pool
db_query = PythonOperator(
    task_id='query_database',
    python_callable=query_db,
    pool='db_pool',  # 使用 db_pool
    pool_slots=2,     # 占用 2 个 slot
    priority_weight=5 # 优先级
)

api_call = PythonOperator(
    task_id='call_api',
    python_callable=call_api,
    pool='api_pool',
    pool_slots=1,
    priority_weight=10  # 更高优先级
)
```

**实战示例：数据库连接池**
```python
# 创建 Pool 限制数据库连接
# airflow pools set postgres_pool 5 "PostgreSQL connection pool"

with DAG('database_operations') as dag:
    # 10 个并行任务，但最多 5 个同时访问数据库
    tasks = []
    for i in range(10):
        task = PythonOperator(
            task_id=f'db_task_{i}',
            python_callable=query_database,
            pool='postgres_pool',
            pool_slots=1,
            priority_weight=i  # 按顺序优先
        )
        tasks.append(task)

    start >> tasks >> end
```

---

## 5. 重试和超时

### 5.1 重试机制

```python
from datetime import timedelta

task = PythonOperator(
    task_id='retryable_task',
    python_callable=unreliable_operation,

    # 重试次数
    retries=3,

    # 重试间隔
    retry_delay=timedelta(minutes=5),

    # 指数退避
    retry_exponential_backoff=True,

    # 最大重试间隔
    max_retry_delay=timedelta(hours=1),
)
```

**重试行为**
```
首次执行: 失败
├─> 5分钟后重试 (attempt 2): 失败
    ├─> 10分钟后重试 (attempt 3, 指数退避): 失败
        ├─> 20分钟后重试 (attempt 4): 失败
            └─> 标记为 FAILED
```

### 5.2 超时控制

```python
# 执行超时
task = PythonOperator(
    task_id='timed_task',
    python_callable=long_running_operation,
    execution_timeout=timedelta(hours=2)  # 2小时后超时
)

# DAG Run 超时
with DAG(
    'timed_dag',
    dagrun_timeout=timedelta(hours=4),  # 整个 DAG 4小时后超时
) as dag:
    pass
```

### 5.3 SLA (Service Level Agreement)

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA 违规回调"""
    print(f"SLA missed for: {[t.task_id for t in task_list]}")
    # 发送告警...

with DAG(
    'sla_dag',
    default_args={'email': ['alert@example.com']},
    sla_miss_callback=sla_miss_callback,
) as dag:

    # 任务级别 SLA
    task = PythonOperator(
        task_id='time_critical_task',
        python_callable=my_func,
        sla=timedelta(minutes=30)  # 30分钟内完成
    )
```

---

## 6. 动态调度

### 6.1 使用 Variable 控制调度

```python
from airflow.models import Variable

# 根据变量决定是否运行
def should_run(**context):
    enabled = Variable.get("dag_enabled", default_var="true")
    if enabled.lower() != "true":
        raise AirflowSkipException("DAG is disabled")

with DAG('dynamic_dag') as dag:
    check = PythonOperator(
        task_id='check_enabled',
        python_callable=should_run
    )

    process = PythonOperator(
        task_id='process',
        python_callable=process_data
    )

    check >> process

# 启用/禁用 DAG
# airflow variables set dag_enabled true
# airflow variables set dag_enabled false
```

### 6.2 跳过不必要的任务

```python
from airflow.exceptions import AirflowSkipException

def conditional_task(**context):
    execution_date = context['execution_date']

    # 只在月初运行
    if execution_date.day != 1:
        raise AirflowSkipException("Skip: not first day of month")

    # 执行任务逻辑
    process_monthly_report()

monthly_task = PythonOperator(
    task_id='monthly_report',
    python_callable=conditional_task
)
```

### 6.3 短路操作（Short Circuit）

```python
from airflow.operators.python import ShortCircuitOperator

def check_condition(**context):
    """返回 False 会跳过下游所有任务"""
    data_available = check_data_availability(context['ds'])
    return data_available

with DAG('short_circuit_dag') as dag:
    check = ShortCircuitOperator(
        task_id='check_data',
        python_callable=check_condition
    )

    # 如果 check 返回 False，这些任务都会被跳过
    process = PythonOperator(...)
    load = PythonOperator(...)

    check >> process >> load
```

---

## 7. 高级调度模式

### 7.1 外部任务依赖

```python
from airflow.sensors.external_task import ExternalTaskSensor

# 等待另一个 DAG 完成
wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_upstream_dag',
    external_dag_id='upstream_pipeline',
    external_task_id='final_task',  # None 表示等待整个 DAG
    timeout=3600,
    mode='reschedule'
)

# 处理时间差异
from datetime import timedelta

wait_with_offset = ExternalTaskSensor(
    task_id='wait_with_offset',
    external_dag_id='hourly_dag',
    external_task_id='process',
    execution_delta=timedelta(hours=1),  # 等待1小时前的运行
)
```

### 7.2 数据驱动调度

```python
from airflow.sensors.filesystem import FileSensor

# 等待文件出现
wait_for_file = FileSensor(
    task_id='wait_for_data',
    filepath='/data/input/{{ ds }}.csv',
    poke_interval=60,
    timeout=3600,
    mode='reschedule'
)

# 等待数据库记录
from airflow.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id='wait_for_records',
    conn_id='postgres_default',
    sql="""
        SELECT COUNT(*) FROM staging
        WHERE date = '{{ ds }}'
        HAVING COUNT(*) > 0
    """,
    poke_interval=300
)

wait_for_data >> process_task
```

### 7.3 触发其他 DAG

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# 触发另一个 DAG
trigger = TriggerDagRunOperator(
    task_id='trigger_downstream',
    trigger_dag_id='downstream_pipeline',
    execution_date='{{ ds }}',
    wait_for_completion=True,  # 等待完成
    poke_interval=60
)

# 条件触发
def conditionally_trigger(**context):
    if should_trigger(context):
        return 'trigger_dag'
    else:
        return 'skip_trigger'

branch = BranchPythonOperator(
    task_id='decide_trigger',
    python_callable=conditionally_trigger
)

trigger_op = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='conditional_dag'
)

skip = DummyOperator(task_id='skip_trigger')

branch >> [trigger_op, skip]
```

---

## 8. 性能优化

### 8.1 减少调度延迟

```ini
# airflow.cfg

[scheduler]
# 减少扫描间隔
dag_dir_list_interval = 60

# 增加解析进程
parsing_processes = 4

# 增加调度频率
scheduler_heartbeat_sec = 5

# 批量处理
max_dagruns_to_create_per_loop = 20
max_tis_per_query = 512
```

### 8.2 任务设计优化

```python
# ❌ 避免：任务粒度太细
for i in range(100):
    task = PythonOperator(
        task_id=f'micro_task_{i}',
        python_callable=small_operation
    )

# ✅ 推荐：合理的任务粒度
process_batch = PythonOperator(
    task_id='process_batch',
    python_callable=lambda: [small_operation(i) for i in range(100)]
)
```

### 8.3 使用 TaskFlow API (Airflow 2.0+)

简化任务定义和数据传递。

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
)
def taskflow_example():

    @task
    def extract():
        return {"data": [1, 2, 3, 4, 5]}

    @task
    def transform(data: dict):
        return {"result": [x * 2 for x in data["data"]]}

    @task
    def load(data: dict):
        print(f"Loading: {data['result']}")

    # 自动处理 XCom
    data = extract()
    transformed = transform(data)
    load(transformed)

dag = taskflow_example()
```

---

## 总结

Airflow 的调度和执行机制提供了：

1. **灵活的调度**: Cron、Timedelta、自定义 Timetable
2. **强大的依赖管理**: 任务依赖、Trigger Rules、外部依赖
3. **并发控制**: 全局、DAG、任务级别、Pool
4. **可靠性**: 重试、超时、SLA
5. **动态性**: 条件执行、分支、触发
6. **性能**: 批量处理、并行执行

理解这些机制是构建高效、可靠数据管道的关键。
