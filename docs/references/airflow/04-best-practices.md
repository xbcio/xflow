# Airflow 最佳实践

## 1. DAG 设计原则

### 1.1 保持 DAG 简洁

**❌ 不推荐：过于复杂的 DAG**
```python
# 一个 DAG 包含100+个任务
with DAG('complex_dag') as dag:
    tasks = []
    for i in range(100):
        task = PythonOperator(...)
        tasks.append(task)
    # 复杂的依赖关系
```

**✅ 推荐：拆分成多个 DAG**
```python
# dag_1.py - 数据提取
with DAG('extract_data') as dag:
    extract_task = PythonOperator(...)

# dag_2.py - 数据转换
with DAG('transform_data') as dag:
    # 使用 ExternalTaskSensor 等待上游
    wait_for_extract = ExternalTaskSensor(
        external_dag_id='extract_data',
        external_task_id='extract_task'
    )
    transform_task = PythonOperator(...)
```

### 1.2 幂等性设计

任务应该可以安全地重复执行而不产生副作用。

**❌ 不推荐：非幂等操作**
```python
def insert_data(**context):
    # 每次执行都会插入重复数据
    db.execute("INSERT INTO users (id, name) VALUES (1, 'John')")
```

**✅ 推荐：幂等操作**
```python
def upsert_data(**context):
    # 使用 UPSERT 或先删除再插入
    db.execute("""
        INSERT INTO users (id, name) VALUES (1, 'John')
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
    """)

    # 或者基于执行日期分区
    execution_date = context['ds']
    db.execute(f"""
        DELETE FROM users WHERE partition_date = '{execution_date}';
        INSERT INTO users SELECT * FROM staging WHERE date = '{execution_date}';
    """)
```

### 1.3 使用增量处理

避免每次全量处理，使用增量更新。

**❌ 不推荐：全量处理**
```python
def process_all_data(**context):
    # 每次处理所有历史数据
    df = db.read_sql("SELECT * FROM huge_table")
    # 处理...
```

**✅ 推荐：增量处理**
```python
def process_incremental(**context):
    # 只处理最近的数据
    execution_date = context['ds']
    df = db.read_sql(f"""
        SELECT * FROM huge_table
        WHERE created_at >= '{execution_date}'
        AND created_at < '{execution_date}'::date + interval '1 day'
    """)
    # 处理...
```

### 1.4 合理设置 catchup

**理解 catchup 行为**
```python
# catchup=True: 回填所有历史运行
with DAG(
    'catchup_true',
    start_date=datetime(2024, 1, 1),  # 如果今天是2024-1-10
    schedule_interval='@daily',
    catchup=True  # 会执行1月1日到1月9日的所有运行
) as dag:
    task = BashOperator(...)

# catchup=False: 只执行当前运行
with DAG(
    'catchup_false',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False  # 只执行当前的一次
) as dag:
    task = BashOperator(...)
```

**何时使用 catchup=True**
- 需要补齐历史数据
- 数据有明确的日期分区
- 任务是幂等的

**何时使用 catchup=False**
- 只关心最新数据
- 处理实时数据流
- 任务不是完全幂等的

---

## 2. 任务设计原则

### 2.1 任务粒度

**❌ 不推荐：粒度太细**
```python
# 每个小操作都是一个任务
create_temp_dir = BashOperator(task_id='create_dir', ...)
download_file_1 = BashOperator(task_id='download_1', ...)
download_file_2 = BashOperator(task_id='download_2', ...)
unzip_file_1 = BashOperator(task_id='unzip_1', ...)
unzip_file_2 = BashOperator(task_id='unzip_2', ...)
# ... 太多小任务
```

**✅ 推荐：合理的粒度**
```python
# 将相关操作组合成一个任务
prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=lambda: prepare_all_data()  # 内部包含多个步骤
)

def prepare_all_data():
    create_temp_directory()
    download_files()
    extract_files()
    # 相关步骤组合在一起
```

### 2.2 避免在 DAG 文件中进行重计算

**❌ 不推荐：在全局作用域执行复杂操作**
```python
# DAG 文件会被 Scheduler 频繁解析
# 这会在每次解析时都执行
import requests

# ❌ 每次解析 DAG 都会调用 API
api_result = requests.get('https://api.example.com/config').json()

with DAG('example') as dag:
    # 使用 api_result
```

**✅ 推荐：在任务执行时获取数据**
```python
# ✅ 只在任务执行时调用 API
def get_config_and_process(**context):
    import requests
    api_result = requests.get('https://api.example.com/config').json()
    # 处理数据

with DAG('example') as dag:
    task = PythonOperator(
        task_id='process',
        python_callable=get_config_and_process
    )
```

### 2.3 使用模板和宏

利用 Jinja 模板引用执行上下文。

```python
# 使用内置模板变量
task = BashOperator(
    task_id='templated_task',
    bash_command="""
        echo "Execution date: {{ ds }}"
        echo "Previous execution: {{ prev_ds }}"
        echo "DAG ID: {{ dag.dag_id }}"
        echo "Task ID: {{ task.task_id }}"
    """
)

# 使用宏
task = BashOperator(
    task_id='macro_example',
    bash_command="""
        echo "Date: {{ macros.ds_add(ds, 7) }}"  # 7天后
        echo "Hour: {{ execution_date.hour }}"
    """
)

# 模板文件
sql_task = PostgresOperator(
    task_id='sql_task',
    postgres_conn_id='postgres_default',
    sql='queries/daily_report.sql',  # 文件内容会被模板化
    params={'threshold': 100}  # 可在 SQL 中使用 {{ params.threshold }}
)
```

**常用模板变量**
```python
{{ ds }}                 # 2024-01-01 (YYYY-MM-DD)
{{ ds_nodash }}          # 20240101
{{ ts }}                 # 2024-01-01T00:00:00+00:00 (ISO format)
{{ execution_date }}     # datetime 对象
{{ prev_ds }}            # 上一次执行日期
{{ next_ds }}            # 下一次执行日期
{{ dag }}                # DAG 对象
{{ task }}               # Task 对象
{{ params }}             # 自定义参数
{{ var.value.my_var }}   # Airflow 变量
{{ var.json.my_json_var }}  # JSON 变量
```

---

## 3. 数据传递最佳实践

### 3.1 小数据使用 XCom

```python
def push_small_data(**context):
    # ✅ 适合小量数据 (< 1MB)
    return {'count': 100, 'status': 'success'}

def pull_small_data(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='push_task')
    print(f"Count: {data['count']}")
```

### 3.2 大数据使用外部存储

```python
def process_large_data(**context):
    import pandas as pd

    # 处理大量数据
    df = pd.read_sql("SELECT * FROM large_table", conn)

    # ❌ 不要通过 XCom 传递
    # return df  # 会导致数据库过大

    # ✅ 保存到外部存储
    output_path = f's3://bucket/data/{context["ds"]}/output.parquet'
    df.to_parquet(output_path)

    # 只返回路径
    return output_path

def use_large_data(**context):
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='process_task')

    # 从外部存储读取
    df = pd.read_parquet(file_path)
```

### 3.3 使用自定义 XCom Backend

对于云环境，可以配置 XCom 自动使用对象存储。

```python
# airflow.cfg
[core]
xcom_backend = airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend

# 或自定义
# my_xcom_backend.py
from airflow.models.xcom import BaseXCom
import boto3

class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3://"

    @staticmethod
    def serialize_value(value):
        if isinstance(value, pd.DataFrame):
            # 大对象保存到 S3
            key = f"xcom/{uuid.uuid4()}.parquet"
            s3 = boto3.client('s3')
            s3.put_object(...)
            return S3XComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result):
        if isinstance(result.value, str) and result.value.startswith(S3XComBackend.PREFIX):
            # 从 S3 读取
            key = result.value.replace(S3XComBackend.PREFIX, "")
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket='xcom-bucket', Key=key)
            return pd.read_parquet(obj['Body'])
        return BaseXCom.deserialize_value(result)
```

---

## 4. 性能优化

### 4.1 合理使用连接池

```python
# 不要在每个任务中创建新连接
def bad_practice(**context):
    # ❌ 每次都创建新连接
    conn = psycopg2.connect(...)
    # ...
    conn.close()

# ✅ 使用 Hook，自动管理连接池
def good_practice(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # Hook 内部使用连接池
    records = pg_hook.get_records("SELECT * FROM users")
```

### 4.2 批量操作

```python
# ❌ 逐行插入
def insert_one_by_one(**context):
    for row in data:
        db.execute(f"INSERT INTO users VALUES ({row})")

# ✅ 批量插入
def batch_insert(**context):
    # 方式1: 使用 executemany
    cursor.executemany(
        "INSERT INTO users VALUES (%s, %s)",
        data
    )

    # 方式2: 使用 DataFrame to_sql
    df.to_sql('users', engine, if_exists='append', method='multi', chunksize=1000)

    # 方式3: 使用 COPY (PostgreSQL)
    from io import StringIO
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor.copy_from(buffer, 'users', sep=',')
```

### 4.3 并行执行

```python
# 利用任务并行
with DAG('parallel_dag') as dag:
    start = DummyOperator(task_id='start')

    # 这些任务会并行执行
    tasks = []
    for i in range(10):
        task = PythonOperator(
            task_id=f'parallel_task_{i}',
            python_callable=process_partition,
            op_kwargs={'partition': i}
        )
        tasks.append(task)

    end = DummyOperator(task_id='end')

    start >> tasks >> end

# 配置足够的并发度
# airflow.cfg
# [core]
# parallelism = 32
# max_active_tasks_per_dag = 16
```

### 4.4 使用 Pool 控制资源

```python
# 创建 Pool（在 UI 或 CLI 中）
# airflow pools set db_pool 5 "Database connection pool"

# 在任务中使用
task1 = PythonOperator(
    task_id='db_task_1',
    python_callable=query_database,
    pool='db_pool',  # 限制并发访问数据库
    priority_weight=10  # 优先级
)

task2 = PythonOperator(
    task_id='db_task_2',
    python_callable=query_database,
    pool='db_pool',
    priority_weight=5
)

# 即使 task1 和 task2 都就绪，也最多只有5个使用此 pool 的任务同时运行
```

---

## 5. 错误处理和监控

### 5.1 完善的日志记录

```python
import logging

def my_task(**context):
    # 使用 Python logging
    logging.info("Task started")
    logging.debug(f"Processing date: {context['ds']}")

    try:
        result = perform_operation()
        logging.info(f"Operation completed: {result}")
    except Exception as e:
        logging.error(f"Operation failed: {str(e)}", exc_info=True)
        raise

    return result
```

### 5.2 告警配置

```python
def send_failure_alert(context):
    """发送失败告警"""
    from airflow.providers.slack.hooks.slack import SlackHook

    task_instance = context['task_instance']
    exception = context.get('exception')
    log_url = task_instance.log_url

    message = f"""
❌ *Task Failed*
- DAG: {task_instance.dag_id}
- Task: {task_instance.task_id}
- Execution Date: {context['execution_date']}
- Exception: {exception}
- Logs: {log_url}
    """

    slack = SlackHook(slack_conn_id='slack_default')
    slack.call(
        api_method='chat.postMessage',
        json={'channel': '#airflow-alerts', 'text': message}
    )

# 在 DAG 中使用
default_args = {
    'on_failure_callback': send_failure_alert,
    'email_on_failure': True,
    'email': ['team@example.com'],
}

with DAG('monitored_dag', default_args=default_args) as dag:
    task = PythonOperator(...)
```

### 5.3 SLA 监控

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA 违规回调"""
    print(f"SLA missed for tasks: {[task.task_id for task in task_list]}")
    # 发送告警

with DAG(
    'sla_dag',
    default_args={'owner': 'data_team'},
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    sla_miss_callback=sla_miss_callback,
) as dag:

    # 任务应该在30分钟内完成
    task = PythonOperator(
        task_id='time_sensitive_task',
        python_callable=my_function,
        sla=timedelta(minutes=30)
    )
```

### 5.4 数据质量检查

```python
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException

def validate_data_quality(**context):
    """数据质量检查"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # 检查1: 记录数
    count = pg_hook.get_first(f"""
        SELECT COUNT(*) FROM daily_data
        WHERE date = '{context['ds']}'
    """)[0]

    if count == 0:
        raise AirflowFailException("No data found!")

    if count < 1000:
        logging.warning(f"Low record count: {count}")

    # 检查2: 数据完整性
    null_count = pg_hook.get_first(f"""
        SELECT COUNT(*) FROM daily_data
        WHERE date = '{context['ds']}' AND critical_field IS NULL
    """)[0]

    if null_count > 0:
        raise AirflowFailException(f"Found {null_count} records with null critical_field")

    # 检查3: 数据范围
    stats = pg_hook.get_first(f"""
        SELECT MIN(amount), MAX(amount), AVG(amount)
        FROM daily_data
        WHERE date = '{context['ds']}'
    """)

    if stats[0] < 0:
        raise AirflowFailException("Found negative amounts")

    if stats[1] > 1000000:
        logging.warning(f"Unusually high amount: {stats[1]}")

    return "quality_passed"

with DAG('data_quality_dag') as dag:
    load_data = PythonOperator(...)

    validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data_quality
    )

    on_success = PythonOperator(...)
    on_failure = PythonOperator(...)

    load_data >> validate >> on_success
```

---

## 6. 安全最佳实践

### 6.1 使用 Connections 存储凭证

```python
# ❌ 不要硬编码凭证
def bad_practice():
    conn = psycopg2.connect(
        host="db.example.com",
        user="admin",
        password="secret123"  # ❌ 不安全
    )

# ✅ 使用 Airflow Connections
def good_practice():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_prod')
    conn = pg_hook.get_conn()
```

### 6.2 使用 Variables 管理配置

```python
# ❌ 硬编码配置
S3_BUCKET = "my-bucket"  # ❌ 不灵活

# ✅ 使用 Variables
from airflow.models import Variable

S3_BUCKET = Variable.get("s3_bucket")
CONFIG = Variable.get("app_config", deserialize_json=True)

# 设置变量（CLI）
# airflow variables set s3_bucket my-bucket
# airflow variables set app_config '{"key": "value"}' --json
```

### 6.3 使用 Secrets Backend

```python
# 配置使用 AWS Secrets Manager
# airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}

# 或使用 HashiCorp Vault
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "airflow/connections", "variables_path": "airflow/variables", "url": "http://vault:8200"}
```

### 6.4 限制 DAG 文件权限

```bash
# DAG 文件应该只有必要的权限
chmod 640 dags/*.py
chown airflow:airflow dags/*.py

# 不要在 DAG 文件中执行用户输入
# ❌ 危险
user_input = "some input"
eval(user_input)  # ❌ 代码注入风险
```

---

## 7. 测试最佳实践

### 7.1 单元测试 DAG

```python
# test_dags.py
import pytest
from airflow.models import DagBag

def test_dag_loaded():
    """测试 DAG 能否正确加载"""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

def test_dag_structure():
    """测试 DAG 结构"""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.get_dag('my_dag')

    # 检查任务数量
    assert len(dag.tasks) == 5

    # 检查任务依赖
    extract_task = dag.get_task('extract')
    transform_task = dag.get_task('transform')
    assert transform_task in extract_task.downstream_list

def test_task_count():
    """测试各 DAG 的任务数"""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"
        assert len(dag.tasks) < 50, f"DAG {dag_id} has too many tasks"
```

### 7.2 测试任务逻辑

```python
# test_tasks.py
from dags.my_dag import extract_data, transform_data

def test_extract_data():
    """测试数据提取"""
    context = {'ds': '2024-01-01', 'ti': MockTaskInstance()}
    result = extract_data(**context)

    assert result is not None
    assert os.path.exists(result)

def test_transform_data():
    """测试数据转换"""
    # 准备测试数据
    test_file = '/tmp/test_input.csv'
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    df.to_csv(test_file, index=False)

    # 模拟上下文
    context = {
        'ds': '2024-01-01',
        'ti': MockTaskInstance(xcom_data={'extract': test_file})
    }

    result = transform_data(**context)

    # 验证结果
    output_df = pd.read_csv(result)
    assert len(output_df) == 3
    assert 'transformed_col' in output_df.columns

class MockTaskInstance:
    def __init__(self, xcom_data=None):
        self.xcom_data = xcom_data or {}

    def xcom_pull(self, task_ids):
        return self.xcom_data.get(task_ids)
```

---

## 8. 文档和维护

### 8.1 DAG 文档化

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

# 使用 doc_md 添加详细文档
dag = DAG(
    'documented_dag',
    description='Short description',
    doc_md="""
    # DAG Documentation

    ## Purpose
    This DAG processes daily user transactions and generates reports.

    ## Schedule
    Runs daily at 2 AM UTC

    ## Dependencies
    - Upstream: `extract_users_dag`
    - Database: `postgres_prod`
    - S3 Bucket: `data-lake-prod`

    ## Contacts
    - Owner: Data Team
    - Slack: #data-engineering
    """,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
)

# 任务级别文档
task1 = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    doc_md="""
    ## Extract Data

    Extracts user transaction data from PostgreSQL database.

    **Query**: Selects all transactions from the last 24 hours
    **Output**: CSV file in /tmp
    """,
    dag=dag
)
```

### 8.2 版本控制

```python
# 在 DAG 中记录版本
DAG_VERSION = "1.2.0"

with DAG(
    'versioned_dag',
    default_args={'owner': 'data_team'},
    tags=['v1.2.0'],  # 使用 tag 标记版本
    params={'version': DAG_VERSION}  # 在参数中记录
) as dag:
    task = PythonOperator(...)
```

```bash
# 使用 Git 管理 DAG 文件
git log --oneline dags/my_dag.py  # 查看历史
git blame dags/my_dag.py          # 查看修改人
```

---

## 9. 代码组织

### 9.1 目录结构

```
airflow-project/
├── dags/
│   ├── __init__.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── user_pipeline.py
│   │   └── order_pipeline.py
│   └── ml/
│       ├── __init__.py
│       └── model_training.py
├── plugins/
│   ├── __init__.py
│   ├── operators/
│   │   ├── __init__.py
│   │   └── custom_operator.py
│   └── hooks/
│       ├── __init__.py
│       └── custom_hook.py
├── include/
│   ├── sql/
│   │   └── queries.sql
│   └── scripts/
│       └── process.py
├── tests/
│   ├── dags/
│   └── plugins/
└── requirements.txt
```

### 9.2 复用代码

```python
# common/operators.py
from airflow.operators.python import PythonOperator

def create_data_quality_check(task_id, table_name, date_column='date'):
    """工厂函数创建数据质量检查任务"""
    def check(**context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        count = pg_hook.get_first(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE {date_column} = '{context['ds']}'
        """)[0]

        if count == 0:
            raise ValueError(f"No data in {table_name}")

    return PythonOperator(
        task_id=task_id,
        python_callable=check
    )

# 在 DAG 中使用
with DAG('my_dag') as dag:
    check_users = create_data_quality_check('check_users', 'users')
    check_orders = create_data_quality_check('check_orders', 'orders')
```

---

## 10. 总结清单

**DAG 设计**
- [ ] DAG 保持简洁（< 30个任务）
- [ ] 任务是幂等的
- [ ] 使用增量处理
- [ ] 合理设置 catchup
- [ ] 添加文档和描述

**性能**
- [ ] 避免在 DAG 文件中执行重计算
- [ ] 使用连接池
- [ ] 批量操作而非逐行处理
- [ ] 合理配置并行度
- [ ] 使用 Pool 控制资源

**数据传递**
- [ ] 小数据用 XCom
- [ ] 大数据用外部存储
- [ ] 只传递路径而非数据本身

**错误处理**
- [ ] 配置重试策略
- [ ] 添加失败告警
- [ ] 实施数据质量检查
- [ ] 设置合理的超时

**安全**
- [ ] 使用 Connections 管理凭证
- [ ] 使用 Variables 管理配置
- [ ] 不要硬编码密码
- [ ] 使用 Secrets Backend

**测试**
- [ ] 单元测试 DAG 加载
- [ ] 测试任务逻辑
- [ ] 验证数据质量
- [ ] 集成测试

**维护**
- [ ] 添加完整文档
- [ ] 使用版本控制
- [ ] 代码复用
- [ ] 定期清理历史数据

遵循这些最佳实践，可以构建健壮、可维护、高性能的 Airflow 数据管道。
