# Airflow 实战示例

## 1. ETL 数据管道

完整的 ETL 示例，从数据源提取、转换到加载。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**context):
    """从 API 提取数据"""
    execution_date = context['ds']

    # 调用 API
    response = requests.get(
        f'https://api.example.com/data',
        params={'date': execution_date}
    )

    data = response.json()

    # 保存到临时存储
    df = pd.DataFrame(data)
    temp_file = f'/tmp/raw_data_{execution_date}.csv'
    df.to_csv(temp_file, index=False)

    # 返回文件路径供下游使用
    return temp_file

def transform_data(**context):
    """数据转换和清洗"""
    ti = context['ti']
    # 获取上游任务的输出
    temp_file = ti.xcom_pull(task_ids='extract')

    # 读取数据
    df = pd.read_csv(temp_file)

    # 数据清洗
    df = df.dropna()  # 删除空值
    df = df.drop_duplicates()  # 删除重复

    # 数据转换
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['amount'] = df['amount'].astype(float)
    df['processed_date'] = context['ds']

    # 计算衍生字段
    df['amount_usd'] = df['amount'] * df['exchange_rate']
    df['category'] = df['type'].map({
        'A': 'Category 1',
        'B': 'Category 2',
        'C': 'Category 3'
    })

    # 保存转换后的数据
    output_file = f'/tmp/transformed_data_{context["ds"]}.csv'
    df.to_csv(output_file, index=False)

    return output_file

def load_data(**context):
    """加载数据到数据库"""
    ti = context['ti']
    transformed_file = ti.xcom_pull(task_ids='transform')

    # 读取转换后的数据
    df = pd.read_csv(transformed_file)

    # 连接数据库
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()

    # 批量插入数据
    df.to_sql(
        'fact_transactions',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

    # 记录加载的行数
    row_count = len(df)
    print(f"Loaded {row_count} rows into database")

    return row_count

def validate_data(**context):
    """数据质量验证"""
    ti = context['ti']
    row_count = ti.xcom_pull(task_ids='load')
    execution_date = context['ds']

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # 验证记录数
    result = pg_hook.get_first(f"""
        SELECT COUNT(*) FROM fact_transactions
        WHERE processed_date = '{execution_date}'
    """)

    db_count = result[0]

    # 数据一致性检查
    if db_count != row_count:
        raise ValueError(f"Data count mismatch! Loaded: {row_count}, In DB: {db_count}")

    # 验证数据质量
    quality_check = pg_hook.get_first(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT id) as unique_ids,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative_amounts
        FROM fact_transactions
        WHERE processed_date = '{execution_date}'
    """)

    if quality_check[2] > 0:
        raise ValueError(f"Found {quality_check[2]} records with negative amounts")

    print(f"Validation passed: {quality_check[0]} total records, {quality_check[1]} unique IDs")

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for transaction data',
    schedule_interval='0 2 * * *',  # 每天凌晨2点
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'production'],
) as dag:

    # 创建目标表（如果不存在）
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS fact_transactions (
                id VARCHAR(50) PRIMARY KEY,
                created_at TIMESTAMP,
                amount DECIMAL(15,2),
                amount_usd DECIMAL(15,2),
                type VARCHAR(10),
                category VARCHAR(50),
                exchange_rate DECIMAL(10,4),
                processed_date DATE
            );

            CREATE INDEX IF NOT EXISTS idx_processed_date
            ON fact_transactions(processed_date);
        """
    )

    # ETL 步骤
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data
    )

    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data
    )

    # 发送成功通知
    from airflow.operators.email import EmailOperator

    notify_success = EmailOperator(
        task_id='notify_success',
        to='team@example.com',
        subject='ETL Pipeline Success - {{ ds }}',
        html_content="""
            <h3>ETL Pipeline Completed Successfully</h3>
            <p><strong>Execution Date:</strong> {{ ds }}</p>
            <p><strong>Records Loaded:</strong> {{ ti.xcom_pull(task_ids='load') }}</p>
        """
    )

    # 定义任务依赖
    create_table >> extract >> transform >> load >> validate >> notify_success
```

---

## 2. 机器学习管道

完整的 ML 工作流：数据准备、特征工程、模型训练、评估和部署。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score

default_args = {
    'owner': 'ml_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def prepare_dataset(**context):
    """准备训练数据集"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # 从数据库提取数据
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    sql = """
        SELECT
            user_id, age, income, credit_score,
            account_age_days, transaction_count,
            label
        FROM ml_training_data
        WHERE created_at >= NOW() - INTERVAL '30 days'
    """

    df = pg_hook.get_pandas_df(sql)

    # 保存到临时文件
    file_path = '/tmp/training_data.csv'
    df.to_csv(file_path, index=False)

    print(f"Dataset prepared: {len(df)} records")
    return file_path

def feature_engineering(**context):
    """特征工程"""
    ti = context['ti']
    data_file = ti.xcom_pull(task_ids='prepare_dataset')

    df = pd.read_csv(data_file)

    # 创建新特征
    df['income_to_age_ratio'] = df['income'] / (df['age'] + 1)
    df['credit_utilization'] = df['credit_score'] / 850  # 标准化
    df['avg_transaction_value'] = df['income'] / (df['transaction_count'] + 1)

    # 处理类别变量
    df = pd.get_dummies(df, columns=['account_type'])

    # 处理缺失值
    df = df.fillna(df.mean())

    # 保存处理后的数据
    output_file = '/tmp/features.csv'
    df.to_csv(output_file, index=False)

    return output_file

def train_model(**context):
    """训练模型"""
    ti = context['ti']
    features_file = ti.xcom_pull(task_ids='feature_engineering')

    # 加载数据
    df = pd.read_csv(features_file)

    # 分割特征和标签
    X = df.drop('label', axis=1)
    y = df['label']

    # 训练集/测试集分割
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 训练模型
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    # 保存模型
    model_path = '/tmp/model.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)

    # 保存测试数据用于评估
    test_data_path = '/tmp/test_data.pkl'
    with open(test_data_path, 'wb') as f:
        pickle.dump((X_test, y_test), f)

    return {'model_path': model_path, 'test_data_path': test_data_path}

def evaluate_model(**context):
    """评估模型"""
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='train_model')

    # 加载模型
    with open(paths['model_path'], 'rb') as f:
        model = pickle.load(f)

    # 加载测试数据
    with open(paths['test_data_path'], 'rb') as f:
        X_test, y_test = pickle.load(f)

    # 预测
    y_pred = model.predict(X_test)

    # 计算指标
    metrics = {
        'accuracy': accuracy_score(y_test, y_pred),
        'precision': precision_score(y_test, y_pred),
        'recall': recall_score(y_test, y_pred),
    }

    print(f"Model Metrics: {metrics}")

    # 检查模型质量
    if metrics['accuracy'] < 0.75:
        raise ValueError(f"Model accuracy too low: {metrics['accuracy']}")

    return metrics

def deploy_model(**context):
    """部署模型到 S3"""
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='train_model')
    metrics = ti.xcom_pull(task_ids='evaluate_model')

    execution_date = context['ds']

    # 上传到 S3
    s3_hook = S3Hook(aws_conn_id='aws_default')

    model_key = f'models/fraud_detection/model_{execution_date}.pkl'
    s3_hook.load_file(
        filename=paths['model_path'],
        key=model_key,
        bucket_name='ml-models',
        replace=True
    )

    # 保存模型元数据
    import json
    metadata = {
        'model_version': execution_date,
        'metrics': metrics,
        's3_path': f's3://ml-models/{model_key}',
        'created_at': str(datetime.now())
    }

    metadata_file = '/tmp/model_metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)

    metadata_key = f'models/fraud_detection/metadata_{execution_date}.json'
    s3_hook.load_file(
        filename=metadata_file,
        key=metadata_key,
        bucket_name='ml-models',
        replace=True
    )

    print(f"Model deployed to s3://ml-models/{model_key}")
    return model_key

with DAG(
    'ml_training_pipeline',
    default_args=default_args,
    description='ML model training and deployment pipeline',
    schedule_interval='@weekly',  # 每周训练一次
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training'],
) as dag:

    prepare = PythonOperator(
        task_id='prepare_dataset',
        python_callable=prepare_dataset
    )

    engineer_features = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        execution_timeout=timedelta(hours=2)
    )

    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model
    )

    deploy = PythonOperator(
        task_id='deploy_model',
        python_callable=deploy_model
    )

    prepare >> engineer_features >> train >> evaluate >> deploy
```

---

## 3. 多数据源整合管道

从多个数据源（API、数据库、文件）整合数据。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def extract_from_api(**context):
    """从 REST API 提取数据"""
    import requests

    response = requests.get('https://api.example.com/users')
    data = response.json()

    df = pd.DataFrame(data)
    file_path = '/tmp/api_data.csv'
    df.to_csv(file_path, index=False)

    return file_path

def extract_from_postgres(**context):
    """从 PostgreSQL 提取数据"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    df = pg_hook.get_pandas_df("""
        SELECT user_id, name, email, created_at
        FROM users
        WHERE updated_at >= NOW() - INTERVAL '1 day'
    """)

    file_path = '/tmp/postgres_data.csv'
    df.to_csv(file_path, index=False)

    return file_path

def extract_from_s3(**context):
    """从 S3 提取数据"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # 下载文件
    local_path = '/tmp/s3_data.csv'
    s3_hook.download_file(
        key='data/user_events.csv',
        bucket_name='data-lake',
        local_path=local_path
    )

    return local_path

def merge_data(**context):
    """合并所有数据源"""
    ti = context['ti']

    # 获取所有提取任务的输出
    api_file = ti.xcom_pull(task_ids='extract_group.extract_api')
    pg_file = ti.xcom_pull(task_ids='extract_group.extract_postgres')
    s3_file = ti.xcom_pull(task_ids='extract_group.extract_s3')

    # 读取数据
    df_api = pd.read_csv(api_file)
    df_pg = pd.read_csv(pg_file)
    df_s3 = pd.read_csv(s3_file)

    # 合并数据
    # 假设所有数据源都有 user_id
    merged = df_api.merge(df_pg, on='user_id', how='outer')
    merged = merged.merge(df_s3, on='user_id', how='outer')

    # 数据清洗
    merged = merged.drop_duplicates(subset=['user_id'])
    merged = merged.fillna('')

    # 保存合并后的数据
    output_file = f'/tmp/merged_data_{context["ds"]}.csv'
    merged.to_csv(output_file, index=False)

    return output_file

def load_to_warehouse(**context):
    """加载到数据仓库"""
    ti = context['ti']
    merged_file = ti.xcom_pull(task_ids='merge_data')

    # 读取数据
    df = pd.read_csv(merged_file)

    # 加载到数据仓库（例如 Redshift）
    from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    engine = redshift_hook.get_sqlalchemy_engine()

    # 批量插入
    df.to_sql(
        'dim_users',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    print(f"Loaded {len(df)} records to data warehouse")

with DAG(
    'multi_source_integration',
    default_args=default_args,
    description='Integrate data from multiple sources',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['integration', 'etl'],
) as dag:

    # 使用 TaskGroup 组织提取任务
    with TaskGroup('extract_group', tooltip="Extract from all sources") as extract_group:

        extract_api = PythonOperator(
            task_id='extract_api',
            python_callable=extract_from_api
        )

        extract_postgres = PythonOperator(
            task_id='extract_postgres',
            python_callable=extract_from_postgres
        )

        extract_s3 = PythonOperator(
            task_id='extract_s3',
            python_callable=extract_from_s3
        )

    merge = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data
    )

    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse
    )

    extract_group >> merge >> load
```

---

## 4. 动态 DAG 生成

根据配置动态创建任务。

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 配置：需要处理的数据源列表
DATA_SOURCES = [
    {'name': 'users', 'table': 'users', 'schedule': '@daily'},
    {'name': 'orders', 'table': 'orders', 'schedule': '@hourly'},
    {'name': 'products', 'table': 'products', 'schedule': '@daily'},
]

def create_etl_dag(source_config):
    """为每个数据源创建一个 DAG"""

    dag_id = f"etl_{source_config['name']}"

    default_args = {
        'owner': 'data_team',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f"ETL pipeline for {source_config['name']}",
        schedule_interval=source_config['schedule'],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['auto-generated', 'etl', source_config['name']],
    )

    with dag:
        extract = BashOperator(
            task_id='extract',
            bash_command=f"python /scripts/extract.py --table {source_config['table']}"
        )

        transform = BashOperator(
            task_id='transform',
            bash_command=f"python /scripts/transform.py --table {source_config['table']}"
        )

        load = BashOperator(
            task_id='load',
            bash_command=f"python /scripts/load.py --table {source_config['table']}"
        )

        extract >> transform >> load

    return dag

# 为每个数据源创建 DAG
for source in DATA_SOURCES:
    dag_id = f"etl_{source['name']}"
    globals()[dag_id] = create_etl_dag(source)
```

---

## 5. 分支和条件执行

根据条件选择不同的执行路径。

```python
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def check_data_quality(**context):
    """检查数据质量并决定分支"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # 获取今天的记录数
    result = pg_hook.get_first(f"""
        SELECT COUNT(*) FROM raw_data
        WHERE date = '{context['ds']}'
    """)

    record_count = result[0]

    print(f"Found {record_count} records for {context['ds']}")

    # 根据记录数决定分支
    if record_count == 0:
        return 'handle_no_data'
    elif record_count < 1000:
        return 'process_small_batch'
    else:
        return 'process_large_batch'

def handle_no_data(**context):
    """处理无数据情况"""
    print(f"No data found for {context['ds']}")
    # 发送告警
    from airflow.providers.slack.hooks.slack import SlackHook
    slack_hook = SlackHook(slack_conn_id='slack_default')
    slack_hook.call(
        api_method='chat.postMessage',
        json={
            'channel': '#data-alerts',
            'text': f"⚠️ No data found for {context['ds']}"
        }
    )

def process_data(batch_type, **context):
    """处理数据"""
    print(f"Processing {batch_type} batch for {context['ds']}")
    # 实际处理逻辑...

with DAG(
    'conditional_pipeline',
    description='Pipeline with conditional branching',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['conditional'],
) as dag:

    check_quality = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )

    no_data_task = PythonOperator(
        task_id='handle_no_data',
        python_callable=handle_no_data
    )

    small_batch_task = PythonOperator(
        task_id='process_small_batch',
        python_callable=lambda **ctx: process_data('small', **ctx)
    )

    large_batch_task = PythonOperator(
        task_id='process_large_batch',
        python_callable=lambda **ctx: process_data('large', **ctx)
    )

    # 汇聚任务：无论走哪个分支都要执行
    final_task = BashOperator(
        task_id='finalize',
        bash_command='echo "Pipeline completed"',
        trigger_rule=TriggerRule.ONE_SUCCESS  # 任一上游成功即执行
    )

    # 定义依赖
    check_quality >> [no_data_task, small_batch_task, large_batch_task]
    [no_data_task, small_batch_task, large_batch_task] >> final_task
```

---

## 6. 使用 Sensor 等待条件

等待外部条件满足后再执行。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

with DAG(
    'sensor_example',
    description='Example of using sensors',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sensor'],
) as dag:

    # 等待文件出现
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/data/input/data_{{ ds }}.csv',
        poke_interval=60,  # 每60秒检查一次
        timeout=3600,      # 1小时超时
        mode='reschedule'  # 释放 worker slot
    )

    # 等待另一个 DAG 完成
    wait_for_upstream_dag = ExternalTaskSensor(
        task_id='wait_for_upstream',
        external_dag_id='upstream_pipeline',
        external_task_id='final_task',
        timeout=7200,
        mode='reschedule'
    )

    # 等待 API 就绪
    wait_for_api = HttpSensor(
        task_id='wait_for_api',
        http_conn_id='api_default',
        endpoint='health',
        request_params={},
        response_check=lambda response: response.json()['status'] == 'healthy',
        poke_interval=30,
        timeout=600
    )

    # 处理数据
    def process(**context):
        print("All conditions met, processing data...")

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process
    )

    # 所有条件满足后才处理
    [wait_for_file, wait_for_upstream_dag, wait_for_api] >> process_task
```

---

## 7. 错误处理和重试

完善的错误处理机制。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import random

def task_with_retry(**context):
    """可能失败但会重试的任务"""
    # 模拟随机失败
    if random.random() < 0.5:
        raise Exception("Random failure occurred")

    print("Task succeeded!")
    return "success"

def on_failure_callback(context):
    """失败回调函数"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    print(f"Task {task_instance.task_id} failed")
    print(f"Execution date: {execution_date}")
    print(f"Exception: {context.get('exception')}")

    # 发送告警
    from airflow.providers.slack.hooks.slack import SlackHook
    slack = SlackHook(slack_conn_id='slack_default')
    slack.call(
        api_method='chat.postMessage',
        json={
            'channel': '#airflow-alerts',
            'text': f"❌ Task Failed: {task_instance.task_id}\nDate: {execution_date}"
        }
    )

def on_retry_callback(context):
    """重试回调函数"""
    task_instance = context['task_instance']
    print(f"Task {task_instance.task_id} is retrying (attempt {task_instance.try_number})")

def on_success_callback(context):
    """成功回调函数"""
    task_instance = context['task_instance']
    print(f"Task {task_instance.task_id} succeeded!")

def critical_task(**context):
    """关键任务，失败不可重试"""
    try:
        # 执行关键操作
        result = perform_critical_operation()

        if result is None:
            # 立即失败，不重试
            raise AirflowFailException("Critical operation returned None")

        return result
    except Exception as e:
        # 记录详细错误
        print(f"Critical error: {str(e)}")
        raise

with DAG(
    'error_handling_example',
    description='Example of error handling and retry',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data_team',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,  # 指数退避
        'max_retry_delay': timedelta(minutes=30),
        'on_failure_callback': on_failure_callback,
        'on_retry_callback': on_retry_callback,
        'on_success_callback': on_success_callback,
    },
    tags=['error-handling'],
) as dag:

    # 带重试的任务
    retry_task = PythonOperator(
        task_id='task_with_retry',
        python_callable=task_with_retry,
        retries=5,  # 覆盖默认重试次数
        execution_timeout=timedelta(minutes=10)
    )

    # 关键任务，失败立即停止
    critical = PythonOperator(
        task_id='critical_task',
        python_callable=critical_task,
        retries=0,  # 不重试
    )

    retry_task >> critical

def perform_critical_operation():
    """模拟关键操作"""
    return "success"
```

---

## 总结

这些实战示例涵盖了 Airflow 的常见使用场景：

1. **ETL 管道**: 完整的数据提取、转换、加载流程
2. **机器学习**: 模型训练和部署自动化
3. **多数据源整合**: 从不同来源整合数据
4. **动态 DAG**: 根据配置自动生成工作流
5. **条件分支**: 根据数据状态选择执行路径
6. **Sensor 使用**: 等待外部条件满足
7. **错误处理**: 完善的重试和告警机制

这些模式可以组合使用，构建适合自己业务需求的复杂数据管道。
