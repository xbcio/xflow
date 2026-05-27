# Airflow 部署指南

## 1. 本地开发环境

### 1.1 使用 virtualenv

```bash
# 创建虚拟环境
python -m venv airflow-venv
source airflow-venv/bin/activate  # Linux/Mac
# 或 airflow-venv\Scripts\activate  # Windows

# 设置 Airflow Home
export AIRFLOW_HOME=~/airflow

# 安装 Airflow
AIRFLOW_VERSION=2.8.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 初始化数据库
airflow db init

# 创建管理员用户
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 启动服务
airflow webserver --port 8080 &
airflow scheduler &
```

### 1.2 使用 Docker Compose

创建 `docker-compose.yml`:

```yaml
version: '3.8'

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.8.0
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    ports:
      - "5432:5432"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow db init
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'

volumes:
  postgres-db-volume:
```

启动:
```bash
# 创建目录
mkdir -p ./dags ./logs ./plugins ./config

# 设置 UID
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 初始化
docker-compose up airflow-init

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

---

## 2. 生产环境部署

### 2.1 单机部署（LocalExecutor）

适合中小规模部署。

**系统要求**
- 4+ CPU cores
- 8+ GB RAM
- PostgreSQL 或 MySQL
- 足够的磁盘空间（日志和数据）

**安装步骤**

```bash
# 1. 安装依赖
sudo apt-get update
sudo apt-get install -y python3-pip postgresql postgresql-contrib

# 2. 创建数据库
sudo -u postgres psql
postgres=# CREATE DATABASE airflow;
postgres=# CREATE USER airflow WITH PASSWORD 'airflow';
postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
postgres=# \q

# 3. 安装 Airflow
pip install apache-airflow[postgres,celery,redis]

# 4. 配置
export AIRFLOW_HOME=/opt/airflow
mkdir -p $AIRFLOW_HOME

# 编辑配置文件
cat > $AIRFLOW_HOME/airflow.cfg <<EOF
[core]
dags_folder = /opt/airflow/dags
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
load_examples = False

[webserver]
web_server_port = 8080
base_url = http://your-domain.com

[scheduler]
dag_dir_list_interval = 60
EOF

# 5. 初始化数据库
airflow db init

# 6. 创建用户
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# 7. 使用 systemd 管理服务
```

**Systemd 服务配置**

`/etc/systemd/system/airflow-webserver.service`:
```ini
[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Environment="AIRFLOW_HOME=/opt/airflow"
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow webserver --pid /run/airflow/webserver.pid
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/airflow-scheduler.service`:
```ini
[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Environment="AIRFLOW_HOME=/opt/airflow"
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

启动服务:
```bash
# 创建用户
sudo useradd -m -s /bin/bash airflow

# 创建目录
sudo mkdir -p /opt/airflow /run/airflow
sudo chown -R airflow:airflow /opt/airflow /run/airflow

# 启用并启动服务
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler

# 查看状态
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler
```

---

### 2.2 分布式部署（CeleryExecutor）

适合大规模部署，支持水平扩展。

**架构图**
```
┌─────────────┐
│Load Balancer│
└──────┬──────┘
       │
   ┌───┴───┐
   │       │
┌──▼──┐ ┌──▼──┐
│Web 1│ │Web 2│
└─────┘ └─────┘
   │       │
   └───┬───┘
       │
┌──────▼──────┐      ┌──────────┐
│  Scheduler  │◄─────┤PostgreSQL│
└──────┬──────┘      └──────────┘
       │
┌──────▼──────┐
│Redis/RabbitMQ│
└──────┬──────┘
       │
   ┌───┴────┐
   │        │
┌──▼───┐ ┌──▼───┐
│Worker│ │Worker│
│  1   │ │  2   │
└──────┘ └──────┘
```

**配置步骤**

1. **安装 Redis**
```bash
sudo apt-get install redis-server
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

2. **配置 Airflow**
```ini
# airflow.cfg
[core]
executor = CeleryExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@db-host:5432/airflow

[celery]
broker_url = redis://redis-host:6379/0
result_backend = db+postgresql://airflow:airflow@db-host:5432/airflow
worker_concurrency = 16

[celery_broker_transport_options]
visibility_timeout = 21600
```

3. **启动 Worker**
```bash
# 在每个 Worker 节点上
airflow celery worker --queues default,high_priority

# 使用 systemd
# /etc/systemd/system/airflow-worker.service
[Unit]
Description=Airflow celery worker daemon
After=network.target

[Service]
Environment="AIRFLOW_HOME=/opt/airflow"
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow celery worker
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

4. **启动 Flower（Celery 监控）**
```bash
airflow celery flower --port 5555
```

---

### 2.3 Kubernetes 部署

最适合云原生环境。

**使用 Helm Chart**

```bash
# 1. 添加 Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# 2. 创建命名空间
kubectl create namespace airflow

# 3. 创建配置文件
cat > values.yaml <<EOF
# 使用 KubernetesExecutor
executor: KubernetesExecutor

# PostgreSQL 配置
postgresql:
  enabled: true
  auth:
    username: airflow
    password: airflow
    database: airflow

# Redis（如果使用 CeleryExecutor）
redis:
  enabled: false

# Web服务器配置
webserver:
  replicas: 2
  resources:
    limits:
      cpu: 1000m
      memory: 2Gi
    requests:
      cpu: 500m
      memory: 1Gi

# Scheduler配置
scheduler:
  replicas: 2
  resources:
    limits:
      cpu: 1000m
      memory: 2Gi
    requests:
      cpu: 500m
      memory: 1Gi

# Workers（CeleryExecutor）
workers:
  replicas: 3
  resources:
    limits:
      cpu: 2000m
      memory: 4Gi
    requests:
      cpu: 1000m
      memory: 2Gi

# DAG 同步
dags:
  gitSync:
    enabled: true
    repo: https://github.com/your-org/airflow-dags.git
    branch: main
    rev: HEAD
    subPath: "dags"

# 持久化
logs:
  persistence:
    enabled: true
    size: 100Gi
    storageClassName: standard

# Ingress
ingress:
  enabled: true
  hosts:
    - name: airflow.your-domain.com
      path: /
  tls:
    enabled: true
    secretName: airflow-tls

# 环境变量
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
  - name: AIRFLOW__WEBSERVER__EXPOSE_CONFIG
    value: "True"
EOF

# 4. 安装
helm install airflow apache-airflow/airflow -n airflow -f values.yaml

# 5. 查看状态
kubectl get pods -n airflow
kubectl get svc -n airflow

# 6. 访问 Web UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow

# 7. 升级
helm upgrade airflow apache-airflow/airflow -n airflow -f values.yaml
```

**自定义 Docker 镜像**

```dockerfile
# Dockerfile
FROM apache/airflow:2.8.0

USER root

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean

USER airflow

# 安装 Python 依赖
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 复制自定义插件
COPY --chown=airflow:root plugins/ ${AIRFLOW_HOME}/plugins/
```

```bash
# 构建和推送
docker build -t your-registry/airflow:2.8.0-custom .
docker push your-registry/airflow:2.8.0-custom

# 在 values.yaml 中使用
images:
  airflow:
    repository: your-registry/airflow
    tag: 2.8.0-custom
```

---

## 3. 高可用配置

### 3.1 多 Scheduler

Airflow 2.0+ 支持多 Scheduler。

```ini
# airflow.cfg
[scheduler]
# 允许多个 Scheduler
max_threads = 2

# 在多个节点启动 Scheduler
# Node 1
airflow scheduler --host scheduler-1

# Node 2
airflow scheduler --host scheduler-2
```

### 3.2 数据库高可用

**PostgreSQL 主从复制**

```bash
# 主节点配置
# postgresql.conf
wal_level = replica
max_wal_senders = 3
wal_keep_segments = 64

# pg_hba.conf
host replication replicator standby-ip/32 md5

# 从节点配置
# standby.signal
standby_mode = 'on'
primary_conninfo = 'host=primary-ip port=5432 user=replicator password=password'
```

**使用 Patroni**

```yaml
# patroni.yml
scope: airflow-cluster
name: node1

restapi:
  listen: 0.0.0.0:8008
  connect_address: node1:8008

etcd:
  host: etcd:2379

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 1048576

  initdb:
  - encoding: UTF8
  - data-checksums

postgresql:
  listen: 0.0.0.0:5432
  connect_address: node1:5432
  data_dir: /var/lib/postgresql/data
  pgpass: /tmp/pgpass
  authentication:
    replication:
      username: replicator
      password: rep-pass
    superuser:
      username: postgres
      password: postgres
```

### 3.3 消息队列高可用

**Redis Sentinel**

```bash
# sentinel.conf
sentinel monitor mymaster redis-master 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1

# Airflow 配置
[celery]
broker_url = sentinel://sentinel-host:26379;sentinel://sentinel-host2:26379/mymaster
```

**RabbitMQ 集群**

```bash
# 在每个节点
rabbitmqctl stop_app
rabbitmqctl join_cluster rabbit@node1
rabbitmqctl start_app

# Airflow 配置
[celery]
broker_url = amqp://user:password@rabbitmq-lb:5672//
```

---

## 4. 监控和日志

### 4.1 日志管理

**远程日志存储 (S3)**

```ini
# airflow.cfg
[logging]
remote_logging = True
remote_base_log_folder = s3://my-bucket/airflow/logs
remote_log_conn_id = aws_default
encrypt_s3_logs = True
```

**远程日志存储 (GCS)**

```ini
[logging]
remote_logging = True
remote_base_log_folder = gs://my-bucket/airflow/logs
remote_log_conn_id = google_cloud_default
```

### 4.2 指标监控

**StatsD + Grafana**

```ini
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

```bash
# 安装 StatsD Exporter
docker run -d -p 9102:9102 -p 8125:8125/udp \
  prom/statsd-exporter

# Prometheus 配置
# prometheus.yml
scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['statsd-exporter:9102']
```

**Prometheus 直接集成**

```bash
# 安装 airflow-exporter
pip install airflow-exporter

# 启动 exporter
airflow-exporter --port 9112
```

**Grafana Dashboard**

导入官方 Dashboard ID: 13629

### 4.3 告警配置

**邮件告警**

```ini
# airflow.cfg
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_port = 587
smtp_mail_from = airflow@example.com
smtp_user = airflow@example.com
smtp_password = your-password
```

**Slack 告警**

```python
from airflow.providers.slack.hooks.slack import SlackHook

def slack_alert(context):
    slack_hook = SlackHook(slack_conn_id='slack_default')
    slack_hook.call(
        api_method='chat.postMessage',
        json={
            'channel': '#airflow-alerts',
            'text': f"Task Failed: {context['task_instance'].task_id}"
        }
    )

default_args = {
    'on_failure_callback': slack_alert,
}
```

---

## 5. 备份和恢复

### 5.1 数据库备份

```bash
# PostgreSQL 备份
pg_dump -h localhost -U airflow airflow > airflow_backup.sql

# 定期备份脚本
#!/bin/bash
BACKUP_DIR=/backups/airflow
DATE=$(date +%Y%m%d_%H%M%S)
pg_dump -h localhost -U airflow airflow | gzip > $BACKUP_DIR/airflow_$DATE.sql.gz

# 保留最近7天的备份
find $BACKUP_DIR -name "airflow_*.sql.gz" -mtime +7 -delete

# 添加到 crontab
0 2 * * * /path/to/backup_script.sh
```

### 5.2 DAG 文件备份

```bash
# 使用 Git
cd $AIRFLOW_HOME/dags
git init
git add .
git commit -m "Backup DAGs"
git push origin main

# 或使用 rsync
rsync -avz $AIRFLOW_HOME/dags/ backup-server:/backups/dags/
```

### 5.3 恢复

```bash
# 恢复数据库
psql -h localhost -U airflow airflow < airflow_backup.sql

# 或从压缩文件恢复
gunzip < airflow_backup.sql.gz | psql -h localhost -U airflow airflow

# 恢复 DAG 文件
git clone https://github.com/your-org/airflow-dags.git $AIRFLOW_HOME/dags
```

---

## 6. 安全加固

### 6.1 认证配置

**启用 RBAC**

```ini
# airflow.cfg
[webserver]
rbac = True
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth
```

**LDAP 认证**

```ini
[webserver]
authenticate = True
auth_backend = airflow.contrib.auth.backends.ldap_auth

[ldap]
uri = ldap://ldap.example.com
user_filter = objectClass=*
user_name_attr = uid
group_member_attr = memberOf
superuser_filter = memberOf=cn=airflow-admins,ou=groups,dc=example,dc=com
bind_user = cn=admin,dc=example,dc=com
bind_password = admin-password
basedn = dc=example,dc=com
```

### 6.2 网络安全

**使用 HTTPS**

```bash
# 生成自签名证书（仅测试用）
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout airflow.key -out airflow.crt

# 配置 Nginx
server {
    listen 443 ssl;
    server_name airflow.example.com;

    ssl_certificate /path/to/airflow.crt;
    ssl_certificate_key /path/to/airflow.key;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**防火墙规则**

```bash
# 只允许特定IP访问
sudo ufw allow from 10.0.0.0/8 to any port 8080
sudo ufw allow from 192.168.1.0/24 to any port 8080
```

### 6.3 加密配置

**Fernet Key**

```bash
# 生成 Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 配置
[core]
fernet_key = your-generated-key

# 加密现有连接
airflow db shell
UPDATE connection SET password = 'encrypted_value' WHERE conn_id = 'my_conn';
```

---

## 7. 性能调优

### 7.1 数据库优化

```sql
-- 创建索引
CREATE INDEX idx_task_instance_state ON task_instance(state);
CREATE INDEX idx_dag_run_state ON dag_run(state);
CREATE INDEX idx_task_instance_dag_run ON task_instance(dag_id, execution_date);

-- 定期清理
DELETE FROM task_instance WHERE execution_date < NOW() - INTERVAL '90 days';
DELETE FROM dag_run WHERE execution_date < NOW() - INTERVAL '90 days';
DELETE FROM xcom WHERE execution_date < NOW() - INTERVAL '30 days';
```

### 7.2 Scheduler 优化

```ini
[scheduler]
# 调整解析进程数
parsing_processes = 4

# 减少数据库查询
min_file_process_interval = 30
dag_dir_list_interval = 300

# 增加批处理大小
max_tis_per_query = 512
```

### 7.3 Worker 优化

```ini
[celery]
# 增加并发
worker_concurrency = 16

# 控制内存
worker_max_memory_per_child = 8000000  # 8GB

# 任务预取
worker_prefetch_multiplier = 1
```

---

## 8. 故障排除

### 8.1 常见问题

**Scheduler 不触发任务**
```bash
# 检查 Scheduler 状态
airflow jobs check --job-type SchedulerJob

# 查看日志
tail -f $AIRFLOW_HOME/logs/scheduler/latest/scheduler.log

# 检查 DAG
airflow dags list
airflow dags show my_dag
```

**任务卡在队列**
```bash
# 检查 Executor
airflow celery workers  # CeleryExecutor

# 检查资源池
airflow pools list

# 手动清除任务
airflow tasks clear my_dag -t my_task -s 2024-01-01 -e 2024-01-02
```

**Web UI 无法访问**
```bash
# 检查 Web Server 进程
ps aux | grep "airflow webserver"

# 检查端口
netstat -tlnp | grep 8080

# 重启 Web Server
pkill -f "airflow webserver"
airflow webserver -D
```

---

## 总结

部署 Airflow 需要考虑：

1. **环境选择**: 本地开发、单机生产、分布式集群、K8s
2. **Executor 选择**: Sequential、Local、Celery、Kubernetes
3. **高可用**: 多 Scheduler、数据库复制、消息队列集群
4. **监控**: 日志、指标、告警
5. **安全**: 认证、加密、网络隔离
6. **性能**: 数据库优化、并发配置、资源管理
7. **备份**: 数据库、DAG 文件、配置

根据实际需求选择合适的部署方案，并持续优化和监控。
