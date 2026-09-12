# xflow 测试环境

podman 拉起的 Redis / Kafka / MySQL，供 `test/integration/` 与 `test/perf/` 使用。

## 前置

- podman 5.x（`podman --version`）
- podman compose 已内置或 `podman-compose` 可用

## 启动

    cp .env.sample .env   # 可选：改端口/密码
    make env-up           # 拉起三服务
    make env-ready        # 等待 Redis / MySQL / Kafka 协议就绪
    make env-migrate      # 灌入 db/xflow_schema.sql（幂等）

> ⚠️ `make env-migrate` 只会创建缺失表，不会转换已有旧版 `MEDIUMBLOB` 表。可丢弃的本地/测试库若已写入 `xflow_supplies.content` 或 `xflow_artifact_blobs.content` 的原始二进制数据，请先重置或重建后再初始化；全新数据库无需额外操作。不要将此重置方式用于生产数据。

## 停止 / 重置

    make env-down         # 停止，保留 volume（数据留存，下次复用）
    make env-reset        # 删 volume，从零开始

## 跑测试

    make test-integration # 真实服务集成测试
    make test-perf        # 性能基准

## 服务地址（默认）

- Redis: localhost:6379
- MySQL: localhost:3306，库 xflow，root 密码 xflow
- Kafka: localhost:9092

测试代码通过环境变量发现：`XFLOW_TEST_REDIS_ADDR`、`XFLOW_TEST_MYSQL_DSN`、`XFLOW_TEST_KAFKA_BROKERS`。未设置时回落 localhost 默认端口；服务不可用时 `t.Skip`。

## 故障排查

- 端口占用：改 `.env` 端口，`make env-reset && make env-up`
- 健康检查超时：`podman logs xflow-test-<svc>`
