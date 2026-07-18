# 受控 host 容量报告模板与 Runbook（D1）

> **状态**：架构与采样准备完成。**容量基线未完成**——本模板在受控 host 多样本填实后才构成基线证据。
> 在真实多样本报告产出前，RELEASE-GATES.md 的 D1 容量基线只能标记为「架构与采样准备完成」，不得标记「容量基线完成」。

## 1. 目的与边界

本模板用于在**受控 host** 上对两条独立路径分别建立多样本容量基线。它是 D1 的容量证据载体，不是 CI 硬门槛。

- CI 采样（`.github/workflows/perf-sample.yml` + `scripts/perf-sample.sh`）只用于趋势与可下载 artifact，因 GitHub-hosted runner 噪声大，**不作为容量承诺**。
- 容量承诺必须用本模板在受控 host 上多样本产出。
- **禁止用 transient（路径 B / SDK cluster transient）数据承诺 durable（server+runner / 路径 A）审批产能**。每份报告必须显式标注适用拓扑。

## 2. 受控 host 准入条件

报告必须记录以下 host 环境并保证各样本间一致：

| 项 | 要求 |
|---|---|
| CPU | 固定核数/型号/频率，独占或 cgroup 限额一致；记录 `nproc` 与 `/proc/cpuinfo` 摘要 |
| 内存 | 固定上限；记录 `free -h` 与 cgroup memory.limit |
| OS/内核 | 记录 `uname -srm`、发行版 |
| 其它负载 | 报告期间 host 无其它 CPU/IO 密集负载 |
| Redis | 记录：单节点/sentinel/cluster、持久化策略（RDB/AOF）、`maxmemory`、是否本机 |
| Kafka | 记录：partition 数、consumer group、`MaxSize`、`FlushInterval`、broker 本机/远程 |
| MySQL（如涉及 sqlstore） | 记录版本、连接池上限、本机/远程 |

不满足上述一致性 → 样本作废，不得纳入基线。

## 3. 多样本协议

1. 每条拓扑每组合至少 **5 个样本**，剔除首个 warmup 样本后取中位与分位。
2. 每样本固定：消息大小、节点数、`--concurrency`/`WithConcurrency`、runner 数。
3. 两次样本间 flush asynq 键（`asynq:*` 命名空间，非 FlushDB）并重启 server+runner，避免残留任务污染。
4. 记录每次样本的 p50/p95/p99、吞吐、失败数、超时数。
5. 跨样本对比：若 p99 跨样本变异系数 > 30%，报告需说明噪声源或重测。

## 4. 报告模板

复制以下模板填实。未填写字段不得留空猜测，写「N/A」+原因。

```markdown
# xflow 容量基线报告 — <拓扑名>

- 日期（多样本起止）：YYYY-MM-DD ~ YYYY-MM-DD
- 拓扑：[ ] server+runner durable（路径 A / 审批）
        [ ] SDK cluster transient（路径 B / 采集）
- 适用范围声明：本报告仅适用于上述拓扑。不得用于承诺其它拓扑产能。
- 报告人：

## host 环境
- CPU：<核数/型号，独占或限额>
- 内存：<上限>
- OS/内核：
- Redis：<单节点/sentinel/cluster，持久化，maxmemory，本机/远程>
- Kafka：<partition 数，consumer group，MaxSize，FlushInterval，本机/远程>
- MySQL：<版本，连接池，本机/远程>

## 负载参数
- 消息大小：<字节>
- 节点数：<workflow 节点数>
- 并发：<--concurrency / WithConcurrency>
- runner 数：<server+runner 拓扑填>

## 样本汇总（≥5，剔除 warmup）
| 样本 | p50 | p95 | p99 | 吞吐(/s) | failed | timeouts |
|---|---|---|---|---|---|---|
| 1 |  |  |  |  |  |  |
| ... |  |  |  |  |  |  |
| 中位 |  |  |  |  |  |  |

## 结论
- 中位 p95 / p99：
- 建议回归阈值（量级，非硬门槛）：
- 已知噪声源：

## 签出
- [ ] 本报告仅声明上述拓扑产能
- [ ] transient 报告未被用于承诺 durable 产能
- [ ] host 环境一致性已核验
```

## 5. 运行步骤

```bash
# 受控 host 上（先满足 §2 准入条件）
make env-up                       # 起 Redis + Kafka + MySQL（或用等价受控实例）
# 路径 A（server+runner durable）
make perf-sample                  # 产出 perf-sample-results.txt（含 perf.metric 行）
# 路径 B（SDK cluster transient）：使用 cluster transient benchmark，
#   见 test/perf/ 与 node/internal/trigger/kafka_benchmark_test.go

# 多样本：重复 ≥5 次，每次前 flush asynq:* 并重启 server+runner
./scripts/perf-sample.sh -v       # verbose 查看单次结果
```

`perf.metric` 行格式（机器可解析，供趋势对比）：

```
perf.metric topology=server-runner test=e2e_load total=1000 workers=16 throughput=800/s failed=0 timeouts=0 p50_ms=... p95_ms=... p99_ms=...
```

## 6. 反声明（必须遵守）

- ❌ 不得用 transient 拓扑的 p95/p99/吞吐承诺 durable 审批产能。
- ❌ 不得用单样本或 CI runner 数据作为容量基线。
- ❌ 不得在报告缺失 §2/§3 字段时声称「容量基线完成」。
- ✅ 审批工作流始终使用 durable mode；transient 仅用于短生命周期采集编排。
