# xflow 修复 Specs

本目录是 2026-06-30 深度评审后的修复计划集。每份 spec 独立可执行；编号沿用评审报告的"修复计划 1–9"，#6（可观测性）单独处理，不在本批。

## 索引

| # | Spec | 维度 | 严重度（verifier 确认后） | 状态 |
| --- | --- | --- | --- | --- |
| 1 | [resource-pool.md](resource-pool.md) | Nodes / Execution | high | 待实现 |
| 2 | [dispatcher-fix.md](dispatcher-fix.md) | Server/Runner | **critical** | 待实现 |
| 3 | [handler-version.md](handler-version.md) | Execution / SDK | high | 待实现 |
| 4 | [expand-gate.md](expand-gate.md) | Engine | med | 待实现 |
| 5 | [dual-write-contract.md](dual-write-contract.md) | Backend / Store | high | 待实现 |
| 6 | _TODO 可观测性_ | 全栈 | high | 单独讨论 |
| 7 | [retry-policy.md](retry-policy.md) | Engine | high | 待实现 |
| 8 | [lua-concurrency-tests.md](lua-concurrency-tests.md) | Backend 测试 | high | 待实现 |
| 9 | [mtls-auth.md](mtls-auth.md) | Server/Runner 安全 | high | 待实现 |

每份 spec 的固定结构：`Problem → Goals → Non-goals → Design → Testing → Acceptance`。Design 段落都给出了具体文件路径与函数名，方便分配实现任务。

## 依赖关系

```
                 ┌──────────────────┐
                 │ #4 expand-gate   │  独立、零依赖（最先做）
                 └──────────────────┘
   ┌─────────────────────────┐
   │ #3 handler-version       │  独立
   └─────────────────────────┘
   ┌─────────────────────────┐
   │ #8 lua-concurrency-tests │  独立；为后续 #2/#5/#7 提供回归网
   └─────────────────────────┘

   ┌─────────────────────────┐
   │ #1 resource-pool         │  改 types.Runtime；独立于 cluster
   └─────────────────────────┘

   ┌─────────────────────────┐
   │ #7 retry-policy          │  改 engine 核心；为 #2 提供 transient 错误抽象
   └─────────────────────────┘
                │
                ▼
   ┌─────────────────────────┐
   │ #2 dispatcher-fix        │  依赖 #7 的 Transient() 接口；引入 lease TTL
   └─────────────────────────┘
                │
                ▼
   ┌─────────────────────────┐
   │ #5 dual-write-contract   │  改 redis_state.go 错误路径；最好在 #2 之后
   └─────────────────────────┘

   ┌─────────────────────────┐
   │ #9 mtls-auth             │  独立，但建议放最后（涉及 cmd/server 启动序）
   └─────────────────────────┘
```

## 推荐批次

### 批 A — 低风险预热（可并行）

- **#3 handler-version**：纯增量、可回退（默认 WarnFallback）
- **#4 expand-gate**：编译期闸门，blast radius 极小
- **#8 lua-concurrency-tests**：只加测试，零运行时改动

成功标准：`make test test-concurrency` 全绿；新增测试能抓出脚本回归。

### 批 B — 节点层

- **#1 resource-pool**：动 `types.Runtime` 公共契约，需要兼容期 type-assertion fallback

成功标准：DatabaseNode / GRPCNode 1k 次并发 Execute 连接数收敛；现有 node 测试不需要语义改动。

### 批 C — 引擎重试

- **#7 retry-policy**：动 `engine.handleNodeError` 入口和 StateStore 接口
  - 子步骤：(1) 错误分类接口；(2) backoff + StateStore.ResetNodeForRetry；(3) compile-in；(4) SDK ergonomics
  - 每个子步骤独立提交，互不阻塞

成功标准：场景测试覆盖 transient 重试、永久错误跳过、重试耗尽走 OnError 三条路径。

### 批 D — 控制面修复

按 spec #2 的"小步落地"建议拆三步：
1. lease TTL + sweeper（最先，对其它部分零依赖）
2. 容量 gating + 错误分类 requeue（依赖 #7 的 Transient 接口）
3. 持久 pending（依赖前两步，可延后到下次 release）

并行做 **#5 dual-write-contract**：因为 #2 也会引入新的 Redis 写点位，最好同期把 `auditWrite` 抽象建立起来。

成功标准：runner 中途 kill 任务在 2×TTL 内被回收；soak 1000 task 无丢失。

### 批 E — 安全

- **#9 mtls-auth**：最后落地，因为它直接影响 cmd/server 启动可见行为；放在前面会让前几批的 E2E 测试都要兼顾 auth 配置。

成功标准：dry-run 模式不破坏现有 E2E；token / mTLS 启用后 E2E 仍通过。

## 横向工作（每批后都要做）

1. `make test test-concurrency` 全绿
2. 更新对应的 `.claude/docs/` 文档（架构图、deployment-topologies 的 limitation 章节随实现进度划掉）
3. 在 spec 文件顶部把 `Status: draft` 改为 `Status: shipped @ <git-sha>`
4. 在 `MEMORY.md` 视情况新增一条简短记忆（实施期发现的非显然约束）

## TODO（spec 阶段之外的事）

- [ ] **#6 可观测性 spec**：与用户单独讨论后补一份 `observability.md`
  - slog vs zap 选型
  - Prometheus vs OTLP exporter
  - Hooks 接线粒度（每个 hook 都接 metric？还是分级）
  - Tracing 范围（每个节点一个 span？还是只关键边界）
  - Audit 事件是否进 Kafka 做事件溯源
- [ ] **观测桩接线**：#1 / #2 / #5 / #9 都留了 `Observer` 接口，需要 #6 完成后批量接入
- [ ] **跨 spec 复用的 transient 错误抽象**：#7 引入 `PermanentError`、#2 引入 `Transient()`，需要在实现前对齐到一套（建议都用 `errors.Is(err, ErrPermanent)`）
- [ ] **CI 集成**：至少把 `make test`、`make test-concurrency`、`golangci-lint run`、`gofmt -l .` 跑成必跑
- [ ] **DSL 文档同步**：#3 / #4 / #7 都新增了 DSL 字段，需要在 `docs/design/DSL-SPECIFICATION.md` 一并更新
- [ ] **deployment-topologies.md 的 MVP limitation 段**：#2 + #5 + #9 实现完毕后，原 limitation 章节里的对应条目划掉或改写为"已实现"

## 怎么开始一个 spec

1. 读 spec 顶部的 Problem / Goals
2. 在 git 上新开一个小的 commit（标题用 `feat(spec-N): ...` / `refactor(spec-N): ...`）
3. 跟 spec 的 Design 段落逐项实现；Testing 段落定义最低验收
4. 每提交一次 commit 都跑 `make test`；如果该 spec 在 `Testing` 段引用了 `test-concurrency` 也一并跑
5. 落地后把 spec 顶部 status 改为 `shipped`

不要在一个 commit 里跨 spec —— spec 是"独立可发布"的最小单位。
