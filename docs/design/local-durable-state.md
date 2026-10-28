# 本地后端可选持久化状态设计

| 项目 | 内容 |
|---|---|
| 状态 | 设计稿（未实现）；2026-10-24 xdesk 侧评审通过，修改意见见第 12 节 |
| 日期 | 2026-10-22 |
| 范围 | `backend/providers/local`、`engine` 的本地调度路径、`sdk/xflow` 的 `NewLocal` |
| 非目标 | 分布式后端 / 多进程 HA、跨机 exactly-once、Redis 语义替换 |
| 代码基线 | 当前工作树（本设计只读代码，未修改任何 `.go`、`go.mod` 或测试） |

## 0. 结论摘要

1. **推荐方案 B：内存继续作为运行期唯一调度权威，SQLite 作为同步写穿的状态存储与持久任务队列。** 默认不启用；启用后每次被确认的状态迁移在释放内存锁之前，以一个 SQLite 事务落盘。这样保留 `memoryState` 里已经验证过的全部迁移语义，同时获得进程崩溃/重启后的恢复能力。
2. **只持久化 StateStore 不够。** 现有 `FlushOutbox` 在把任务放进内存队列后立刻 `AckOutbox`（`engine/atomic.go:595-643`），而内存队列没有任何持久化（`backend/providers/local/memory_queue.go:50-71,300-377`）。进程在这两步之间崩溃，任务既不在 outbox 也不在队列里，租约扫描器也看不到它。因此方案 B 必须同时包含**持久任务队列**：让 `Enqueue` 在返回成功前把任务行提交到 SQLite（引擎现有的“先 enqueue、后 ack”顺序不必改），并且 `TryEnqueue` / `EnqueueDelayed` 也不能异步落盘。
3. **本地后端目前不会启动租约扫描循环。** `Bind` 只启动内存队列、outbox dispatcher 和 queue-timeout sweeper（`backend/providers/local/backend.go:292-315,334-350`），没有启动 `service/control/lease_sweeper.go` 的对应逻辑。持久化模式必须新增启动恢复扫描和周期性租约回收。
4. **`engine/pin_data.go` 不能作为生产级恢复机制。** 它是工作流定义级的静态 mock，只在无环、非 group/map/FAF/supply、未禁用的普通节点上生效（`engine/graph/pin_data.go:21-109`，`engine/pin_data.go:27-117,121-134`），无法表达“某一次执行已经完成了哪些节点”。只能在 xdesk 侧作为简单 DAG 的显式“重放已完成前缀”降级手段，且会改变 graph hash（`engine/graph/snapshot.go:380-417,440-442`），不能冒充崩溃恢复。

---

## 1. 现状与约束

### 1.1 内存是唯一调度权威

`local.New` 无条件调用 `newMemoryState()`，`WithStore` 只是给 `state.db` 赋一个 `store.Store`（`backend/providers/local/backend.go:159-187`）。`memoryState` 的 30 多个 map 覆盖了执行、节点、计数器、outbox、挂起、信号、子执行、并发槽、准入与幂等（`backend/providers/local/memory_state.go:30-114`）。`WithStore` 的文档明确写着：内存是唯一调度权威、镜像只用于元数据、重启后不会恢复/重投/修复（`backend/providers/local/backend.go:70-117`）。

`store.Store` 的记录模型（`store/models.go:12-82`）虽然带 `WorkflowDef []byte`，但它只是**提交上下文携带时的原始 DSL**（`backend/providers/local/store_mirror.go:567-577`，来源 `engine/submission_context.go:36-50`），不是提交时编译好的 `graph.Graph`；而且 `Executions.CreateExecution` 等接口不包含 outbox、计数器、租约时间戳、并发槽或子执行（`store/interfaces.go:22-25`）。因此现有镜像在结构上就不是可恢复状态。

### 1.2 三个决定方案边界的事实

**事实一：ack-after-enqueue 的丢失窗口。** `FlushOutbox` 对普通任务先 `Enqueue`/`TryEnqueue`（`engine/atomic.go:595-602`），成功后 `AckOutbox`（`engine/atomic.go:638-643`）。内存队列的 `Enqueue` 只是把任务塞进 channel（`backend/providers/local/memory_queue.go:300-320`）。如果进程在这两步之间死亡，outbox 已经删除，队列内容随进程消失。分布式后端没有这个问题，因为 Redis 的投递本身是持久的，SQL 只是审计（`backend/providers/distributed/internal/rstate/doc.go:1-26`）。

**事实二：outbox 是唯一的删除路径，且已有交付租约。** 分布式与本地都规定 `AckOutbox` 是唯一移除路径；本地 outbox 条目带 `leasedUntil`，防止并发 flush 重复投递（`backend/providers/local/atomic_state.go:15-26,407-425,464-478`，`engine/atomic.go:394-408`）。这套机制足以支撑重启后的重投，但前提是 ack 之前任务已经落到持久介质。

**事实三：单进程独占是最简单且正确的边界。** 本地后端没有跨进程协调（`memoryState` 的单一 mutex 就是全部并发控制，`memory_state.go:30-31`）。持久化模式必须用文件锁保证同一状态目录只有一个本地引擎实例；否则两个进程会各自从 SQLite 装载一份内存状态并同时调度。

### 1.3 分布式后端的参考语义

`rstate` 明确了需要对齐的语义：Redis 是唯一调度真相，重启完全从 Redis 恢复；SQL 审计是尽力而为（`rstate/doc.go:1-26`）。状态迁移用 Lua 原子完成，提交、推进、准入、死信重放各是一条原子脚本（`rstate/doc.go:69-100`）。outbox 是至少一次投递，ack 是唯一删除路径（`rstate/doc.go:95-100`）。并发槽采用“先注册、再计费”，并有槽位/队列/容量/截止时间的独立键（`backend/providers/distributed/internal/rstate/concurrency_state.go:70-112,190-227,264-308,545-612`）。本地持久化不要求 Redis 的分布式能力，但这些**状态迁移的原子边界**必须一一对应。

### 1.4 可用的 SQLite 基座

`store/sqlstore/sqlitestore` 使用 `ncruces/go-sqlite3`（纯 Go/WASM，无 cgo），默认单连接以避免 SQLite 单写者竞争（`store/sqlstore/sqlitestore/sqlitestore.go:109-125`），连接固定 `journal_mode(wal)`、`synchronous(normal)`、`busy_timeout(5s)`（`sqlitestore.go:346-378`）。`WithAutoMigrate` 只增表/增列（`sqlitestore.go:187-203`），`New` 负责打开文件与执行迁移（`sqlitestore.go:228-262`），已有测试证明关闭后重开数据仍在（`sqlitestore_test.go:157`）。这适合本地优先桌面应用，但持久状态表需要自己的版本化迁移，不能依赖 AutoMigrate 做数据语义升级。

---

## 2. 状态清单（问题 1）

分类原则：

- **P0 必须持久化**：丢失会导致重复调度、错误完成、无法继续执行或无法恢复。
- **P1 建议持久化**：不影响本次恢复的正确性，但影响跨重启的功能/审计语义（死信、准入、幂等冲突检测、trigger 去重）。
- **R 可重建**：可由已持久化状态、宿主重新注册或图定义重建。
- **C 纯缓存/进程内**：只服务于当前进程的等待、通知、镜像串行化。

### 2.1 P0：执行正确性必需

| 状态 | 位置 | 内容 | 为什么必须 |
|---|---|---|---|
| `executions` | `memory_state.go:32,116-119` | `ExecutionSnapshot`：ID、编译后的 graph、params、runtime、scope、trace、parent/depth、namespace、workflow id/version、status、error、test_run、时间戳 | 恢复的根对象；graph 必须原样恢复，不能重新编译当前定义 |
| `nodes` | `memory_state.go:33`，`engine/types.go:393-476` | 每个节点的状态、lease id/token/attempt、activation/auto-depth、lease 签发时间/TTL/task type/payload/generation、committed token、输出、private 标记、port、error/details、child execution id | 租约回收、重复提交判定、输出读取、子工作流关联都依赖这些字段 |
| `inDegrees` / `activeIns` | `memory_state.go:34-35` | 每个 unit 的入度与已到达输入数 | 决定 fan-in 是否就绪；重放语义复杂，直接持久化最可靠 |
| `remaining` / `failed` | `memory_state.go:36-37` | 未完成 unit 数、失败 unit 数 | 触发执行终态；跳过/分组/种子的计数不能简单由节点状态重建 |
| `advanced` | `memory_state.go:38` | advance 幂等标记 | 防止重复投递的 advance 再次扣减入度 |
| `scheduled` | `memory_state.go:39` | 每个 unit 的 execute/skip/seed 决策 | 防止重复调度，保证 skip 级联幂等 |
| `outbox` | `memory_state.go:40`，`atomic_state.go:936` | 条目 ID、Task、AvailableAt、CreatedAt、Attempts | 持久化调度意图；ack 是唯一删除路径 |
| `outputs` | `memory_state.go:47` | 运行时输出（含 group/entry 退出） | 下游表达式、恢复后继续执行、Inspect 都需要 |
| `privateOutputs` | `memory_state.go:48-51` | 单调隐私标记 | 决定输出是否可公开；重启后不能退化 |
| `suspended` | `memory_state.go:52` | `SuspendSpec`（mode/signals/quorum/timer/timeout） | 重启后仍要匹配信号/计算超时 |
| `signals` | `memory_state.go:53` | 预先投递、尚未被消费的信号 | 节点尚未挂起时到达的信号不能丢 |
| `signalSets` | `memory_state.go:54` | 多信号模式已收集的信号集合 | 达到 quorum 才能恢复 |
| `subExecs` | `memory_state.go:56`，`expand_state.go:15-123` | map/split 父节点下每个子执行的 ID/序号/状态/结果 | 父节点等待与批量结果聚合 |
| `subWorkflowLinks` | `memory_state.go:57-62`，`subworkflow_state.go:24-72` | 子执行 ID、generation、depth、等待截止、租约身份 | 回收重投后要恢复同一个子执行，而不是新建 |
| `groupUnits` | `memory_state.go:63`，`group_state.go:19-37` | group unit 状态、lease/token/attempt/deadline、committed token、执行身份 | group 租约回收、提交恢复 |
| `workflowSlots` / `slotHolders` / `queued` | `memory_state.go:68-76,222-243` | 每个 workflow 的 limit/holders/FIFO 队列/queue deadline | 重启后槽位不能泄漏或超发 |
| `cancelingSince` | `memory_state.go:82-87` | 进入 canceling 的时间 | 重启后继续取消并清理 |
| `admissions` | `memory_state.go:64`，`entry_admission.go:84-255` | admission key → execution id/result hash | 触发入口的重复投递要得到同一执行，而不是再建一个 |
| `idempotency` | `memory_state.go:65-67`，`idempotency.go:29-64` | fingerprint/token/confirmed/leaseUntil/expiresAt | 跨重启保留幂等冲突检测；即使 claim 丢失，已有 execution 行也能挡住重复（见 4.11） |

> 说明：`remaining`/`inDegrees` 等“理论上可由图重放重建”，但重放必须精确复刻 arrival folding、skip 级联和已入队但未消费的 advance/skip 意图；一旦重放错一次就会重复扣减或错误完成。持久化这些很小的计数器，比为它们写第二套重放逻辑更安全。

### 2.2 P1：跨重启功能语义

| 状态 | 位置 | 说明 |
|---|---|---|
| `deadOutbox` + `deadMeta` | `memory_state.go:41`，`atomic_state.go:31-35,608,666` | 死信与重放守卫；不参与普通恢复，但重启后应仍可列出/重放 |
| `replayReceipts` / `replayEntryIdx` | `memory_state.go:42-46`，`atomic_state.go:841` | 死信重放的幂等回执；丢失会导致同一 RequestID 重放行为不一致 |
| terminal executions | `memory_state.go:32` | 只影响 Inspect/审计/重跑；可以按保留期裁剪 |
| trigger `Dedup` / `State` | `trigger_runtime.go:13-24,43-92` | 去重键与 trigger 节点状态；丢失会导致重启后重复触发或重复执行调度 |
| trigger `locks` / `lockSeed` | `trigger_runtime.go:13-24,64` | TTL 锁；单进程重启后旧持有者已不存在，可清空并重建 |

### 2.3 R：可重建

| 状态 | 位置 | 重建方式 |
|---|---|---|
| `workflowRegistry`（定义/版本/投影） | `workflow_registry.go:48-61` | 宿主持久化自己的定义并在启动时重新 `AddWorkflow`；引擎侧不需要持久化（但恢复必须等注册完成，见 4.1） |
| 进程内 handler registry / 版本策略 | `sdk/xflow/engine.go:102-160`，`execution/registry.go:12-37` | 代码与进程配置，无法持久化；恢复前必须注册 |
| 图编译缓存 | `engine/graph/*` | 由持久化的 `graph.Graph` 直接反序列化（`engine/graph/snapshot.go:538,610`） |
| `doneCh` | `memory_state.go:79` | 进程内等待通道；恢复后新的 Wait 会重新建立 |
| `eventWatchers` | `memory_state.go:80` | 进程内事件订阅；重启后订阅者需要重新订阅 |
| `resumed`（resume lock） | `memory_state.go:55`，`memory_state.go:1742-1751` | 只用于防止同一进程内并发 resume；重启时不可能有在途 resume，清空即可 |

### 2.4 C：纯缓存/镜像控制

`mirrorLocks`、`mirrorSkip`、`mirrorCreatePending`、`mirrorConflict`、`rolledBack`（`memory_state.go:96-113`）都是为尽力而为镜像服务的进程内控制状态。持久化模式不依赖它们；恢复时按空值初始化即可。`rolledBack` 尤其不能持久化：它表示“本次进程中已回滚的执行”，重启后不存在的执行本来就不会被访问。

### 2.5 不在 `memoryState` 里、但必须处理的状态

| 组件 | 位置 | 处理方式 |
|---|---|---|
| `memoryQueue` 的待执行/延迟任务 | `memory_queue.go:50-71,107-169` | 持久化模式改为 SQLite 任务表；内存队列只作为唤醒/热路径 |
| worker 的 in-flight 执行 | `memory_queue.go:118-169` | 重启后不存在；由持久任务行 + 租约扫描恢复 |
| outbox delivery lease `leasedUntil` | `atomic_state.go:17-24` | **不需要持久化**：进程重启后旧交付者已死，清空反而能立即重投；持久化只会把恢复延迟到 5 秒 TTL 之后 |
| outbox `Attempts` | `engine/atomic.go:33` | 必须持久化，否则死信阈值会跨重启重置 |
| workflow 定义注册表 | `workflow_registry.go:48-61` | 宿主重新注册；恢复屏障 |
| trigger runtime 状态 | `trigger_runtime.go:13-24` | 见 2.2；锁可重建，dedup/state 建议持久化 |

---

## 3. 三种方案对比（问题 2）

### 3.1 方案 A：SQLite 作为 `StateStore` 对等实现

新建一个 `sqliteState`，完整实现 `engine.StateStore` 及本地当前声明的全部可选能力（约 30 个，见 `backend/providers/local/capability_manifest_test.go:19-51`），把 `memoryState` 的每个迁移翻译成 SQL/事务。

**优点**

- 数据库是唯一权威，不存在内存与磁盘双写不一致。
- 每个迁移天然是一个 SQLite 事务，崩溃语义清晰。
- 可以直接把 `RunStateStoreContract` 跑在 SQLite 实现上（`backend/internal/statestoretest/state_store_contract.go:32`）。
- 恢复就是查询，不需要“装载 + 重放”。

**缺点**

- 需要重写 `memoryState` 里最微妙的迁移：arrival folding、skip 级联、cyclic activation、map expansion、group commit、suspend/resume、并发队列提升、镜像回滚等。每一处都是独立语义，SQL 版本与内存版本会长期漂移。
- `execution_snapshot`、`NodeSnapshot`、`SuspendSpec`、`SubWorkflowLink`、`groupUnitState`、outbox 的字段很多，schema 设计本身就要覆盖所有这些能力。
- 契约测试只能覆盖公开可观察行为，覆盖不到 `store_mirror` 的写点、`held outbox`、`mirrorCreatePending` 等内部约定。
- 首版工作量最大，且在没有持久化需求时也永远多一套需要维护的状态机。

### 3.2 方案 B：内存权威 + SQLite 同步写穿（推荐）

运行期仍然是 `memoryState` 的单一 mutex 和现有迁移代码；持久化层是“投影 + 持久任务队列”，不是第二套状态机。

- 每次成功的内存迁移在释放 `s.mu` 之前，把该迁移影响的实体以**绝对值行**写入一个 SQLite 事务；写失败则进入 fail-stop（见 3.5）。
- 恢复时从 SQLite 装载回 `memoryState` 的 map，再执行租约扫描、outbox 重投和持久任务重放。
- `outbox` 的 `AckOutbox` 只在任务行已经持久写入后发生；任务行是 outbox 与 worker 之间的持久交接层。
- 所有写入都是幂等的绝对值（不是“增量操作日志”），重复写、崩溃后重放都不会改变结果。

**优点**

- 只有一套迁移语义，不会出现“内存版和 SQL 版行为不同”。
- 现有 `memoryState` 的契约测试、能力清单、`store_mirror` 行为全部可复用；持久化是横切关注点，不是重写。
- 可以按实体分批落地：先执行/节点/outbox，再挂起/信号/子流程，最后并发/准入/幂等。
- 恢复后内存结构与崩溃前完全一致，Inspect/GetOutputs/Wait 等读路径不需要改。

**缺点**

- 写点分散在大量方法里，漏一个就会造成“内存已变、磁盘未变”。必须用写点清单 + 测试期全量对比来防漏（见 7.2）。
- 进程内是双写；写失败时无法简单回滚内存，只能 fail-stop。
- 需要持久任务队列和启动顺序控制，改动不止“加一张表”。
- 全量镜像写放大高；必须按实体粒度写（见 3.5）。

### 3.3 方案 C：扩展现有 `WithStore` 镜像

把 `store.Store` 扩展成可恢复状态，或在 `store_mirror.go` 上补齐字段。

**结论：不可行，等价于绕远路做 A。** 现有镜像的契约是：create 必需，后续 projection 尽力而为，2 秒超时后丢弃（`store_mirror.go:72-106`）；`buildMirrorNodeRecord` 只投影了 NodeSnapshot 的一部分字段（`store_mirror.go:350`）；`store.Store` 没有 outbox、计数器、挂起 spec、并发槽、子执行、group unit（`store/models.go:12-82`）。要让它可恢复，必须先把镜像从“尽力而为审计”改成“同步权威”，再扩 schema 和全部写点——这正是方案 A 的工作量，却保留了 `store.Store` 面向审计的接口包袱。

### 3.4 对比表

| 维度 | A：SQLite StateStore | B：内存 + 写穿 | C：扩展镜像 |
|---|---|---|---|
| 状态迁移正确性 | 需重写全部迁移，漂移风险高 | 复用现有迁移，风险集中在写点遗漏 | 需先改镜像契约，风险同 A |
| 原子性 | 每个 SQL 事务天然原子 | 每个迁移一个事务，需保证边界 | 当前 best-effort，不满足 |
| 与 rstate 语义对齐 | 可对齐，但需逐条翻译 | 直接复用同一套迁移代码 | 无 |
| 改动面 | 最大（新状态机） | 中（写点 + 队列 + 恢复） | 大（改契约 + schema + 写点） |
| 写放大 | 可控（按行） | 可控（按实体；全量镜像不可接受） | 不可控（镜像本就丢字段） |
| 契约测试复用 | 可直接跑 | 同一实现，直接跑 | 无意义 |
| 恢复实现 | 查询 + 重建内存 | 装载 + 扫描 | 仍要装载 |
| 主要风险 | 语义漂移、长期双维护 | 漏写点、双写不一致 | 契约被破坏 |

### 3.5 推荐方案 B 的具体形态

**（1）存储布局。** 使用规范化表，而不是“整个 execution 一个 JSON blob”。原因很直接：map 展开时父节点的 `subExecs` 会随子项数量增长（`expand_state.go:35-40,78-80`），每次子项完成都重写父执行全量镜像会退化成 O(n²)；`nodes`、`outputs`、`outbox`、`subExecs`、`group_units` 都必须有独立主键，按实体更新。JSON 只用于单个字段值（graph、params、output、spec、payload），不用于整个执行。

方案 B 不另建逻辑 WAL：SQLite 的 WAL 就是物理日志，规范化的状态表就是“最新快照”。恢复 = 装载最新快照 + 清空进程内租约 + 重投 outbox/持久任务；不需要重放一套自定义操作日志。

**（2）写穿边界。** 每个对外可变方法在持有 `s.mu` 期间完成内存迁移，随后调用 `s.durable.flushLocked(execIDs...)`，在同一个临界区内提交 SQLite 事务，然后才解锁返回。关键迁移必须把**多个实体放在同一个事务**里：

- `CreateExecutionWithOutbox`：execution + counters + 初始 outbox。
- `CommitNode`：node 终态 + output + remaining/failed + 下游 outbox + skip 标记。
- `AdvanceNode`：advanced 标记 + 下游 execute/skip outbox。
- `Suspend`：node 状态/清租约 + suspend spec + resume outbox。
- `DeliverSignalWithOutbox`：消费信号 + signalSets + resume outbox。
- `ReclaimLeaseWithOutbox`：撤销租约 + requeue outbox。
- `BeginSubWorkflowWait`：node Waiting + link + outbox。
- `CommitGroup`：group unit + 下游 arrivals/outbox。
- 并发准入/释放：slot + holder + queued + execution 状态。

这些边界与 `rstate` 的 Lua 原子脚本一一对应（`rstate/doc.go:69-100`），是恢复正确性的核心。

**（3）写点清单。** 不依赖“开发者记得在每个方法末尾加一行”，而是维护一份写点清单（类似 `store_mirror.go` 的写点契约，`store_mirror.go:18-70`）：

| 写点/辅助函数 | 负责的行 |
|---|---|
| `createExecutionLocked` / `finishExecutionLocked` / `updateExecutionStatusLocked` | `executions`、`counters`、`canceling`、`slots`、`queue` |
| `upsertNodeLocked` / `acquireTaskLeaseLocked` / `commitNodeLocked` | `nodes` |
| `PutOutput` / `preserveOutputPrivacyLocked` / commit 输出写入 | `outputs`、`private_outputs` |
| `putOutboxLocked` / `AckOutbox` / `ReleaseOutbox` / `RecordOutboxFailure` / 死信与回执 | `outbox`、`dead_outbox`、`replay_receipts` |
| `SuspendOrConsume` / `DeliverSignalWithOutbox` / `AcquireResumeLock` / `ResuspendAtomic` | `suspended`、`signals`、`signal_sets` |
| `BeginSubWorkflowWait` / `GetSubWorkflowLink` | `subworkflow_links` |
| `BeginTaskExpansionWithOutbox` / `CreateExpandedSubExecution` / `CompleteExpandedSubExecution` | `sub_execs` |
| `AcquireGroupLease` / `CommitGroup` / `RevokeGroupLeaseWithOutbox` | `group_units` |
| `chargeOrParkLocked` / `releaseSlotLocked` / `ExpireQueuedExecutions` | `slots`、`slot_holders`、`queue` |
| `SeedExecutionFromEntry` / `SeedExecution` / `ClaimIdempotency` / `ConfirmIdempotency` | `admissions`、`idempotency`、`executions`、`counters`、`outbox` |

清单需要配套测试：在测试构建下，每次迁移后比较“内存快照”和“数据库快照”，任何未标记的变更都会失败（见 7.2）。这比事后靠人工 review 找漏点可靠。

**（4）写放大与 fsync 策略。** 默认 `WAL + synchronous=NORMAL`。`sqlitestore` 的注释已经说明其取舍：断电可能丢掉最后几次提交，但不会损坏文件（`sqlitestore.go:357-361`）。对桌面应用，进程崩溃（不是整机断电）不会丢已写入 WAL 的提交；即使断电丢最后几次，至少一次语义会让节点重跑、outbox 重投，不会静默丢工作。需要更强保证时提供 `SyncFull`。连接级 `busy_timeout` 默认是 5 秒（`sqlitestore.go:346-378`），但调度路径不应被数据库锁拖住 5 秒：持久化写应使用更短的超时（例如 2 秒，与镜像写超时同量级，`store_mirror.go:84`），超时即 fail-stop 并由宿主重启恢复。**不要对租约续约、outbox 交付租约、事件通道等心跳型写入做同步落盘**：重启后这些状态本来就要重建，持久化它们只会增加写放大和恢复延迟。`RenewTaskLease`、`RenewOutbox`、`RenewGroupLease` 属于这一类。

**（5）fail-stop。** SQLite 写失败（磁盘满、IO 错误、锁超时）时，内存状态已经前进，无法安全回滚；正确做法是：

- 记录 `durableErr`，后续所有可变操作返回 `ErrDurableStateUnavailable`；
- 停止队列消费和 outbox 投递；
- 让进程退出/由宿主重启，从最后一次已提交事务恢复；
- 绝不“继续跑但只写内存”，否则重启后会回到更早的状态并产生不可解释的重复。

**（6）为什么不选 A。** A 的诱惑是“单一真相”，但它的代价是把 `memoryState` 的 2000 多行迁移逻辑用 SQL 再写一遍，并长期维护两份。只要契约测试不能覆盖内部写点（例如 `holdOutboxLocked`、`mirrorCreatePending`、arrival folding 的 skip 级联），A 的“单一真相”就只是表面上的。B 的双写风险可以通过写点清单、全量对比测试和 fail-stop 控制到可接受范围；A 的语义漂移风险则没有等价的自动防线。

---

## 4. 恢复流程（问题 3）

### 4.1 启动顺序（顺序本身是正确性的一部分）

1. 宿主打开状态目录：`OpenDurableState` 获取排他文件锁（见 6.3），打开 SQLite，检查 schema 版本，执行向前迁移。
2. 装载所有未终态执行（以及保留期内的终态执行，供 Inspect）到 `memoryState`：executions、nodes、counters、outbox、outputs、suspend、signals、subexecs、links、group units、slots、admissions、idempotency、canceling。
3. 清空进程内状态：`resumed`、`leasedUntil`、`doneCh`、`eventWatchers`、镜像控制字段、`rolledBack`。持久任务表中上一进程遗留的 `claimed` 行重置为 `pending`（单进程独占，旧 worker 必然已死）。
4. 解码并校验：graph JSON 必须能反序列化（`engine/graph/snapshot.go:610`），执行 ID/状态/计数器自洽；无法解码的执行标记为 failed，错误写回并在 Inspect 可见（见 6.2）。
5. **先不解除投递屏障。** 在 outbox/持久任务开始派发之前，宿主完成 `AddWorkflow` / handler 注册；本地 SDK 需要在 `NewLocal` 返回后显式调用恢复入口（见第 5 节）。队列 worker 可以已经启动，但恢复屏障必须让 `ListOutbox` / 任务领取返回空，直到 `Recover` 完成。这是必要的：`newFromConfig` 在构造时就启动队列（`sdk/xflow/engine.go:195-211`），而 `AddWorkflow` 只能在 Engine 构造之后调用（`sdk/xflow/workflow_registry.go:50`）。
6. `Recover(ctx)`：
   - 强制回收所有上一进程遗留的租约（见 4.2）；
   - 执行一次完整 outbox drain（到期条目立即投递，延迟条目按 `AvailableAt` 等待）；
   - 重放持久任务表；
   - 重算并发槽并清理 queue timeout；
   - 释放 outbox 的“恢复屏障”，队列开始正常消费。
7. 启动周期性维护：租约扫描、queue timeout、outbox dispatcher。可以复用 `service/control/lease_sweeper.go` 的扫描逻辑（`Run` 在启动时会先跑一次 maintenance，`lease_sweeper.go:479-501`，`SweepOnce` 在 `1036`），但本地不应直接依赖 service 层；建议把核心扫描抽成可复用组件，或在 local 内实现等价的小循环。

如果宿主在 `Recover` 之前就提交新执行，执行会被持久化但不会派发；这是可接受的 fail-closed 行为，文档必须写清楚。

### 4.2 Running / Committing / Waiting 且没有 worker

- 崩溃后所有旧租约都失去持有者。不要等待 TTL 自然到期：启动时调用一次“回收全部活跃租约”的扫描，等价于 `ListExpiredLeases(ctx, now + maxLeaseTTL)`，或者新增一个语义明确的 `ListRecoverableLeases`。
- `ListExpiredLeases` 会返回 Running/Committing/Waiting 节点以及 running 的 group unit，并从节点/租约记录重建 TaskType/Payload/Generation/ActivationID（`memory_state.go:1299-1354`）。这正是恢复 resume/requeue 任务所需的信息。
- 对每个过期租约走 `ReclaimLease`：原子路径用 `RevokeLeaseWithOutbox` 在同一个锁内撤销租约并写入 requeue outbox（`engine/lease.go:522-587`，`memory_state.go:1364`），随后 `FlushOutbox` 投递。token 栅栏保证旧 runner 的迟到提交不会生效。
- **Pending 且无租约的节点**是传统非原子路径的已知不可恢复窗口（`engine/lease.go:495-511`）。本地是 AtomicStateStore，正常不会走到；但恢复时仍应做一次防御性扫描：非终态 unit、无 outbox、无租约、无 scheduled 标记的，补一个重新调度意图或以明确错误失败。不要假设“不可能发生”。
- 重复执行是允许的：handler 可能已经产生副作用但提交未落盘。节点重跑后，若旧提交迟到，`CommitOutcomeStaleToken` 或 `CommitOutcomeDuplicateTerminal` 会处理（`engine/types.go:167-175`）。

### 4.3 outbox 未投递

- outbox 行必须持久化 `ID`、`Task`、`AvailableAt`、`CreatedAt`、`Attempts`；delivery `leasedUntil` 不持久化，启动时视为未租用。
- 恢复屏障解除后，`OutboxDispatcher` 通过 `ListOutboxExecutions` 发现待投递执行（`engine/atomic.go:1116-1196`），`FlushOutbox` 只领取到期且未租用的条目（`backend/providers/local/atomic_state.go:464-478`）。
- 关键改造：**`AckOutbox` 只能在持久任务行写入成功之后调用**——通过把 `Enqueue` 做成同步持久写来实现，而不是修改引擎的 ack 顺序。如果任务行写入失败，保留 outbox，让 dispatcher 重试；如果任务行写入成功但 ack 失败，重启后 outbox 会再次投递，持久任务表按确定 ID 去重。两种顺序都不会丢工作。
- 任务行生命周期：`pending` → worker 领取后 `claimed` → handler 返回 `nil` 后删除；handler 返回错误则按与内存队列相同的退避重试，超过上限进入任务死信/告警。启动时把上一进程遗留的 `claimed` 重置为 `pending`。删除发生在 handler 返回之后，所以“执行到一半崩溃”会重投；节点租约、终态和重复提交栅栏吸收重复。
- 持久队列的 `Enqueue` / `TryEnqueue` / `EnqueueDelayed` 必须在返回成功之前完成 SQLite 事务提交；`TryEnqueue` 的“非阻塞”只能表示“队列满时返回 `ErrQueueFull`”，不能表示“异步落盘”。否则 engine 仍会在任务未持久时 ack，回到原来的丢失窗口。`EnqueueDelayed` 还要持久化 `AvailableAt`，恢复后由 dispatcher 按时间重投。
- 任务行的主键必须来自 outbox 条目 ID，而不是随机值。建议给 `engine.Task` 增加一个内部字段（例如 `IntentID string`，`json:"-"`），由 `FlushOutbox` 从 `OutboxEntry.ID` 填充；持久队列用它做幂等键。直接入队（不经过 outbox）的路径需要一个稳定的回退键，例如 `(exec,node,unit,activation,type,generation,payload hash)`，并在测试中覆盖冲突/重试场景。

### 4.4 paused

- 暂停只改执行状态，不清空意图、不释放并发槽；`dispatchHeldLocked` 让 ready-set 读取返回空（`execution_pause.go:28-66,106-109`，`engine/interfaces.go:339-358`）。
- 持久化 `executions.status = paused` 即可。重启后执行仍在暂停，槽位仍被占用；调用 `ResumeExecution` 后继续。恢复流程不能“自动恢复所有 paused 执行”，否则违背暂停语义。

### 4.5 waiting-signal

- 持久化 `suspended`、`signals`、`signalSets`。`DeliverSignalWithOutbox` 在同一个锁内消费信号并写 resume outbox（`memory_state.go:1671-1740`），因此崩溃不会出现“信号已消费但恢复意图丢失”。
- 重启时清空 `resumed`（进程内 resume 栅栏）；`suspended` 与已收集信号保留。
- 进程停机期间到达的外部信号无法自动找回。恢复后：
  - 已消费并写入 outbox 的 resume 会重投；
  - 尚未投递到本进程的信号需要调用方重试（HTTP/SDK 的 at-least-once 责任）；
  - 若调用方在停机期间“成功”投递而本地没有持久入口，则该信号确实丢失。这是本方案无法在本地单进程内解决的边界，必须在 xdesk 的接入层使用持久触发器/重试队列，或增加一个本地信号收件表（开放问题）。
- timer/timeout 是 outbox 延迟条目，`SuspendOutboxEntries` 用 `AvailableAt` 表达（`engine/atomic.go:425-462`）；恢复后到期即投递。机器休眠导致的迟到是墙钟语义，不是丢失。

### 4.6 waiting-subworkflow

- 父节点 Waiting，`subWorkflowLinks` 保存 child execution id、generation、depth、wait deadline 和租约身份（`subworkflow_state.go:24-72`）。
- 回收重投后，`executeSubWorkflow` 会重新读取 link 和 child 状态；如果 child 已终态，`concludeTerminalChild` 是恢复兜底（`engine/subworkflow_dispatch.go:122-261,230`）。
- child execution 本身是独立的 execution 行，按 4.2/4.3 恢复。父子两边都恢复后，父节点要么继续等待，要么收敛终态。
- 恢复前必须确保 workflow registry 已重新注册 child 定义；否则重试路径可能无法解析子工作流。解析失败应让该节点失败并给出明确错误，而不是回退到“最新版本”。
- 如果 child 定义在停机期间被修改，重试解析可能拿到新版本。恢复时应优先使用 link 中保存的 child execution id 和定义身份；如果必须重新解析，校验解析结果与 link 记录的定义哈希/版本，不一致就失败（`RecoverStrict`），不要静默用新定义继续。

### 4.7 timer / timeout

- 都已经是带 `AvailableAt` 的 outbox 条目（`engine/atomic.go:425-462`），不需要单独的 timer 表。
- 恢复时清理交付租约，过期的立即投递，未来的继续等待；无需重建 goroutine timer。

### 4.8 并发槽重算

- 持久化 `workflowSlots`（key、limit、FIFO）、`slotHolders`、`queued`（deadline）。
- 启动时重算：
  1. 对每个 holder，若对应 execution 不存在或已终态，释放槽；
  2. 对每个非终态 execution，若图声明了并发限制但不在 holder/queue 中，按图重新计费（防止旧版本写入不完整）；
  3. 按持久化的 FIFO 顺序提升 queued，直到达到 limit；
  4. 对超过 deadline 的 queued 执行 `ExpireQueuedExecutions`（`memory_state.go:586`）。
- 由于是单进程且启动时没有在途释放，重算可以做到确定性；不需要 rstate 的槽 TTL/索引修复，但语义应与 rstate 的“先注册、再计费”保持一致（`rstate/concurrency_state.go:70-112,264-308,545-612`），避免 limit 变更或取消时出现悬挂 holder。

### 4.9 group unit 与 map 展开

- group unit 的租约/提交状态在 `groupUnits`（`group_state.go:19-37`）；`ListExpiredLeases` 会把它作为 `TaskTypeGroupExec` 返回（`memory_state.go:1299-1354`），`ReclaimLease` 走 group 分支（`engine/lease.go:522-587` 的 group 路径）。
- 恢复时 group 重投必须保留 `groupName`、`entryNodeIdx`、`activationID`、`unitIdx`，否则无法重建 group 任务。
- map/split 父节点的 `subExecs` 按 `(execID,nodeName,leaseID)` 分组（`expand_state.go:125-131`）。恢复后重投父任务时，`CompleteExpandedSubExecution` 只在所有子项终态时返回 `done`（`expand_state.go:111-122`），因此重复完成某个子项不会提前聚合。
- 注意 at-least-once 边界：父节点被回收后重新执行会得到新 lease，从而生成新的 child generation（child ID/outbox ID 都含 leaseID，`engine/expand.go:149-210`）。旧 generation 的完成会被父租约栅栏拒绝（`matchesExpansionLease`，`expand_state.go:125-127`），但旧 generation 的子项可能已经产生了副作用，新 generation 会再跑一遍。要减少这种重复，需要在 expansion 之前识别已存在的 generation（当前 child ID 含 leaseID，不能直接复用），这是后续优化/开放问题，首版必须按“子项可能重跑”来设计业务幂等。
- 子执行的输出与状态在各自的 execution 行中；不要只把结果放在父节点的 `subExecs` 里。

### 4.10 canceling / admission / idempotency

- `cancelingSince` 持久化后，重启时恢复取消流程：撤销/回收租约、停止新派发，最终写终态。`Cancel` 本身是可重入的，但它只在原请求存活期间推进；崩溃会留下 canceling，需要 stale-canceling reaper 重新调用（参考 `engine/cancel.go:44-135` 与 `service/control/lease_sweeper.go:720-765`；本地目前没有启动该 reaper，持久化模式必须在 `Recover` 或维护循环里补上）。注意 `AcquireTaskLeaseChecked` 只拒绝终态执行；canceling 仍被视为 live（`memory_state.go:1147-1151`），所以不能指望租约层自动拒绝，恢复流程必须主动撤销。 
- admission：重启后同一 admission key + 同一 result hash 返回 `accepted + duplicate`，不同 hash 返回 `conflict`（`entry_admission.go:94-120`）。这避免了触发源重投时创建第二个执行。
- idempotency：claim 持久化后，确认过的 key 跨重启仍返回 duplicate；未确认且租约过期的 claim 可被 takeover（`idempotency.go:29-64`）。即使 claim 丢失，`ClaimIdempotency` 在发现 execution 已存在时也会返回 duplicate（`idempotency.go:42-46`），所以“重复执行”仍被 execution 行挡住；丢失的只是 fingerprint conflict 的精确判定。

### 4.11 幂等约束与重复执行的暴露

**约束（必须写进 xdesk 的节点开发规范）**

- 本方案提供 **at-least-once**，不是 exactly-once。handler 可能因崩溃重跑；有副作用的节点必须使用外部幂等键、去重表或可重入操作。
- 引擎侧提供三层栅栏：
  1. lease token：旧租约的提交被拒绝（`CommitOutcomeStaleToken`）；
  2. 终态重复提交：返回 `CommitOutcomeDuplicateTerminal`，不重复计数/不重复推进（`engine/types.go:167-175`，`backend/providers/local/atomic_state.go:109-125`）；
  3. 执行已失活：`CommitOutcomeExecutionInactive`，不再接受提交。
- outbox 的 `ID` 是确定性的，`AckOutbox` 是唯一删除路径；重复投递由 `leasedUntil`/token 栅栏和上述提交结果吸收。
- admission/idempotency key 不能替代节点级幂等：它们只保证“同一个请求不会创建两个执行”，不保证“同一个执行里的副作用只发生一次”。
- 响应丢失同样属于至少一次：迁移可能已经落盘，但进程在返回前崩溃，调用方看到的是错误/超时。重试 `CommitNode` 会得到 duplicate terminal，重试 `CreateExecution` 会命中已有 execution，重试信号投递需要调用方幂等；这是持久化后新增的、必须写入 API 文档的语义。

**如何暴露重复**

- 引擎内部有 `CommitObserver.OnCommitOutcome`（`engine/observers.go:15-17,447-452`），可以统计 accepted/duplicate/stale/inactive。
- 观测指标已有 `xflow_commit_outcomes_total`（`observability/metrics/metrics.go:425`，测试断言见 `observability/metrics/metrics_test.go:166`）。
- SDK 目前没有把 `engine.WithCommitObserver` 暴露成 `xflow.Option`（`sdk/xflow/option.go` 中没有对应项）；xdesk 若要直接感知重复，需要在 SDK 增加选项，或依赖指标/日志。
- 节点 `Attempt` 与 `ActivationID` 会随重试/循环推进（`NodeSnapshot` 字段，`engine/types.go:399-423`）；Inspect 可读出“这个节点跑了几次”。
- 建议在持久化模式新增恢复指标：恢复执行数、回收租约数、重投任务数、恢复失败数、恢复耗时。没有这些指标，现场只能看到“重复执行”，无法判断是正常 at-least-once 还是状态损坏。

---

## 5. 接口与选项（问题 4）

### 5.1 `backend/providers/local`

```go
// DurableStateConfig 是持久化模式的全部配置。零值必须等价于“关闭”。
type DurableStateConfig struct {
    Path              string        // SQLite 文件路径；必填
    Sync              SyncMode      // SyncNormal(默认) | SyncFull
    BusyTimeout       time.Duration // 默认 5s，与 sqlitestore 一致
    SweepInterval     time.Duration // 租约扫描周期
    GraphMismatch     GraphMismatchPolicy // FailExecution(默认) | RefuseStart
    TerminalRetention time.Duration // 终态执行保留期；0 = 不自动裁剪
}

// OpenDurableState 打开/创建状态目录，获取排他文件锁，检查 schema，
// 并把可恢复状态装载成一个已 hydrate 的 memoryState。
// 任何解码/版本/锁错误都在这里返回，避免把错误藏到 New 里。
func OpenDurableState(ctx context.Context, cfg DurableStateConfig) (*DurableState, error)

func (d *DurableState) Close() error

// WithDurableState 让 New 使用已经打开并 hydrate 的状态；nil 等价于关闭。
func WithDurableState(d *DurableState) Option

// NewDurable 是便捷入口：OpenDurableState + New + 安装持久化写点。
func NewDurable(ctx context.Context, cfg DurableStateConfig, opts ...Option) (*Backend, error)

// Recover 在宿主完成 workflow/handler 注册后调用：
// 强制回收旧租约、drain outbox、重放持久任务、重算并发槽，然后解除投递屏障。
func (b *Backend) Recover(ctx context.Context) error
```

同时建议让 local 实现 `backend.StartBinder`（当前只实现了 `Bind`，`sdk/xflow/engine.go:195-211` 会走 deprecated 路径并打印警告）。`StartBinding` 可以在持久化模式下做“hydrate 已完成、队列启动但投递被屏障挡住”的确定性启动，并把打开失败作为 error 返回。

### 5.2 `sdk/xflow`

```go
// LocalDurableStateConfig 是 SDK 层的配置镜像，避免把 backend 类型泄漏到公共 API。
type LocalDurableStateConfig struct {
    Path              string
    SyncMode          LocalSyncMode
    SweepInterval     time.Duration
    GraphMismatch     LocalGraphMismatchPolicy
    TerminalRetention time.Duration
}

// WithLocalDurableState 只对 NewLocal 生效；NewCluster 应返回配置错误（fail closed），不能静默忽略。
func WithLocalDurableState(cfg LocalDurableStateConfig) Option

// RecoverDurableState 在 AddWorkflow / 节点注册完成后调用；未启用持久化时返回 nil。
func (e *Engine) RecoverDurableState(ctx context.Context) error
```

`NewLocal` 已经返回 `(*Engine, error)`（`sdk/xflow/local.go:32`），因此打开/迁移/锁错误可以在这里直接返回；`WithLocalDurableState` 只存配置，不打开文件（保持 Option 无 error 的签名）。`newFromConfig` 记录一个 `recoverDurable func(context.Context) error`，由 `RecoverDurableState` 调用（provider 需要实现一个可选的 `Recoverer` 接口）。

**典型启动顺序**

```go
eng, err := xflow.NewLocal(xflow.WithLocalDurableState(cfg))
if err != nil { return err }
defer eng.Stop()

// 1. 重新注册所有 workflow / handler / trigger 定义。
for _, wf := range xdeskWorkflows { if _, err := eng.AddWorkflow(ctx, wf); err != nil { return err } }

// 2. 注册完成后才允许恢复投递。
if err := eng.RecoverDurableState(ctx); err != nil { return err }

// 3. 开始接受外部请求。
```

如果宿主忘记第 2 步，已恢复的执行会保持“已装载但不派发”；这比“在 handler 未注册时派发”安全。文档和示例必须把这一步写成强制步骤。

### 5.3 与 `WithStore` 的关系

- **共存，不合并、不替换。** `WithStore` 仍是可选的元数据/审计镜像（`backend.go:70-117`）；持久化状态是调度恢复的权威。两者可以同时开启。
- 两者都写 SQLite 时，建议使用**不同的数据库文件**。如果必须共用同一文件，需要共享同一个 `*gorm.DB` 或接受 `busy_timeout` 竞争；`WithStore` 的写超时是 2 秒（`store_mirror.go:84`），小于 SQLite 的 5 秒 busy_timeout，因此镜像写入在高竞争下会失败——这是 best-effort 契约允许的，但会让审计缺行。
- 持久化模式**不**通过 `store.Store` 写恢复状态，也不读它。`WithStore` 的“不会恢复”文档在启用持久化后需要补充一句：恢复能力来自 durable state，而不是镜像。
- 未来可以增加“用持久化表生成审计投影”的选项，让 xdesk 只开一个状态目录；这不是 M1 的目标，也不应改变 `WithStore` 的既有语义。

### 5.4 关闭时零开销

- `config.durable == nil` 时，`New` 走与今天完全相同的路径：不打开文件、不取锁、不启动额外 goroutine、不增加每次迁移的分配；只在 `memoryState` 多一个 nil 指针和每次迁移一次 nil 判断。
- 选项不存在时，连 schema 检查都不应发生；不能因为“默认值”而在临时目录创建 SQLite 文件。
- 默认内存路径的行为/性能必须由基准测试锁定（见 7.3）。

---

## 6. 数据格式、版本与独占（问题 5）

### 6.1 Schema 与版本

建议表（均带 `xflow_local_` 前缀，避免与审计表冲突）：

| 表 | 主键 | 关键列 |
|---|---|---|
| `meta` | `key` | `schema_version`、`format_version`、`created_at`、`backend` |
| `executions` | `exec_id` | namespace、workflow_id/version、status、graph_json、params_json、runtime_json、scope_json、trace、parent/depth、test_run、error、created/updated |
| `counters` | `exec_id` | remaining、failed、in_degrees_json、active_ins_json、advanced_json、scheduled_json |
| `nodes` | `(exec_id,name)` | 完整 NodeSnapshot 列（租约、activation、committed token、输出、private、port、error、child_exec_id） |
| `outputs` | `(exec_id,name)` | data_json、private（`privateOutputs` 是独立单调标记，可能没有对应输出；也可拆成 `private_outputs(exec_id,name)`，或允许 data_json 为空） |
| `outbox` | `(exec_id,entry_id)` | task_json、available_at、created_at、attempts |
| `dead_outbox` | `(exec_id,entry_id)` | task_json、dead meta |
| `replay_receipts` | `(exec_id,request_id)` | entry_id、audit_id、outcome |
| `suspended` | `(exec_id,node)` | spec_json |
| `signals` | `(exec_id,name)` | data_json |
| `signal_sets` | `(exec_id,node,name)` | data_json |
| `sub_execs` | `(exec_id,node,lease_id,child_exec_id)` | batch_index、status、result_json |
| `subworkflow_links` | `(exec_id,node)` | link_json |
| `group_units` | `(exec_id,unit_idx)` | status、lease、attempt、deadline、committed_token、identity |
| `admissions` | `admission_key` | execution_id、result_hash、created_at |
| `idempotency` | `exec_id` | fingerprint、token、confirmed、lease_until、expires_at |
| `slots` | `slot_key` | limit |
| `slot_holders` | `exec_id` | slot_key |
| `queue` | `exec_id` | slot_key、deadline、enqueued_seq |
| `canceling` | `exec_id` | since |
| `durable_tasks` | `task_id` | exec/node/unit/type、payload_json、activation/auto_depth/port/generation、deliverable_at/intent_created_at、state、attempts、claimed_at、last_error |
| `trigger_state` / `trigger_dedup` | `(scope,key)` / `key` | value_json / expires_at（P1，可后置） |

- `schema_version` 用 `PRAGMA user_version` 与 `meta` 双写，打开时两者必须一致；不一致按“损坏”处理，不猜测。
- 迁移只允许向前；遇到高于当前代码支持的版本直接拒绝启动（fail closed），避免旧代码用错误语义解释新数据。
- 每个 blob 列不做独立版本号，统一由 `schema_version` 控制；如果某种 JSON 结构需要独立演进，再加 `*_format` 列。
- 迁移前做一次备份（SQLite backup 或停机文件复制）；迁移在一个事务内完成，失败则回滚并拒绝启动。

- **不要直接 `json.Marshal(engine.Task)`。** `Task` 的 `ActivationID`、`AutoDepth`、`Port`、`UnitIdx`、`DeliverableAt`、`IntentCreatedAt` 都标了 `json:"-"`（`engine/types.go:42-100`），直接序列化会静默丢掉重投所需的字段。持久任务行必须使用显式的 record struct（或给这些字段单独的列），不能复用公开 runner JSON 契约。
- **JSON 数字类型。** `json.Unmarshal` 会把整数解码成 `float64`；分布式后端也采用同样的普通 JSON 解码（`backend/providers/distributed/internal/rstate/output_codec.go:152-156`），所以这是与分布式对齐的行为。但本地纯内存模式在重启前后可能观察到 `int` 到 `float64` 的类型变化；依赖精确数字类型的节点/表达式必须显式规范化，测试要覆盖该差异。

### 6.2 graph 与 handler 版本变化

**graph 变化**

- 不能依赖镜像里的 `workflow_def` 作为 graph 的恢复来源：它只在提交上下文通过 `WithWorkflowDef` 携带定义时写入（`backend/providers/local/store_mirror.go:567-577`，`engine/submission_context.go:36-50`），可能是内联/空定义，且是原始 DSL 而非提交时编译后的图。新持久化层必须自己保存 `graph_json`。
- 执行里持久化的是提交时编译好的 `graph.Graph`（`engine/types.go:279-281`），`Graph` 有完整的 JSON 往返（`engine/graph/snapshot.go:538,610`，`graphSerializedForm` 在 `494`）。恢复时**原样反序列化并继续执行旧图**，不按当前 workflow registry 重新编译。原因：定义可能已经被编辑，重新编译会让一个“提交时 A 版本”的执行突然跑 B 版本，且节点索引/unit 结构可能不兼容。
- 如果 `UnmarshalJSON` 失败（截断、损坏、旧格式缺字段、grouped snapshot 缺 unit IR、body 缺 package，见 `snapshot.go:610-672`），该执行不能安全恢复：标记为 `failed`，错误写回 execution 行，保留原始数据供诊断。不要删除、不要猜测、不要用最新定义替换。
- 建议额外存一列 `graph_checksum`（对 `graph_json` 的 SHA-256），hydrate 时校验字节完整性；这与 `GraphHash` 是两件事：前者防文件损坏/截断，后者是图语义身份。
- graph hash 在持久化 JSON 里带有 `GraphHash` 字段，`UnmarshalJSON` 直接采用它（`snapshot.go:673`），不会重新计算；本地文件是可信输入，因此不强制重算。若需要防篡改，可在打开时用同一 payload 重算并比较（`assignGraphHash` 的 payload 在 `snapshot.go:380-417`），但这要求当前编译器的 hash 规则与写入时一致；跨版本升级时可能误报，建议只在同 schema 版本内校验。

**handler 版本变化**

- 节点元数据带 `Version`，`Registry.Get` 在 pinned version 缺失时按策略处理：默认 `VersionWarnFallback` 回退到最新并告警；`VersionStrict` 返回 `ErrHandlerVersionMismatch`（`execution/registry.go:12-37,195-242`，SDK 选项 `sdk/xflow/option.go:180-189`）。
- 持久化恢复不能默默改变语义。建议新增恢复策略：
  - `RecoverStrict`（推荐默认）：`Recover` 预检所有待执行节点的 handler 版本；缺失则把该 execution 标记 failed，错误为“durable recovery: handler version X not registered”。
  - `RecoverWarn`：沿用 `VersionWarnFallback`，记录指标/日志，允许继续。适合 xdesk 自己保证版本兼容的场景。
- 预检必须在 `Recover` 时做，而不是等到节点第一次 `AcquireTaskLease`：否则会变成无限重试/死信，现场很难定位。
- 子工作流的定义解析同理：恢复前宿主必须重新注册 child 定义；解析失败应 fail 该节点/执行，而不是用“最新同名定义”兜底。

### 6.3 单进程独占与文件锁

- 仅靠 SQLite 的文件锁不够：两个进程可以同时读写同一个数据库，并各自维护一份内存状态，导致双重调度。必须在状态目录上做应用级排他锁。
- 建议锁文件 `<db-path>.lock`，进程生命周期内持有 `LOCK_EX | LOCK_NB`；失败返回 `ErrDurableStateInUse`，并在错误里带路径与（尽力写入的）PID。
- Unix 用 `syscall.Flock`，Windows 用 `LockFileEx`（`golang.org/x/sys` 目前是间接依赖，`go.mod:90`；提升为直接依赖或写 build-tag 实现需要维护者确认）。不要用“锁文件存在与否”判断占用——崩溃会留下文件；必须用操作系统锁。
- 打开前对路径做 `filepath.Abs` + `EvalSymlinks`（若存在），避免同一文件被不同路径绕过锁。
- SQLite 文件权限设为 `0600`，目录 `0700`。状态里可能包含 params、输出、信号 payload 等敏感数据；纯 Go SQLite 没有 SQLCipher，至少依赖操作系统全盘加密，字段级加密作为开放问题。

### 6.4 保留、裁剪与容量

- 终态执行/节点/输出可以按 `TerminalRetention` 裁剪；裁剪只删除没有未完成 outbox、没有活跃子执行的执行。
- 死信、重放回执、admission、idempotency 不能和普通终态一起立刻删：它们有各自的幂等窗口。admission/idempotency 需要 TTL；死信默认保留到人工清理/重放。
- 恢复时不要因为“行数太多”而静默丢弃；超限应该拒绝启动或按明确策略裁剪并记录指标。
- `PRAGMA quick_check` 可以在启动时跑；全量 `integrity_check` 对大型库可能很慢，作为显式维护命令而不是默认启动路径。发现损坏时 fail closed，不自动重建。

---

## 7. 测试策略（问题 6）

### 7.1 共享契约测试

- 现有 `RunStateStoreContract(t, manifest, state)`（`backend/internal/statestoretest/state_store_contract.go:32`）已经在 `TestMemoryStateStoreContract` 与 `TestMemoryStateStoreContractWithStore` 上运行（`backend/providers/local/state_store_contract_test.go:11-27`）。新增 `TestDurableStateStoreContract`：用 `OpenDurableState` 创建临时文件，跑同一套契约，再关闭/重开跑第二遍（验证 hydrate 后行为一致）。
- 契约套件要求 AtomicStateStore、OutboxLeaser/Releaser、ExecutionStatusReader、SuspendedNodeCanceler、DurableSignalDeliverer 等能力（`state_store_contract.go:32-42`）；持久化实现必须原样通过，不能因为“落盘了”而改变返回值/顺序。
- 能力清单 `memoryCapabilities` 与 `Verify`/`CheckSet`（`capability_manifest_test.go:19-51,66-71`）要继续通过；新增的可选接口（如果有）必须同步登记。
- 在契约之外增加“重启不变式”测试：同一操作序列，分别在内存模式和持久模式下执行，比较 `Inspect`/`GetAllOutputs`/outbox 可见状态；重启后再比较一次。

### 7.2 崩溃注入

**进程内故障点**（快速、可覆盖所有迁移）：

- 在持久化层定义 `FaultPoint`：`beforeMemoryApply`、`afterMemoryApplyBeforePersist`、`duringPersist`、`afterPersistBeforeQueueInsert`、`afterQueueInsertBeforeAck`、`afterAck`、`beforeLeaseSweep`、`afterLeaseRevokeBeforeOutbox`、`afterSuspendBeforeOutbox`、`afterSignalConsumeBeforeOutbox`、`afterSlotChargeBeforeCreate`、`afterCreateBeforeSlotPersist` 等。
- 测试钩子可以在某个点返回 `panic`/`os.Exit`；用子进程 `SIGKILL` 比 panic 更接近真实（panic 还会跑 defer）。用 `exec.Command(os.Args[0], "-test.run=TestDurableCrashChild")` 的模式，环境变量指定故障点。
- 每个故障点后重新 `OpenDurableState` + `Recover`，断言：
  - 已 ack 的迁移不丢；
  - outbox 不丢条目（除非已死信且有记录）；
  - 节点最多只有一个有效租约；
  - 执行最终能到达终态，或明确停在 paused/waiting；
  - 槽位守恒；没有“非终态执行但既不在 holder 也不在 queue”；
  - 重复提交返回 duplicate/stale，不重复推进图。

**端到端崩溃**：

- 用真实 `NewLocal` + 临时目录 + 一个会阻塞/崩溃的测试 handler；在指定点 `SIGKILL` 子进程，父进程重启并等待完成。
- 覆盖：线性 DAG、扇出/扇入、cycle、group、map（含 1k 子项）、signal、timer、subworkflow、paused、queued（并发限制）、canceling、admission 重投、idempotency 重试。

**随机化/模型测试**：

- 生成随机图（DAG/循环/分组/挂起/映射）+ 随机故障点 + 随机重投顺序；跑固定 seed 集合。恢复后检查一组不变式，而不是只比较最终输出（最终输出可能因 at-least-once 重跑而相同，掩盖中间错误）。

### 7.3 性能基准

必须同时测“关闭”和“开启”，否则无法证明零开销：

- **关闭**：与当前 `memoryState` 对比 submit、commit、fan-out 的 ns/op、allocs/op、p99；目标是无统计显著回退（建议 <1%，且不新增 goroutine）。
- **开启，小工作流**：10 节点线性、1KB 输出；测每次迁移的持久化延迟、端到端耗时、SQLite 文件大小、WAL 大小。目标建议：`SyncNormal` 下 p99 迁移落盘 < 10ms（桌面 SSD），端到端 < 2x 内存模式；这些数值需要维护者确认。
- **开启，大扇出**：1000/10000 项 map；测 O(n) 行为，确认没有“每次子项完成重写父全量”的 O(n²)。
- **恢复**：10k/100k 执行的 hydrate 时间、租约扫描时间、outbox drain 吞吐；目标建议 10k 执行 < 2s。
- **写放大**：统计每次迁移写入的字节/行数；对比全量镜像与实体级写。若实体级仍不达标，再考虑批量提交/压缩/异步 checkpoint，但不能牺牲 ack 前的持久性。
- **fsync 对比**：`SyncNormal` vs `SyncFull` 的 p50/p99 与断电模拟；把结果写进文档，作为默认值依据。

### 7.4 soak / 故障演练

- 24 小时持续提交 + 周期 kill/restart；检查磁盘增长、WAL checkpoint、文件锁释放、goroutine 泄漏、恢复指标。
- 模拟磁盘满、只读文件系统、SQLite busy、迁移中断、schema 版本过高；确认全部 fail-stop 且错误可诊断。
- 模拟宿主忘记调用 `Recover`：确认执行不会派发，且日志/指标明确提示。

---

## 8. 里程碑（问题 7）

每个里程碑都应可独立合并，且默认内存路径行为不变。

| 里程碑 | 范围 | 验收标准 | 主要风险 |
|---|---|---|---|
| **M0 设计与不变式** | 本设计稿；故障点/写点清单；契约测试矩阵；恢复不变式文档 | 维护者评审通过；不变式可被测试表达；不改变任何运行时行为 | 设计假设与实现细节偏差 |
| **M1 持久状态底座** | `OpenDurableState`、文件锁、schema v1、hydrate、execution/node/counter/outbox/output 写穿；测试专用构造入口，不暴露公共选项 | 临时目录关闭/重开状态一致；契约套件在持久模式通过；关闭模式零开销基准通过；简单 DAG 在“仅 outbox 重投”范围内可恢复 | 漏写点；JSON 数字/时间格式偏差；WAL 写放大 |
| **M2 恢复激活** | 持久任务队列、outbox ack 顺序改造、启动租约回收、`Recover` 屏障、`WithDurableState` / `WithLocalDurableState` / `RecoverDurableState`、local 的 `StartBinding` | 在“commit 后/入队后/ack 后”三个 kill 点重启，任务不丢；Running/Committing 节点被回收重投；简单 DAG/扇出可完整恢复；默认关闭 | 队列重复/丢失语义；启动顺序；SDK 兼容性 |
| **M3 等待态恢复** | suspend/signal/timer、subworkflow link、map subExecs、group unit 的持久化与恢复 | 每个等待态各有一个 SIGKILL 端到端用例；timer 到期/过期恢复；child 在父进程停机期间完成可收敛；group 重投/提交不重复 | 信号在停机期间丢失（外部责任）；group/map 身份字段遗漏 |
| **M4 并发与入口语义** | slots/holders/queued、queue timeout、admission、idempotency、canceling、trigger dedup/state | 重启后槽位守恒、FIFO 保持、超时生效；admission 重投返回同一执行；idempotency 冲突检测跨重启；canceling 最终收敛 | 槽位重算错误；admission/idempotency 表膨胀；trigger 状态语义 |
| **M5 版本、迁移与运维** | handler/graph 版本策略、schema 迁移与备份、保留/裁剪、损坏处理、恢复指标、文档与 xdesk 接入示例 | 旧 schema 前向迁移成功；高版本拒绝启动；handler 缺失按策略失败；24h soak + 故障注入矩阵通过；运维文档可执行 | 迁移数据丢失；版本策略误杀；磁盘增长/敏感数据 |
| **M6 性能优化（按需）** | 实体级脏集/批量提交/压缩/检查点调优；必要时把全量验证从生产路径移除 | 基准达到 M0 约定目标；写放大可解释；不牺牲 ack 前持久性 | 优化引入新的漏写/顺序错误 |

建议先做 M1+M2 的最小闭环（能恢复 Running 节点和 outbox），再按 M3/M4 扩展等待态与并发语义；M5 不应拖到最后才做，因为 schema/版本策略一旦上线就很难改。

---

## 9. 开放问题（需要 xflow 维护者决策）

1. **公共 API 归属**：`WithLocalDurableState` 是否进入 `sdk/xflow` 的稳定 API，还是只作为 xdesk 可用的实验/内部选项？一旦公开，schema 和恢复语义就是兼容性承诺。
2. **存储后端抽象**：首版是否只支持 SQLite，还是把 `DurableState` 定义成接口（SQLite 默认，未来可换嵌入式 KV）？抽象会显著增加接口设计和测试面。
3. **文件锁实现**：是否接受新增 `golang.org/x/sys` 直接依赖（或 `gofrs/flock`）？Windows/macOS 双平台行为、网络文件系统（NFS/SMB）上的语义需要明确；是否允许在 NFS 上运行？
4. **fsync/RPO 默认值**：默认 `NORMAL`（进程崩溃不丢、整机断电可能丢最后几次提交）是否满足 xdesk 的产品承诺？还是默认 `FULL`、由用户显式降级？
5. **写失败策略**：fail-stop（推荐）是否可接受？还是需要“只读降级/继续内存运行 + 告警”？后者会破坏“已确认即持久”的契约。
6. **handler 版本缺失**：恢复时默认 fail execution 还是 warn fallback？是否允许按 workflow/节点配置覆盖？默认策略会直接影响升级体验。
7. **workflow 定义持久化**：是否要把 workflow registry / trigger 定义也持久化，做到“打开即恢复”，还是要求宿主在 `Recover` 前重新注册？后者简单，但把启动顺序责任推给了 xdesk。
8. **保留与配额**：终态执行、死信、admission、idempotency 各保留多久？磁盘配额/超限行为（拒绝启动、裁剪、告警）由谁决定？
9. **at-least-once 的产品表述**：xdesk 是否接受“节点可能重跑、副作用需幂等”？如果不接受，就需要在节点 SDK 层提供幂等键/去重存储，而不是只靠后端。
10. **多实例/多窗口**：同一用户打开第二个 xdesk 窗口时，是拒绝、只读，还是等待锁？错误信息如何呈现？
11. **敏感数据**：SQLite 明文包含 params/outputs/signals。是否要求字段级加密？纯 Go SQLite 不支持 SQLCipher，需明确依赖 OS 全盘加密还是实现应用层加密。
12. **signal 收件箱**：停机期间的外部信号是否需要一个持久收件表（本地 HTTP 入口先落库，恢复后消费）？这会把“至少一次”责任从调用方收回本地，但增加一个入口路径。
13. **与 `WithStore` 的长期关系**：是否在未来用持久化表生成审计投影、最终替代 `WithStore`？还是永久保持两套独立配置？
14. **时钟语义**：timer/timeout 使用墙钟 `AvailableAt`；机器休眠/改时间后的行为是否就是“迟到触发”？是否需要单调时钟补偿？

---

## 10. `engine/pin_data.go` 评估

### 10.1 机制

- 编译期 `assignPinData` 把 `pin_data` 写进 `NodeMeta.PinOutput`，默认模式 `test_only`，可选 `always`（`engine/graph/pin_data.go:21-109`）。
- 运行期 `handlePinnedNode` 在派发前检查：graph 有 `PinDataMode`、无环、节点索引/名称合法、unit 是普通 Node、执行仍活跃、`pinAppliesTo` 为真；满足则直接提交 `NodeStatusPinned`（System=true），不申请租约、不调用 handler（`engine/pin_data.go:27-117`，状态写入在 `89`）。
- `pinAppliesTo`：`always` 对所有执行生效；`test_only` 只对 `snap.TestRun` 生效（`engine/pin_data.go:121-134`）。
- 启用 pin data 会改变 graph hash：`NodeMeta.PinOutput` 参与 `Nodes` 哈希，`PinDataMode` 也进入 hash payload（`engine/graph/snapshot.go:57-60,380-417,440-442`）。因此它不是“同一个工作流的透明重放”，而是另一个图身份。

### 10.2 明确不能做的事

- **不能表达“某一次执行完成了哪些节点”。** pin data 是定义级静态数据，不是执行级进度；要让新执行跳过已完成节点，必须为这次重放克隆/构造一个带 pin 的定义。
- **覆盖范围有限。** 编译器会忽略 disabled、supply、group 成员、有环、FAF、map body 成员（`engine/graph/pin_data.go:21-109`）。因此 group 边界结果、map 子项结果、子工作流 child 的结果都不能靠它表达。
- **不能替代真实成功。** pinned 节点的状态是 `pinned`，不是 `success`；没有真实 handler 的副作用、日志、错误分类和审计。下游只看到 mock 输出。
- **`always` 是全局开关。** 文档本身警告它“仅适用于调试场景”（`engine/graph/pin_data.go:106`）。对生产定义开启会让**每一次**执行都跳过这些节点，而不仅是重放的那一次。
- **`test_only` 需要 TestRun。** 走 `test_only` 必须把执行标记为测试（`WithTestRun` 或 HTTP `test:true`），这会改变执行语义/展示；见 `engine/types.go:365-370`。
- **静态输出无法表达运行时依赖。** pin 的 mock 必须是 object，且 required 字段只做 warning（`engine/graph/pin_data.go:21-109`）；依赖本次参数/上游动态值的输出需要 xdesk 自己计算后写入新定义。
- **graph hash 变化。** 带 pin 的定义与原始定义不是同一个图身份，可能影响版本、编辑器元数据、审计和幂等键；不能悄悄替换原定义。

### 10.3 作为 xdesk 降级方案的条件

只有在以下条件全部满足时，才可以把它作为“重放已完成前缀”的显式功能（不是崩溃恢复）：

1. 工作流是无环普通 DAG，且不含 group、map、FAF、supply、disabled 节点、子工作流等待（否则 pin 会被忽略或语义不完整）。
2. xdesk 已经通过 Inspect/GetAllOutputs 拿到每个已完成节点的输出，并能把它们作为 object 写入 pin_data。
3. xdesk 为这次重放构造一个新的 workflow 定义/版本（或 inline 定义），设置 `always`，并明确展示“这是带 mock 的重放，不是原执行恢复”。
4. 下游表达式只依赖 pin 输出和本次输入；xdesk 负责校验 required 字段。
5. 重放执行与原执行有可追踪的关联（xdesk 侧记录 parent/replay-of 关系），且不会覆盖原执行记录。
6. 用户明确知道 `pinned` 节点没有真实副作用；如果副作用必须发生，不能使用 pin。

它不能解决：进程崩溃后继续原执行、outbox 重投、租约回收、等待信号/timer/subworkflow、并发槽恢复、幂等。把这些需求交给 pin_data 会在第一次遇到 group/map/信号时失效。

### 10.4 结论

**pin_data 是一个测试/调试期的静态 mock 能力，不是持久化恢复机制。** 对 xdesk 的简单 DAG，它可以作为“用户主动选择的重放已完成前缀”的降级功能，但必须显式、受限、可见，并且不能宣传为崩溃恢复。生产级恢复应使用本设计的 durable state；如果暂时不做 durable state，xdesk 至少要把“原执行已失败/已丢失，以下是一次新的重放执行”如实呈现。

---

## 11. 关键引用

- `backend/providers/local/backend.go:70-117` — `WithStore` 的能力边界：镜像、不会恢复。
- `backend/providers/local/backend.go:159-187` — `New` 无条件创建内存状态。
- `backend/providers/local/backend.go:292-315` — 本地只启动队列、outbox dispatcher、queue-timeout sweeper。
- `backend/providers/local/memory_state.go:30-114` — 内存状态全集。
- `backend/providers/local/memory_state.go:1299-1354` — 过期租约发现（含 group unit）。
- `backend/providers/local/atomic_state.go:395-478` — outbox 读取/交付租约/ready 规则。
- `backend/providers/local/atomic_state.go:498-605` — ack、失败计数、死信。
- `engine/atomic.go:394-462` — 交付租约、确定性 outbox ID、挂起恢复条目。
- `engine/atomic.go:505-643` — `FlushOutbox` 的 enqueue→ack 顺序（关键丢失窗口）。
- `engine/lease.go:495-587` — 租约回收与 legacy Pending 窗口。
- `backend/providers/distributed/internal/rstate/doc.go:1-110` — Redis 权威、Lua 原子迁移、outbox 至少一次。
- `backend/providers/distributed/internal/rstate/concurrency_state.go:70-112,264-308,545-612` — 并发槽/队列参考语义。
- `store/sqlstore/sqlitestore/sqlitestore.go:109-125,346-378` — 纯 Go SQLite、WAL、NORMAL、单连接。
- `store/models.go:12-82`、`store/interfaces.go:22-25` — 现有镜像 schema 不足以恢复。
- `backend/internal/statestoretest/state_store_contract.go:32`、`backend/providers/local/state_store_contract_test.go:11-27` — 契约测试入口。
- `engine/graph/snapshot.go:380-417,538,610` — graph hash 与 JSON 往返。
- `execution/registry.go:12-37,195-242` — handler 版本策略。
- `engine/observers.go:15-17,447-452`、`observability/metrics/metrics.go:425` — 重复提交结果的观测。
- `engine/graph/pin_data.go:21-109`、`engine/pin_data.go:27-134` — pin data 的编译期/运行期约束。

---

## 12. 评审意见（xdesk 侧，2026-10-24）

结论：**采用方案 B**。实现前先按以下 4 条修改设计，第 9 节的开放问题按 12.2 的答案处理。

### 12.1 修改意见

1. **counters 也要按 unit 拆行。** `counters` 表里的 `in_degrees_json`、`active_ins_json`、`advanced_json`、`scheduled_json` 是每个执行一个 JSON，每次节点提交都要整份重写，节点多的图会退化成 O(n²)。这和 3.5（1）里 map `subExecs` 的问题一样，应改成 `(exec_id, unit_idx)` 一行，只有 `remaining` / `failed` 留在执行级。
2. **大字段去重和外置。**
   - 节点输出不要在 `nodes` 和 `outputs` 两张表各存一份，只在 `outputs` 里存，`nodes` 里引用；
   - `graph_json` 按 `graph_checksum` 单独建表去重，执行行只存 checksum，避免同一个工作流跑 N 次就存 N 份图；
   - 超过阈值（例如 64 KB）的 params / output / signal payload 存成 blob（内容寻址），表里只放引用。
   
   这与 distributed 后端的大参数整改（P4，大字段外置到 objectstore）是同一个问题，两边应共用一套「大字段存储」接口，local 用文件或 SQLite blob 实现，distributed 用 objectstore 实现。
3. **fail-stop 只停引擎，不退出进程。** xdesk 里引擎和桌面应用在同一个进程。要求：
   - 进入 fail-stop 后，可以在进程内 `Stop` 引擎、释放文件锁，再重新 `NewLocal` + `RecoverDurableState`；
   - 提供状态查询或回调，让宿主知道引擎已进入 fail-stop 以及原因。
   
   5.1 和 3.5（5）里「让进程退出」的表述要改。
4. **把「恢复后重跑」告诉 handler（加进 M2）。** xdesk 要求自动化步骤声明是否幂等。对没声明幂等的步骤，恢复后不自动重跑，而是转成人工确认。现在只能事后从 `Attempt` 推断，来不及拦截。需要：
   - handler 的执行上下文带上「这是崩溃恢复后的重新投递」标记（例如 `Attempt.Recovered bool`，或者恢复时递增的 `RecoveryEpoch`）；
   - SDK 暴露 `WithCommitObserver`，或提供等价的恢复事件回调（见 4.11）。

### 12.2 第 9 节开放问题的答案

| # | 答案 |
|---|---|
| 1 公共 API | 先作为实验选项（命名或文档里标明 experimental），xdesk 用稳定后再承诺兼容 |
| 2 存储后端抽象 | 只支持 SQLite，不抽象成接口 |
| 3 文件锁 | 把 `golang.org/x/sys` 升为直接依赖；不支持 NFS/SMB，文档写明 |
| 4 fsync 默认值 | 默认 `NORMAL`，`FULL` 作为选项 |
| 5 写失败 | fail-stop，但只停引擎（见 12.1 第 3 条） |
| 6 handler 版本缺失 | 默认 `RecoverStrict`：明确失败，宿主重新注册后可以重试 |
| 7 workflow 定义 | 不持久化，由宿主在 `Recover` 前重新注册 |
| 8 保留与配额 | 终态执行 30 天；死信保留到人工清理；admission / idempotency 的 TTL 7 天。超出配额只告警，不拒绝启动。这些都可配置 |
| 9 at-least-once | 接受，前提是 12.1 第 4 条 |
| 10 多实例 | 由宿主保证单实例；xflow 返回 `ErrDurableStateInUse` 即可 |
| 11 敏感数据 | 不做字段级加密：宿主不把机密放进 params（只放引用名）；文件 0600、目录 0700，依赖系统全盘加密 |
| 12 signal 收件箱 | 暂不需要，宿主先记录再投递，丢了可以重投 |
| 13 与 `WithStore` | 保持独立；xdesk 不开 `WithStore` |
| 14 时钟 | 休眠或改时间后按「迟到触发」处理，同一个 timer 只触发一次，不补发 |

### 12.3 xdesk 需要的里程碑

至少做到 M1–M3，外加 M5 里的版本策略和 schema 迁移。M4（并发槽、admission、trigger）可以后做。12.1 的第 1、2 条要在 M1 定 schema 时就落实，第 3、4 条进 M2。
