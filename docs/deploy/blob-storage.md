# Blob 大对象层（claim-check）运维

本文档面向运维 xflow-server 的 blob 层：把 map 节点的大 items 数组放进对象存储，让 Redis 与任务队列只携带一个小引用。启动参数与校验在 `cmd/server/blob_options.go`，库层在 `backend/providers/distributed/blob.go`、`blob_gc.go` 与 `store/blob/`；所有 flag 名与默认值以 `cmd/server/main.go` 里的 `--blob-*` 定义为准。本文档如与代码不一致，以代码为准。

## 1. 概述

- 解决的问题：map 节点展开时，如果不做外置，每个批次任务都携带整个 items 数组（`items` 加 `all_items`），队列与 outbox 里的字节数随"批次数 × 条目数"增长（`engine/batch_items.go` 中 `BatchItemsStore` 的注释）。开启 blob 层后，items 数组只存一份为 blob，每个批次任务只携带 `items_ref` 和自己的 `[items_start, items_end)` 窗口。
- 当前接线范围：只有 map 节点的 items 数组走这条路径（`engine.externalizeExpansionBatches` 调用 `rstate.Store.ExternalizeItems`）。`store/blob` 的包注释还提到节点输出与任务体，但 `blob.Policy.Externalize` 的唯一非测试调用方就是 `rstate.Store.ExternalizeItems`，没有别的外置入口。
- 引用与落盘布局：引用是 `blob.Ref`（`store/blob/ref.go`），包含命名空间、未压缩字节的 SHA-256、大小、落盘大小与编码（空串或 `zstd`）。对象在目录下的路径是 `<blob-dir>/blobs/<命名空间的小写 hex>/sha256/<hex 前 2 位>/<hex 第 3-4 位>/<完整 hex>`（`store/blob/key.go` 的 `objectKey`）；每个对象的第一个字节是帧头，`0x00` 表示后面是原始字节，`0x01` 表示一个 zstd 帧（`store/blob/doc.go`）。
- 默认完全关闭：不带任何 `--blob-*` 时 `serverConfig.blobRequested()` 为假，`buildBlobConfig` 返回 `nil, nil` 且不碰数据库与文件系统，`appendBlobOption` 原样返回选项也不记日志，行为与没有这一层时相同。
- "是否请求了 blob 层"的判定（`blobRequested`）：`--blob-dir` 非空、`--blob-inline-max` 非 0、`--blob-enable-ref-writes`、`--blob-compress`、`--blob-gc-interval` 非 0，或 `--blob-gc-grace` / `--blob-gc-batch` 取了与默认值不同的非零值，任一成立即视为请求。所以只改 `--blob-gc-grace` 而没给 `--blob-dir` 会启动失败（第 8 节）；把它们显式写成默认值则不算请求。

## 2. 前置条件

以下几项缺一不可。`validateBlobFlags` 在解析参数阶段、尚未打开任何连接前，就会拒绝不满足 1-3 项的组合（报错见第 8 节）。

1. Redis 后端：必须配 `--redis` 或 `--redis-mode`，不能是 `--memory`。没有配任何 Redis 或 HA 拓扑参数时，`parseServerConfig` 会把 `cfg.memory` 置为真，同样被拒。
2. `--mysql-dsn`：blob 的生命周期元数据（对象与 link）存在数据库里，`sqlstore.NewBlobIndex` 使用与 SQL store 同一个 `*gorm.DB`（`cmd/server/main.go` 里的 `enrollDB`）。
3. `--blob-dir`：字节存放的目录，对应 `objectstore.NewFSStore(cfg.blobDir)`。**目录必须事先存在**：`NewFSStore` 不创建根目录，启动探针 `probeBlobDir` 在目录不存在时直接报错、也不会隐式创建（缺失的挂载点不应被静默建成本地空目录）。`FSStore.PutObject` 只会在根目录之下按需 `MkdirAll` 子目录。
4. 数据库里要有 `xflow_blob_objects` 与 `xflow_blob_links` 两张表（见下文"数据库表"）。

### 共享卷（第一硬要求）

`--blob-dir` 背后是 `objectstore.FSStore`：把对象键直接映射成目录下的本地文件路径，没有任何跨机器同步（`store/objectstore/fsstore.go` 的 `FSStore`、`resolve`）。因此：

- 多个 xflow-server 副本时，所有副本必须把同一个共享卷挂到各自 `--blob-dir` 指向的路径上，`--blob-dir` 的帮助文案同样这么要求。副本各自使用本地盘时，副本 A 写入的对象副本 B 读不到。
- 读不到的后果不是重试，而是批次被判失败：`FSStore.GetObject` 返回 not found，经 `ObjectBacked.readObject`（`store/blob/objectbacked_read.go`）变成 `blob.ErrNotFound`，再被 `rstate.classifyResolveError`（`backend/providers/distributed/internal/rstate/blob.go`）标成永久错误，详见第 7 节。
- 本文档不指定共享存储产品。"一个副本写入后另一个副本是否立即可读"取决于所选文件系统的语义，代码里没有对此做假设也没有检查，需自行评估。
- 读取 blob 的代码在 server 进程内（`control.Core.dispatchSubgraphLease` 调用 `BuildSubgraphLease`，再到 `resolveBatchView`）。`sdk/runner` 与 `service/runner` 中没有对 `store/blob` 的引用，据此判断 runner 进程不需要挂这个卷（推断，未做运行验证）。

**人工验证共享卷。** 启动探针通过不代表共享（见第 7 节 (b)），要实测：在副本 A 上往 `--blob-dir` 根目录写一个名为 `.shared-check-<主机名>` 的小文件（`<主机名>` 换成该副本的主机名，便于区分），到副本 B 上对该路径 `ls`/`cat` 核对，再在任一副本删除。这个文件不在 `blobs/` 前缀下、不进 Index，回收器不会处理它，必须手工删除。文件在副本 B 上可见的延迟取决于所选文件系统的语义（见上一条）：暂时看不到时多等几秒再看，仍看不到就是没共享。不要用启动探针代替这一步——探针只在本进程内写、读、删。

### 数据库表

blob 元数据用两张表，DDL 在 `db/xflow_schema.sql`：

| 表 | 内容 |
|---|---|
| `xflow_blob_objects` | 每个对象一行，主键 `(namespace, digest)`；记录未压缩大小、落盘大小、编码、围栏用的 `generation`、状态（`live` 或 `deleting`）、最近引用时间 `last_ref_at` |
| `xflow_blob_links` | 执行对 blob 的引用，主键 `(namespace, exec_id, digest)`，带过期时间 `expire_at` |

- 需要手工执行 DDL，不会自动迁移。`sqlstore.AutoMigrate` 的注释写明只用于开发与测试，生产 schema 由 `db/xflow_schema.sql` 管理（`store/sqlstore/provider.go`）；`mysqlstore.New` 不建表；`cmd/server` 里只有 dev 的 SQLite 执行存储会自动迁移。
- `sqlstore.NewBlobIndex` 不检查表是否存在（`store/sqlstore/blob_index.go`），启动时的 blob 接线（`buildBlobConfig`、`ConfigureBlob`）也不查表。缺表不会让进程启动失败，而是在第一次写 blob 或第一次回收时才报错（回收的日志见第 5 节）。上线前先确认两张表已建。
- DDL 对 `namespace`、`exec_id`、`digest` 列使用 `utf8mb4_0900_bin` 排序规则，目的是逐字节比较、避免 `Tenant` 与 `tenant` 折成同一行。目标库是否支持该排序规则需自行确认。

## 3. flag 参考

全部定义在 `cmd/server/main.go`（约 306-312 行）。`--blob-gc-grace` 的 1h 与 `--blob-gc-batch` 的 200 是**本二进制替 CLI 选的默认**（常量 `defaultBlobGCGrace` / `defaultBlobGCBatch`，`cmd/server/blob_options.go`）；库层的 `distributed.BlobConfig.Grace` 与 `Batch` 刻意没有默认值——写者的 link 过期边距与回收器的 grace 必须一致，只能由调用方选定（见 `blob_options.go` 开头注释与 `backend/providers/distributed/blob.go` 的字段注释）。

| flag | 类型 | 默认值 | 含义 | 校验 |
|---|---|---|---|---|
| `--blob-dir` | string | `""` | blob 目录（FSStore 根）。设为非空即开启这一层 | 其他 `--blob-*` 给出时必须非空 |
| `--blob-inline-max` | int | `0` | 保持内联的编码负载最大字节数；`0` 表示库默认 64 KiB（`defaultBlobInlineMax = 64 << 10`，`backend/providers/distributed/blob.go`） | 不得为负 |
| `--blob-enable-ref-writes` | bool | `false` | 是否把新的大负载写成 Ref。`false` 时这一层只读已有 Ref、从不落新对象 | 需要 `--blob-dir` |
| `--blob-gc-grace` | duration | `1h` | 无引用 blob 在回收器可以删除之前还要保留多久 | 必须为正 |
| `--blob-gc-batch` | int | `200` | 单轮回收最多清扫的过期 link 数、也是最多检查的候选对象数 | 必须为正 |
| `--blob-gc-interval` | duration | `0` | 两轮回收之间的等待；`0` 表示采集循环自己的默认 5m（`gcDefaultTick`，`blob_gc.go`） | 不得为负 |
| `--blob-compress` | bool | `false` | 落盘时用 zstd 压缩（只有确实变小才压） | 需要 `--blob-dir` |

校验入口是 `validateBlobFlags`，在 `parseServerConfig` 里、任何连接打开之前执行。两条容易踩的规则：

- **数值校验与这一层开不开无关**：`--blob-inline-max` / `--blob-gc-grace` / `--blob-gc-batch` / `--blob-gc-interval` 即使没配 `--blob-dir` 也会先查（注释原话：开启当天才发现的拼写错误比启动时发现的更糟）。
- **任何 `--blob-*` 偏离默认都必须给 `--blob-dir`**，并同时给 Redis 后端与 `--mysql-dsn`，否则拒绝启动。

启动成功时 `appendBlobOption` 记一行日志，内容只有 `ref_writes` / `inline_max` / `compress` 三个值，**不含目录路径**：

```
xflow-server: blob store enabled (ref_writes=true inline_max=0 compress=false)
```

## 4. 上线与回滚顺序（不可逆性）

开启 ref 写入是这个功能里唯一的不可逆动作，代码里**没有**任何版本协商机制：进程不会声明"我认识 Ref"，也不会检查对端是否认识，保证完全靠运维顺序。按以下两步走：

**第一步：所有副本先带能力、不带写入。** 给每个 xflow-server 配 `--blob-dir` + `--mysql-dsn` + Redis 后端，`--blob-enable-ref-writes` 保持 `false`。**每个副本都必须配上 `--blob-dir`，不能留着不带 `--blob-*`**：没有 blob 配置的副本在解析 Ref 时得到 `rstate.ErrBlobNotConfigured`。这个错误刻意不标成永久（`rstate/blob.go` 的注释：它描述的是该进程的装配而不是数据，别的副本也许能读），因此不会被判失败，而是走重试，但这个副本自己永远读不出来。此状态下 `rstate.Store.ExternalizeItems` 在未启用时直接返回"未外置"（`rt == nil || !rt.enabled`），负载全部维持内联；而 `ResolveItems` 不看 `Enabled`——读路径与写开关无关，即使当前一个 Ref 都还没写过，这也正是第二步的安全前提。

**第二步：确认所有副本都在运行认识 Ref 的版本之后**，再以 `--blob-enable-ref-writes=true` 滚动。此后超过 `--blob-inline-max` 的 items 数组开始变成 `items_ref` 写进任务负载。

**为什么不可逆**：`--blob-enable-ref-writes` 的帮助文案写明"A version that does not know Refs silently loses that data"。旧二进制具体怎么丢、丢在哪一步，不在本仓库的代码里（旧版本没有 `store/blob`），本文档不做推断；运维上只需记住：开启 ref 写入之后，不得再让不认识 Ref 的进程接触这些任务。

**回滚到不认识 Ref 的旧二进制时的做法**（代码不会替你做这件事，以下顺序是按代码行为推出的）：

1. 先把所有副本的 `--blob-enable-ref-writes` 关回 `false`，其余 blob flag 保留——此时读路径还在，已写入的 Ref 仍可解析。
2. 等已写成 Ref 的负载处理完毕或过期，再降级。等待多久**需要自行评估**，代码里没有一个"还有多少活跃引用"的查询接口。可用来估算的量（`blobLinkExpiry`，`backend/providers/distributed/internal/rstate/blob.go`）：link 过期时间 = 外置时刻 + 2×执行保留期 + grace；执行保留期取"进程已知值"与"执行创建时持久化的保留记录"中较大的一个，进程默认 24h（`rstate.DefaultExecTTL`）；没有任何非测试代码调用 `distributed.WithExecTTL`，所以服务端用默认值，单次调用可由 SDK 的 `WithExecutionTTL` 覆盖；挂起会经 `extendExecTTL` 续期，续期后的执行可能活得比 link 长（见第 7 节缺口 a）。link 过期之后，对象还要满足最近引用时间早于 `--blob-gc-grace` 才会成为回收候选（`BlobIndex.Candidates`）。
3. 只有确认队列与 outbox 里已不存在携带 `items_ref` 的任务之后再降级；否则就是上面说的静默丢数据场景。代码里没有直接查询这一点的接口，需自行评估。

## 5. GC 与保留

### Collector 做什么

`blob.Collector.Run`（`store/blob/collector.go`）一轮依次做三件事：

1. `Index.PruneLinks`：删除最多 `Batch` 条已过期的 link（只是给表瘦身，不改变哪些对象是候选）。
2. `Index.Candidates`：列出最多 `Batch` 个垃圾候选。SQL 实现（`store/sqlstore/blob_index.go` 的 `BlobIndex.Candidates`）的条件是：`last_ref_at < now - grace`，**并且**没有任何 `expire_at > now` 的 link 指向它；按最近引用时间从旧到新排序。
3. 对每个候选做 `MarkDeleting`（在对象锁下复查并领取围栏）、再 `Purge`（持锁调用 `Store.Delete` 删字节，成功才删元数据）。写者在领取与删除之间把对象救活时，这次删除被放弃，计入 `Fenced`；被别的采集器抢先或不再是垃圾的，计入 `Skipped`；字节删除失败的计入 `Failed`，对象保持被领取状态，下一轮重试。

### 什么时候、在哪里跑

- 只在**消费进程**里跑：采集循环在 `Backend.bindHandler` 里启动，且 `if !b.consumer` 先于它返回（`backend/providers/distributed/backend.go`）；`WithBlobStore` 的注释也写明 `WithConsumer(false)` 的纯 API 实例不会启动它。`cmd/server` 经 `sdk/xflow.NewServer` 装配 `service/apiserver`，后者固定用 `WithConsumer(true)`，所以每个 xflow-server 副本各跑一个循环；多副本并发是允许的，由 Index 仲裁。
- 节奏（`backend/providers/distributed/blob_gc.go`）：启动后**等一个间隔才跑第一轮**（重启不立即付出一轮代价）；间隔默认 5 分钟（`gcDefaultTick`），由 `--blob-gc-interval` 覆盖。某一轮检查的候选数恰好等于 `Batch` 时，说明可能还有剩余，立刻接着跑下一轮，一个间隔内最多连跑 10 轮（`gcDefaultMaxRuns`）。按默认值，一个间隔内最多检查 10 × 200 个候选。
- 失败行为：一轮出错记日志、计入失败指标，结束本次间隔，下个间隔再来（日志含 `blob gc pass failed`）；`skipped` 或 `failed` 非零时另记一行 `blob gc pass`。唯一让循环永久退出的是 `blob.ErrDeleteUnsupported`（日志含 `blob gc stopped: the blob store cannot delete objects`）；`FSStore` 实现了删除，所以这条对本部署不会出现。
- 可观测：`xflow_blob_gc_deleted_total`（实际删除数）、`xflow_blob_gc_skipped_generation_total`（被围栏放弃的数）与 `xflow_blob_gc_failed_total`（失败数：本轮对象删除失败数，整轮失败时再加 1，进程关闭导致的取消不计）。`Skipped` 仍只有日志；`Failed` 与整轮失败既记日志也计入指标（`observability/metrics/blob.go` 注释）。

### grace 与 link 过期边距

- 写者侧：map 的 items 外置后，用 `blobLinkExpiry` 写一条 link，过期时间 = `now + 2×ttl + grace`（`backend/providers/distributed/internal/rstate/blob.go`）。`ttl` 是执行的保留期；系数 2 覆盖"死信条目在第一个 ttl 之后拿到新 ttl"这第二个窗口；`grace` 是回收器自己的边距。
- 回收侧：同一个 `--blob-gc-grace` 同时给 `Collector.Grace`（候选条件里的 `last_ref_at` 门槛）。同一进程内两边来自同一份配置（`backend.go` 在 `ConfigureBlob` 前只补一次默认值，再由补过的那份配置构造回收器，注释的意图是让 grace 约定的两侧看到同一个值）。
- **所有副本必须使用相同的 `--blob-gc-grace`**。`cmd/server/blob_options.go` 开头注释：写者的 link 边距与回收器的 grace 必须一致；`store/blob/index.go` 的 `Index.Link` 注释进一步要求 `expireAt` 大于"执行保留期 + 回收器的 grace"，否则 link 什么也保护不了。副本间 grace 不同，就等于让某个副本写的 link 比另一个副本的回收器认为的安全边距短。
- 各副本的系统时钟要同步：`buildBlobConfig` 用 `sqlstore.NewBlobIndex(db, nil)`，时间取进程自己的 `time.Now`，不是数据库时间；`CollectorOptions.Grace` 的注释要求 grace 大于"写者从 `Register` 到 `Link` 的最慢耗时 + 共享 Index 的各节点间时钟偏差"。

### 调小 grace 的风险

- 写者的 `Put`（先 `Register`）与 `Link` 之间有一个窗口。grace 小于这个窗口加时钟偏差时，回收器可能在两步之间领取对象。代码对此有兜底：`Link` 返回 `ErrFenced` / `ErrNotFound` 时 `Policy.Externalize` 重写并重新 link，最多 3 次，仍失败则该负载留在内联、不是失败（`store/blob/policy.go`），并计入 `xflow_blob_put_total{result="error"}`。后果是多写字节、外置失效，不是丢引用。
- 更实质的风险是 link 边距变短：新写的 link 过期得更早，执行活得比 link 长的概率变大，见第 7 节缺口 a。已写入的 link 保留写入时算出的过期时间（`Link` 只会把过期时间往后移），所以改 grace 只影响此后新写的 link；需要所有副本一起改并重启。
- 调大 `--blob-gc-grace` 只是让垃圾多留一会儿，换来更大的安全边距和更多磁盘占用。

## 6. InlineMax 口径

- 判据是**未压缩**的编码字节数：对 map 的 items 数组做 `json.Marshal` 得到紧凑 JSON，其长度 `<= InlineMax` 就留在内联，多一个字节就外置（`rstate.Store.ExternalizeItems` 的 `len(data) <= rt.inlineMax`；`blob.Policy.Externalize` 用同样的比较，`PolicyOptions.InlineMax` 注释写明"size is that of the uncompressed bytes"）。它不是调用方提交的原始请求体大小。
- 与 `--blob-compress` 无关：压缩发生在 Store 内部、判据之后。`--blob-compress` 只改变落盘大小；只有 blob 不小于 1024 字节（`blob.DefaultMinCompress`）且 zstd 帧确实更小时才压缩，否则存原始字节（`ObjectBacked.encode`）。
- `--blob-inline-max=0` 在 server 里表示 **65536 字节**（64 KiB）：`distributed.BlobConfig.withDefaults` 把 0 换成 `defaultBlobInlineMax = 64 << 10`。库层 `rstate.BlobConfig` 的 `0` 本来是"外置所有非空负载"，但 server 路径永远先经过 `withDefaults`，所以通过 CLI 无法得到这种设置。
- 单个 blob 的上限是 `blob.DefaultMaxBytes`（64 MiB，未压缩），`buildBlobConfig` 没有覆盖它，CLI 上也没有对应 flag。items 数组超过它时 `Store.Put` 返回 `blob.ErrTooLarge`，`ExternalizeItems` 把这种情况当作"保持内联"而不是失败（避免开启这一层反而让原本能跑的 map 报错），并计入 `xflow_blob_put_total{result="too_large"}`。

## 7. 已知缺口

### (a) link 没有续期

link 只在外置的那一刻写一次，过期时间固定为 `now + 2×ttl + grace`。`ResolveItems` 的注释把这一点列为 Known gap：执行保留期可被 `extendExecTTL` 续期，续期后的执行可以活得比 link 长；续期 link 需要执行 ID，而 Ref 和 `BatchItemsStore.ResolveItems` 的签名都不带执行 ID，所以这里无法调用 `Index.Link`，也没有尝试。

后果：一个批次在 link 过期且 blob 被回收之后才被重试或才开始执行，读取得到 `blob.ErrNotFound`。这是永久错误，**不会无限重试**——该批次被判为失败（机制见下文 (c)），map 节点按自己的 `OnError` 收尾。代码里没有"link 快过期"的提前告警或续期入口。按上述成因推断，挂起时间长的执行、重试间隔拉得很长的批次风险最高；这是推断，未做故障注入验证。

### (b) 启动探针不证明共享卷

`probeBlobDir` 在目录里写一个 `.probe-<16 位 hex>` 对象、读回、再删除，同一进程里完成。它能证明目录存在、可写可读可删，**不能证明**这个目录是各副本共用的同一份存储：每个副本都挂着自己的本地目录时探针一样会通过。共享卷是否真的共享，只能靠部署层面验证（例如在一个副本写文件，到另一个副本核对）。

另外两条与这个目录有关的现状，都是读代码得出的，没有跑过故障注入：

- 进程恰好在探针写入与删除之间被杀，可能在 `--blob-dir` 根下遗留 `.probe-*` 文件。它不登记在 Index 里，Collector 不会清理。
- FSStore 写入用 `.tmp-*` 临时文件再改名。崩溃遗留的 `.tmp-*`，代码里没有清理它的路径：FSStore 的清扫只在 `MaxBytes > 0` 时运行（`fsstore.go` 的 `enforceBudget`），而 `buildBlobConfig` 构造的 FSStore 没有设置 `MaxBytes`。

### (c) 读不到对象时的失败模式与可观测信号

读取走 `rstate.Store.ResolveItems`（`backend/providers/distributed/internal/rstate/blob.go`）。下表是各种错误的归类：

| 条件 | 归类 | 处理 |
|---|---|---|
| `blob.ErrNotFound`、`ErrCorrupt`、`ErrNamespaceMismatch`、`ErrInvalidRef`、`ErrTooLarge`；引用不是 blob 引用；读回的字节不是 JSON 数组 | 永久，包进 `types.ErrPermanent`（`markPermanent`） | 不重试，该批次判失败 |
| `blob.ErrTransient`（Store 或 Index 的未分类故障，如 I/O、网络、数据库错误、上下文取消） | 不标永久 | 按普通错误重试；`dispatchSubgraphLease` 路径上是"claim 重新排队并把错误作为 poll 错误返回"，`group_control_loop.go` 的注释指出 runner 的 pollLoop 遇到任何 poll 错误都会结束其会话 |
| `rstate.ErrBlobNotConfigured`（该进程没有 blob 配置） | 不标永久（它描述的是装配，别的副本也许读得到） | 同上。某个副本漏配 `--blob-dir` 时，按这段代码的行为推断，落到它身上的批次会反复重新排队而不是失败（推断，未做运行验证） |

判失败的过程：

- 服务端建批次租约时（`engine.BuildSubgraphLease` 经 `resolveBatchView` 读取），`service/control/group_control_loop.go` 的 `dispatchSubgraphLease` 对 `types.IsPermanent(err)` 的错误调用 `Engine.FailBatchTask`，并记一条 Warn 日志：`batch lease permanently unbuildable; failing the batch`，字段有 `execution_id`、`node_name`、`claim`、`err`。引擎直接执行批次的路径（`engine.ExecuteBatch`，`engine/expand.go`）对永久错误调用同一个 `settleUnreadableBatch`；这条路径上是否有对应的日志行未确认。
- `settleUnreadableBatch` 把批次按失败提交，原因写入批次结果的 `_error`，文案以 `batch items unavailable: ` 开头，后面原样拼接上游错误。读不到对象时，经 `dispatchSubgraphLease` 路径的完整文案形如：`batch items unavailable: build subgraph lease: resolve batch items: xflow: permanent error: blob: get for sha256:<摘要前 12 位> (<字节数> bytes): object not found`；经 `ExecuteBatch` 路径时没有 `build subgraph lease: ` 这一段。末尾 blob 错误只含固定用语、不含路径，原因取自固定词表：`object not found`、`content does not match ref`、`ref belongs to another namespace`、`invalid ref`、`data too large`（`store/blob/policy_errors.go` 的 `permanentClasses`）。
- 失败批次之后 map 节点按它自己的 `OnError` 收尾。`FailBatchTask` 的注释写明：失败批次是结果里的一个洞，`continue_on_error` 补不上。
- 注意这个故障面：共享卷没挂上、但目录本身存在（空目录）时，启动探针照样通过，之后所有读取都得到 `object not found`，于是读到的每个 Ref 对应的批次都会被判失败。排查时先确认挂载。

可观测信号（`observability/metrics/blob.go`；帮助文案在 `observability/metrics/metrics.go`）：

| 指标 | 含义 |
|---|---|
| `xflow_blob_put_total{result}` | 尝试把负载存为 blob 的次数。`result` 为 `ok`、`too_large`、`error`；留在内联的小负载不计入 |
| `xflow_blob_get_total{result}` | blob 读取次数。`result` 为 `ok`、`missing`、`corrupt`、`namespace`、`error`；集合之外的值被导出为 `unknown`，应保持为 0 |
| `xflow_blob_gc_deleted_total` | 回收器实际删除的 blob 数 |
| `xflow_blob_gc_skipped_generation_total` | 被围栏放弃的删除数（写者在领取与删除之间把对象救活） |
| `xflow_blob_gc_failed_total` | 回收失败数：本轮对象删除失败数，整轮失败（`err != nil` 且非进程关闭取消）再加 1；任一增长都值得排查 |

- 五个指标都不带 namespace 标签，且是**惰性注册**：没开 blob 层、或还没发生对应事件时没有序列（`xflow_blob_gc_*` 只有第一次删除、第一次围栏或第一次失败之后才出现），告警规则不要依赖"序列存在"。
- 只有给 server 配了 `--metrics-addr` 才有指标（`cmd/server/main.go` 里 `metrics.New()` 只在它非空时调用）。
- `xflow_blob_get_total` 的 `missing` 与 `corrupt` 对应上表的永久错误。帮助文案本身就建议对任意 `corrupt` 或 `missing` 告警。`namespace` 是隔离围栏，应为 0。
- 引擎在进程内缓存已解析的 items 数组（`batchItemsCache`，最多 4 项），同一个 map 的后续批次命中缓存时不会读 Store，所以 `get_total` 不等于批次数。
- 回收器的 `Skipped` 没有指标，只有日志；`Failed` 与整轮失败除日志外还计入 `xflow_blob_gc_failed_total`（见第 5 节）。

## 8. 排障

**先看这一点：blob 相关的启动错误文案都不含 `--blob-dir` 的值。** `cmd/server/blob_options.go` 的注释写明原因（目录路径可能含用户名），`probeError` 把标准库路径错误重建为只剩"操作名 + 底层 errno"。启动日志 `blob store enabled (...)` 也不带路径。所以报错时要对照你自己实际配置的 `--blob-dir` 与挂载点逐项检查，日志不会告诉你是哪个目录。

### 参数解析阶段（`validateBlobFlags`，尚未连接任何后端）

报错以裸文案输出，不带前缀：

| 报错文案 | 原因 | 处理 |
|---|---|---|
| `--blob-inline-max must not be negative (0 means the library default)` | `--blob-inline-max` 为负 | 改为 0 或正数 |
| `--blob-gc-grace must be positive` | `--blob-gc-grace` 小于等于 0 | 改成正的时长，如 `1h` |
| `--blob-gc-batch must be positive` | `--blob-gc-batch` 小于等于 0 | 改成正整数 |
| `--blob-gc-interval must not be negative (0 means the collection loop's own default)` | `--blob-gc-interval` 为负 | 改为 0 或正的时长 |
| `--blob-* settings need --blob-dir, the directory blobs are stored in` | 有 `--blob-*` 偏离了默认值，但没给 `--blob-dir` | 补 `--blob-dir`，或去掉这些 flag |
| `--blob-dir needs a Redis backend (--redis or --redis-mode); it cannot take effect with --memory or without Redis` | 没配 Redis，或处于 `--memory` | 配 `--redis` 或 `--redis-mode` |
| `--blob-dir needs --mysql-dsn: blob metadata is kept in the database` | 没配 `--mysql-dsn` | 补 `--mysql-dsn` |

前四条数值校验与是否开启 blob 层无关，没配 `--blob-dir` 也会触发。

### 启动接线阶段（`appendBlobOption`，在 MySQL store 打开之后）

这一步失败时，外层会加前缀 `blob store: `（`cmd/server/main.go`）。因为它排在打开 MySQL store 之后，数据库连不上会先于下表的错误出现。

| 报错文案（去掉 `blob store: ` 前缀） | 原因 | 处理 |
|---|---|---|
| `blob dir probe: stat: stat: no such file or directory` | `--blob-dir` 指向的目录不存在。探针不会替你创建 | 确认共享卷已挂载、路径与配置一致，建好或挂好后重启 |
| `blob dir probe: stat: not a directory` | 路径存在但不是目录 | 检查路径是否指到了文件 |
| `blob dir probe: write: open: permission denied` | 目录不可写。末尾 errno 文案来自操作系统，只读挂载等其他原因会显示别的 errno | 检查运行 server 的用户对目录的写权限，以及挂载是否只读 |
| `blob dir probe: read: ...`、`blob dir probe: delete: ...` | 写成功，但读回或删除失败 | 检查文件系统与共享存储的健康状态 |
| `blob dir probe: read: content read back differs from content written` | 读回内容与写入不一致 | 存储不可靠，或有别的进程在改这个目录 |
| `blob dir probe: <write/read/delete> failed` | 底层错误不是路径错误，只保留阶段名 | 查看存储侧日志 |
| `blob dir probe: nonce: ...` | 系统随机数读取失败 | 检查运行环境 |
| `blob config: --blob-dir needs a database (--mysql-dsn)` | `buildBlobConfig` 拿到的数据库句柄为空 | 正常 CLI 路径已被上一节的校验挡住，出现说明接线被改动过 |

探针遗留对象的清理：探针写入的 `.probe-*` 对象在每条路径上都会被 `defer` 删除；读写失败的路径同样清理，除非进程被强杀（见第 7 节 (b)）。以 root 运行时目录权限位不起作用（`cmd/server/blob_options_test.go` 里对此有说明），只读权限造成的失败在 root 下可能不会被探针发现。

库层的校验错误（`distributed backend: blob config ...`、`distributed: configure blob: ...`、`distributed: start blob gc: ...`、`apiserver: Config.Blob requires a Redis-backed control plane ...`）在正常 CLI 路径上被上面的校验挡在前面，只有直接嵌入 SDK 的调用方会看到。

### 启动成功但运行异常

- 缺表：启动不报错（第 2 节）。表没建时的报错来自数据库驱动，文案需自行对照；回收器侧表现为日志 `blob gc pass failed`，其 `error` 字段已经过 `sanitizeLogError` 脱敏。
- 批次成批失败、`batch items unavailable: blob: get ... object not found`：先查共享卷是否在所有副本上挂载到同一路径，再对照第 7 节。
- 看不到 `xflow_blob_*` 指标：确认配了 `--metrics-addr`，并确认对应事件已发生过（指标惰性注册，第 7 节 (c)）。
- 磁盘只增不减：确认至少有一个消费进程在跑（纯 API 实例不启动回收器，第 5 节），再看 `xflow_blob_gc_deleted_total` 与 `--blob-gc-grace`、link 过期时间。
