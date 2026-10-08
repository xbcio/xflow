# 运行时缺口 遗留项

2026-10-03 一轮缺口收口（KEK 轮换、server/runner 凭据热加载、ADR-D4 编辑器元数据、
Node Group 组级 `on_error`、gRPC `AckActivation`、G1 clean-SHA 重签 `c39a271`）之后
仍开放的条目。按「不做会怎样」排序，不按工作量。

2026-10-08 第二轮收口（分支 `fix/runtime-gaps-closeout`，相对基线 `e3478d7` 共
42 个 commit）关闭了原条目 1–6、9（配置面）、12（工具面）、13、14，以及同期新
登记的 gRPC 组结果传输缺口；新登记的另外三处（`disabled` 的运行时消费者、组结果
失败成员的对外类型、map batch 续期的目录元数据）仍然开放。条目已按体例重排编号，
旧号保留在「已关闭」条目的括注里。

每条都写明出处：缺口本身的现状说明留在所属设计文档里，这里只记「打算改的东西」。
关闭一条时，把它移到文末「已关闭」并写上 commit。

范围：只收运行时缺口。Relay Gateway / remote SDK（D1 支持矩阵已排除）与已被显式
否决的设计（如 supply 的 `default: <bytes>`）不在此列；出处文档与实现之间的纯陈旧
偏差也不单独立项，在关闭对应条目时顺带标注。

## 待办

### P1 — 会造成重复执行、静默失效或发布门不稳

1. **`disabled` 节点没有运行时消费者。** `NodeDef.Disabled` 在非测试代码里只有三处
   读取：`execution/param_validation.go` 只跳过参数校验、`backend/workflowhash` 只让
   它参与哈希、`engine/graph/pin_data.go` 只在 pin 语境下发一条告警。
   [DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) §3.1「节点禁用行为」承诺的语义
   （编译后变 `skipped`、计为完成、下游读 nil、保留边）均未实现——disabled 节点照样
   真实执行。要么实现运行时语义，要么在 DSL 面显式拒绝，避免静默失效。（§7.3 已
   注明未实现。）

### P2 — 功能不完整，现有部署可绕开

2. **runner 的 Runner Protocol HTTP 传输不走环境代理。** 可重载 transport 有意置空
   `Proxy`（经代理的 HTTPS 隧道会绕过可重载 CA 池，`0551269`）。必须经 HTTP 代理
   访问控制面的部署目前无法使用；需要的话改为自建 CONNECT 隧道并在隧道上用实时 CA
   握手。附注：出站代理行为目前不一致——gRPC 传输默认读环境代理，静态客户端装 TLS 时
   不走代理。

3. **entry-seed 托管只支持 Kafka。** 其他 trigger 类型接到 entry-seed 路径上会
   fail-closed（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)「Non-Kafka
   trigger types are fail-closed」一条）。

4. **pull 模式的 `xflow.supply.http` 未实现。** runner 主动按计划拉取 supply 的路径
   不存在，只能用 `xflow.supply.external` / `static`
   （[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) Supply 一节已标记「尚未实现，
   不要在 DSL 里使用」）。

5. **子工作流复用（`xflow.subworkflow`）未实现。**
   [DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) §6.4 标为「计划扩展，当前版本未实现」。

6. **`types.GroupExecResult` 没有失败成员名的承载字段。** 触发路径的
   `groupExecTriggerRuntime.ExecuteGroup` 从 `subgraph.Result` 拿得到完整分类（含
   `FailedMember`），但对外类型装不下，Kafka 批量消费端拿不到失败成员名
   （`service/runner/group_exec_trigger_runtime.go` 注释自述）。修法：给
   `GroupExecResult` 补承载字段，或把失败分类并入 `Error` 的结构化载荷。

### P3 — 已知限制，记录在案

7. **ADR-D4 刻意延后的两项：** 草稿与发布分离（`WorkflowDraft`）、定义版本历史
   （`WorkflowDefinitionVersion`）。见
   [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

8. **map batch 的租约续期仍走单值索引。** `LeaseLookupKey` 的 NodeName/NodeIdx 只在
   汇报路径填充（2026-10-08 的 D2 修复），续期路径不带——map 节点与其 batch 落在同一
   runner 上时，batch 的续期可能解析到父或兄弟 assignment；共享同一 lease 使引擎侧
   续期目标相同，但 `RefreshLeaseMeta` 只刷新被解析 assignment 的元数据 TTL，长跑
   batch 的目录元数据可能提前过期。未深入分析。彻底修法需要续期也携带任务定位
   （协议变更），或给 batch 独立租约身份。

### 需要真实环境或人工批准，不能靠改代码关闭

9. **G2：多副本 control-plane HA soak 与真实多 namespace 隔离验收。** 要求见
    [RELEASE-GATES.md](./RELEASE-GATES.md) 的 G2 定义；`make test-soak` 不是该证据
    （见 README 的说明）。

10. **RELEASE-GATES §6 的 D1–D8** 全部为「OPEN — 未批准」，需要对应负责人裁定。

11. **KEK 轮换（`xflow supply reseal`）与 server/runner 凭据热加载都没有在真实环境
    演练过**，runbook 的 owner 与期限属于 D6。

## 已关闭

- **gRPC 传输不续租约**（原 P1-1）—— `f11425c`（2026-10-08）：`runner.proto` 加
  `RenewLease` RPC 与 `RenewLeaseRequest/Response`（deadline 用 unix nano），服务端
  复用 `Core.renewLease`，客户端补 `GRPCClient.RenewLease`，续租环经既有
  `leaseRenewClient` 类型断言自动启用；含往返与拒绝用例；`make proto-check` 无漂移。

- **`make web-ci` 的 admin e2e 偶发超时**（原 P1-2）—— `c459d7f`（2026-10-08）：
  首个 heading 的 `toBeVisible` 放宽到 45s，其余断言与全局 expect.timeout 不动。
  根因（dev server 就绪时序）未深挖，以等待放宽处理。附注：单独跑 `pnpm e2e` 前
  必须先 `make web-build`（`@xflow/preview` 的 dist 缺失会确定性失败）；`make web-ci`
  的依赖链已含 build。

- **`TestSubgraphBinaryE2E/KillRunnerMidExpansionThenRestart` 偶发不收敛**（原 P1-3）
  —— 2026-10-08 静态定性（miniredis 对真实 Lua 复现；无真实环境运行，结论转述）：
  不是慢，是 runner directory 层的两个恢复缺陷。**D1**（`5d336c8`）：batch 的
  AssignmentID 不含父代际，死 runner 留下的 leased 记录使后续每一代同号 batch 入队
  被判 `duplicate` 静默 ack（`HandleTask` 曾丢弃 enqueued 标志），该代 barrier 只能
  等 5 分钟一次的 `ReapStrandedLeases` 解开——修法：batch ID 追加
  `@<parent_lease_id>`，`oversizeReport` 同步保留该字段。**D2**（`fb3860a`）：父 map
  与所有 batch 共享 LeaseID/Token，而 `lease:by-token`/`lease:by-id` 是单值索引——
  汇报会解析到兄弟 batch 被拒、map 的 release 会误删 batch 记录；修法：显式 ID 的
  存储 token 匹配时优先（Redis Go+Lua 与 memory 双侧），汇报解析在索引不匹配时经
  既有 per-runner leased-assignments 集合按任务匹配（无新索引，围栏不放宽）。附带
  `1e7c42e`：duplicate 的 batch 入队计入
  `xflow_dispatch_dropped_total{reason="batch_duplicate"}`。复核阶段修正：`36d1908`
  （sweeper 重建过期租约任务时保留 `TaskType`）、`bda9fea`（单值索引不匹配时按任务
  两遍扫描回退，换代际后不再解析落空）。修复中确认的事实：
  `engine.Task.ActivationID/AutoDepth` 是 `json:"-"`，从 echo 的 lease 重建的
  AssignmentID 与入队侧从不相等的（Invoke 起的执行全 activation>0），汇报路由此前
  完全依赖 token 单值索引。未验证：真环境 e2e（本机无 podman/MySQL，对照实验未跑）。
  遗留见 P3-8（renew 路径）。

- **`pin_data` / `settings.pin_data_mode` 没有运行时消费者**（原 P1-4）—— `43624e6`、
  `24289a9`、`35bb063`、`9483af8`、`17e3cb4`、`f77f5c3`、`b2f9e60`、`28158e4`
  （2026-10-08）：编译期把 mock 写入 `NodeMeta.PinOutput`（omitempty，pin 关闭时图
  哈希不变；未知 mode 报编译错）；`handleSystemTask` 对 pinned 节点做一次系统提交
  （状态 `pinned`、mock 作输出、下游走 `main`，不签发 lease）；`types.NodeStatusPinned`
  为终态（rstate 全部 10 处 Lua 谓词与 `timeout/monitor` 均已纳）；`test_only` 需要
  的执行上下文由 `WithTestRun` 提供（SDK / HTTP 请求体 `test` 字段，持久化在 Redis
  `test_run` 键；SQL 不投影，与 ExecutionRecord 既有的 Scope/TraceCarrier 同口径）；
  pin 不支持场景（组成员、body 成员、supply、allow_cycles、faf、不存在节点、非对象
  mock）编译告警后忽略 pin、节点真实执行。注意：`settings.pin_data_mode` 的未知值
  由编译期静默忽略改为编译报错，存量定义需先确认取值合法。复核阶段修正：`59ff4c4`
  （系统任务提交后的认领结算并入 inactive 分支，消除 `ErrSystemTaskHandled` 的重复
  入队循环）。待验证：真 Redis 契约（本机无 Redis，miniredis+memory 通过）、浏览器端。
  DSL-SPEC §7 已重写（去掉「未实现」横幅），§7.5 第三规则改「凡配 always 即告警」。

- **gRPC 传输不代理指标**（原 P2-5）—— 2026-10-08 按裁定与
  [SUPPLY-NODE-TODO.md](./SUPPLY-NODE-TODO.md) 的「已知且接受的代价，不单独立项」
  对齐，本条按已知限制关闭，不再作为缺口跟踪。

- **runner 凭据热加载没有覆盖全部出站客户端**（原 P2-6）—— `20f6bb4`、`0c11025`、
  `b914aad`、`4562ee6`、`eb35cd0`、`fd7e030`、`a597526`、`769bb8f`（2026-10-08）：
  可重载 bearer transport 按 scheme+host 精确匹配把 token 只发往控制面 origin；
  artifact 拉取、entry-seed、supply 拉取全部改走可重载客户端；gRPC 凭据每次握手
  现读实时 CA 池（不再构造期快照）；identity 续期改经
  `Runner.ControlPlaneHTTPClient`（基于 reloader、token 只发 runner 自身 origin，
  签名不含可指定 host 的参数）；`CredentialReloader.Reload` 拒绝削弱服务器验证的
  变更（TLS 已配置→全空、私有 CA 被清）并保留旧材料；`reloadMu` 把 swap/SetToken/
  关空闲连接串成一步。两轮专项安全审查 PASS（无 Critical/High；并发测试对 mutex
  属回归检查而非必要性证明，如实记录）。未验证：真实 SIGHUP 演练、mTLS 下的续期。
  runbook §4.3 覆盖表与已知边界（CA bundle 内容不可校验、客户端应建一次复用）已同步。
  复核阶段修正：`be892ef`（`OverrideServerName` 不再写凭据内部状态）、`9c6238e`
  （无法解析的 runner origin 构造期报错，不再静默丢掉 token）、`956e7d3`（身份续期
  请求体不再携带旧 token）。

- **MAP 的 runner 级资源治理只剩配置面与证据缺口**（原 P2-9）—— `c0b39ca`
  （2026-10-08）：独立 runner CLI/YAML 暴露 `--map-batch-concurrency` /
  `--map-item-concurrency`（YAML `runner.map_batch_concurrency` /
  `map_item_concurrency`，0=GOMAXPROCS 合法）并透传 SDK。剩余仅发布证据：RELEASE-GATES
  的 clean-SHA 证据缺口属发布流程，见本文件「需要真实环境」区，不随本条关闭。

- **没有 Redis 与 sqlstore 的执行状态比对工具**（原 P2-12）—— `667f806`
  （2026-10-08）：`xflow execution diff`（`cmd/xflow/execution_diff.go`），只读窄
  接口，`--redis-addr/--mysql-dsn/--namespace/--execution-id/--json`，namespace
  隔离经 miniredis 验证。剩余：SQL 侧仅编译期覆盖（本机无 MySQL，运行面未验证）。

- **组级 `error_output` 的错误载荷没有结构化的失败成员名**（原 P3-13）—— `13cc289`、
  `2b2836a`（2026-10-08）：失败成员身份从 `subgraph.Result` 结构化上传
  （`Result.FailedMember` → `engine.GroupResult.FailedMember` → 组错误载荷
  `data["failed_members"]`；载荷形状用 `[]any` 保持跨后端一致，见 `f7eb946`），
  `protocol.GroupResultWire` 补 `failed_member,omitempty` 双向映射。gRPC 传输的组
  结果承载缺口在复核阶段一并关闭，见下一条。

- **gRPC 传输的 `ReportResult` 整体丢弃 GroupResult**（2026-10-08 收口期间新登记）
  —— `f85540a`、`022bcfc`、`36c4a1e`（2026-10-08）：`runnerpb` 的 `ReportResult` 补
  `bytes group_result_json = 6`，双向复用 `protocol.MarshalGroupResult`/
  `UnmarshalGroupResult`（nil 与空字节的 presence 规则无歧义），组结果现在经 HTTP
  与 gRPC 两条传输都完整到达控制面；`engine.CommitTaskResultWithOutcome` 补组租约
  守卫（组租约必须走组提交路径），旧客户端经 gRPC 上报组结果时得到明确拒绝。实情
  修正：修复前的失败形态不是「空成功」，而是零值结果提交被后端围栏按过期 token
  拒绝——错误信息误导、组结果永不成功上报；HTTP 传输一直正常。
  [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) 的失败成员段已同步（`36c4a1e`）。

- **嵌入式 `Server.ReplaceWorkflow` 会清空编辑器元数据**（原 P3-14）—— `6501ad0`
  （2026-10-08）：新增 `ReplaceWorkflowWithMetadata`（apiserver 侧
  `ReplaceWorkflowReportWithMetadata` 薄封装），嵌入式宿主可显式携带元数据；原
  `ReplaceWorkflow` 的清空行为与既有测试不变。

- **entry unit 的显式 activation 副本扩展**（原 P2-8）—— 控制面 per-replica emit 与
  `ActivationReplicas` 见 `0d30009`（2026-08-28），能力位常量
  `FeatureEntryActivationReplicaV1` 见同日 `8e1aa3e`；graph IR 流转与 rstate 副本键
  见 `5520a01`（2026-09-01），该 commit 在 clean 候选 `c39a271` 之内。出处文档已同步
  （2026-10-08）：[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) §12.2 的
  单副本说明已改为已实现。
