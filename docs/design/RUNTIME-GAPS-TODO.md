# 运行时缺口 遗留项

2026-10-03 一轮缺口收口（KEK 轮换、server/runner 凭据热加载、ADR-D4 编辑器元数据、
Node Group 组级 `on_error`、gRPC `AckActivation`、G1 clean-SHA 重签 `c39a271`）之后
仍开放的条目。按「不做会怎样」排序，不按工作量。

2026-10-08 第二轮收口（分支 `fix/runtime-gaps-closeout`，相对基线 `e3478d7`；截至
`fd5d8e9` 共 48 个 commit，含并入的 main 提交 `786835a` 与 merge 提交 `e1486ce`）
关闭了原条目 1–6、9（配置面）、12（工具面）、13、14，以及同期新登记的 gRPC 组
结果传输缺口与组失败上报缺口；新登记的另外三处（`disabled` 的运行时消费者、组
结果失败成员的对外类型、map batch 续期的目录元数据）当时仍然开放。条目已按体例
重排编号，旧号保留在「已关闭」条目的括注里。

2026-10-08 第三轮收口（分支 `fix/runtime-gaps-remainder`，自 `3833377`；截至本提交
共 9 个 commit）关闭了第二轮新登记的三处（本轮编号 1、6、8）：`disabled` 的运行时
语义、组结果失败成员的对外类型、map batch 续期的目录元数据。条目已再次重排编号，
旧号保留在「已关闭」条目的括注里；本轮验证同时登记了两处新缺口（见 P3），剩余条目
均为功能缺口或需要真实环境/人工裁定。

每条都写明出处：缺口本身的现状说明留在所属设计文档里，这里只记「打算改的东西」。
关闭一条时，把它移到文末「已关闭」并写上 commit。

范围：只收运行时缺口。Relay Gateway / remote SDK（D1 支持矩阵已排除）与已被显式
否决的设计（如 supply 的 `default: <bytes>`）不在此列；出处文档与实现之间的纯陈旧
偏差也不单独立项，在关闭对应条目时顺带标注。

## 待办

### P2 — 功能不完整，现有部署可绕开

1. **runner 的 Runner Protocol HTTP 传输不走环境代理。** 可重载 transport 有意置空
   `Proxy`（经代理的 HTTPS 隧道会绕过可重载 CA 池，`0551269`）。必须经 HTTP 代理
   访问控制面的部署目前无法使用；需要的话改为自建 CONNECT 隧道并在隧道上用实时 CA
   握手。附注：出站代理行为目前不一致——gRPC 传输默认读环境代理，静态客户端装 TLS 时
   不走代理。

2. **entry-seed 托管只支持 Kafka。** 其他 trigger 类型接到 entry-seed 路径上会
   fail-closed（[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)「Non-Kafka
   trigger types are fail-closed」一条）。

3. **pull 模式的 `xflow.supply.http` 未实现。** runner 主动按计划拉取 supply 的路径
   不存在，只能用 `xflow.supply.external` / `static`
   （[DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) Supply 一节已标记「尚未实现，
   不要在 DSL 里使用」）。

4. **子工作流复用（`xflow.subworkflow`）未实现。**
   [DSL-SPECIFICATION.md](./DSL-SPECIFICATION.md) §6.4 标为「计划扩展，当前版本未实现」。

### P3 — 已知限制，记录在案

5. **ADR-D4 刻意延后的两项：** 草稿与发布分离（`WorkflowDraft`）、定义版本历史
   （`WorkflowDefinitionVersion`）。见
   [ADR-D4-runtime-editor-metadata-split.md](./ADR-D4-runtime-editor-metadata-split.md)。

6. **两个目录后端的租约解析围栏不一致**（2026-10-08 第三轮收口验证新登记）。
   (i) Redis 在 by-token 未命中、by-id 命中时不校验存储 token，memory 校验：持有
   正确 leaseID 但 token 不匹配的请求（同一 runner+session）在 Redis 上解析到该
   租约、memory 上被拒；此形态在 P3-8 修复前后一致（修复两个版本都以 scratch 测试
   确认 `found=true`，pre-existing）。修法方向是解析命中后补存储 token 对比，但需
   先确认「按 leaseID 解析」的既有调用方都携带 token 且不依赖该放行。(ii) memory
   的租约记录不含会话，重新注册后旧会话 finalize 的租约对新会话仍可解析，Redis 按
   `assignmentSession` 拒绝——目标语义未裁定，可达性未分析。两条均未深入分析、
   未实现。

7. **续期刷新的按身份走查有成本**（2026-10-08 第三轮收口随 P3-8 修复引入）。每次
   成功续期从 1 次 Lua 变为对 runner 全部活跃分配的一次走查（每个候选约 4 次未
   pipeline 的往返 + 每次身份命中 1 次 Lua；per-runner 索引短于 `runnerLeaseCount`
   时还要全量 `HGetAll`）。walker 不像 `replayLease` 那样在确认 live 后回填索引，
   短索引会持续触发全表扫直到下一次 poll 顺势修复。成本影响面未量化（本机无真实
   Redis）。建议：pipeline HGET，或在第二趟确认 live 时回填索引；验证判定风险中。

### 需要真实环境或人工批准，不能靠改代码关闭

8. **G2：多副本 control-plane HA soak 与真实多 namespace 隔离验收。** 要求见
    [RELEASE-GATES.md](./RELEASE-GATES.md) 的 G2 定义；`make test-soak` 不是该证据
    （见 README 的说明）。

9. **RELEASE-GATES §6 的 D1–D8** 全部为「OPEN — 未批准」，需要对应负责人裁定。

10. **KEK 轮换（`xflow supply reseal`）与 server/runner 凭据热加载都没有在真实环境
    演练过**，runbook 的 owner 与期限属于 D6。

## 已关闭

- **`disabled` 节点没有运行时消费者**（2026-10-08 第二轮收口期间新登记）—— `1aa019e`、
  `55e8c46`、`35892ec`、`f568a82`（2026-10-08）：编译期 `assignDisabledNodes` 把
  `NodeMeta.Disabled` 标记进图（参与图哈希），六类引擎无法拦截的形态（trigger、
  supply、co-location 组成员、body 成员、allow_cycles、faf）由静默真实执行改为
  编译报错；运行时 `handleDisabledNode` 拦截 disabled 节点的 `TaskTypeNodeExec`
  任务，以系统提交原子落 `skipped` + 端口 `main` 并推进下游（不签发租约，下游照常
  调度、读到 nil）；系统围栏（rstate Lua 与 local 后端）新增第三种合法形态——
  无 skip 标记的 execute 单元上的 `skipped`（contract 用例
  `runDisabledNodeSkippedOnExecuteUnit` 覆盖）；pin 与 disabled 同配时 disabled
  优先（编译告警）；DSL-SPECIFICATION §3.1/§7.5 已重写（去掉「尚未实现」横幅）。
  独立复核（verifier-remainder）：PASS、0 阻断；4 处 mutation 验证（图标记调用点、
  rstate 与 local 两端围栏、engine 端口）。非阻断遗留：`Graph.UnmarshalJSON` 不
  复核 disabled 形态合法性；`compile.go` 重复调用 `assignPinData` 造成告警文本
  重复（既有，非本 diff）；三条可选测试空白（保留边的直接边数断言、混合 fan-in、
  wait_any 与 disabled 同图）。

- **`types.GroupExecResult` 没有失败成员名的承载字段**（2026-10-08 第二轮收口期间
  新登记）—— `1e0ebd4`（2026-10-08）：对外类型补 `FailedMember`（普通节点名；
  环境性失败或组整体未跑时为空，仅 `Outcome != "success"` 时有意义），runner 的
  `ExecuteGroup` 从 `subgraph.Result` 拷贝，Kafka 批量消费端拿到失败成员名；
  types/runner/kafka 测试同步更新，mutation 验证。

- **map batch 的租约续期仍走单值索引**（2026-10-08 第二轮收口期间新登记）——
  `5c07169`、`7f288c2`（2026-10-08）：侦察更正了本条登记时的判断——续期受损不要求
  父子同 runner（索引可指向另一个 runner 的分配或已释放的分配），后果也更重：续期
  被拒（`lease not found`）后 runner 在首个续期 tick（min(TTL/3, 10s)）取消健康
  batch；另一条路径是 `RefreshLeaseMeta` 只刷新被解析的 assignment，共享身份下其余
  分配的目录元数据在 finalize 时的窗口（TTL + 回收余量，默认约 90s）到期后再也无法
  续期。修法（服务端，无协议变更——本条登记时设想的协议补 NodeName 或 batch 独立
  身份均不需要）：索引未命中时按身份回退到本 runner 的活跃分配集合（per-runner
  索引，短于 `runnerLeaseCount` 时补扫全状态哈希），且优先解析到 batch
  （`SubgraphPayload != nil`）——解析出的租约喂给 `group_control_loop` 的续期
  deadline 兜底，只有父 map 节点的租约带 `ExecutionDeadline`，解析到父会让仍在推进
  的 batch 续期触发父的超时提交；`RefreshLeaseMeta` 改为刷新身份下本 runner 的全部
  活跃分配。Redis 与 memory 双侧，20 个新测试（含全链路续期与 deadline 用例，
  以及跨 runner、过期会话、错误 token、refresh 作用域、短索引补扫、batch 释放后
  回落父节点 6 类围栏反例，后者 `3093295`）；7 处 mutation 验证（回退两处 fallback、
  反转两种后端的 batch 偏好、只刷新一个分配、砍掉 walker 第二趟、丢掉 refresh
  身份过滤，各自转红）。独立复核（verifier）：PASS、0 阻断（围栏反例与 mutation
  在 `/tmp` 副本复核，未动工作树）；两条非阻断发现已登记为待办 6、7。未验证：
  真实 Redis 契约（与 P1-3/P1-4 同口径，miniredis + memory 覆盖）。

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
  遗留的 renew 路径已由第三轮收口关闭（见文末 map batch 续期条目）。

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
  守卫（组租约必须走组提交路径）；组租约上缺组结果的上报的完整处置（显式拒绝、
  容量释放与错误映射）见下一条。
  [NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md) 的失败成员段已同步（`36c4a1e`）。

- **组任务的失败/超限上报走不进组结果路径**（2026-10-08 复核阶段新登记）——
  `c0d2619`、`eac88b3`、`3a859c4`（2026-10-08）：runner 侧组执行 error 与结果
  oversize 分支改为合成 `GroupOutcomeFailed` 的 `GroupResult`（身份字段齐全；
  组失败与普通节点失败一样走结果提交路径；组级重试本就完全由 lease 过期驱动，
  失败结果不触发重试）；core 侧把「组租约 + 空组结果」显式判定为 stale-token
  等价结果——立即释放目录容量（对齐修复前行为），并新增
  `ErrGroupResultMissing` 与 `ReportRejectedGroupResultMissing` 埋点。复核阶段
  发现 `eac88b3` 的哨兵错误在传输层没有映射（HTTP 落 500、gRPC 落
  `codes.Internal`）：`3a859c4` 补齐——三条传输（HTTP handler、gRPC unary、
  Connect 流）统一经 `isStaleTokenEquivalent` 按 stale-token 契约带内回传，
  HTTP 409 + Accepted=false、gRPC Accepted=false、Connect 的 Ack 帧且不断流，
  runner 收到的是明确拒绝而非 500。未验证：真实 transport 端到端（两分支与
  三处映射由进程内/handler 级测试 + mutation 验证覆盖；wire 层由 `f85540a`
  覆盖）。

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
