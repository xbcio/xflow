# Runner Protocol Wire Error Taxonomy

- **日期**: 2026-07-17
- **关联**: `2026-07-17-server-production-readiness-design.md` §A3
- **状态**: wire DTO 与 action classifier 基础已实现；local + server-runner 两拓扑测试已建立，但 SDK cluster parity、完整 structured 断言和真实运行 artifact 未完成，A3 仍为部分完成（2026-07-19 复验）

## 1. 问题

action 错误此前有三种并存表达：`Output.Error *Error`（业务错误）、`Port:"error"` + `Data["error"]` 字符串（事实标准）、Go `error` 返回值（系统/编程错误）。Runner Protocol 把 `TaskResult.Error` 序列化为字符串（`service/protocol/types.go` 的 `taskResultJSON.Error`），跨进程后 `types.ErrPermanent` 标记塌缩为裸文本，server 只能 `errors.New(string)` 重建 —— 永远判为 transient，retry 直到 MaxAttempts。dispatcher 的 `PermanentConfiguration` 分类在 embedded 模式保留，但 server-runner 模式丢失。

## 2. 错误矩阵

| 类别 | 来源 | retry | on-error | 节点终态 | 路由 |
|---|---|---|---|---|---|
| transport/system transient | 连接拒绝、DNS、超时、5xx | 是（指数退避至 MaxAttempts） | 落 OnError | Failed/继续 | 不路由 error port |
| permanent configuration | 参数缺失、URL 非法、4xx、凭证错 | 否 | 落 OnError | Failed/继续 | 不路由 error port |
| routable business error | 宿主业务逻辑显式产出 | 否（业务语义，非系统失败） | 按 OnError | Failed/Success | error 输出端口 |
| explicit error-port output | action 仅给 Port="error" 无结构分类 | 是（视为 transient 至 MaxAttempts） | 落 OnError | Failed | error 输出端口 |

判定原则：**不通过错误文本推断策略**。permanent 由 `errors.Is(err, types.ErrPermanent)` 决定；跨进程由 wire DTO `error_detail` 重建。

## 3. Wire DTO

`types.ClassifiedError`（`types/errors.go`）字段：`kind` / `code` / `message` / `retryable` / `permanent` / `details`。

- `Error()`：`code: message` 或 `message`。
- `Is(target)`：`target == ErrPermanent && e.Permanent` —— 使 `types.IsPermanent` 与现有 sentinel 机制统一。
- 构造器：`NewPermanentError(code, msg)` / `NewTransientError(code, msg)`。

## 4. 协议兼容窗口

`taskResultJSON` 同时携带：

```json
{ "error": "<message>", "error_detail": { "kind":..., "permanent": true, ... } }
```

- **marshal**：`result.Error` 非 nil 时始终写 `error` 字符串（供旧 peer）；若是 `*ClassifiedError` 或 `types.IsPermanent`（含 `errors.Join(ErrPermanent,…)`），同时写 `error_detail`。
- **unmarshal**：`error_detail` 非空 → 恢复 `*ClassifiedError`（保留分类）；否则回退 `errors.New(error)`（等价旧行为，transient）。
- **新旧混部**：new runner → old server：old server 读 `error` 字符串，丢分类但不 break；old runner → new server：仅 `error` 字符串，server 判 transient（等价现状）；new ↔ new：分类保真。
- proto 不改字段（仍 `bytes result_json`）；非 Go runner 未来可通过解析 JSON 的 `error_detail` 产出分类。

## 5. 已实现

- `types.ClassifiedError` + `ErrorKind` + 构造器。
- `service/protocol` marshal/unmarshal 保留 `ErrPermanent` 分类 + 旧字符串兼容。
- parity 测试（`service/protocol/error_parity_test.go`）：permanent（`NewPermanentError` 与 `errors.Join(ErrPermanent,…)`）跨 wire 保真；transient 保持 transient 且消息不丢；legacy 字符串 payload 解码为 transient；新 runner 仍发 legacy 字符串供旧 peer。
- HTTP action 参数/配置错误（url 缺失/非法、marshal body、create request）迁移为 `NewPermanentError` —— 修正「坏配置被无谓重试至 MaxAttempts」的 bug，embedded 与 server-runner 模式均受益。

## 6. 分阶段后续（action IO 分类迁移）

DTO 基础就绪后，按矩阵迁移 action 的 IO/响应错误分类。每批配 parity 测试。

- **HTTP**：✅ 已完成（2026-07-18）。`client.Do` 连接失败/超时 → `NewTransientError`（Go error，retryable）；4xx → permanent；5xx → transient；408/429 按 RFC 显式重试语义分类（transient，不归 4xx permanent）。IO 错误返回 ClassifiedError 而非 error-port output。
- **gRPC**：✅ 已完成。按 status code 分类（Unavailable/DeadlineExceeded → transient；InvalidArgument/NotFound/PermissionDenied/Unauthenticated/AlreadyExists/Unimplemented/FailedPrecondition/OutOfRange → permanent；其余 → transient）。
- **Database**：✅ MySQL 范围已实现（2026-07-18）。共享 `classifyDBError` 按 MySQL SQLState/number 分类，不靠字符串：lock-wait-timeout(1205)/deadlock(1213)/serialization(40001)/connection-lost(driver.ErrBadConn/EOF/net) → transient；constraint(1062/1451/1452/1048/23000)/syntax(1064/1146/1054/42000)/access-denied(1045/28000) → permanent；未知 → 保守 transient。IO 错误返回 ClassifiedError 而非 error-port output；buildWhere 列校验返回 permanent 配置错误。**PostgreSQL 不在支持范围**（2026-07-22 确认收窄）：classifier 仅 `errors.As` 到 `*mysqldriver.MySQLError`，PG 错误落保守 transient 兜底；不实现 PG typed classifier、不靠 MySQL 结论外推。`db_errors.go` 已移除误导性的 PG `40P01` 死分支并注明 MySQL-only。
- **code/script**：✅ 已完成（2026-07-18）。`xflow.function` 与 `xflow.script` 的 config 错误（缺参、未注册函数、未知 engine、凭证解析失败）→ `NewPermanentError`；`expr` 求值失败 → `NewPermanentError function.expr_eval`；函数/脚本执行 deadline/ctx 取消 → `NewTransientError`（script.timeout / function.timeout）。用户函数/脚本自身抛出的确定性错误继续走显式 `Port:"error"` 输出（legacy explicit error-port output 矩阵行），保持现有 OnError 路由语义。

风险：改变既有 retry/on-error 行为；每批需 local/cluster/server-runner parity 覆盖 transient/permanent/HTTP 4xx-5xx/业务错误/显式 error port/retry exhausted/各 on-error 策略，不以字符串匹配决定 retry。

**Parity 现状**（2026-07-22 R6 闭合）：
- wire DTO parity 已覆盖（`service/protocol/error_parity_test.go`）：permanent/transient/legacy 字符串跨 wire 保真。
- **三拓扑 parity 已落地，Database 为 real-pair 精确 parity + local-fake 独立 contract（非全三拓扑同构）**：`test/integration/action_parity_*.go`（core/HTTP/gRPC/script/function/OnError/Database）均跑 local embedded + server-runner + durable SDK cluster（`RunParityCluster`，`distributed.New(WithConsumer(true))`+`StartBinding`，真 Redis/Asynq in-process consumer），`assertParityThreeWay` 统一断言 attempt/终态/port/`error_code`/`error_kind`/`error_retryable`/`handler_invocations`/DAG 推进。HTTP/gRPC/script 跨三拓扑统一；Database 只做 server-runner↔cluster real-MySQL 精确 parity，local-fake 与 real MySQL 的已知 code 分叉（bad-conn `database.connection_lost` vs `database.unknown`、lock-wait-timeout 1205 vs deadlock 1213）是 fake driver 与 real driver 错误形态差异，保留在 `TestDatabaseActionErrorParity` 单独 contract 断言，不宣称三拓扑同构。gRPC 经 `grpcPoolWrapper` 自注入 pool（cluster 路径零生产改动）；Database 经 `databaseParityHandler` 自注入 cred+pool（无需 `distributed.WithCredentialResolver`）。
- `ParityOutcome.ErrKind`/`ErrRetryable` 从 runtime commit receipt 实测（`applyActualFromClassification` drain 该拓扑 `RuntimeEvidenceBuffer`，选唯一 `Applied==true` commit event，按 `ErrorSource`/`Classified`/`Kind`/`Retryable` 派生——**production-derived，非 fixture 回填**）。`stampExpectedKind`/`parityKindFromName` 已删除；`WantKind`/`WantRetryable` 仅作 contract oracle，比较方向 `runtime receipt actual vs manifest expected`，不向 actual 写期望。`HandlerInvocations` 实测（counting wrapper，per-topology 隔离导出）。`PARITY_MATRIX` artifact 三行（local/server-runner/cluster-durable）含非空 `error_kind`/`error_retryable`/`handler_invocations`。`error_port_retry_exhausted`（默认 `OnError=stop`）的 terminal receipt 落 `ErrorSource=unclassified`（stop 不设 `RoutePort="error"` 且 retryErr 为裸 `errors.New`），其 actual kind 为空——诚实反映引擎当前行为，旧 `"error_port"` 是已删除的 `stampExpectedKind` 回填。
- 生产 runner ResourcePool/credential resolver 接线已完成（`TestGRPCActionErrorParityProductionWiring`/`TestDatabaseActionErrorParityServerRunnerProductionWiring`）。
- **保留**：`error_kind`/`error_retryable` 是 fixture 期望值（test-side），非 node 记录恢复；G0 整体未签出（CI artifact ENV-GATED，需真实 CI run 产出可核验 artifact）。PostgreSQL 已收窄为不在支持范围（见 §6 Database），不再列为待办。

## 7. 非目标

- proto 字段级 message 化（仍 JSON bytes，注释更新即可）。
- `NodeSnapshot.Error` 从 string 升级为结构化（state store schema 变更，独立立项）。
- handler exactly-once（见总 spec 非目标）。
