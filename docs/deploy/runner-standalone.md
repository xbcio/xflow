# 独立 runner 进程部署

本文档面向以 `cmd/runner` 独立进程（而非 `sdk/xflow` 嵌入式）方式部署 xflow runner 的运维场景。所有 flag 名与默认值以 `cmd/runner/run.go` 的 `bindRunnerFlags` / `cmd/runner/config.go` 的 `defaultRunnerConfig` 为准；本文档如与代码不一致，以代码为准。

## 身份与注册

runner 的身份（`runner_id` + `token`）来自入册（enrollment），由 `cmd/runner/identity.go` 与 `cmd/runner/enroll.go` 实现。

- `--identity-store=ephemeral`（默认）：身份只保存在内存里，进程重启后会丢失，需要重新入册（消耗一个新的注册码）。
- `--identity-store=file --identity-file=<path>`：身份以 JSON 持久化到 `<path>`，重启后复用，不会再消耗注册码。文件权限要求 0600；`fileIdentityStore.Load` 会拒绝加载任何 group/other 可读（`mode & 0o077 != 0`）的身份文件，并报错要求手工收紧权限，而不是静默忽略权限问题。
- `--registration-code`（或 `XFLOW_RUNNER_REGISTRATION_CODE`）：仅在身份存储里**还没有**身份时才会被使用（`resolveRunnerIdentity` 的优先级：已存身份 > 注册码 > 都没有则维持原样，即走已配置的静态 `--id`/`--token`）。**注册码是一次性的**：只要使用 `ephemeral` 存储（或每次重启都清空 `--identity-file`），每次重启都要一个新码；只有 `--identity-store=file` 且文件持久化在磁盘上才能免去这一步。
- **入册后 `--id` 不生效。** `--id`（即 `ProposedRunnerID`）只作为审计提示随入册请求一起发给服务端；服务端文档明确写了永不采纳这个提议 ID（否则"以已存在的 ID 入册"就是身份接管路径）。入册成功后，`cfg.runnerID` 会被服务端签发的 ID 整体覆盖。
  **`--id` 在未入册路径下仍然生效**：如果既没有已存身份、也没有配置 `--registration-code`（纯静态 `--id` + `--token` 部署，或走 YAML 显式将 `runner.id` 置空），`--id` 的值会原样作为 `runnerID` 使用；后者（YAML 里显式 `id: ""`）还会触发 `cmd/runner/run.go` 的 `runWithSignals` 里的兜底：`cfg.runnerID == ""` 时自动填 `fmt.Sprintf("runner-%d", os.Getpid())`。
- enroll 端点只挂在控制面的 HTTP 服务上，**没有 gRPC 版本**。`resolveRunnerIdentity` 入册请求始终经由 HTTP 发出，与 `--transport` 无关；因此 `--transport=grpc` 的 runner 若要入册，`--server` 仍必须是一个可达的 http(s) origin（同时还要配 `--grpc-target` 供任务流量使用）。

## 传输安全

runner 默认拒绝在没有任何 TLS 材料的情况下启动，因为 bearer token 会明文过网。该门禁分两处，判据略有不同：

- **常规启动**（`cmd/runner/config.go` 的 `validateTransportSecurity`）：`--transport=http` 时，`https://` 的 `--server` 视为已加密；`--transport=grpc` 时没有 URL scheme 可看，只认 TLS 材料（`--tls-server-ca` / `--tls-client-cert` / `--tls-client-key` 三者任一非空即可，mTLS 再加 `--tls-client-cert`/`--tls-client-key`）。两种 transport 下都可以用 `--allow-plaintext` 显式放行。
- **入册请求**（`cmd/runner/enroll.go` 的 `validateEnrollTransportSecurity`）：由于入册请求固定走 HTTP（见上一节），这里单独判 `--server` 的 scheme 必须是 `https://`，否则同样要求 `--allow-plaintext`。这一判据独立于 `--transport`：一个 `--transport=grpc` 且已配好 gRPC TLS 材料的 runner，如果 `--server` 仍是 `http://` 且没有 `--allow-plaintext`，入册这一步依然会被拒绝。

`--allow-plaintext` 一旦打开，对两处门禁同时生效，因为它绕开的是"是否需要 TLS"这个判断本身，不是分别配置。

## 探针

`/healthz` 与 `/readyz` 由 `cmd/runner/lifecycle.go` 的 `registerLifecycleProbes` 挂在 `--metrics-addr` 指定的同一个监听端口上（与 `/metrics` 共用一个端口）。**不配 `--metrics-addr`（默认为空）就没有探针，也没有 `/metrics`。**

`/readyz` 依次检查 **5 个条件**（`lifecycleState.Ready()`，命中第一个为假的即返回 503，响应体是原因文案）：

1. 没有 fatal 错误（`--require-supply-encryption` 触发的启动失败等，见下一节）；
2. 已成功向控制面注册（`registered`）；
3. 最近一次心跳成功（`heartbeatOK`——是"最近一次"，不是"曾经成功过"：连接刚断开时即使几分钟前注册成功过，也会立刻转为 not ready）；
4. supply gate 装配结果已知（`supplyGateKnown`——在装配完成上报之前一律视为未就绪，不会把"还不知道"误读成"没有 gate"）；
5. 若该 runner 确实有 supply gate（`supplyGatePresent`），则要求至少成功拉取过一次供应（`supplyFetched`）；没有 gate 则跳过这一条。

`/healthz` 只反映进程存活（`Live()` 恒为 true），与控制面连接状态无关：与控制面失联不是进程故障，杀掉它无助于恢复，把它从负载轮转里摘掉（`/readyz` 转为不健康）才是正确反应。

## `--require-supply-encryption` 与 `verify` 子命令

- `--require-supply-encryption`：默认关闭。开启后，如果 runner 成功注册但控制面在注册响应里没有签发供应加密密钥，视为致命错误——runner 会把这个错误记为 `lifecycleState.fatal`（此时 `/readyz` 恒 503，原因是这条错误文案本身），并取消运行上下文使进程退出，而不是继续以明文方式拉取供应内容。默认关闭是因为"控制面没配供应加密器"本身是一种合法部署形态。
- `xflow-runner verify` 子命令做同样的检查，但发生在启动之前：它复用与 `run` 完全相同的配置翻译路径（`toSDKRunnerConfig` + `xflowsdk.VerifyRunner`），如果 `--require-supply-encryption` 与实际注册结果不符，会在终端直接报错退出，而不是等到进程跑起来再 CrashLoopBackOff。

## ActivationReplicas 与 HPA 的手工同步

`ActivationReplicas` 有两层：

- **DSL 里调用的 builder 方法**：`sdk/xflow/group.go:38` 的 `func (g *GroupRef) ActivationReplicas(replicas uint32) *GroupRef`，以及 `sdk/xflow/builder.go:238` 的 `func (n *NodeRef) ActivationReplicas(replicas uint32) *NodeRef`。
- **落到 workflow 定义里的序列化字段**（`uint32`，JSON 标签 `activation_replicas,omitempty`）：`types/group.go:33`（`GroupDef`）与 `types/workflow.go:108`（触发入口）。本节说的"写在 workflow 定义里"指的是这个字段，不是上面的 builder 方法本身。

这个值由**服务端**的 entry activation manager 展开成多份虚拟激活单元，分派到不同 runner 上实现同源触发（如 Kafka 分区）的 sibling 反亲和。`cmd/runner`、`sdk/xflow/runner.go`、`service/runner` 里都没有任何代码读取或感知这个值——runner 进程本身完全不知道自己是第几个激活副本。

**它与 HPA（或任何手工设置的）runner 副本数之间没有任何自动同步机制。** 因此每次调整 runner Deployment 的副本数后，必须手工检查并调整 workflow 定义：

1. 记录扩缩后的 runner 副本数 N。
2. 对每个使用了 `ActivationReplicas` 的 workflow，确认其值 ≤ N。值大于 N 时，多出来的虚拟激活单元没有 runner 可落，会一直处于待分配状态，且没有告警会提示这一点。
3. 修改后重新发布 workflow 定义。

这一步目前只能靠发布流程里的人工检查项兜底。
