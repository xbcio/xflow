# 测试闸门卫生 遗留项

2026-09-10 的一次全仓库盘点结果。主题只有一个：**测试跳过时打印 ok，让人误以为覆盖了。**

仓库对这个缺陷类已经有成熟解法——`test/integration/harness.go:65-117` 的
`requireRedis` / `requireMySQL` / `requireKafka`：依赖不可达时 `t.Skip`，但在
`XFLOW_REQUIRE_*_INTEGRATION=1` 下改为 `t.Fatal`，所以 CI 里缺依赖不会被当成通过的闸门。
`requireMySQL` 还额外做对了一件事：**不回显 DSN**，因为其中嵌着 `MYSQL_ROOT_PASSWORD`
（`harness.go:88-92`）。

下面每一条都是「同一个包里已有正解，但这个文件手写了自己的闸门绕开它」。
仓库自己在 `backend/providers/distributed/internal/rstate/deadletter_replay_test.go:1124`
记录过完全同型的历史事故：六个真 Redis 契约测试各自内联 `os.Getenv` + `t.Skip`、
没有升级分支，导致它们在 required 模式下也只报 skip。

按「不做会怎样」排序。

---

## 待办（四条已于 2026-09-10 全部关闭，下方保留原始判断与实际改法）

### 1. skip 文本回显含密码的 DSN ✅ 已完成

**不做会怎样**：任何一次没起 MySQL 的 `go test`，测试输出里就有一行明文
`MYSQL_ROOT_PASSWORD`。测试日志进 CI 制品、进终端回滚缓冲、进粘贴板。这不是覆盖问题，
是凭据泄露——`harness.go:88-92` 专门写了注释说明不能这么做，这几处没听。

| 位置 | 形态 |
|---|---|
| `cmd/xflow/dead_letter_reconcile_test.go:43` | `t.Skipf(... dsn ...)` |
| `store/sqlstore/registration_code_repo_test.go:54,58,61` | 三个分支各一次 |
| `store/sqlstore/mysqlstore/mysqlstore_registration_test.go:101` | 同 |
| `node/internal/action/database_mysql_test.go:86,92` | 两个分支各一次 |

**正解已存在**：只打 `localhost:$MYSQL_PORT`。

**实际改法**：四个文件各加一对包内 helper——`*Endpoint()` 返回不含凭据的端点
（调用方自带 `XFLOW_TEST_MYSQL_DSN` 时返回字面串「the DSN in $XFLOW_TEST_MYSQL_DSN」，
**命名而不解析**，别人的密码就不会因为一个解析 bug 落进日志），和 `skipOrFail*()`
在 `XFLOW_REQUIRE_MYSQL_INTEGRATION=1` 下改 `t.Fatal`。所以第 1、2 条的 MySQL 半边
是一起修的。

复核：`grep -nE 'Skipf\([^)]*dsn|Fatalf\([^)]*dsn'` 全仓库剩 4 处
（`cmd/runner/config_test.go:551`、`sdk/xflow/runner_config_test.go:62`、
`execution/runner_test.go:233,347`），都是配置解析测试里对**测试自造的假 DSN**
的断言文本，不是真凭据。

### 2. 四处手写闸门绕开 `XFLOW_REQUIRE_*` 升级机制 ✅ 已完成

**不做会怎样**：CI 的 required 模式对它们无效。依赖挂了，这些测试报 skip，套件报 ok，
没人知道这几块真 Redis / 真 MySQL 覆盖当天没跑。

| 位置 | 闸的是什么 | 同包/同目录的正解 |
|---|---|---|
| `backend/providers/distributed/internal/rstate/entry_activation_test.go:37,46` | `XFLOW_TEST_REDIS_ADDR` | 同包 `deadletter_replay_test.go` 的 `realRedisAddr` |
| `test/integration/i_remote_trigger_hosting_e2e_test.go:420,427` | 同上 | 同目录 `harness.go` 的 `requireRedis` |
| `backend/providers/distributed/internal/timeout/monitor_test.go:54,61` | 同上 | 同上 |
| `cmd/xflow/dead_letter_reconcile_test.go:37` | MySQL DSN | `requireMySQL` 的形状 |

`entry_activation_test.go:37` 的注释写着 "Skip cleanly ... rather than failing every
subtest"——是主动选择，不是疏漏。但它选错了对象：required 模式要的正是 fail。

**实际改法**：`entry_activation_test.go` 直接换成同包 `realRedisAddr(t)`（顺带删掉
不再使用的 `os` import）；`i_remote_trigger_hosting_e2e_test.go` 换成同目录
`requireRedis(t)`（`os` 与 `go-redis` 两个 import 随之删除）；`monitor_test.go` 的
`realRedisOrSkip` 两条分支各补升级；`dead_letter_reconcile_test.go` 见第 1 条。

`requireRedis` 在 `XFLOW_TEST_REDIS_ADDR` 未设时会退到 `REDIS_PORT` / `localhost:6379`，
比原来的手写闸门更宽——连不通仍然 skip，所以行为只增不减。

### 3. 跳过条件不由环境变量控制的三条 ✅ 已完成

**不做会怎样**：前两类至少「起了依赖就一定跑」。这三条**即使依赖全在线也可能跳过**，
且没有任何开关能把它们变红。

- **`exprx/template_test.go:50`** ——最重的一条。`RenderTemplate` 没返回 error 时跳过，
  而这条测试断言的是**错误信息不得回显 secret**。expr 哪天开始容忍这个表达式，
  断言就永久静默失效，skip 文本只说 "pick another failing form"，没人会看见。
- **`store/sqlstore/mysqlstore/mysqlstore_registration_test.go:146`** ——12 路并发首次
  Bind 里没真撞出 MySQL 1213 死锁就跳过。skip 文本自陈
  "this test verifies nothing about New's registration wiring when that happens"。
  于是 `init() -> RegisterTransientClassifier` 的接线在没复现的那些运行里完全没被验证。
- **`test/integration/internal/evidence/verifier_test.go:594`** ——信封里没有可复制的
  runtime event 就跳过，「duplicate event ID across producers」这条校验可能一次都没跑过。

仓库对这个形状有防御先例：`verifier_test.go:725` 的 `TestVerifyRejectsSkip`、
`cmd/xflow/dead_letter_reconcile_failure_test.go:86` 的 `stats.Skipped == 0`、
`node/internal/code/script/record_skip_test.go:64`。

**实际改法**，三条各不相同：

- `exprx/template_test.go`：skip 换成 `t.Fatal`。`$params.secret` 是 string，
  取 `.nonexistent` 在任何 expr 版本都必须失败；真被容忍了那是要查的变化，不是要绕开的。
  实跑确认当前 expr 仍然拒绝，所以这条 `t.Fatal` 不会误伤。
- `mysqlstore_registration_test.go`：死锁复现本身是概率性的，不能靠 `t.Fatal`。
  改成最多 5 轮重试，**每轮换新 namespace**（复用会让第 2 轮变成幂等 re-bind，
  走 `artifact.go` 里 `Bind` 的另一条分支、根本撞不出死锁，重试就成了空转），
  全部落空才 skip，required 模式下连 skip 也不给。
- `verifier_test.go`：这里还藏着第二个问题——`mutate` 闭包捕获的是**外层** `t`，
  那句 `t.Skip` 会把整个父测试标成 skip 而不只是这一个子用例。改法是把
  「fixture 必须有 runtime event」这条不变量上移到循环体里、用正确的子测试 `t` 断言，
  一次保护所有依赖该切片的用例。

### 4. `COMMIT-PATH-TODO.md` 的开放项已完成但未标记 ✅ 已完成

`COMMIT-PATH-TODO.md:240-260` 说 `commitAcyclicNodeError`（当时记作
`engine/atomic_commit.go:63`）与 `commitLegacyNodeError`（当时记作 `engine/commit.go:213`）
「逐字相同」待去重，并给出合并方式「提取公共前置逻辑」。上面两个行号是**当时那份文档
的原话转述**，不是活指针，不随代码更新——留着是为了让人能对上被更正的是哪一句。

**该重构已经做完了**：`engine/atomic_commit.go` 的 `nodeErrorCommitFunc` 类型 +
同文件的 `commitNodeErrorOutcome` 正是那个公共前置逻辑，两个函数现在各是一行委托。
文档的行号现在指向委托后的新形态，读者会被误导去找一份不存在的重复。

只改文档，代码无需再动。

**实际改法**：`COMMIT-PATH-TODO.md` 的「可能的收敛形状」一节改写为「错误分支已收敛，
入口合一仍开放」，并在「已关闭」表补一行指向 `62d68a0`（2026-08-30）。

> **2026-09-10 更新：这条「实际改法」自己也过期了。** 上面写的「三层收敛里仍未做的只剩
> 入口合一（`taskResultExpands` 分流）」在
> `refactor(engine): merge the two task-result commit entries into one`（`b591a0e`）里
> 做完了，`COMMIT-PATH-TODO.md` 随后由 `6a320ac` 收口，现自陈「本文件目前没有仍开放的
> 条目」。保留原句不删，是因为本节记的就是「文档落后于代码」这类账——这条自己变成同一
> 类账的样本，比一句被悄悄改掉的话更有说明力。
>
> 另外记一笔：那句「用 `taskResultExpands` 分流」照字面做会改掉生产行为。实际合并**没有**
> 换这个谓词，理由见 `COMMIT-PATH-TODO.md` 收口引用块里的第二条裁定。
>
> 本节原先的四处行号（`atomic_commit.go:73`/`:83-96`/`:63-65`、`commit.go:241-243`）已随
> 这次合并全部漂移，现已改为指符号名。行号会漂，符号名不会——这正是本节要说的那件事。

---

## 已知且接受的代价

- **`//go:build integration` 下约 40 个文件在默认 `go test ./...` 里连 skip 行都不打印**，
  包直接报 ok。这比 `t.Skip` 更彻底的静默，但它有守卫：`Makefile:130-140` 的
  `test-integration` / `test-integration-required` 做了「discovered vs assigned 必须逐个
  匹配」的分片校验。不改。
- **`testing.Short()` 分支**（`test/perf/e2e_load_bench_test.go:78`、
  `test/stress/group_throughput_test.go:93,167,228`、
  `execution/subgraph/map_body_scale_test.go:55`）在 `make test` 下会执行——`test:` 目标
  不传 `-short`。只有手工加 `-short` 才略过。不改。
- **能力型跳过**（`backend/internal/statestoretest/node_lease_renew_contract.go:216`、
  `group_state_contract.go:364,575,579`）：后端未实现某可选接口时整段契约子测试消失并报 ok，
  且没有清单强制哪个后端必须实现。`group_state_contract.go:355-362` 记录过同型事故
  （老断言是 `len(OutboxIDs) != 0`，「Deleting the wait_any branch outright from BOTH
  backends left the entire ./backend/... tree green」）。这条值得做，但它要先回答
  「哪个后端必须实现哪个接口」这个设计问题，不属于本批的机械修复。
- **`test/security/tracked_content_test.go:72`**（不在 git 工作树时跳过）与
  **`node/layering_test.go:34`**（go 工具链不在 PATH 时跳过）：跳过条件合理——前者无树可扫，
  后者已经写得较好（`LookPath` 也失败才 skip，否则 `t.Fatalf`）。不改。
