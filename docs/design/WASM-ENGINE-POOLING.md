# WASM 引擎池化与两阶段初始化设计（D5）

> Status: **P1 + P2 + P3 已实现**（reactor 引擎 + 实例池 + 两阶段初始化落地于 `node/internal/code/script/wasm/{pool.go,host.go,reactor.go}`；磁盘 CompilationCache + startup warmup 落地于 `cache.go` 与 `cmd/runner/run.go`；换池协议由 supply 驱动，见 §6.4 与 [SUPPLY-NODE.md](./SUPPLY-NODE.md)，单测全绿含 `-race`）
> 关联：[NODE-GROUP-COLOCATION.md](./NODE-GROUP-COLOCATION.md)、[HIGH-THROUGHPUT-INGESTION.md](./HIGH-THROUGHPUT-INGESTION.md)、[SUPPLY-NODE.md](./SUPPLY-NODE.md)
> 现状代码：`node/internal/code/script/wasm/{wasm.go,wazero.go,pool.go,host.go,reactor.go,cache.go,config_loader.go,supply_consumer.go}`、`testdata/{reactor,reactorspin,reactormin}/`、`node/internal/code/script/{engine/warmup.go,warmup.go}`、`node/node.go`（`WarmupScriptEngines`/`PrewarmWasmModule`）、`cmd/runner/run.go`

## 0. 背景与目标

当前 `wasm/wazero` 引擎是 **command model**：每次 `Execute` 都 `InstantiateModule` 建一个新实例、跑完 `_start`、`mod.Close`。对标准 Go 编译的 wasip1 模块，这意味着**每次调用重跑一遍 Go 运行时 bootstrap**（GC / scheduler / allocator 初始化）。

本设计把它改成 **reactor model + 实例池 + 两阶段初始化**：模块用 `_initialize` 常驻，通过 `//go:wasmexport` 导出的函数被反复调用；配置（如清洗打标规则）在 `configure` 阶段编译一次并常驻，热路径 `eval` 只做求值。

**目标不限于流量采集**：任何「初始化昂贵、随后高频调用」的 wasm 用例都受益，尤其是初始化伴随配置加载的场景（规则集、词典、模型参数、编译好的表达式）。

### 0.1 实测基线（本机 Apple M3，Go 1.26，wazero v1.9.0，guest = Go+expr-lang/expr）

| 指标 | command model（现状） | reactor + 池化（本方案） |
|---|---|---|
| 每次调用固定开销 | 20.9 ms（其中 11.9 ms 纯 bootstrap） | **345 µs**（20 条规则，含内存读写） |
| 单线程吞吐（20 规则） | 48 msg/s | **2897–3638 msg/s** |
| 8 实例池聚合吞吐（20 规则） | — | **12508 msg/s** |
| 每次分配 | 26 MB/op、4519 allocs/op | 常驻，热路径几乎零分配 |
| 模块编译（冷） | 2.23 s | 一次性，可磁盘缓存 |

> 数字为一次性环境基线，**不能外推为容量承诺**（遵循 HIGH-THROUGHPUT-INGESTION.md §1 口径）。

## 1. 已实测确立的约束（决定架构的硬事实）

以下每条都由临时 PoC 实测得出（测试已删除，结论记录于此）：

1. **状态跨调用存活。** guest 包级变量（编译好的 `*vm.Program`）在多次导出函数调用间保留；`initCount` 连续调用返回 1,2,3 证明。这是整个方案成立的前提。

2. **单实例不能并发调用。** 8 goroutine 同时调用同一 `api.Module` → **进程级 fatal fault**（不是数据错乱，是崩溃）。原因：linear memory 与 guest 全局共享，`alloc` 指针会被并发覆写。→ **必须用实例池，一个实例同一时刻只被一个 goroutine 持有。**

3. **超时只杀单个实例，不牵连同 runtime 的其他实例。** 一个实例在 `ctx` deadline 下 spin 超时后，同 runtime 的其他实例、以及新建实例都正常。→ **可以「一个 runtime 托管整个池」，不必每实例一个 runtime。**（这一条推翻了我最初的假设。）

4. **但超时后的实例自身永久不可用。** 被 `WithCloseOnContextDone` 关掉的实例，后续任何调用返回 `module closed with context deadline exceeded`。→ **池必须丢弃超时实例并补建，不能归还复用。**

5. **`CompilationCache` 跨 runtime 共享有效。** 第一个 runtime 编译 2.85 s，后续共享同一 cache 的 runtime 仅 45–48 ms（≈60× 提速）。

6. **磁盘 `CompilationCacheWithDir` 跨进程重启有效。** 首次 3.18 s，进程重启后新建 cache 对象 + 新 runtime 仅 82 ms。→ **解决冷启动，runner 重启不再付 2 s+ 编译。**

7. **热重配置可行且干净。** 对常驻实例重复调 `configure`：旧规则正确失效（`inits:2`，v1 规则不再命中），50 次重配 200 规则后 linear memory 从 5 MiB 涨到 10 MiB 后**稳定**（guest GC 回收）。→ **规则热更新 = 重调 configure，无需重建实例。**

8. **16 MiB 内存上限够用。** 500 条规则、200 规则 ×50 次重配均未 trap（`DefaultWasmMemoryPages = 256`）。

9. **吞吐随规则数线性下降**（求值成本主导）：20 规则 3638/s、100 规则 784/s、500 规则 185/s（单线程）。× 池大小近似线性放大。

10. **坏配置整体拒绝有效。** 新实例 Fresh 态 `configure`：提交含语法错的配置 → `configure` 返回 <0。→ B 案下直接翻译成「新池不 warm 成功就不换,老池 last-good 保留」（见 §6.3 不变量 1）。

11. **原地重配会造成池内代次 skew（这条实测否掉了 A 案）。** 只 reconfigure 一个活实例后，该实例 `gen=2`、其余 `gen=1`。→ 原地热重配（A 案）必须引入 cfgGen + lazy reconfigure 才能处理 skew,复杂度高。**本方案改用 B 案「配置变即换整池」,一池实例代次天然一致、无 skew**（见 §6.3）。此测量是选型依据,非当前实现所需。

12. **大配置切换耗时可观。** configure：20 规则 17ms、100 规则 57ms、500 规则 484ms、1000 规则 1.18s。→ B 案下这笔 configure 成本落在**后台新池实例**上,老池全程正常服务,`active` 指针原子替换不阻塞任何 borrow（见 §6.3 不变量 3）。

## 2. 参考项目的设计（借鉴点）

> 注：本节结论经四个调研 agent 于 2026-07-29-30 用一手源（proxy-wasm/spec 与 cpp-host 源码、wazero v1.9.0 tag 源码 + godoc、Extism kernel 源码、Bytecode Alliance/Fastly/Fermyon 官方文档）核实。仍有个别标「不确定」处已注明。

### 2.1 Envoy proxy-wasm —— 三层 context 生命周期（源码级核实）

proxy-wasm ABI 把生命周期分三层，正是「初始化 vs 配置 vs 请求」的经典分离：

- **VM context**（`proxy_on_vm_start`）：VM 级一次性初始化，整个 VM 生命周期一次；返回 `false` 整体拒绝该 VM。
- **Root/plugin context**（`proxy_on_configure`）：**配置加载层**。host 经 `proxy_get_buffer_bytes(PLUGIN_CONFIGURATION)` 把配置交给 guest；返回 `false` 整体拒绝该配置。
- **Stream context**（`proxy_on_context_create` per request）：每请求一个轻量 context，共享 root 的配置。

**关键核实点(直接印证本方案 B 案)**：proxy-wasm **配置从不原地重配**——cpp-host 用 `makePluginKey = Sha256(root_id || plugin_configuration || key)` 做 key,配置一变 → key 变 → **造一个带全新 root context 的新 PluginHandle 并跑 `onConfigure`**,旧 root context 走 `proxy_on_done`/`_delete` 拆除,VM 保持不变。这就是我们 §6.3 的「配置变即换池」。并发模型上,proxy-wasm 是**每 worker 线程一个线程本地 VM、单 VM 内串行多路复用**;我们因约束 #2(Go guest 不能并发)改用**实例池**替代它的「每线程一 VM」。

### 2.2 wasmtime / Fastly —— InstancePre + Pooling Allocator + CoW

- **InstancePre**：把「实例化前的链接/校验」预计算好，`instantiate` 只剩内存分配，实例化降到微秒级。**wazero 无等价 API(已核实)**——它是两阶段模型：`CompileModule` 已合并了解码/校验/编译 + host 绑定,`InstantiateModule` ≈ wasmtime 的 `InstancePre::instantiate`(只做每实例内存/表/全局分配 + 跑 start)。没有单独物化的 pre-instance 对象。官方推荐「compile once, instantiate many」,但**「常驻 warm 实例池」不是 wazero 文档化的推荐模式**,是合理的用户侧优化。→ 我们用预热常驻实例池近似 InstancePre 效果。
- **Pooling allocator + CoW**：预分配 slot,实例内存来自「初始化后镜像」的 copy-on-write,初始化只做一次、每个新实例从镜像 fork。wasmtime 实测 SpiderMonkey.wasm 实例化 ~2ms → ~5µs(≈400×)。**wazero 无内置等价能力(已核实)**:无 `PoolingAllocationConfig`、无 snapshot/fork/CoW,默认每实例线性内存是 Go 堆上 `[]byte`(非 mmap),每次 `InstantiateModule` 都分配全新内存并重跑 init。**唯一可复用的是编译产物(CompilationCache),对实例内存/init 状态无帮助。这正是我们必须池化复用、而不能学 Spin/Fastly「instance-per-request」的根本原因**——它们的 5µs 实例化靠 wasmtime CoW,wazero 差四个数量级(~45ms)。
  - **DIY 逃生路线(实验性,列为后续探索)**:`experimental.WithMemoryAllocator`(经 `context.Context` 传入,~v1.7.1+,API 可能变更)是分配钩子,可自建 mmap 池 + 手动 memcpy 恢复捕获的 post-init 内存镜像。issue #2499 有人这么做,3 MB 模块 memcpy 恢复 ~78µs。**这是 Wizer 死路之外唯一能真正跳过 Go 运行时 init 的路子**,但需自己实现 CoW 逻辑、依赖实验性 API,收益(省的是每实例 ~12ms bootstrap)需与复杂度权衡。**不进 MVP,记为 §8 未决探索项。**

### 2.3 Wizer —— 预初始化快照（最相关的技术）

Wizer 把 wasm 模块「跑一遍初始化，再把结果内存快照回写进 `.data` 段」，生成一个「已初始化」的新模块。加载它就跳过 init。**这正好对应用户说的「后续也会存在初始化的情况」**：如果某些配置是**静态的**（编译期已知的规则、词典），可以用 Wizer 预烘焙进模块，运行时零初始化。

**限制（已核实,见 §8）**：Wizer 在字节层面盲拍内存,唯一有先例的是 TinyGo+`-scheduler=none`,标准 Go 因 `_start` bootstrap 冲突无可行先例;且 TinyGo 缺函数反射使 expr-lang 仅受限可用、`recover` 失效。→ **Wizer/TinyGo 路线放弃,冷启动优化改由 P2 磁盘 CompilationCache 承担。**

### 2.4 Extism —— Plugin 复用 + host function 内存约定（源码核实）

Extism 的内存约定与本方案 §4 一致,核实到 kernel 源码:**offset 与 length 是分开的两个值**(不打包进单个 i64);导出函数**不**把 output offset 当返回值,而是写进新分配块 → 调 `output_set(offset, length)` → **返回状态码(0 成功/非 0 错误)**,host 再读 `output_offset()`/`output_length()`。这正是我们 §4.1 的 `eval`→返回长度/错误码 + `out_ptr`/`out_len` 分离读取。Extism 的 `Plugin` **可跨多次调用复用、显式状态持久,但非线程安全**(Rust `call` 取 `&mut self`,Go SDK 明标 not thread-safe)→ 官方解法是 `CompiledPlugin` 编译一次 + 每并发者一实例。**与我们约束 #2「编译一次 + 每并发者一实例 + 自建池」完全同构。**

### 2.5 仓库内既有先例：qjs

`js/qjs.go` 就是 QuickJS 编译成 wasm 跑在 wazero 上，且 `js/goja.go` 的 `sharedGoja` 已经是 `sync.Pool` of VMs 的池化模式。**架构上不是新东西**，本方案是把同一模式推广到 `wasm/wazero`，并加上两阶段初始化。

## 3. 架构总览

```
                        ┌─────────────────────────────────────────┐
                        │  wazero.Runtime (进程级单例)              │
                        │  + CompilationCache (磁盘, 跨重启)        │
                        │  + WithCloseOnContextDone(true)          │
                        └─────────────────────────────────────────┘
                                        │ CompileModule (一次, 磁盘缓存)
                                        ▼
                        ┌─────────────────────────────────────────┐
                        │  CompiledModule (sha256 keyed, 现有 LRU)  │
                        └─────────────────────────────────────────┘
                                        │ InstantiateModule × N (预热)
                                        ▼
   pool[moduleHash] ──►  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐   chan *instance
                         │ inst │ │ inst │ │ inst │ │ inst │   (borrow/return)
                         └──────┘ └──────┘ └──────┘ └──────┘
                          每个: _initialize 已跑 + configure 已加载规则
                                        │
   Execute(ctx, code, globals) ─────────┤ borrow → alloc+write globals →
                                        │ eval → read outPtr → return-to-pool
                                        │ (超时/出错 → 丢弃并补建)
```

## 4. Guest ABI 约定（reactor 生命周期契约）

ABI 不是一堆平铺的导出函数，而是**按生命周期分层的契约**：每一层对应 §6.1 的一个生命周期层级、§6.2 状态机的一组转换。宿主的生命周期管理器只跟这些导出函数打交道，guest 作者只要实现这套契约就能被池化托管。**这是本方案能做「生命周期管理」而非仅「跑一次」的根基。**

内存传递统一约定 **host-allocates-for-input, guest-allocates-for-output**：host 调 `alloc` 拿输入缓冲、写入、调对应函数、再从 out 通道读回。

### 4.1 分层导出函数

配置版本管理放在 **host 侧**（host 知道每个实例是用哪一版配置实例化+configure 的，见 §6.3 B 案），所以 guest 不暴露 `config_gen`；出错的实例一律 doom 补建（§5.6），所以 guest 不需要 `reset`/`health`。契约因此收敛到一组小而稳的函数：

| 层 | 导出函数 | 签名 | 语义 | MVP |
|---|---|---|---|---|
| **协商** | `abi_version` | `() int32` | 返回 guest 实现的 ABI 版本；host 启动校验，不匹配拒绝加载 | 必需 |
| **VM（一次性）** | `_initialize` | WASI 标准 | reactor 启动，跑 Go 运行时 bootstrap + 包级 init，**每实例一次** | 必需 |
| **内存** | `alloc` | `(size int32) i32`(ptr) | guest 分配 size 字节输入缓冲，返回指针；host 写入 | 必需 |
| **配置（Instance 层）** | `configure` | `(n int32) int32` | 读 inBuf[:n]=配置 JSON，**原子编译并替换**，返回规则数（≥0）或错误码（<0）。见 §6.3 | 必需 |
| **调用（Call 层）** | `eval` | `(n int32) int32` | 读 inBuf[:n]=单条输入 JSON，求值，写 outBuf，返回输出长度（≥0）或错误码（<0） | 必需 |
| | `out_ptr` | `() i32`(ptr) | 返回 outBuf 指针 | 必需 |
| | `out_len` | `() int32` | 返回上次 `eval`/`configure` 写入 outBuf 的长度；与函数返回值一致，供分离读取路径 | 推荐 |
| **停机** | `teardown` | `() int32` | 优雅停机钩子：flush/释放 guest 持有的外部资源（对应 proxy-wasm `proxy_on_done`）。纯计算 guest 可空实现。host 在 `mod.Close` 前调 | 可选 |

**分层的意义**：状态机（§6.2）每条转换都落到具体函数——`Fresh` 由 `_initialize` 产生；`Fresh→Ready(gen=G)` 由 `configure` 驱动（**每个实例一生只 configure 一次**，见 §6.3 B 案）；`Ready→InUse→Ready` 是 `eval`；`→Doomed` 由 `eval<0`/超时触发；停机走 `teardown`+`Close`。MVP 只需「必需」项即可跑通池化；`out_len`/`teardown` 是让读取路径与停机更规整的增量。

> **为何砍掉 `config_gen`/`health`/`reset`（用户拍板 + 调研佐证）**：B 案下配置版本是 host 造实例时就绑定的，无需向 guest 查询代次；proxy-wasm 也不给 guest 留 `reset` 钩子，脏实例一律重建。少三个导出函数 = guest 契约更小、更难写错、更易做 ABI 一致性测试。

### 4.2 错误码约定（负返回值）

`configure`/`eval` 的负返回值是**分类错误码**，同时 guest 把结构化错误详情 JSON 写进 outBuf（host 按 `out_len` 读出用于日志/告警，绝不回传给业务侧原文，遵守敏感信息不外泄）：

```
 0..N  成功（configure=规则数，eval=输出字节数）
  -1   ERR_DECODE     输入 JSON 解码失败
  -2   ERR_UNCONFIGURED  未 configure 就 eval（Fresh 态被误用）
  -3   ERR_CONFIG     配置编译失败（坏规则）→ 该实例作废，不投入池（§6.3 B 案）
  -4   ERR_EVAL       求值 panic/错误 → 实例进 Doomed（§5.6 防污染）
  -5   ERR_OUTPUT     输出超 DefaultMaxOutputBytes
```

host 依错误码决定实例去向：`-3` 出现在新实例的初次 `configure`（B 案下每实例只 configure 一次）→ 丢弃该新实例、保留旧池不换（配置不缩水，见 §6.3）；`-4` 实例 Doomed 补建；`-1/-2/-5` 是调用级错误，实例可留用。**错误码与实例回收决策解耦**——宿主能区分「这次调用坏了」（实例留用）和「这个实例坏了」（Doomed 补建）。

### 4.3 调用序列（host 侧）

热路径 `eval`（每条消息）：
1. `p := alloc(len(input))` → `mem.Write(p, input)`
2. `n := eval(len(input))`；`n < 0` → 按 §4.2 处理，读 outBuf 拿错误详情
3. `op := out_ptr()` → `mem.Read(op, n)` → **拷出**（`mem.Read` 返回内存视图，必须 copy）

配置切换（低频，B 案，§6.3）：**不在活实例上重配**。host 后台按新配置**新建一批实例**：`InstantiateModule`（共享 CompilationCache）→ `configure(新 cfg)`；全部成功 → 原子换入池、老实例 drain 后 `teardown`+`Close`；任一新实例 `configure` 返回 <0 → 整批丢弃、老池不动（last-good 保留）。

> 单实例串行约束（约束 #2）在此层强制：同一 `api.Module` 的 `alloc→eval→out_ptr` 三步必须由同一 goroutine 独占完成，池保证一个实例同一时刻只被一个借出方持有。

### 4.4 与现有 stdin/stdout ABI 的关系

现有 `wasm.go` 的 `encodeStdin`/`decodeStdout` 是 command model 专用。**保留它作为 `wasm/wazero` 的 legacy 模式**（向后兼容已有 guest），新增 `wasm/wazero-reactor` 作为独立 runtime 名注册，或在模块探测到导出 `eval`+`abi_version` 时自动走 reactor 路径。推荐**独立 runtime 名**，语义清晰、不破坏现有测试。

## 5. 实现要点

### 5.1 实例池（`node/internal/code/script/wasm/pool.go`，新增）

```go
// 一个 activePool 是「某一版配置」对应的一池实例。配置切换 = 造一个新
// activePool、原子换掉指针（B 案，§6.3）。老 pool 里的实例 drain 完再拆。
type reactorEngine struct {
    rt      wazero.Runtime
    cm      wazero.CompiledModule
    active  atomic.Pointer[activePool] // 当前生效的池；配置切换时原子替换
    size    int
}

type activePool struct {
    gen  uint64                 // 配置代次（= 造这个池所用的 cfg 版本），仅 host 侧记录
    cfg  []byte                 // 造这池实例时用的配置快照（补建实例时重放）
    free chan *pooledInstance   // 容量 = poolSize
}

type pooledInstance struct {
    mod  api.Module
    mem  api.Memory
    alloc, eval, outPtr api.Function
    // 无 gen 字段：一个 pooledInstance 天生属于某个 activePool，其代次即 pool.gen。
}
```

- **borrow**：`p := active.Load(); inst := <-p.free`。**借出即用,不做 lazy reconfigure**——池里的实例造出来就已是本代配置。
- **return**：正常 → `p.free <- inst`；`eval` 出错/超时（约束 #4）→ `inst.mod.Close()` 并异步按 `p.cfg` 补建一个新实例投回**同一** pool。
- **补建**：`InstantiateModule`（共享 CompilationCache，约束 #5，~45ms）+ `configure(p.cfg)`。
- **配置切换（§6.3 B 案）**：后台造新 `activePool`（新建 poolSize 个实例 + 各 `configure(newCfg)`）→ 全部成功才 `active.Store(newPool)` 原子换入 → 老 pool 停止接新 borrow、在途实例归还后 `teardown`+`Close`。任一新实例 `configure<0` → 整批丢弃、`active` 不动（last-good 保留、不缩水）。

### 5.2 两阶段初始化与配置来源

**关键设计：配置加载与 wasm 引擎解耦。** 引擎只认「一段 config bytes」，不关心来自哪。三种喂法：

| 来源 | 适用 | 热更新 |
|---|---|---|
| 静态内嵌（节点参数 `config`） | 规则随工作流发布 | 重新注册工作流 |
| HTTP 拉取（前置 `xflow.http` 节点或引擎内 loader） | 规则在外部服务 | TTL 轮询 → 重放 configure |

对流量采集场景：走 HTTP 拉 SAS 的 `clean-rule/list` + `tagrule/list`，引擎持有 TTL 缓存，到期重拉；version 变化才触发一次「造新池、原子换入」（代次 +1，B 案）。这解决了之前「规则集跨消息放哪」的问题——**放在常驻实例的 guest 内存里**。配置会变，其变更语义与实例生命周期深度耦合，单列为 §6。

### 5.3 超时隔离（约束 #3/#4）

- 一个进程级 runtime，`WithCloseOnContextDone(true)`。
- 每次 `eval` 用 `context.WithTimeout(ctx, DefaultScriptTimeout)`。
- 超时 → 该实例 close（约束 #4），池补建。**blast radius 仅该实例**（约束 #3 已验证）。

### 5.4 冷启动（约束 #6）

- runtime 用 `NewCompilationCacheWithDir(<runner-data-dir>/wasm-cache)`。
- 首次编译 ~2–3 s 落盘；进程重启后 ~80 ms 命中。
- 磁盘缓存目录名 keyed `wazero-<version>-<GOARCH>-<GOOS>`(已核实源码),版本/架构/OS 升级自动失效,安全。
- **两个已核实的坑**:(1) 磁盘缓存**无 flock**——顺序重启复用安全,但同一目录被多进程并发写是未文档化行为;runner 单进程使用无碍,若多 runner 共享数据卷需各自子目录隔离。(2) reactor 必须用 `ModuleConfig.WithStartFunctions("_initialize")`——默认 `_start`(command 模块)会在实例化后关闭模块,导出函数不可再调。

### 5.5 预热钩子（复用现有 `engine.Warmup`）

**发现：`engine.Warmup` 已定义但无任何生产调用点**（`cmd/runner`、`cmd/server`、`service/` 均未调用；qjs 的 330ms 冷启动因此落在首个请求上）。本方案：

1. wasm 引擎 `init()` 里 `engine.RegisterWarmer(warmupPool)`。
2. `warmupPool` 编译模块 + 预建 poolSize 个实例 + 初次 configure。
3. **在 `cmd/runner` 启动路径补上 `engine.Warmup(ctx)` 调用**（顺带修掉 qjs 的既有问题）。

### 5.6 状态污染防护（约束 #7 的另一面）

reactor 的代价：guest 全局状态跨调用存活，一次 `eval` 若污染全局会影响下次。防护：

- guest 侧：`eval` 只用局部变量 + 只读 `progs`，不写全局（除 `outBuf`）。这是 guest 作者的约定，写进 ABI 文档。
- host 侧：`eval` 返回错误（`ERR_EVAL(-4)` 或 panic）→ 丢弃实例补建（保守，避免脏状态扩散），对齐 goja 的 `cleanup()` 丢弃污染 VM 的策略。**不做 guest 侧轻量复位**（proxy-wasm 同样不给 guest 留 reset 钩子，脏了就重建）——补建走共享 CompilationCache（~45ms）成本可接受。
- **单实例串行由池强制**:约束 #2 已双重确认(wazero godoc 明说 `Function.Call` 非 goroutine-safe + issue #2217 `-race` 实测,连不同 `Function` 句柄并发也不安全),池保证一实例同一时刻仅一个借出方。

## 6. wasm 程序生命周期管理

reactor 模型下实例是常驻的，「什么时候建、什么时候配、什么时候弃、配置怎么换」构成一个必须显式管理的状态机。这一章是本方案与 command model 的核心差异所在。

### 6.1 三个层级的生命周期（对齐 Envoy proxy-wasm 的三层 context）

| 层级 | 对象 | 建立时机 | 销毁时机 | 数量 |
|---|---|---|---|---|
| **Module（编译产物）** | `wazero.CompiledModule` | 首次用到该 code hash（warmup 或首个请求） | LRU 逐出（sha256 keyed）或 runtime.Close | 每 code hash 一份，全进程共享 |
| **Pool（配置代次）** | `activePool`（一版配置 + 一池实例） | warmup 首建 / 配置切换新建 | 被新代次原子换下、实例 drain 完 | 每 code hash 同时 1 个生效（切换期间瞬时 2 个） |
| **Instance（常驻实例）** | `api.Module` + 导出函数 | 造池时预建 / 池内补建 | eval 错误、超时、优雅停机、所属池被换下 | poolSize 个/每 activePool |
| **Call（单次调用）** | 一次 `alloc→eval→outPtr` | 借出实例 | 归还实例 | 瞬时 |

Module 昂贵、稳定、共享；Pool 绑定一版配置、整体原子替换；Instance 中等成本、有状态、池化、**一生只 configure 一次**；Call 廉价、无状态、串行占用一个实例。**配置（规则）挂在 Pool 层**——配置变更即换池,而非在活实例上原地改（§6.3 B 案，对齐 proxy-wasm 配置变即新建 root context）。

### 6.2 实例状态机

```
                 InstantiateModule + _initialize
   (nil) ──────────────────────────────────────► Fresh
                                                    │ configure(cfg)   一生一次
                                    configure<0 ┌───┤
                                    丢弃该实例   ▼   ▼
                                  (不投入池)  (X)  Ready ◄──────┐
                                                    │    │      │ 归还
                                               借出 │    │      │
                                                    ▼    │      │
                                                 InUse ──┘──────┘
                                                    │
                                                    │ eval<0 / 超时
                                                    ▼
                                    Doomed ──► teardown? ──► mod.Close()
                                       │
                        ┌──────────────┴───────────────┐
                        │ 因 eval/超时被 doom            │ 因所属池被换下
                        ▼                               ▼
              异步按 pool.cfg 补建 Fresh          不补建，随池整体拆除
```

关键转换（每条都有 §1 实测支撑，括号内是驱动它的 §4.1 导出函数）：

- **(nil) → Fresh**：`InstantiateModule` + `_initialize`。
- **Fresh → Ready**：`configure` 加载配置，**每个实例一生只此一次**。返回 <0（见 §4.2）→ 丢弃该实例、不投入池；若发生在造新池阶段 → 整批放弃、老池不动（§6.3）。
- **Ready → InUse → Ready**：正常借出/归还，热路径走 `alloc`/`eval`/`out_ptr`。归还前若 `eval` 返回 `ERR_EVAL(-4)` 或 panic → Doomed（约束 #7 防污染）。
- **任意 → Doomed**：`eval` 出错或超时后实例永久 `module closed`（约束 #4）。**若因 eval/超时**：异步按 `pool.cfg` 补建同代实例投回本池（共享 CompilationCache，约束 #5，~45ms），不阻塞借出方——从池里换下一个即可。**若因所属池被新代次换下**：不补建，随池整体拆除。
- **配置切换**：不改活实例，造新 `activePool` 原子换入（§6.3）。
- **任意 → 停机**：`teardown` → `mod.Close`（§6.6）。

### 6.3 配置变更协议（核心，B 案：配置版本 = 实例代次）

**决策（用户拍板 + 四项调研佐证）**：配置**从不在活实例上原地重配**。配置一变 → host 后台按新配置**造一整池新实例** → warm 好后**原子换入** → 老池 drain 拆除。这正是 proxy-wasm/cpp-host 的做法（配置哈希建 key、配置变即新建 root context，源码 `makePluginKey` 证实）、Envoy 的 warm-then-atomic-swap + xDS NACK-keeps-last-good，以及 Javy「不变量烘焙一次 + 变量运行时喂入」的分离思路。

> **为何不用原地热重配（否掉的 A 案）**：原地重配需要在 guest 内正确实现「临时 slice 全量编译成功才替换」的原子语义，把不变量 1 的正确性负担压在**每个 guest 作者**身上、难验证；还要引入 cfgGen 代次 + 借出时 lazy reconfigure 来处理池内代次 skew（实测确有 skew：只重配一个实例后 inst0=gen2 其余=gen1）。这套复杂度唯一价值是省「配置切换时的实例化开销」,而配置是 TTL 低频变更、池又小,收益不抵复杂度。**业界无一成熟项目在活实例上原地改配置。**

B 案下三条不变量**由 host 免费获得**，不再依赖 guest 正确性：

**不变量 1 — 原子替换，坏配置整体拒绝，绝不缩水。**

host 造新池时,每个新实例在 Fresh 态 `configure(newCfg)`。任一新实例返回 <0（坏规则）→ **整批新池丢弃、`active` 指针不动**,老池继续服务。guest 的 `configure` 只需「编译失败就返回 <0」,**无需实现原地原子替换**——它面对的永远是一个全新实例的首次配置。

> 原实测证据仍成立(约束 #10):坏配置导致 `configure` 返回 <0。B 案下这直接翻译成「新池不 warm 成功就不换」,last-good 天然保留。§4.2 里点名的「`progs=progs[:0]` 静默缩水」反例在 B 案下**根本不可能发生**,因为没有活实例被原地改。

**不变量 2 — 代次一致,无 skew。**

一个 `activePool` 里所有实例都是用同一份 `cfg` 造的,代次天然一致——**B 案消除了 A 案的 skew 问题**(约束 #11 的 skew 是 A 案原地重配才有的现象,B 案不存在)。`active atomic.Pointer[activePool]` 的一次 `Store` 就是全局代次推进,读侧 `Load` 无锁。

**不变量 3 — 切换不阻塞在途请求,也不丢消息。**

- 造新池在后台进行(实测 500 规则 484ms、1000 规则 1.18s 的 configure 成本落在后台新实例上),老池全程正常服务。`active.Store` 是一次原子指针替换,不阻塞任何 borrow。
- 已借出的老实例照常跑完 `eval` 再归还;老池的 `free` channel 排空后统一 `teardown`+`Close`。
- 因 at-least-once(见 HIGH-THROUGHPUT-INGESTION.md),切换期间即便有实例 Doomed,消息走另一实例或重投,不丢。
- **代价(已接受)**:切换瞬间双份内存(2 × poolSize × ~5.5MiB,如 8 实例约 88MiB),持续到老池拆完;低频切换可忽略。

**正面(B 案的一个免费副作用)——免疫 Flink Broadcast State 的副本发散问题。** Flink 的 Broadcast State 把配置广播给每个 task 实例,但配置的应用(如何把字节变成可执行规则)仍由每个 task 实例的 guest 代码各自完成——多副本收敛与否取决于每个实例的处理逻辑是否确定性、无副作用,Flink 本身只在编译期做类型窄化,不做运行时一致性检查。B 案不需要这份信任:一个 `activePool` 里的所有实例都是用 `buildPool` 对同一份 `p.cfg` 字节逐一 `configure` 造出来的(`pool.go` `buildPool`),收敛由**构造方式**保证——同一份字节喂给同一份 guest `configure` 实现——而不是靠每个 guest 作者写出确定性代码。跑偏的可能性被整池共享同一构造路径这件事本身排除了,不需要额外校验。

**反面(必须写清,不试图消除)——B 案的原子性只在单进程内成立,跨 runner 不同步。** `active.Store` 是这个进程里的一次原子指针替换;它对同一进程内所有 borrow 生效,但对其他 runner 上的 `reactorEngine` 毫无影响——每个进程独立收到配置变化通知(通过 supply 分发,见 [SUPPLY-NODE.md](./SUPPLY-NODE.md) §3)、独立换池。跨 runner 的换池时刻天然错开,最坏倾斜 10–20 秒;在 12000 msg/s 的吞吐下,这意味着窗口内约 12–24 万条消息被两套规则版本混合打标。设计不试图消除这个窗口——消除需要一个全局 barrier 挂起整条流,代价是秒级停摆,没有可比系统这么做。可观测性替代消除:每条结果都带 `config_generation`(`reactor.go` 的 `ConfigGenerationKey`,值取自 `pool.revision`,即产出这条结果时实际生效的 `SupplyResource.Revision`),下游可按 `(key, config_generation)` 分组、定位落在旧版本窗口内的记录、按需重算。

### 6.4 配置来源与换池驱动

引擎内置一个 `ConfigLoader` 接口（`node/internal/code/script/wasm/config_loader.go`，可插拔），负责把外部配置变化翻译成一次「造新池、原子换入」：

```go
// config_loader.go:18-24
type ConfigLoader interface {
    // Load returns the current config bytes and a version identifier.
    Load(ctx context.Context) (cfg []byte, version string, err error)
}
```

- **静态内嵌**：`StaticLoader(cfg)`（`config_loader.go:29`）直接返回节点参数，version = cfg 的 sha256，一次 warmup 之后永不再触发换池。
- **后台 TTL 轮询骨架仍在**：`startWatcher`（`config_loader.go:107`）按 `ttl` 周期性调 `Load`,version 变化才 `swapConfig`；version 不变零开销。但这个骨架**没有、也不会有 HTTP 实现**——曾经设计过在引擎内置一个 HTTP 拉取 loader（后台 goroutine 定期拉 SAS 规则接口），已被否决,原因是三条独立且都站得住的阻塞（不是"暂不做",是**架构上不能做**）：
  1. **凭证在 warmup 期不可达。** `Input.Credential` 是按请求依赖注入的（resolver + namespace 绑定当前调用的租户上下文）；wasm 引擎的后台 goroutine（`startWatcher`）没有 `Input`，也没有触发它的那次请求的租户上下文。要在引擎内部拉凭证，唯一办法是开一个进程级凭证后门，绕开每次调用的租户边界——这正是 secure-coding 规则明确禁止的「架空多租户隔离」。
  2. **会重造并绕过 `xflow.http` 节点的 `HTTPHostPolicy`。** `xflow.http` 节点对出站 HTTP 有统一的 SSRF 防护策略（allowlist、私有 IP 阻断等，见 `http.go`）。引擎内置一个独立的 HTTP 客户端等于重新实现一遍这套策略，且天然会漏掉它——一个新的、未经审查的出站请求路径，正是 SSRF 防护要防的那类口子。
  3. **架空 workflow 抽象。** 一次引擎内部发起的 HTTP 拉取不产生 execution 记录、不受 `OnError` 策略约束、没有 retry/重试语义——它完全绕过了 workflow 引擎对"一次调用"的全部契约保证，把一个应该可观测、可重试、可审计的操作，变成了引擎内部一段不透明的副作用。
- **实际生产路径：supply 通道。** 配置变化通过 `$supplies` + 声明的 `dependency_edges` + `RegisterSupplyConsumer`（`supply_consumer.go:74`）流入——一个模块注册为某个 supply 节点的消费者后即被标记 `configFromSource`，配置变化经 [SUPPLY-NODE.md](./SUPPLY-NODE.md) 描述的采集/分发机制推送到 `Registry.Apply` → `supplyConsumer.OnSupplyChanged` → `swapConfig`。这条路径是 execution-agnostic 的：拉取发生在 activation 期的 `SupplyGate.Admit`（一次显式、可观测、受 `require_ready` 门控的调用），不是引擎内部悄悄发起的网络请求。`ConfigLoader`/`startWatcher` 骨架仍保留供旧的内嵌/测试调用者使用，但**不是生产路径**（`reactor.go:12-21` 的 `reactorConfigGlobal` 注释明确标注该 legacy 路径为 Deprecated）。

### 6.5 空配置与边界语义

- **空规则集**（`[]`，`{"rules":[]}`）实测被接受为「0 规则」，`configure` 返回 `0` 而非负数——这是**合法配置**，不是错误。`normalizeConfig(nil)` 的默认值本身就是 `{"rules":[]}`（`pool.go:513-514`）。对清洗打标，空规则集意味着「放行一切/不打标」，是业务上有效的一档，新池正常 warm 换入。
- **这与「无内容」是两件不同的事，边界必须分清：**
  - **空规则集**——引擎收到了配置字节，字节解出 0 条规则。合法，正常建池服务。
  - **无内容**——引擎从未收到任何配置字节（`e.active.Load() == nil`，即 `AvailUnavailable`，`pool.go:461-463`）。这不是「配置是空的」，而是「配置还没来」，由 activation 期的门控处理（`require_ready`，见 [SUPPLY-NODE.md §6](./SUPPLY-NODE.md#6-activation-time-gate)），不是引擎自己决定要不要接受。
  - **两档 `require_ready` 的可观测信号**：`true`（默认）→ 门控拒绝该 activation，Kafka offset 不前进，`xflow_supply_not_ready` 置 1；`false` → 门控放行、`Execute` 面对无池的情形（见下一条 transient error），`xflow_supply_unavailable_serving` 置 1。两档都不会静默用空规则集顶替「没配置」。
  - **绝不用空内容静默放行**：引擎从未把「没有配置」翻译成「按 0 规则跑」——`newInstance`/`buildPool` 从不在无 cfg 时调用 `configure("")`；未配置就是 `active == nil`，`borrow` 直接报 `"no active pool (unconfigured)"`（`pool.go:322`）。空规则集必须是配置源明确送来的 `{"rules":[]}` 字节，而不是引擎替配置源做的默认判断。
- **配置源不可达**（后台 loader/supply 拉取失败）：**保留当前生效池继续服务**，不造新池，记 metric + 告警。绝不因配置源抖动而清空规则。
- **新配置坏**（拉到了但含坏规则）：造新池时某新实例 `configure<0` → 整批新池丢弃、`active` 不动（§6.3 不变量 1），记 `rejected` metric + 告警。
- **首次配置就失败**（warmup 时配置源不可达或首份配置坏）：`active` 为 nil、无可用池；`Execute` 返回 transient error 触发上层重试/backpressure，而非用空配置静默放行。

### 6.6 优雅停机与资源回收

- runner 收到停机信号：停止借出 → 等在途 Call 归还（有界超时）→ 逐个 `teardown` + `inst.mod.Close()` → `runtime.Close()`。
- 磁盘 CompilationCache **不删**（下次启动复用，约束 #6）。
- 每实例常驻 ~5.5 MiB linear memory（实测 8 实例 44 MiB）；poolSize × 5.5 MiB 是稳态内存预算；**配置切换瞬间为双份**（旧池未拆完 + 新池），需在容量规划里留出这个峰值。

### 6.7 可观测性（新增 metric）

生命周期每个转换都要可观测，对齐 `observability/metrics/` 现有风格。**Task 17 之后全部已实现**——以下 13 个 metric（5 个 `xflow_supply_*` + 8 个 `xflow_wasm_*`）都在 `observability/metrics/metrics.go` 的 `metricHelp` 里有对应键，且都经 `observability/metrics/supply.go` 真实打点，不是仅有声明：

wasm 侧（`node/internal/code/script/wasm/pool.go`/`host.go` 触发）：

- `xflow_wasm_instance_total{state=ready|doomed}` gauge（`OnInstanceCount`）。**`state` 没有 `fresh` 档**——`Fresh` 是 `newInstance` 内 instantiate→configure 之间的瞬时态，实例进池时已经是 `Ready`（`buildPool` 的循环里 `configure` 成功才 `pool.free <- inst`），永远观测不到,故不设该标签值。
- `xflow_wasm_instance_recycled_total{cause=timeout|eval_error|shutdown|pool_swapped}` counter（`OnInstanceRecycled`）。
- `xflow_wasm_pool_swap_total{result=applied|rejected}` counter + `xflow_wasm_pool_swap_duration_seconds` histogram（`OnPoolSwap`，造新池到换入的耗时）。**只有这两档 result**——没有独立的 `source_error` 档：配置源不可达（loader/gate 拉取失败）不会走到 `swapConfig`，因此也不会产生一次 `pool_swap` 记录，这类失败由 `xflow_supply_fetch_total{result=error}`（下方）承担。
- `xflow_wasm_config_generation` gauge（`OnPoolSwap` 仅在 `result=applied` 时设置）。**语义是内容的服务端版本号，不是本进程池代次**：值取自 `swapConfig` 的 `revision` 参数——即产出这次换池的 `SupplyResource.Revision`——而不是 `activePool.gen`（`gen` 只是本进程内的换池计数，无法跨 runner 比较）。**字节相同但 revision 变化时不换池，也不更新该值**：`swapConfig` 只在 `buildPool` 成功后才写 `newPool.revision`（`pool.go:419`，必须在 `e.active.Swap(newPool)` 之前完成，因为 `Swap` 一执行,读者立刻能看到新指针,事后再写字段就会跟无锁读者竞态)，字节不变的情况下配置源侧一般不会触发新的 `swapConfig` 调用；即使触发,只要 `cfg` 字节相同,建出来的池仍是同样内容,该值也仍是"实际加载的那份字节对应的 revision"，不会因为 revision 号跳了而单独刷新。
- `xflow_wasm_config_rule_count` gauge（`OnPoolSwap`，同样只在 `result=applied` 且规则数可辨识时设置）。
- `xflow_wasm_pool_borrow_wait_seconds` histogram（`OnBorrowWait`，借出等待，判断 poolSize 是否不足）。
- `xflow_wasm_module_compile_total{result=hit|miss}` counter（`OnModuleCompile`）。**只有这两档**——没有独立的 `disk_hit`：内存 LRU 命中与磁盘 CompilationCache 命中都算 `hit`，wazero 的 `CompilationCacheWithDir` 对调用方是透明的,host 侧看不到"这次是内存缓存还是磁盘缓存命中"的区分。

supply 侧（`node/supply/registry.go`/`service/runner/supply_gate.go` 触发，详见 [SUPPLY-NODE.md](./SUPPLY-NODE.md)）：

- `xflow_supply_age_seconds` gauge（`OnConfigAge`）——当前生效内容的存活时长，配置源卡死时唯一还在变化的信号。
- `xflow_supply_fetch_total{name,result=ok|error}` counter（`OnSupplyFetch`）。
- `xflow_supply_not_ready{workflow,supply}` gauge，0/1（`OnSupplyNotReady`）。
- `xflow_supply_unavailable_serving{name}` gauge，0/1（`OnSupplyServingUnavailable`）。
- `xflow_supply_consumers{name}` gauge（`OnConsumerCount`）。


## 7. 分阶段落地

| 阶段 | 内容 | 验收 |
|---|---|---|
| **P1 MVP** ✅ 已实现 | reactor ABI「必需」项（`abi_version`/`_initialize`/`alloc`/`configure`/`eval`/`out_ptr`，§4.1）+ 实例池（单 `activePool`）+ 内存 CompilationCache + 超时/出错补建 + 实例状态机（§6.2） | ✅ 单测覆盖约束 #2（`-race` 下 32 goroutine×50 并发无 race）/#4（超时 doom+异步补建自愈不死锁）/#7（重配旧规则失效）；吞吐 18.5× command（205µs vs 3.8ms/op）|
| **P2** ✅ 已实现 | 磁盘 CompilationCache（进程级共享，两个 wasm runtime 复用）+ `engine.Warmup` 接入 runner 启动 + `out_len`/`teardown` 句柄实例化时解析 | ✅ 进程重启冷启动 **62 ms**（含建满整池），< 100 ms 达标 |
| **P3** ✅ 已实现（Task 1–19，2026-07-30 分支） | 换池协议（B 案，§6.3）落地为「supply 驱动」而非「HTTP+TTL loader 驱动」（§6.4 的三条否决理由）：DSL 新增 `NodeKindSupply`/`DependencyEdge`（Task 1）+ 图编译校验与两层图排除（Task 2–5）+ SDK 支持（Task 6）+ `SupplyResource` 存储层与 HTTP 端点（Task 7–9）+ `$supplies` 引用推导与编译期校验（Task 10）+ 进程内分发 `Registry`/`Consumer`（Task 11）+ `$supplies` 表达式根（Task 12）+ `xflow.supply.external`/`.static` 节点类型（Task 13）+ 需求随 activation 透传（Task 14）+ activation 期就绪门控 + 拉取客户端（Task 15）+ wasm 消费侧三级可用性与锁移出热路径（Task 16）+ `config_generation` 与全套 metric（Task 17）+ 两条回归测试（Task 18）+ 心跳 hint 与 `Observed()` 上报（Task 19） | ✅ 坏配置整批拒绝不缩水（`TestScriptNodeRulesComeFromSupplyNotConfig` 等）；`TestSupplyGateLosesNoMessages`/`TestSupplyGateRecoversOnRestart` 端到端验证门控不丢消息、重启可恢复；详见 [SUPPLY-NODE.md](./SUPPLY-NODE.md) |
| **P4（可选，可行性存疑）** | Wizer 预烘焙 / TinyGo guest ——**调研结论：标准 Go+Wizer 无可行先例，TinyGo 缺 `reflect.Value.Call` 使 expr-lang 仅受限可用（见 §8）。冷启动优化优先走 P2 的磁盘 AOT cache，不依赖内存快照。** | —— |

> **P1 实测（本机 Apple M3，Go 1.26.5，wazero v1.9.0，guest = 标准 Go + expr-lang，3 条规则）**：
> - reactor 热路径 `Execute`：**205 µs/op、7.7 KB/op、62 allocs**（`RunParallel`，8 核）；单线程 238 µs/op（~4200 msg/s）。
> - command model 基线（同机、echo guest）：**3.8 ms/op、11 MB/op、2320 allocs**。→ **18.5× 提速、每调用分配从 11 MB 降到 7.7 KB**。
> - **关键实现修正（原设计未预见）**：`Execute` 的 `code` 是多 MB 模块的 base64，**每次调用 base64-decode + sha256 花 ~5 ms**，会淹没池化收益。P1 在 host 侧加了一层 **base64-code-string 为 key 的 LRU**（`host.go` `engineForCode`）跳过解码/哈希，命中走 Go map 的 AES-NI 字符串哈希（µs 级）。未加此层时 reactor 为 952 µs/op、7 MB/op；加了之后降到 205 µs/op、7.7 KB/op。sha256 dedup 仍是 miss 路径的正确性兜底。
> - 数字为一次性环境基线，不作容量承诺（遵循 HIGH-THROUGHPUT-INGESTION.md §1 口径）。

> **P2 实测（同机）**：
> - **冷启动**：首次部署（空 cache 目录）建满整池 **1.37 s** → 模拟进程重启（暖 cache 目录）**62 ms**，达成 <100 ms 验收。单看 `CompileModule` 是 1.46 s → **41.5 ms**。测量走的是 runner 真实调用的 `warmup` 路径，**含实例化 + configure 建满整池**，不是孤立的编译调用。
> - **热路径无回归**：加了磁盘 cache + 句柄预解析后 `BenchmarkReactor_Eval` 仍是 **206 µs/op**（P1 为 205 µs/op），即冷启动收益不以稳态吞吐为代价。
> - **cache 目录**：默认 `os.UserCacheDir()/xflow/wasm`，`XFLOW_WASM_CACHE_DIR` 覆盖，设为 `off` 关闭。**fail-open**：目录不可用（如容器无 HOME）时降级为内存 cache 并记录原因，不阻断 wasm 执行。wazero 按自身版本 + GOOS/GOARCH 给目录分命名空间，升级自动失效旧产物。
> - **已知限制**：wazero 的文件 cache **无跨进程锁**，多进程共享同一目录会重复写同一条目（单文件写入是原子的，故是冗余而非损坏）。要彻底避免可用 `XFLOW_WASM_CACHE_DIR` 给不同部署分开目录。
> - **ABI 契约收紧**：新增 `testdata/reactormin`——只实现 §4.1「必需」项、故意不导出 `out_len`/`teardown` 的最小 guest。`abi_test.go` 用它锁住「可选项真的可选」（host 侧句柄为 nil 时优雅降级：无错误明细、无停机钩子，但不崩、不拒绝加载），并补了 ABI 版本不匹配必须拒绝加载的用例。

## 8. 未决与风险

- **TinyGo 编译 expr-lang 仅「受限模式」可用（已核实）。** TinyGo 缺 `reflect.Value.Call`/`MethodByName`/`NumIn/NumOut` 等函数反射，`expr-lang/expr` 维护者(issue #800)确认:必须遵守三条——用 `map[string]any`(不用 struct)、不做 struct 字段访问、所有函数走 `expr.Function()` 注册——否则运行期撞 `reflect.Value.Call` panic。更严重的是 **TinyGo+wasm 下 `recover()` 不工作**,表达式求值 panic 会直接杀死整个模块实例,无法被宿主兜住——对「用户提供表达式」场景是重大隐患。→ **标准 Go(reflect 完整、recover 可用)是本方案 guest 的默认且唯一稳妥选择;TinyGo 不进主线。**
- **标准 Go + Wizer 无可行先例（已核实）。** Wizer 在字节层面盲拍 linear memory+globals,不理解 Go 运行时语义;维护者(wizer issue #24)指出导出函数假设 `_start` 最先运行并在其中做 runtime/ctor 初始化,与 Wizer 执行模型冲突。唯一有先例的是 **TinyGo + Wizer 且必须 `-scheduler=none`**,集成 PR(tinygo #3655)未确认稳定合并。Wizer 的真正适用场景是快照嵌入式解释器(SpiderMonkey JS),其 GC 堆在 linear memory 内、指针是相对偏移——与标准 Go 的活 GC/scheduler 状态不同。→ **Wizer 路线放弃;冷启动优化走 P2 磁盘 CompilationCache(AOT 编译产物落盘),不依赖内存快照。**
- **poolSize 调参**：内存（~5.5 MiB/实例）× 并发度。建议 = runner CPU 核数，与 `local.WithConcurrency` 对齐。配置切换瞬间为双份内存（§6.6），容量规划需留峰值。
- **（探索项）实验性 `MemoryAllocator` + 手工 CoW 跳过 init**：wazero 无内置 CoW/pooling,但 `experimental.WithMemoryAllocator`(v1.7.1+,API 不稳定)可自建 mmap 池 + memcpy 恢复 post-init 内存镜像(issue #2499:3MB 模块 ~78µs)。这是唯一能真正跳过每实例 ~12ms Go bootstrap 的路子,但复杂度高、依赖实验 API。**仅当 P1/P2 后实例补建率高、bootstrap 成为瓶颈时才评估。**
- **wazero pin 在 v1.9.0**（`go.mod:19`，因 `fastschema/qjs@v0.0.6` 依赖）。本方案所有 API（`CompilationCache`、`CompilationCacheWithDir`、`WithCloseOnContextDone`、`WithStartFunctions("_initialize")`、`//go:wasmexport` reactor）均在 v1.9.0 核实可用，**不需要升级 wazero**。

## 9. 与流量采集迁移的关系

本设计是流量采集迁移的**使能项之一**，但两者可解耦推进：

- wasm 池化让「清洗打标跑在 node group 的 wasm 节点里」性能达标（12508 msg/s 池化实测）。
- 但迁移仍受三条独立阻塞制约（见 NODE-GROUP-COLOCATION.md §12.2 与本仓 Kafka 现状）：**Kafka SASL/SCRAM 缺失**、**aggregate 模式在远程托管被硬禁用（只能 per-message）**、**每 entry unit 单活跃 runner**。这些与 wasm 引擎无关，需单独做。
