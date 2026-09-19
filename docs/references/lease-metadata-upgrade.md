# Assignment lease metadata Redis 升级说明（U-7）

本文面向负责 Redis 与 runner/control-plane 发布的操作员，说明 U-7 对
assignment lease metadata 的存储格式变更，以及升级、清理和回滚的边界。
它不包含 Redis 地址、数据库编号、用户名、密码、URI、证书路径或真实
assignment ID；所有连接参数和标识符都必须仅在受控运维环境中提供。

> **关键结论：不要把此变更作为带有在飞 lease 的普通滚动发布。** 新旧版本
> 不会双读或双写两种 metadata 格式。先排空和协调切换。旧 Hash 上**已经不可达**
> 的 field 由 control-plane 自动清理（见 §6），但该 Hash 的整键删除仍然是需要
> 人工批准的操作。

## 1. 受影响的键与生命周期

当前实现使用的逻辑前缀是 `xflow:runner-directory:{control}`。这是公开的键模式，
不是 Redis 地址、数据库、用户名、密码、TLS 配置或其它部署标识；`<assignment-id>`
仍是仅在受控环境中处理的占位符。

| 版本 | 键 | Redis 类型 | 生命周期 |
|---|---|---|---|
| 旧版 | `xflow:runner-directory:{control}:assignment:lease-meta` | 一个 shared **Hash**；field 为 assignment ID | 整键没有 TTL。当前版本**不读取也不写入**它；它只作为删除目标出现：终止路径（ClearAssignment）删除该 assignment 的精确 field，control-plane 的后台 reaper 删除**不再可达**的 field（见 §6）。因此 Hash 会自动收缩，但它**不会**自己在没有任何 field 时被主动删除——最后一个 field 被 HDEL 后键才消失，整键删除仍需人工批准（§4）。 |
| U-7 新版 | `xflow:runner-directory:{control}:assignment:lease-meta:<assignment-id>` | 每个 assignment 一个 **String** | 正常写入时带有限期 TTL；期限覆盖 lease 的存活期再加一个 claim-recovery 窗口。正常 release/drop 会删除该精确键，TTL 是额外的有界兜底。 |

新键的 `PTTL` 应为有限的非负毫秒数；它会随时间减少。`PTTL = -1` 表示没有
过期时间，属于异常，需要调查；`PTTL = -2` 表示键在检查前已消失，通常是正常
过期或并发 release 的竞态，不能据此删除其它键。

旧 Hash 的 `TTL = -1` 只表示该 Hash 没有过期时间，不表示其 field 仍可供当前
版本使用。旧 Hash field 数和新 String 键数由不同生命周期管理，**不能**以二者
相等或不相等作为发布成功、排空或删除安全性的判据。

## 2. 发布前只读盘点

所有下面的 `redis-cli` 命令都假定操作员已经通过批准的 TLS/鉴权连接路径连上
目标 Redis。为避免在文档、shell history 或工单中泄露环境信息，示例特意不写
任何连接参数、密码或真实 assignment ID，也不读取 metadata 内容（metadata 可能
含任务输入）。

先在受控 shell 中仅设置本次操作所需的**逻辑键模式**：

```sh
# 固定的逻辑 schema 名称；不要在此处加入连接参数、凭据或真实 assignment ID。
DIRECTORY_PREFIX='xflow:runner-directory:{control}'
LEGACY_KEY="${DIRECTORY_PREFIX}:assignment:lease-meta"
NEW_KEY_PREFIX="${LEGACY_KEY}:"
```

### 2.1 检查旧 Hash 的类型、TTL 和 field 数

先检查类型，只有 `hash` 时才运行 `HLEN`，以避免对错误类型执行命令：

```sh
legacy_type="$(redis-cli --raw TYPE "$LEGACY_KEY")"
legacy_ttl="$(redis-cli --raw TTL "$LEGACY_KEY")"
printf 'legacy type=%s ttl_seconds=%s\n' "$legacy_type" "$legacy_ttl"

case "$legacy_type" in
  hash)
    # 只返回 field 数，不读取 field 名或 metadata 值。
    redis-cli --raw HLEN "$LEGACY_KEY"
    ;;
  none)
    printf 'legacy Hash is absent\n'
    ;;
  *)
    printf 'STOP: legacy key has unexpected type %s; do not delete it.\n' \
      "$legacy_type" >&2
    ;;
esac
```

预期旧格式存在时 `TYPE` 为 `hash`，且通常 `TTL` 为 `-1`。`none` / `TTL = -2`
表示该键不存在。任何其它类型、Redis 错误或无法解释的计数都应停止变更并调查，
而不是尝试“修复”它。

### 2.2 使用 SCAN 盘点新 String 的数量、类型和 TTL

下面的 Bash/Zsh 片段使用 `redis-cli --scan`（底层为 `SCAN`），**不使用
`KEYS`**。它只输出汇总计数，不读取 String 值，也不会把真实 assignment ID 打印
到终端。临时清单仍包含键名，所以须在受控主机上以受限权限保存，并在检查后删除。

`SCAN` 不是原子快照：并发写入、过期或 release 时可能出现重复、遗漏或 `-2`。
在排空/暂停写入的窗口运行一次作为正式盘点；若不能暂停，至少多次运行并记录
时间戳和结果范围，不能把一次扫描当作精确的稳定基线。

```sh
# Bash/Zsh；umask 保护本地临时清单。不要上传或附加该清单。
umask 077
inventory="$(mktemp "${TMPDIR:-/tmp}/lease-metadata.XXXXXX")"

# Redis glob 匹配可能比字面前缀宽；case 的字面前缀检查会滤掉非成员。
redis-cli --scan --pattern "${NEW_KEY_PREFIX}*" |
  while IFS= read -r key; do
    case "$key" in
      "$NEW_KEY_PREFIX"*) printf '%s\n' "$key" ;;
      *) printf 'ignored a non-exact SCAN match\n' >&2 ;;
    esac
  done | LC_ALL=C sort -u >"$inventory"

new_total=0
new_strings=0
new_wrong_type=0
new_finite_ttl=0
new_no_ttl=0
new_disappeared=0
new_unexpected_ttl=0

while IFS= read -r key; do
  new_total=$((new_total + 1))
  key_type="$(redis-cli --raw TYPE "$key")"
  key_pttl="$(redis-cli --raw PTTL "$key")"

  case "$key_type" in
    string) new_strings=$((new_strings + 1)) ;;
    none) new_disappeared=$((new_disappeared + 1)); continue ;;
    *) new_wrong_type=$((new_wrong_type + 1)); continue ;;
  esac

  case "$key_pttl" in
    -1) new_no_ttl=$((new_no_ttl + 1)) ;;
    -2) new_disappeared=$((new_disappeared + 1)) ;;
    ''|*[!0-9]*) new_unexpected_ttl=$((new_unexpected_ttl + 1)) ;;
    *) new_finite_ttl=$((new_finite_ttl + 1)) ;;
  esac
done <"$inventory"

printf '%s\n' \
  "new_keys_inventory=${new_total}" \
  "new_strings=${new_strings}" \
  "new_finite_pttl=${new_finite_ttl}" \
  "new_no_ttl=${new_no_ttl}" \
  "new_disappeared_during_check=${new_disappeared}" \
  "new_wrong_type=${new_wrong_type}" \
  "new_unexpected_pttl=${new_unexpected_ttl}"

rm -f "$inventory"
```

在稳定的新版本运行中，`new_wrong_type`、`new_no_ttl` 和
`new_unexpected_pttl` 应为零。`new_disappeared_during_check` 在有并发
release/过期时可以出现；应在安静窗口复查，而非手工删除新 String。该盘点仅用于
验证格式和观察数量，不能证明所有业务任务已完成。

## 3. 推荐升级顺序

1. **记录只读证据。** 记录旧 Hash 的 `TYPE`、`TTL`、`HLEN`，以及新键盘点的
   汇总和时间戳；不要记录 metadata 值或把键名清单贴入非受限位置。
2. **协调排空。** 停止新任务准入，drain 所有 runner/control-plane 实例，并确认
   没有活动或可恢复的旧版 claim/lease。至少覆盖该环境中最长的 lease 时限和一个
   claim-recovery 窗口；以实际运行状态和恢复告警为准，不能仅凭旧 Hash 是否为空。
3. **停止所有旧版本。** 确认没有旧二进制仍可能连接同一 runner-directory 状态。
   若仍计划回滚，保留旧 Hash，且不要让新版产生的 in-flight lease 留给旧版处理。
4. **协调切换到新版。** 启动全部 U-7 版本，再恢复准入。先用一个受控的可幂等
   工作负载验证新键是 `string` 且有有限 `PTTL`，然后观察恢复、重试和 runner
   健康状态。
5. **经过稳定观察期后再决定清理。** control-plane 会自动排空旧 Hash 中**不再
   可达**的 field（§6），但不会删除整键。`HLEN` 因此应随时间单调下降并最终趋于
   0；若它长时间不降，按 §6 的残留风险排查，而不是直接删键。只有在 §4 的批准
   条件全部满足后，才考虑删除这个唯一的旧键。

如果平台只能做逐实例滚动发布，必须先用 drain/维护窗口把版本重叠期间的活动
claim/lease 降为零；否则不要在同一 runner-directory 状态上混合新旧版本。不要
通过临时改 Redis DB、前缀或手工搬运键来绕过该限制，除非该隔离方案已由负责该
部署拓扑的工程团队验证为完整且可恢复。

## 4. 经人工确认后的旧 Hash 精确删除

删除旧 Hash 是不可逆的 metadata 删除，不是升级的自动步骤。执行者必须在同一
变更窗口内重新运行 §2.1，并在**人工批准后**同时确认以下全部条件：

- 所有会读取 shared Hash 的旧版进程已经停止，且不会因自动扩缩、回滚或灾备而
  重新接入该状态；
- 已完成 §3 的排空与协调切换，最长旧 lease/recovery 风险窗口已经过去，并已处理
  未决重试、断连 runner 和恢复告警；
- 新版已验证会写入 TTL String，且不存在 `TYPE`/TTL 异常；
- 已确认回滚决策和可用的、经过演练的恢复路径。不要为了恢复一个 Hash 而在有新
  流量的 Redis 上盲目恢复整个数据库；
- 变更审批人明确批准删除**下面这个精确的旧键**，而不是批准一个模式匹配。

完成上面的人工确认后，先再次做只读检查：

```sh
redis-cli --raw TYPE "$LEGACY_KEY"
redis-cli --raw TTL "$LEGACY_KEY"
# 仅当 TYPE 仍为 hash 时：
redis-cli --raw HLEN "$LEGACY_KEY"
```

若类型不再是 `hash`、检查结果与审批记录不一致，或仍存在旧版消费者，停止操作。
若检查一致，由执行者亲自键入确认词后才运行精确 `DEL`：

```sh
# [DESTRUCTIVE] 先完成上述人工审批；确认词不替代审批记录。
printf 'Type DELETE_LEGACY_HASH only after human approval: '
IFS= read -r confirmation
case "$confirmation" in
  DELETE_LEGACY_HASH)
    # 只删除 $LEGACY_KEY 的一个精确名称；不是 glob，也不是扫描结果。
    redis-cli --raw DEL "$LEGACY_KEY"
    ;;
  *)
    printf 'No deletion performed.\n'
    ;;
esac

# DEL 返回 1 后，确认旧键已消失；返回 0 或任何错误都应重新评估。
redis-cli --raw EXISTS "$LEGACY_KEY"
redis-cli --raw TYPE "$LEGACY_KEY"
```

成功时 `DEL` 应返回 `1`，随后 `EXISTS` 应为 `0`、`TYPE` 应为 `none`。若 `DEL`
返回 `0`，不要把它自动视为成功：可能是并发变更、键已被其它操作移除或连接目标
错误，必须重新核对并记录结果。

**严禁**使用下列做法：`KEYS`、`FLUSHDB`、`FLUSHALL`、带通配符的 `DEL`、
`DEL $(redis-cli --scan ...)`，或根据 SCAN 结果批量删除新 String。新键由其 TTL
和正常生命周期处理；`PTTL = -1` 或类型异常应走故障调查，而不是本迁移的清理脚本。

## 5. 回滚、滚动发布与任务重试风险

### 格式兼容性

旧版只在 shared Hash 中查找 lease metadata；U-7 只在 assignment-scoped String
中查找。二者没有双读/双写桥接。因此：

- 旧版创建或需要 replay 的 lease，可能无法被新版从新 String 键中重建；
- 新版创建的 lease，旧版也无法从旧 shared Hash 中重建；
- 版本切换时 runner 断连、进程重启或 lease replay 会放大该差异，不能把“两个
  版本暂时都健康”当作兼容性证明。

所以，有在飞任务时的混合版本池不受支持。协调排空比普通滚动发布更重要。

### 回滚边界

在旧 Hash 尚未删除时，回滚仍必须先 drain 新版创建的 lease：旧版不认识这些新
String。旧 Hash 的存在不能使旧版理解新格式。旧 Hash 删除后，回滚为旧版会失去
它原先可用的 shared metadata；除非进入经过批准和演练的数据恢复流程，否则应把
该删除视为不可回滚。切勿在运行中的新版 Redis 上以整库恢复的方式“找回”该 Hash。

### 重试与副作用

系统的任务执行语义是 at-least-once。格式不兼容、进程中断或在 lease metadata
过期/缺失时发生的恢复，可能导致 lease 无法 replay、任务重新入队、重试，或需要
人工处置；它们不提供 exactly-once 保证。所有 handler 的外部副作用都必须使用
业务幂等键。不要通过提前删除旧 Hash 或新 String 来“触发重试”——这会破坏恢复
依据，并可能扩大重复执行或任务卡住的风险。

升级后应持续观察 runner reconnect、lease replay、任务失败/重试和队列积压；若
出现 metadata 类型异常、无 TTL 的新 String 或无法解释的恢复行为，应暂停后续
发布与清理，保留现有键，并按事件响应流程调查。

## 6. 自动清理：可达性判据与残留风险

本节描述 U-7 之后新增的实现行为，它改变了 §1 表格中旧 Hash 的生命周期，但**没有**
改变 §3 的升级顺序，也没有改变 §5 的双读/双写结论：新旧版本仍然互不读取对方的
metadata 键。

### 6.1 两条自动删除路径

1. **终止路径。** `ClearAssignment` 在删除该 assignment 的全部共享记录的同一条
   Lua 脚本里，额外 `HDEL` 旧 Hash 中该 assignment 的 field。此时该 assignment 的
   `assignment:data` / `state` / `claim` / `runner` / `session` / `lease-id` /
   `lease-token` 已在同一步被删除，因此这个 field 对两个版本都已不可达——它不再
   是一个「可读的记录」，只是残留。
2. **后台 reaper。** control-plane 的 lease sweeper 以 leader-gated、自带节奏
   （默认 5 分钟）、单次有界的批次调用
   `ReapOrphanedLegacyAssignmentLeaseMeta`。它用 `HSCAN` 遍历**这一个键**
   （单键、单 slot、游标推进），对每个 field 检查 §6.2 的判据，只删除不可达的
   field。它不返回也不记录 field 名或值，只记录删除数量。

### 6.2 判据：为什么可以「在线」删除

U-7 只移动了 metadata 的键；两个版本读取的共享 per-assignment 记录
（`assignment:data`、`assignment:state`、`assignment:claim`、`assignment:runner`、
`assignment:session`、`assignment:lease-id`、`assignment:lease-token`）完全一致。
旧版在**每一条**读取旧 Hash 的路径上，都是先读取并匹配上述记录之后才去
`HGET` 旧 Hash。因此：

> 一个旧 Hash field，如果它的 assignment 在上述记录中已经不存在，那么旧版二进制
> 也读不到它——它是残留，而不是在用状态。

这就是 reaper 唯一的删除判据。它不依赖任何版本标记（版本标记本身可能过期），也
不依赖「旧实例是否已经停止」这一无法在进程内证明的事实，因此：

- **可以在一台实例上在线运行**，与新版 control-plane 并存；
- **可以在混合版本窗口内运行**，不需要先排空；
- **不会破坏回滚**：被删除的 field 全部属于在飞之外的 assignment，回滚需要排空的
  是**在飞**lease，而那些 field 的共享记录仍在，判据会保留它们。

### 6.3 不会做的事

- **不删除整键。** 即使最后一个 field 被删掉、键随之消失，reaper 也不会主动
  `DEL`/`UNLINK` 旧 Hash——整键删除仍是 §4 的人工批准动作。这样做的原因是：判据
  证明的是「没有 field 处于在用状态」，而整键删除会让「回滚时需要旧数据」这一
  决策无法覆盖；把不可逆的那一步留在人手里。
- **不做双读/双写。** 不读旧 Hash 作为 metadata 来源，也不向旧 Hash 写入新数据。
  §5 的不兼容结论仍然成立：不要让新旧版本共享同一个 runner-directory 状态。
- **不使用前缀 `SCAN`。** Redis Cluster 下 `SCAN` 按节点应答，会漏掉其它节点的
  field；单键 `HSCAN` 是唯一在多节点下正确且有界的做法。

### 6.4 残留风险（必须明确）

1. **判据依赖旧版的读取顺序。** 6.2 的推理来自 v0.0.6 的实际代码：只有两个 Go
   读取点，且都在匹配共享记录之后；六个 Lua 脚本均不读取旧 Hash。如果存在**第三个**
   未在此仓库中验证的旧版消费者（例如某个直接连 Redis 的运维脚本、或更早的
   预发布版本），判据对它不成立，它可能读到已被删除的 field。上生产前请确认没有
   这类消费者。
2. **`HLEN` 不下降不等于出错。** 阈值受 §6.1 的节奏影响，且只有「不在飞的
   assignment」才会被删。若 `HLEN` 长期不降，应按活跃 assignment 数对照排查，
   而不是直接把键删掉。
3. **`DEL` 的阻塞风险。** 若旧 Hash 在 reaper 生效前已经积累到很大（例如数百万
   field），§4 的 `DEL` 会在共享 Redis 上阻塞。先让它被 reaper 排空到很小的规模
   再执行 §4；如果必须一次性删除一个大键，用 `UNLINK`（异步回收）替代 `DEL`，
   并把 §4 的确认流程原样保留。
4. **reaper 是可选的类型断言能力。** 内存目录和其它 `RunnerDirectory` 实现不
   实现该能力，行为不变；但如果一个部署更换了目录实现，自动排空会静默消失，
   旧 Hash 会重新变成永久残留。

## 7. 自动回收：`leased` 状态的滞留 assignment

§6 处理的是旧 Hash 的残留 field。本节处理另一种残留——目录里仍处于
`assignment:state = 'leased'`、但其 lease metadata 键已经因为 TTL 到期而消失的
assignment。这不需要旧版本参与，纯属当前控制面自身的覆盖缺口。

### 7.1 为什么它是可达性缺口

runner 目录里一个 assignment 的完整生命周期是 `queued → claimed → leased`。
三条现有的回收路径都看不到「state 为 `leased`、metadata 已过期」这一形态：

1. **claim 回收（`ReclaimExpiredClaims`）。** 它只枚举**尚未被 finalize** 的
   claim 索引。一个 assignment 一旦进入 `leased`，`claim:assignment` /
   `claim:runner` / `claim:session` / `claim:expiry` 已经在写入 `leased` 的**同一条
   Lua 原子步骤**里被删除；该脚本枚举的正是这些键，所以 `leased` 的 assignment
   根本不会进入它的视野。**因此把该脚本的状态判据从 `claimed` 放宽到同时接受
   `leased` 不会产生任何效果——那是死代码**，不要把它当作修复。
2. **lease sweeper 的过期扫描。** 它枚举的是 engine 侧 lease 索引，而该索引与这份
   metadata 共享同一个瞬态 TTL；metadata 过期的同一时刻，它也从该索引里消失。
3. **runner 自身的 poll 重放（`replayLease`）。** 它确实认识这一形态并会释放，
   但只在**持有该 lease 的 runner 仍在轮询时**才会执行。runner 被 kill、被
   drain、Pod 被驱逐之后，再没有谁运行这段检查，assignment 会一直占着该 runner
   的容量，直到目录被清空。

因此，**只要 runner 消失，这个 assignment 就同时失去了所有回收者**；它占用的
`runner:lease-count` 不会回落，该 runner 的容量永久泄漏。

> 与之相对的是 runner 仍然存活的情形：一次「状态已提交、但紧随其后的 outbox 投递
> 失败」的提交，现在会回报它**实际取得**的分类（`accepted` / `duplicate_terminal`）
> 而不是 `transient_error`，于是 control-plane 会照常释放该 runner 的 `leased`
> 容量。该分类只表示「状态迁移已完成，只是投递失败」，与「迁移根本没发生」不同。
> 本节描述的 reaper 覆盖的是另一半：连这条路径也不会再被触发的时候。

### 7.2 自动 reaper 的行为

control-plane 的 lease sweeper 以 leader-gated、自带节奏（默认 5 分钟）、单次
有界（默认 256 条）的批次调用 `ReapStrandedLeases`。它只在同时满足下面两点时才
释放一个 assignment：

- `assignment:state` 仍为 `leased`；且
- 该 assignment 的 lease metadata 键**不存在**（`assignment:lease-meta:<id>`
  已过期或已被删除）。

第二条是整个操作的安全前提：只要 metadata 仍在，无论它看起来多旧，都可能是某个
存活 runner 正在执行的 lease，reaper **绝不触碰**。

释放本身复用正常的、以 lease identity（lease ID + token）为栅栏的
`ReleaseExpiredLease`：并发提交/续约如果已经推进到新一代 lease，reaper 会得到
token 不匹配并安静放弃。释放后它按 runner 自己的重放路径同样地维护 per-runner
租约索引，并结清释放路径**刻意保留**的 `finalized` handoff 记录
（`SettleFinalizedHandoff`）。

### 7.3 两趟枚举，各覆盖一半

reaper 用两个独立来源找候选，因为任一趟单跑都会漏掉一类：

1. **per-runner 租约索引。** 按已注册 runner 遍历
   `runner:leased-assignments:<runner-id>`，逐条用 `assignment:state` 与
   `assignment:runner` 复核状态和归属（索引只是提示，不是权威）。当索引中的存活
   条目数**少于** `runner:lease-count` 时，说明该 runner 存在索引写入之前由旧
   控制面 finalize 的 lease，于是对该 runner 做一次全哈希扫描补齐——这与 runner
   自身重放的做法一致。
2. **handoff 账本。** 遍历 `handoff:state` 中仍为 `finalized` 的记录，从中取出
   assignment。这一趟专门覆盖**per-runner 索引已不可用**的情形（例如 runner 已从
   注册表移除、索引条目丢失），此时第一趟根本不会枚举到它。

### 7.4 边界与不做的事

- **不释放 metadata 仍在的 lease**，因此可以对着在线目录跑。
- **不改变双读/双写结论**：它只操作当前版本的键，与 §5 无关。
- **不会掩盖 §6 的旧 Hash 残留**：两者对象不同，各自独立排空。
- **是可选的能力（类型断言）**：内存目录和其它 `RunnerDirectory` 实现不实现它，
  行为不变；但更换目录实现会让这个回收静默消失，届时应由运维侧监控
  `runner:lease-count` 是否长期不回落。
- **不使用前缀 `SCAN`**：与 §6 同样的原因，Redis Cluster 下按节点应答会漏键。

