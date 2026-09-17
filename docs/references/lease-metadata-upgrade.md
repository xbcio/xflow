# Assignment lease metadata Redis 升级说明（U-7）

本文面向负责 Redis 与 runner/control-plane 发布的操作员，说明 U-7 对
assignment lease metadata 的存储格式变更，以及升级、清理和回滚的边界。
它不包含 Redis 地址、数据库编号、用户名、密码、URI、证书路径或真实
assignment ID；所有连接参数和标识符都必须仅在受控运维环境中提供。

> **关键结论：不要把此变更作为带有在飞 lease 的普通滚动发布。** 新旧版本
> 不会双读或双写两种 metadata 格式。先排空和协调切换，旧 Hash 在确认无旧版
> 依赖后才可由人工精确删除。

## 1. 受影响的键与生命周期

当前实现使用的逻辑前缀是 `xflow:runner-directory:{control}`。这是公开的键模式，
不是 Redis 地址、数据库、用户名、密码、TLS 配置或其它部署标识；`<assignment-id>`
仍是仅在受控环境中处理的占位符。

| 版本 | 键 | Redis 类型 | 生命周期 |
|---|---|---|---|
| 旧版 | `xflow:runner-directory:{control}:assignment:lease-meta` | 一个 shared **Hash**；field 为 assignment ID | 没有由 U-7 添加的 TTL。U-7 **不会**迁移、读取或自动删除这个旧 Hash。它可能无限保留，直到旧版自己的正常 field 清理或一次经批准的人工删除。 |
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
5. **经过稳定观察期后再决定清理。** U-7 不会自动清理旧 Hash；保留它不会让
   新版本回读它。只有在 §4 的批准条件全部满足后，才考虑删除这个唯一的旧键。

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
