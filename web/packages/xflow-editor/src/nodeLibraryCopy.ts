// Display copy for the node library (Doc C §6.3).
//
// The server's descriptors carry the display_name but no description, and the
// trigger names end in "触发器". The library is a display surface, so both are
// normalised here instead of in the Go descriptors:
// - nodeLibraryLabel: drops the 触发器 suffix ("Webhook 触发器" → "Webhook").
//   "定时触发器" becomes "定时器" because a bare "定时" is not a noun; the
//   offline library already calls that type "定时器".
// - nodeTilePurpose: the tile's second line. A descriptor description wins
//   when one exists (custom/runner types can supply it); builtins otherwise use
//   the short purpose table; anything unknown falls back to the type id, which
//   keeps a type present in the search and the tooltip.
// - nodeLibraryMatches: the search predicate, which also matches the
//   descriptor description so a P1 one-line summary is discoverable.

const TRIGGER_SUFFIX = "触发器";

/** The node type's display name, with the trigger suffix removed. */
export function nodeLibraryLabel(label: string, type?: string): string {
  const trimmed = label.trim();
  if (!trimmed) return type ?? "";
  const stripped = trimmed.endsWith(TRIGGER_SUFFIX) ? trimmed.slice(0, -TRIGGER_SUFFIX.length).trim() : trimmed;
  if (!stripped) return type ?? trimmed;
  return stripped === "定时" ? "定时器" : stripped;
}

/**
 * One short Chinese purpose per builtin type, written in the baseline's voice
 * ("HTTP 回调触发", "按表达式周期触发"). Keep each line short enough for the
 * 52px library row; the tile title still carries the type id.
 */
export const nodePurposeByType: Readonly<Record<string, string>> = {
  "xflow.trigger.webhook": "HTTP 回调触发",
  "xflow.trigger.kafka": "消费 Kafka 消息",
  "xflow.trigger.cron": "按表达式周期触发",
  "xflow.trigger.timer": "按固定间隔触发",
  "xflow.trigger.redis": "订阅 Redis 消息",
  "xflow.start": "工作流入口",
  "xflow.end": "结束执行",
  "xflow.switch": "多路条件分支",
  "xflow.if": "true / false 两路",
  "xflow.merge": "汇合上游分支",
  "xflow.wait": "延时或等待信号",
  "xflow.map": "逐项遍历子流程",
  "xflow.subworkflow": "调用子工作流",
  "xflow.http": "调用外部 API",
  "xflow.grpc": "调用 gRPC 服务",
  "xflow.database": "查询与写入",
  "xflow.notification": "邮件 / IM 通知",
  "xflow.browser.cdp": "远程浏览器采集",
  "xflow.supply.external": "声明外部供应内容",
  "xflow.supply.static": "声明随定义携带的内容",
  "xflow.supply.http": "按计划拉取供应内容",
  "xflow.function": "运行注册函数",
  "xflow.script": "执行脚本",
  "xflow.transform.set": "设置字面量与表达式字段",
  "xflow.transform.pick": "按字段名选取子集",
  "xflow.transform.filter": "按条件保留条目",
  "xflow.transform.rename": "按映射重命名字段",
  "xflow.transform.sort": "按字段排序数组",
  "xflow.transform.limit": "截取数组前 N 项",
  "xflow.transform.aggregate": "聚合数组",
  "xflow.transform.remove_duplicates": "按字段去重",
  "xflow.approval": "通过 / 驳回 / 退回"
};

/** The tile's second line: descriptor description → builtin purpose → type id. */
export function nodeTilePurpose(descriptor: { type: string; description?: string }): string {
  const description = descriptor.description?.trim();
  if (description) return description;
  return nodePurposeByType[descriptor.type] ?? descriptor.type;
}

/**
 * Does a library tile match the search box? The label and type id always
 * participate; P1 descriptors also make their one-line description searchable.
 * An empty query matches everything.
 */
export function nodeLibraryMatches(
  descriptor: { label: string; type: string; description?: string },
  query: string
): boolean {
  const needle = query.trim().toLowerCase();
  if (!needle) return true;
  const description = descriptor.description?.trim().toLowerCase();
  return (
    descriptor.label.toLowerCase().includes(needle) ||
    descriptor.type.toLowerCase().includes(needle) ||
    (description !== undefined && description.includes(needle))
  );
}

