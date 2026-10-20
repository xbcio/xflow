import type { SemanticColor } from "./tokens";

/**
 * Node category table (spec §5). Keyed by the node type's `category`; when a
 * descriptor does not declare one, {@link categoryOf} maps the type once, in
 * one place.
 */
export type NodeCategory = "trigger" | "control" | "action" | "code" | "transform" | "human" | "unknown";

export type CategoryIconKey = "flag" | "branch" | "globe" | "code" | "filter" | "user" | "grid";

export interface NodeCategoryMeta {
  label: string;
  /** Example node types, shown in the token gallery. */
  types: string;
  /** Palette colour the category is drawn in (`--xflow-<color>`, with `-fg` / `-bg-subtle` / `-border`). */
  color: SemanticColor;
  icon: CategoryIconKey;
}

export const nodeCategoryMeta: Record<NodeCategory, NodeCategoryMeta> = {
  trigger: { label: "触发器", types: "webhook / cron / kafka / redis / timer", color: "orange", icon: "flag" },
  control: { label: "流程控制", types: "if / switch / merge / map / wait / start / end", color: "suspend", icon: "branch" },
  action: { label: "动作", types: "http / grpc / database / notification / browser", color: "primary", icon: "globe" },
  code: { label: "代码", types: "function / script", color: "accent", icon: "code" },
  transform: { label: "转换", types: "transform.*", color: "cyan", icon: "filter" },
  human: { label: "人工", types: "approval", color: "warning", icon: "user" },
  // 方格而不是「?」：未知不是错误，只是还没被描述。键与预览已验收的 typeIcon.grid 一致。
  unknown: { label: "未知", types: "runner 自定义类型、未加载描述符", color: "neutral", icon: "grid" },
};

const categories = new Set<string>(Object.keys(nodeCategoryMeta));

/**
 * Type name → category. An entry matches the type itself or any dotted child
 * (`xflow.if` matches `xflow.if` and `xflow.if.x`, never `xflow.iffy`).
 * `xflow.` is the built-in namespace; the bare forms cover node types that
 * runners register without it.
 */
const categoryByPrefix: ReadonlyArray<readonly [prefix: string, category: NodeCategory]> = [
  ["xflow.trigger", "trigger"],
  ["trigger", "trigger"],
  ["xflow.transform", "transform"],
  ["transform", "transform"],
  ["xflow.approval", "human"],
  ["approval", "human"],
  ["xflow.if", "control"],
  ["xflow.switch", "control"],
  ["xflow.merge", "control"],
  ["xflow.map", "control"],
  ["xflow.wait", "control"],
  ["xflow.start", "control"],
  ["xflow.end", "control"],
  ["xflow.function", "code"],
  ["xflow.script", "code"],
  ["xflow.http", "action"],
  ["xflow.grpc", "action"],
  ["xflow.database", "action"],
  ["xflow.notification", "action"],
  ["xflow.browser", "action"],
];

/**
 * A node type's category. The descriptor's declared `category` wins when it is
 * one of the known keys; otherwise the type is mapped by prefix; anything else
 * is `unknown`. Never throws and never returns `undefined`.
 */
export function categoryOf(type: string | undefined, declared?: string): NodeCategory {
  if (declared !== undefined && categories.has(declared)) return declared as NodeCategory;
  const name = type ?? "";
  for (const [prefix, category] of categoryByPrefix) {
    if (name === prefix || name.startsWith(`${prefix}.`)) return category;
  }
  return "unknown";
}
