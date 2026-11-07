/**
 * 常用 group support (§6.0.1, 2026-11-07): the node types the user added most
 * recently, shared across workflows through localStorage. Storage can be
 * absent (SSR, a blocked origin) or hold foreign data, so every access is
 * wrapped and anything but an array of strings reads as empty.
 */

export const RECENT_NODE_TYPES_KEY = "xflow-editor.recent-node-types";

/** How many recent types the 常用 group remembers (§6.0.1). */
export const RECENT_NODE_TYPES_LIMIT = 6;

/**
 * The 常用 group's fallback, in order, when fewer than six types were added:
 * HTTP 请求、If、函数、Switch、Set、合并. A type the registry does not carry is
 * skipped by the caller.
 */
export const DEFAULT_COMMON_NODE_TYPES: readonly string[] = [
  "xflow.http",
  "xflow.if",
  "xflow.function",
  "xflow.switch",
  "xflow.transform.set",
  "xflow.merge"
];

function storageOrUndefined(): Storage | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

/** The remembered types, most recent first; never more than the limit. */
export function readRecentNodeTypes(storage: Storage | undefined = storageOrUndefined()): string[] {
  try {
    const raw = storage?.getItem(RECENT_NODE_TYPES_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    const types = parsed.filter((item): item is string => typeof item === "string" && item !== "");
    return types.slice(0, RECENT_NODE_TYPES_LIMIT);
  } catch {
    return [];
  }
}

/**
 * Moves `type` to the front of the list, drops the duplicate and trims the
 * tail. Returns the new list so the mounted library updates without a reread.
 */
export function rememberRecentNodeType(
  type: string,
  storage: Storage | undefined = storageOrUndefined()
): string[] {
  const next = [type, ...readRecentNodeTypes(storage).filter((item) => item !== type)].slice(0, RECENT_NODE_TYPES_LIMIT);
  try {
    storage?.setItem(RECENT_NODE_TYPES_KEY, JSON.stringify(next));
  } catch {
    // A full or read-only storage must not break adding a node.
  }
  return next;
}

/**
 * The 常用 group's rows: the recent types that the registry still carries,
 * then the default set, deduplicated and capped at six. Descriptors keep the
 * registry's order, so callers pass them in display order and get the same
 * object back for the row renderer.
 */
export function commonNodeTypes<T extends { type: string }>(
  descriptors: readonly T[],
  recent: readonly string[]
): T[] {
  const byType = new Map(descriptors.map((descriptor) => [descriptor.type, descriptor]));
  const picked: T[] = [];
  const seen = new Set<string>();
  for (const type of [...recent, ...DEFAULT_COMMON_NODE_TYPES]) {
    if (picked.length >= RECENT_NODE_TYPES_LIMIT) break;
    const descriptor = byType.get(type);
    if (!descriptor || seen.has(type)) continue;
    seen.add(type);
    picked.push(descriptor);
  }
  return picked;
}
