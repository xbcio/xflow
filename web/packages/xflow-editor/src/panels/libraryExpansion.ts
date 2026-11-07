/**
 * Node-library fold state (§6.0.1, 2026-11-07). Each group has a default and
 * the user's own choice overrides it; only the choices are stored, so a group
 * the user never touched keeps following the default — including when the
 * workflow gains its first trigger.
 */

export const LIBRARY_GROUP_FOLD_KEY = "xflow-editor.library-groups";

/** The group pinned above the type groups (§6.0.1). */
export const COMMON_NODE_GROUP = "常用";

/** True when the workflow already has a trigger node. */
export function workflowHasTrigger(nodes: readonly { type?: string; kind?: string }[]): boolean {
  return nodes.some(
    (node) => node.type === "xflow.start" || Boolean(node.type?.startsWith("xflow.trigger.")) || node.kind === "trigger"
  );
}

/**
 * The default state of one group: 常用 is open; 触发器 is open while the
 * workflow has none; every other group is folded.
 */
export function defaultGroupExpanded(group: string, hasTrigger: boolean): boolean {
  if (group === COMMON_NODE_GROUP) return true;
  if (group === "触发器") return !hasTrigger;
  return false;
}

function storageOrUndefined(): Storage | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

/** The user's own fold choices; unreadable storage reads as none. */
export function readGroupOverrides(storage: Storage | undefined = storageOrUndefined()): Record<string, boolean> {
  try {
    const raw = storage?.getItem(LIBRARY_GROUP_FOLD_KEY);
    if (!raw) return {};
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const overrides: Record<string, boolean> = {};
    for (const [group, value] of Object.entries(parsed)) {
      if (typeof value === "boolean") overrides[group] = value;
    }
    return overrides;
  } catch {
    return {};
  }
}

/**
 * Persists the whole fold map. The caller owns the in-memory copy, so a
 * storage-less environment (tests, a blocked origin) still folds correctly
 * within the session instead of losing every earlier toggle.
 */
export function persistGroupOverrides(
  overrides: Readonly<Record<string, boolean>>,
  storage: Storage | undefined = storageOrUndefined()
): void {
  try {
    storage?.setItem(LIBRARY_GROUP_FOLD_KEY, JSON.stringify(overrides));
  } catch {
    // Storage is an enhancement: a blocked one must not break the toggle.
  }
}

/** The effective fold state of every group, overrides first. */
export function expandedGroups(
  groups: readonly string[],
  hasTrigger: boolean,
  overrides: Readonly<Record<string, boolean>>
): Set<string> {
  const expanded = new Set<string>();
  for (const group of groups) {
    if (overrides[group] ?? defaultGroupExpanded(group, hasTrigger)) expanded.add(group);
  }
  return expanded;
}
