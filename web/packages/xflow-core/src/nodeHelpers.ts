import type { RuntimeNodeSnapshot, RuntimeSnapshot } from "./index";

/**
 * Whether a key event belongs to a text field that owns the keystroke. React
 * Flow skips its own Delete handling in exactly this case, so matching it keeps
 * a backspace while renaming a node from acting on the canvas behind it.
 */
export function isTextEntryTarget(target: EventTarget | null): boolean {
  return typeof HTMLElement !== "undefined"
    && target instanceof HTMLElement
    && (target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName));
}

/**
 * Supply nodes feed values to their consumers instead of taking part in the
 * control flow. A node counts as one by its declared kind or, when the kind is
 * absent (a hand-written definition), by the builtin `xflow.supply.*` type.
 */
export function isSupplyNode(node: { kind?: string; type?: string } | undefined): boolean {
  return node?.kind === "supply" || node?.type?.startsWith("xflow.supply.") === true;
}

/**
 * A node's runtime state, defaulting to pending. Hosts index the snapshot by
 * node name (the execution API reports nodes by name), so the name is tried
 * first; the id is the fallback for snapshots keyed by id.
 */
export function runtimeForNode(
  runtime: RuntimeSnapshot | undefined,
  node: { name?: string; id?: string }
): RuntimeNodeSnapshot {
  const nodes = runtime?.nodes;
  // Own keys only: a node named `constructor` must not inherit a member of
  // Object.prototype and be reported as if the run had reached it.
  const own = (key: string | undefined) =>
    key && nodes && Object.hasOwn(nodes, key) ? nodes[key] : undefined;
  return own(node.name) ?? own(node.id) ?? { status: "pending" };
}
