// Write semantics (Doc B §6): write(), applyPatches(), patch batching.
//
// Guarantees implemented here (rule numbers from §6):
//   1. clearing is `unset` (never `set null`); `emptyAs: "keep"` keeps "".
//   2. empty containers collapse upward only through ancestors bound by a
//      visible valueKind "object" element, never across an array index.
//   3. `unset` of an array-index path is rejected.
//   4. core never writes defaults (write only runs on explicit user writes).
//   5. only paths whose value actually changes produce patches.
//   7. applyPatches copies only the objects along changed paths.
//   8. `/$ctx` is read-only: writes are rejected with a warning.
// Rule 6 (microtask batching + overlay) lives in composer/react; the
// same-path merge it relies on is mergePatches/createPatchBatch below.

import { deepEqual, formatPointer, getIn, isArrayIndexToken, isPlainObject, isWithin, parsePointer } from "./pointer";
import { adoptRowIdentity } from "./rows";
import { resolveTarget } from "./targets";
import type { BindTarget, Patch, ResolvedTree, ValueKind, Warning } from "./types";

export interface WriteState {
  /** Current value: the host value plus any pending overlay. */
  value: unknown;
  /** Resolve result providing repeat row mapping and object-bound ancestors. */
  tree?: Pick<ResolvedTree, "repeats" | "objectTargets">;
  /** "keep" writes "" as a real value instead of unsetting it. */
  emptyAs?: "unset" | "keep";
  onWarning?: (warning: Warning) => void;
}

const CTX = "$ctx";

/** Empty per rule 1: null/undefined always; "" for scalars; [] for arrays; {} for objects. */
export function isEmptyValue(next: unknown, valueKind: ValueKind, emptyAs: "unset" | "keep" = "unset"): boolean {
  if (next === undefined || next === null) return true;
  switch (valueKind) {
    case "array":
      return Array.isArray(next) && next.length === 0;
    case "object":
      return isPlainObject(next) && Object.keys(next).length === 0;
    default:
      return next === "" && emptyAs !== "keep";
  }
}

export function write(target: BindTarget, next: unknown, valueKind: ValueKind, state: WriteState): Patch[] {
  const warn = (code: string, message: string, path?: string) =>
    state.onWarning?.({ code, message, ...(path === undefined ? {} : { path }) });
  const repeats = state.tree?.repeats ?? {};
  const path = resolveTarget(target, state.value, repeats);
  if (path === null) {
    warn("row-gone", "write dropped: the target row no longer exists");
    return [];
  }
  const tokens = parsePointer(path);
  if (tokens[0] === CTX) {
    warn("ctx-readonly", "write to /$ctx rejected: host context is read-only", path);
    return [];
  }
  if (tokens.length === 0) {
    warn("root-write", "write to the value root rejected", path);
    return [];
  }
  const current = getIn(state.value, tokens);

  if (isEmptyValue(next, valueKind, state.emptyAs)) {
    if (current === undefined) return []; // rule 5: already absent
    if (Array.isArray(getIn(state.value, tokens.slice(0, -1)))) {
      warn("unset-array-index", "unset of an array element rejected; arrays are written by their container", path);
      return [];
    }
    const objectPaths = new Set<string>();
    for (const objectTarget of state.tree?.objectTargets ?? []) {
      const resolved = resolveTarget(objectTarget, state.value, repeats);
      if (resolved !== null) objectPaths.add(resolved);
    }
    let unset = tokens;
    while (unset.length > 1) {
      const ancestor = unset.slice(0, -1);
      if (ancestor[0] === CTX) break;
      // Never collapse across an array element (the ancestor is a row).
      if (Array.isArray(getIn(state.value, ancestor.slice(0, -1)))) break;
      if (!objectPaths.has(formatPointer(ancestor))) break;
      const holder = getIn(state.value, ancestor);
      if (!isPlainObject(holder)) break;
      const keys = Object.keys(holder);
      if (keys.length !== 1 || keys[0] !== unset[unset.length - 1]) break;
      unset = ancestor;
    }
    return [{ op: "unset", path: formatPointer(unset) }];
  }

  if (deepEqual(current, next)) return []; // rule 5
  // The path must be creatable: no primitive ancestor, array steps in range.
  let node: unknown = state.value;
  for (let i = 0; i < tokens.length - 1; i++) {
    if (node === undefined) break;
    if (!canStep(node, tokens[i])) {
      warn("invalid-path", `write rejected: ${formatPointer(tokens.slice(0, i + 1))} cannot hold children`, path);
      return [];
    }
    node = getIn(node, [tokens[i]]);
    if (node !== undefined && node !== null && typeof node !== "object") {
      warn("invalid-path", `write rejected: ${formatPointer(tokens.slice(0, i + 1))} is not a container`, path);
      return [];
    }
  }
  if (node !== undefined && !canStep(node, tokens[tokens.length - 1])) {
    warn("invalid-path", "write rejected: parent cannot hold this key", path);
    return [];
  }
  return [{ op: "set", path, value: next }];
}

function canStep(container: unknown, token: string): boolean {
  if (Array.isArray(container)) {
    return token === "-" || (isArrayIndexToken(token) && Number(token) <= container.length);
  }
  return isPlainObject(container);
}

// ------------------------------------------------------------ applyPatches

/**
 * Applies patches immutably. Only objects on changed paths are copied; the
 * copies inherit repeat row identity from their originals. Throws on
 * /$ctx, root, array-index unset and non-container paths.
 */
export function applyPatches<T>(value: T, patches: readonly Patch[]): T {
  let result: unknown = value;
  for (const patch of patches) {
    const tokens = parsePointer(patch.path);
    if (tokens[0] === CTX) throw new Error("applyPatches: /$ctx is read-only");
    if (tokens.length === 0) throw new Error("applyPatches: cannot replace the value root");
    result = patch.op === "set" ? setIn(result, tokens, 0, patch.value) : unsetIn(result, tokens, 0);
  }
  return result as T;
}

function copy<C extends object>(container: C): C {
  const clone = (Array.isArray(container) ? container.slice() : { ...container }) as C;
  adoptRowIdentity(clone, container);
  return clone;
}

function setIn(container: unknown, tokens: string[], depth: number, value: unknown): unknown {
  const token = tokens[depth];
  const last = depth === tokens.length - 1;
  const created = container === undefined || container === null;
  const base = created ? {} : container;
  if (Array.isArray(base)) {
    const index = token === "-" ? base.length : isArrayIndexToken(token) ? Number(token) : -1;
    if (index < 0 || index > base.length) throw new Error(`applyPatches: bad array index ${token}`);
    const child = last ? value : setIn(base[index], tokens, depth + 1, value);
    if (index < base.length && Object.is(base[index], child)) return base;
    const clone = copy(base);
    clone[index] = child;
    return clone;
  }
  if (!isPlainObject(base)) throw new Error(`applyPatches: ${formatPointer(tokens.slice(0, depth))} is not a container`);
  const has = Object.prototype.hasOwnProperty.call(base, token);
  const existing = has ? base[token] : undefined;
  const child = last ? value : setIn(existing, tokens, depth + 1, value);
  if (!created && has && Object.is(existing, child)) return base;
  const clone = created ? base : copy(base);
  clone[token] = child;
  return clone;
}

function unsetIn(container: unknown, tokens: string[], depth: number): unknown {
  const token = tokens[depth];
  const last = depth === tokens.length - 1;
  if (Array.isArray(container)) {
    if (last) throw new Error(`applyPatches: unset of array element ${formatPointer(tokens)} is not allowed`);
    if (!isArrayIndexToken(token) || Number(token) >= container.length) return container;
    const child = unsetIn(container[Number(token)], tokens, depth + 1);
    if (child === container[Number(token)]) return container;
    const clone = copy(container);
    clone[Number(token)] = child;
    return clone;
  }
  if (!isPlainObject(container) || !Object.prototype.hasOwnProperty.call(container, token)) return container;
  if (last) {
    const clone = copy(container);
    delete clone[token];
    return clone;
  }
  const child = unsetIn(container[token], tokens, depth + 1);
  if (child === container[token]) return container;
  const clone = copy(container);
  clone[token] = child;
  return clone;
}

// ---------------------------------------------------------------- batching

/**
 * Merges a sequence of patches: a patch is dropped when a later patch
 * writes the same path or an ancestor of it (the later write wins). The
 * surviving patches keep their relative order.
 */
export function mergePatches(patches: readonly Patch[]): Patch[] {
  const out: Patch[] = [];
  for (let i = 0; i < patches.length; i++) {
    let superseded = false;
    for (let j = i + 1; j < patches.length && !superseded; j++) {
      superseded = isWithin(patches[i].path, patches[j].path);
    }
    if (!superseded) out.push(patches[i]);
  }
  return out;
}

export interface PatchBatch {
  push(...patches: Patch[]): void;
  readonly size: number;
  /** Returns the merged batch and resets it. */
  flush(): Patch[];
}

/** Accumulator for one commit window (composer/react flushes it per microtask). */
export function createPatchBatch(): PatchBatch {
  let pending: Patch[] = [];
  return {
    push(...patches) {
      pending.push(...patches);
    },
    get size() {
      return pending.length;
    },
    flush() {
      const merged = mergePatches(pending);
      pending = [];
      return merged;
    }
  };
}
