// Condition evaluation (Doc B §3.3). Shared semantics with the Go side via
// testdata/cond-truth-table.json:
//   - a missing path compares as null;
//   - `truthy` is JavaScript truthiness;
//   - numbers compare numerically (1 == 1.0);
//   - any evaluation error anywhere in the condition makes the whole
//     condition evaluate to TRUE and reports the error. All branches are
//     evaluated (no short-circuit) so the error outcome does not depend on
//     operand order.

import { deepEqual, isPlainObject, isValidPointer, joinPointer, jsTruthy } from "./pointer";
import type { Cond } from "./types";

export const LEAF_OPS = ["eq", "neq", "gt", "gte", "lt", "lte", "in", "truthy"] as const;
type LeafOp = (typeof LEAF_OPS)[number];
const LEAF_OP_SET: ReadonlySet<string> = new Set(LEAF_OPS);

export interface CondScope {
  /** Reads an absolute pointer; missing paths return undefined. */
  get(pointer: string): unknown;
  /** Reads a field of the current repeat row; absent outside a repeat. */
  item?: (field: string) => unknown;
}

export interface CondResult {
  value: boolean;
  error?: string;
}

export function evalCond(cond: Cond | undefined, scope: CondScope): CondResult {
  if (cond === undefined) return { value: true };
  const errors: string[] = [];
  const value = evaluate(cond, scope, errors);
  if (errors.length > 0) return { value: true, error: errors[0] };
  return { value };
}

function evaluate(cond: unknown, scope: CondScope, errors: string[]): boolean {
  if (typeof cond === "boolean") return cond;
  if (Array.isArray(cond)) return all(cond, scope, errors);
  if (!isPlainObject(cond)) {
    errors.push(`condition must be a boolean, array or object, got ${describe(cond)}`);
    return true;
  }
  if ("$and" in cond) {
    if (!Array.isArray(cond.$and)) return fail(errors, "$and expects an array");
    return all(cond.$and, scope, errors);
  }
  if ("$or" in cond) {
    if (!Array.isArray(cond.$or)) return fail(errors, "$or expects an array");
    let result = false;
    for (const child of cond.$or) result = evaluate(child, scope, errors) || result;
    return result;
  }
  if ("$not" in cond) return !evaluate(cond.$not, scope, errors);
  return leaf(cond, scope, errors);
}

function all(conds: unknown[], scope: CondScope, errors: string[]): boolean {
  let result = true;
  for (const child of conds) result = evaluate(child, scope, errors) && result;
  return result;
}

function leaf(cond: Record<string, unknown>, scope: CondScope, errors: string[]): boolean {
  let actual: unknown;
  if (typeof cond.$state === "string") {
    actual = scope.get(cond.$state);
  } else if (typeof cond.$item === "string") {
    if (!scope.item) return fail(errors, "$item used outside a repeat");
    actual = scope.item(cond.$item);
  } else {
    return fail(errors, "condition leaf needs $state or $item");
  }
  const ops = Object.keys(cond).filter((key) => LEAF_OP_SET.has(key)) as LeafOp[];
  if (ops.length !== 1) return fail(errors, `condition leaf needs exactly one operator, got ${ops.length}`);
  const op = ops[0];
  const expected = cond[op];
  // Missing paths compare as null (Doc B §3.3).
  const value = actual === undefined ? null : actual;
  switch (op) {
    case "eq":
      return deepEqual(value, expected);
    case "neq":
      return !deepEqual(value, expected);
    case "in":
      if (!Array.isArray(expected)) return fail(errors, "in expects an array operand");
      return expected.some((candidate) => deepEqual(value, candidate));
    case "truthy":
      if (typeof expected !== "boolean") return fail(errors, "truthy expects true or false");
      return jsTruthy(value) === expected;
    default: {
      if (!isFiniteNumber(value) || !isFiniteNumber(expected)) {
        return fail(errors, `${op} needs numeric operands, got ${describe(value)} and ${describe(expected)}`);
      }
      if (op === "gt") return value > expected;
      if (op === "gte") return value >= expected;
      if (op === "lt") return value < expected;
      return value <= expected;
    }
  }
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function fail(errors: string[], message: string): boolean {
  errors.push(message);
  return true;
}

function describe(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
}

/** Static shape check used by validateSpec. Returns `{path, message}` per problem. */
export function condShapeErrors(
  cond: unknown,
  at: string,
  opts: { inRepeat: boolean }
): { path: string; message: string }[] {
  const out: { path: string; message: string }[] = [];
  walk(cond, at);
  return out;

  function walk(node: unknown, path: string): void {
    if (typeof node === "boolean") return;
    if (Array.isArray(node)) {
      node.forEach((child, index) => walk(child, joinPointer(path, index)));
      return;
    }
    if (!isPlainObject(node)) {
      out.push({ path, message: "condition must be a boolean, array or object" });
      return;
    }
    const keys = Object.keys(node);
    for (const combinator of ["$and", "$or"] as const) {
      if (combinator in node) {
        if (keys.length !== 1) out.push({ path, message: `${combinator} must be the only key` });
        const list = node[combinator];
        if (!Array.isArray(list)) out.push({ path: joinPointer(path, combinator), message: `${combinator} expects an array` });
        else list.forEach((child, index) => walk(child, joinPointer(path, combinator, index)));
        return;
      }
    }
    if ("$not" in node) {
      if (keys.length !== 1) out.push({ path, message: "$not must be the only key" });
      walk(node.$not, joinPointer(path, "$not"));
      return;
    }
    const hasState = "$state" in node;
    const hasItem = "$item" in node;
    if (hasState === hasItem) {
      out.push({ path, message: "condition leaf needs exactly one of $state or $item" });
      return;
    }
    if (hasState && !isValidPointer(node.$state)) {
      out.push({ path: joinPointer(path, "$state"), message: "$state must be a JSON Pointer" });
    }
    if (hasItem) {
      if (typeof node.$item !== "string") out.push({ path: joinPointer(path, "$item"), message: "$item must be a string" });
      if (!opts.inRepeat) out.push({ path: joinPointer(path, "$item"), message: "$item used outside a repeat" });
    }
    const ops = keys.filter((key) => key !== "$state" && key !== "$item");
    const known = ops.filter((key) => LEAF_OP_SET.has(key));
    for (const key of ops) {
      if (!LEAF_OP_SET.has(key)) out.push({ path: joinPointer(path, key), message: `unknown condition operator ${key}` });
    }
    if (known.length !== 1) {
      out.push({ path, message: "condition leaf needs exactly one operator" });
      return;
    }
    const op = known[0];
    const operand = node[op];
    if (op === "in" && !Array.isArray(operand)) out.push({ path: joinPointer(path, op), message: "in expects an array" });
    if (op === "truthy" && typeof operand !== "boolean") {
      out.push({ path: joinPointer(path, op), message: "truthy expects true or false" });
    }
    if ((op === "gt" || op === "gte" || op === "lt" || op === "lte") && !isFiniteNumber(operand)) {
      out.push({ path: joinPointer(path, op), message: `${op} expects a number` });
    }
  }
}
