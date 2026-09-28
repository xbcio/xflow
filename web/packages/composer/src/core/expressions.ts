// Value expressions (Doc B §3.2) and the defineExpression registry (§8).
// Built-ins ($state, $bindState, $item, $bindItem, $index, $cond) are
// registered through the same API as host expressions.

import type { CondResult } from "./cond";
import { getIn, isPlainObject, isValidPointer, parsePointer } from "./pointer";
import type { BindTarget, Cond, DataState } from "./types";

export interface RowScope {
  /** Repeat instance id. */
  repeat: string;
  uid: string;
  index: number;
  value: unknown;
}

export interface ExpressionContext {
  /** Reads an absolute pointer (`/$ctx/...` reads host context). */
  get(pointer: string): unknown;
  cond(cond: Cond): CondResult;
  /** Evaluates a nested value that may itself contain expressions. */
  evaluate(value: unknown): unknown;
  /** Reads a field of the current repeat row (throws outside a repeat). */
  item(field: string): unknown;
  row?: RowScope;
  data: Readonly<Record<string, DataState>>;
}

export interface ExpressionDefinition {
  /** Expression key, starting with "$" (e.g. "$state"). */
  readonly name: string;
  resolve(args: Record<string, unknown>, ctx: ExpressionContext): unknown;
  /** Present for binding expressions: returns the write target. */
  writable?(args: Record<string, unknown>, ctx: ExpressionContext): BindTarget;
  /** Optional static validation; returns messages. */
  validate?(args: Record<string, unknown>, at: { inRepeat: boolean }): string[];
}

export type ExpressionRegistry = ReadonlyMap<string, ExpressionDefinition>;

export class ExpressionError extends Error {}

export function defineExpression(
  name: string,
  def: Omit<ExpressionDefinition, "name">
): ExpressionDefinition {
  if (!name.startsWith("$") || name.length < 2) {
    throw new Error(`expression name ${JSON.stringify(name)} must start with "$"`);
  }
  return { name, ...def };
}

/** Reads a row field: "" is the row itself, "/a/b" is a relative pointer, else a single key. */
export function readRowField(row: unknown, field: string): unknown {
  if (field === "") return row;
  return getIn(row, field.startsWith("/") ? parsePointer(field) : [field]);
}

function stringArg(args: Record<string, unknown>, key: string): string {
  const value = args[key];
  if (typeof value !== "string") throw new ExpressionError(`${key} expects a string`);
  return value;
}

function pointerArg(args: Record<string, unknown>, key: string): string {
  const value = stringArg(args, key);
  if (!isValidPointer(value)) throw new ExpressionError(`${key} expects a JSON Pointer`);
  return value;
}

function requireRow(ctx: ExpressionContext, name: string): RowScope {
  if (!ctx.row) throw new ExpressionError(`${name} used outside a repeat`);
  return ctx.row;
}

function onlyKeys(args: Record<string, unknown>, allowed: string[]): string[] {
  return Object.keys(args)
    .filter((key) => !allowed.includes(key))
    .map((key) => `unexpected key ${key}`);
}

const pointerValidator = (key: string, extra: string[] = []) => (args: Record<string, unknown>) => [
  ...onlyKeys(args, [key, ...extra]),
  ...(isValidPointer(args[key]) ? [] : [`${key} expects a JSON Pointer`])
];

const rowValidator = (key: string) => (args: Record<string, unknown>, at: { inRepeat: boolean }) => [
  ...onlyKeys(args, [key]),
  ...(typeof args[key] === "string" ? [] : [`${key} expects a string`]),
  ...(at.inRepeat ? [] : [`${key} used outside a repeat`])
];

export const builtinExpressions: readonly ExpressionDefinition[] = [
  defineExpression("$state", {
    resolve: (args, ctx) => ctx.get(pointerArg(args, "$state")),
    validate: pointerValidator("$state")
  }),
  defineExpression("$bindState", {
    resolve: (args, ctx) => ctx.get(pointerArg(args, "$bindState")),
    writable: (args) => ({ path: pointerArg(args, "$bindState") }),
    validate: pointerValidator("$bindState")
  }),
  defineExpression("$item", {
    resolve: (args, ctx) => ctx.item(stringArg(args, "$item")),
    validate: rowValidator("$item")
  }),
  defineExpression("$bindItem", {
    resolve: (args, ctx) => ctx.item(stringArg(args, "$bindItem")),
    writable: (args, ctx) => {
      const field = stringArg(args, "$bindItem");
      const row = requireRow(ctx, "$bindItem");
      if (field !== "" && !isPlainObject(row.value)) {
        throw new ExpressionError("$bindItem on a scalar row; scalar lists are written by the container only");
      }
      return { repeat: row.repeat, row: row.uid, field };
    },
    validate: rowValidator("$bindItem")
  }),
  defineExpression("$index", {
    resolve: (_args, ctx) => requireRow(ctx, "$index").index,
    validate: (args, at) => [
      ...onlyKeys(args, ["$index"]),
      ...(args.$index === true ? [] : ["$index expects true"]),
      ...(at.inRepeat ? [] : ["$index used outside a repeat"])
    ]
  }),
  defineExpression("$cond", {
    resolve: (args, ctx) => {
      const result = ctx.cond(args.$cond as Cond);
      if (result.error) throw new ExpressionError(`$cond: ${result.error}`);
      return ctx.evaluate(result.value ? args.then : args.else);
    },
    validate: (args) => onlyKeys(args, ["$cond", "then", "else"])
  })
];

export function createExpressionRegistry(extra: readonly ExpressionDefinition[] = []): ExpressionRegistry {
  const registry = new Map<string, ExpressionDefinition>();
  for (const def of builtinExpressions) registry.set(def.name, def);
  const seen = new Set<string>();
  for (const def of extra) {
    if (seen.has(def.name)) throw new Error(`duplicate expression ${def.name}`);
    seen.add(def.name);
    registry.set(def.name, def);
  }
  return registry;
}

export type ExpressionMatch =
  | { kind: "none" }
  | { kind: "known"; def: ExpressionDefinition }
  | { kind: "unknown"; name: string };

/**
 * Classifies a value: a plain object with a "$"-prefixed key is an
 * expression. Exactly one registered "$" key selects the definition; any
 * other "$" key alone is an unknown expression.
 */
export function matchExpression(value: unknown, registry: ExpressionRegistry): ExpressionMatch {
  if (!isPlainObject(value)) return { kind: "none" };
  let unknown: string | undefined;
  for (const key in value) {
    if (key.charCodeAt(0) !== 36 /* $ */) continue;
    const def = registry.get(key);
    if (def) return { kind: "known", def };
    unknown ??= key;
  }
  return unknown === undefined ? { kind: "none" } : { kind: "unknown", name: unknown };
}

/**
 * Evaluates a value tree, replacing expressions by their results. Subtrees
 * without expressions are returned by reference. Throws ExpressionError.
 */
export function evaluateValue(value: unknown, registry: ExpressionRegistry, ctx: ExpressionContext): unknown {
  if (Array.isArray(value)) {
    let out: unknown[] | undefined;
    for (let i = 0; i < value.length; i++) {
      const next = evaluateValue(value[i], registry, ctx);
      if (next !== value[i] && !out) out = value.slice(0, i);
      if (out) out.push(next);
    }
    return out ?? value;
  }
  if (!isPlainObject(value)) return value;
  const match = matchExpression(value, registry);
  if (match.kind === "known") return match.def.resolve(value, ctx);
  if (match.kind === "unknown") throw new ExpressionError(`unknown expression ${match.name}`);
  let out: Record<string, unknown> | undefined;
  for (const key of Object.keys(value)) {
    const next = evaluateValue(value[key], registry, ctx);
    if (next !== value[key]) {
      out ??= { ...value };
      out[key] = next;
    }
  }
  return out ?? value;
}

/** Visits every expression in a value tree (for validateSpec). */
export function visitExpressions(
  value: unknown,
  registry: ExpressionRegistry,
  at: string,
  visit: (match: ExpressionMatch & { kind: "known" | "unknown" }, node: Record<string, unknown>, path: string) => void,
  join: (base: string, key: string | number) => string
): void {
  if (Array.isArray(value)) {
    value.forEach((child, index) => visitExpressions(child, registry, join(at, index), visit, join));
    return;
  }
  if (!isPlainObject(value)) return;
  const match = matchExpression(value, registry);
  if (match.kind !== "none") {
    visit(match, value, at);
    // Nested values ($cond then/else) may contain expressions as well.
    if (match.kind === "known" && match.def.name === "$cond") {
      for (const key of ["then", "else"]) {
        if (key in value) visitExpressions(value[key], registry, join(at, key), visit, join);
      }
    }
    return;
  }
  for (const key of Object.keys(value)) visitExpressions(value[key], registry, join(at, key), visit, join);
}

