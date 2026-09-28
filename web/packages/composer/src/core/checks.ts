// Validation checks (Doc B §3.5) and the defineCheck registry (§8).

import type { CondResult } from "./cond";
import { FORMATS, isFormatName, isJsonText } from "./formats";
import { deepEqual, isPlainObject, isSet, isValidPointer } from "./pointer";
import type { Cond, Issue } from "./types";

export interface CheckContext {
  /** Reads any absolute pointer (cross-field checks). */
  get(pointer: string): unknown;
  cond(cond: Cond): CondResult;
  /** Concrete pointer of the element's primary binding, if any. */
  path?: string;
}

/** A check result. The spec's `message` is the primary message; `message` here becomes `detail`. */
export type CheckIssue = Partial<Pick<Issue, "path" | "message" | "detail">>;

export type CheckFn = (value: unknown, args: Record<string, unknown>, ctx: CheckContext) => CheckIssue[];

export interface CheckDefinition {
  readonly type: string;
  readonly fn: CheckFn;
  /** Static argument validation used by validateSpec; returns messages. */
  readonly validateArgs?: (args: Record<string, unknown>) => string[];
  /** Runs even when the element is hidden (built-in: oneOf). */
  readonly evaluateWhenHidden?: boolean;
}

export type CheckRegistry = ReadonlyMap<string, CheckDefinition>;

export function defineCheck(
  type: string,
  fn: CheckFn,
  options: Pick<CheckDefinition, "validateArgs" | "evaluateWhenHidden"> = {}
): CheckDefinition {
  if (!type) throw new Error("check type must be non-empty");
  return { type, fn, ...options };
}

const FAIL: CheckIssue[] = [{}];
const PASS: CheckIssue[] = [];

function numberArg(key = "value", integer = false) {
  return (args: Record<string, unknown>): string[] => {
    const value = args[key];
    if (typeof value !== "number" || !Number.isFinite(value)) return [`args.${key} must be a number`];
    if (integer && (!Number.isInteger(value) || value < 0)) return [`args.${key} must be a non-negative integer`];
    return [];
  };
}

const noArgs = (args: Record<string, unknown>) => Object.keys(args).map((key) => `unexpected args.${key}`);

function length(value: unknown): number | null {
  if (typeof value === "string") return [...value].length;
  if (Array.isArray(value)) return value.length;
  return null;
}

/** Only `required` and `oneOf` look at unset values; everything else passes them. */
function whenSet(fn: CheckFn): CheckFn {
  return (value, args, ctx) => (isSet(value) ? fn(value, args, ctx) : PASS);
}

const regexCache = new WeakMap<object, RegExp>();

export function compilePattern(args: Record<string, unknown>): RegExp {
  let compiled = regexCache.get(args);
  if (!compiled) {
    compiled = new RegExp(String(args.pattern), typeof args.flags === "string" ? args.flags : undefined);
    regexCache.set(args, compiled);
  }
  return compiled;
}

export type OneOfMode = "exactly" | "at_most" | "at_least";
const ONE_OF_MODES: readonly OneOfMode[] = ["exactly", "at_most", "at_least"];

export const builtinChecks: readonly CheckDefinition[] = [
  defineCheck("required", (value) => (isSet(value) ? PASS : FAIL), { validateArgs: noArgs }),
  defineCheck(
    "min",
    whenSet((value, args) => (typeof value === "number" && value < (args.value as number) ? FAIL : PASS)),
    { validateArgs: numberArg() }
  ),
  defineCheck(
    "max",
    whenSet((value, args) => (typeof value === "number" && value > (args.value as number) ? FAIL : PASS)),
    { validateArgs: numberArg() }
  ),
  defineCheck(
    "minLength",
    whenSet((value, args) => {
      const n = typeof value === "string" ? length(value) : null;
      return n !== null && n < (args.value as number) ? FAIL : PASS;
    }),
    { validateArgs: numberArg("value", true) }
  ),
  defineCheck(
    "maxLength",
    whenSet((value, args) => {
      const n = typeof value === "string" ? length(value) : null;
      return n !== null && n > (args.value as number) ? FAIL : PASS;
    }),
    { validateArgs: numberArg("value", true) }
  ),
  defineCheck(
    "pattern",
    whenSet((value, args) => (typeof value === "string" && !compilePattern(args).test(value) ? FAIL : PASS)),
    {
      validateArgs: (args) => {
        if (typeof args.pattern !== "string") return ["args.pattern must be a string"];
        try {
          compilePattern(args);
          return [];
        } catch (error) {
          return [`args.pattern is not a valid regular expression: ${(error as Error).message}`];
        }
      }
    }
  ),
  defineCheck(
    "format",
    whenSet((value, args) => {
      if (args.allowExpression === true && args.format !== "expression" && FORMATS.expression(value)) return PASS;
      return FORMATS[args.format as keyof typeof FORMATS](value) ? PASS : FAIL;
    }),
    {
      validateArgs: (args) => [
        ...(isFormatName(args.format) ? [] : [`args.format must be one of ${Object.keys(FORMATS).join(", ")}`]),
        ...(args.allowExpression === undefined || typeof args.allowExpression === "boolean"
          ? []
          : ["args.allowExpression must be a boolean"])
      ]
    }
  ),
  defineCheck(
    "minItems",
    whenSet((value, args) => (Array.isArray(value) && value.length < (args.value as number) ? FAIL : PASS)),
    { validateArgs: numberArg("value", true) }
  ),
  defineCheck(
    "maxItems",
    whenSet((value, args) => (Array.isArray(value) && value.length > (args.value as number) ? FAIL : PASS)),
    { validateArgs: numberArg("value", true) }
  ),
  defineCheck(
    "uniqueItems",
    whenSet((value) => {
      if (!Array.isArray(value)) return PASS;
      for (let i = 0; i < value.length; i++) {
        for (let j = i + 1; j < value.length; j++) if (deepEqual(value[i], value[j])) return FAIL;
      }
      return PASS;
    }),
    { validateArgs: noArgs }
  ),
  defineCheck(
    "oneOf",
    (_value, args, ctx) => {
      const paths = args.paths as string[];
      const count = paths.filter((path) => isSet(ctx.get(path))).length;
      const mode = (args.mode as OneOfMode | undefined) ?? "exactly";
      const ok = mode === "exactly" ? count === 1 : mode === "at_most" ? count <= 1 : count >= 1;
      return ok ? PASS : FAIL;
    },
    {
      evaluateWhenHidden: true,
      validateArgs: (args) => [
        ...(Array.isArray(args.paths) && args.paths.length > 0 && args.paths.every(isValidPointer)
          ? []
          : ["args.paths must be a non-empty array of JSON Pointers"]),
        ...(args.mode === undefined || ONE_OF_MODES.includes(args.mode as OneOfMode)
          ? []
          : [`args.mode must be one of ${ONE_OF_MODES.join(", ")}`])
      ]
    }
  ),
  defineCheck("json", whenSet((value) => (typeof value === "string" && !isJsonText(value) ? FAIL : PASS)), {
    validateArgs: noArgs
  })
];

export function createCheckRegistry(extra: readonly CheckDefinition[] = []): CheckRegistry {
  const registry = new Map<string, CheckDefinition>();
  for (const def of builtinChecks) registry.set(def.type, def);
  const seen = new Set<string>();
  for (const def of extra) {
    if (seen.has(def.type)) throw new Error(`duplicate check ${def.type}`);
    seen.add(def.type);
    registry.set(def.type, def);
  }
  return registry;
}

export function isCheckArgs(value: unknown): value is Record<string, unknown> {
  return isPlainObject(value);
}
