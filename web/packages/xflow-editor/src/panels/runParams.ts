import type { WorkflowParam } from "@xflow/core";

/**
 * Run-time parameters (POST /v1/workflows/{id}/execute `params`). The server
 * checks them against the workflow's top-level `params` schema
 * (`engine/graph/params.go`): a missing required param, a type mismatch and a
 * numeric `validation.min` / `validation.max` miss are all 400
 * `workflow_params_invalid`. This module mirrors those checks so the editor can
 * point at the field before the request leaves.
 *
 * The form holds RAW TEXT per param. Nothing reaches the request until
 * {@link parseRunParams} accepts every field, so a half-typed number or an
 * unbalanced JSON object can never be sent.
 */

/** How one declared param is entered. */
export type RunParamKind = "string" | "number" | "boolean" | "object" | "array" | "json";

export interface RunParamField {
  name: string;
  label: string;
  kind: RunParamKind;
  required: boolean;
  defaultValue?: unknown;
  /** `validation.min`, applied to numbers only (the server ignores it elsewhere). */
  min?: number;
  /** `validation.max`, applied to numbers only. */
  max?: number;
}

export type RunParamValues = Record<string, string>;
export type RunParamErrors = Record<string, string>;

const NUMBER_TEXT = /^-?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?$/i;

function kindOf(type: string | undefined): RunParamKind {
  switch (type) {
    case "string":
    case "number":
    case "boolean":
    case "object":
    case "array":
      return type;
    default:
      // An undeclared or unknown type is not checked by the server, so any
      // JSON value is allowed.
      return "json";
  }
}

function boundOf(validation: Record<string, unknown> | undefined, key: "min" | "max"): number | undefined {
  const value = validation?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

/**
 * The params the run form asks for. Required ones come first (the server
 * returns the schema as a name-sorted map, so declaration order is lost); the
 * rest keep their order.
 */
export function declaredRunParams(params: Record<string, WorkflowParam> | undefined): RunParamField[] {
  const fields = Object.entries(params ?? {}).map(([name, param]): RunParamField => {
    const kind = kindOf(param?.type);
    return {
      name,
      label: param?.display_name?.trim() || name,
      kind,
      required: param?.required === true,
      defaultValue: param?.default,
      min: kind === "number" ? boundOf(param?.validation, "min") : undefined,
      max: kind === "number" ? boundOf(param?.validation, "max") : undefined
    };
  });
  return [...fields.filter((field) => field.required), ...fields.filter((field) => !field.required)];
}

function textOf(field: RunParamField, value: unknown): string {
  if (value === undefined || value === null) return "";
  switch (field.kind) {
    case "string":
      return typeof value === "string" ? value : String(value);
    case "number":
      return String(value);
    case "boolean":
      return value === true ? "true" : value === false ? "false" : "";
    default:
      return JSON.stringify(value, null, 2) ?? "";
  }
}

/** Declared defaults, pre-filled; `seed` (a previous attempt's raw text) wins per field. */
export function initialRunParamValues(fields: RunParamField[], seed?: RunParamValues): RunParamValues {
  const values: RunParamValues = {};
  for (const field of fields) {
    values[field.name] = seed && field.name in seed ? seed[field.name] : textOf(field, field.defaultValue);
  }
  return values;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

type Parsed = { ok: true; value: unknown } | { ok: false; error: string };

function parseNumber(field: RunParamField, text: string): Parsed {
  if (!NUMBER_TEXT.test(text)) return { ok: false, error: "请输入有效数字" };
  const value = Number(text);
  if (!Number.isFinite(value)) return { ok: false, error: "请输入有效数字" };
  if (field.min !== undefined && value < field.min) return { ok: false, error: `不能小于 ${field.min}` };
  if (field.max !== undefined && value > field.max) return { ok: false, error: `不能大于 ${field.max}` };
  return { ok: true, value };
}

function parseJson(field: RunParamField, text: string): Parsed {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return { ok: false, error: "JSON 格式错误" };
  }
  // JSON null is "no value" to the server, so it passes the shape check below
  // and is dropped by parseRunParams instead of being refused as the wrong type.
  if (value === null) return { ok: true, value };
  if (field.kind === "object" && !isPlainObject(value)) return { ok: false, error: "需要 JSON 对象，如 {}" };
  if (field.kind === "array" && !Array.isArray(value)) return { ok: false, error: "需要 JSON 数组，如 []" };
  return { ok: true, value };
}

function parseField(field: RunParamField, raw: string): Parsed {
  const text = raw.trim();
  switch (field.kind) {
    case "string":
      return { ok: true, value: raw };
    case "number":
      return parseNumber(field, text);
    case "boolean":
      return text === "true" || text === "false"
        ? { ok: true, value: text === "true" }
        : { ok: false, error: "请选择是或否" };
    default:
      return parseJson(field, text);
  }
}

/**
 * Checks every field and builds the `params` object. A blank field is left out
 * (the server injects its default); it is an error only when the param is
 * required and has no default to fall back on. A JSON `null` counts as blank,
 * matching the server, which treats a nil param as absent.
 */
export function parseRunParams(
  fields: RunParamField[],
  values: RunParamValues
): { params: Record<string, unknown>; errors: RunParamErrors } {
  const params: Record<string, unknown> = {};
  const errors: RunParamErrors = {};
  for (const field of fields) {
    const raw = values[field.name] ?? "";
    const blank = raw.trim() === "";
    if (blank) {
      if (field.required && (field.defaultValue === undefined || field.defaultValue === null)) {
        errors[field.name] = field.kind === "boolean" ? "请选择是或否" : "必填";
      }
      continue;
    }
    const parsed = parseField(field, raw);
    if (!parsed.ok) {
      errors[field.name] = parsed.error;
    } else if (parsed.value !== null) {
      params[field.name] = parsed.value;
    } else if (field.required && (field.defaultValue === undefined || field.defaultValue === null)) {
      errors[field.name] = "必填";
    }
  }
  return { params, errors };
}
