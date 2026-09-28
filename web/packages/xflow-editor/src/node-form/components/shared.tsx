// Shared pieces of the node-form host components (Doc C §3, §3.2): prop
// schema fragments, the runtime shape check behind ShapeGuard /
// ExpressionInput, and the read-only raw view that sends the user to the
// JSON tab. Nothing here writes.

import { containsExpression } from "@xflow/composer/core";
import { z } from "zod";
import type { ShapeExpectation } from "../components";
import type { FieldType } from "../schema";

export const NODEFORM = "xflow-editor-nodeform";

export function cx(...names: (string | false | null | undefined)[]): string {
  return names.filter(Boolean).join(" ");
}

/** Field chrome every host field accepts (mirrors composer/form `fieldShape`). */
export const chromeShape = {
  label: z.string().optional(),
  description: z.string().optional(),
  /** Marker only; enforcement is the `required` check next to it. */
  required: z.boolean().optional(),
  disabled: z.boolean().optional()
};

export interface ChromeProps {
  label?: string;
  description?: string;
  required?: boolean;
  disabled?: boolean;
}

const fieldType = z.enum(["string", "number", "boolean", "array", "object"]);

export const expectationSchema = z.strictObject({
  type: fieldType,
  item: fieldType.optional(),
  values: fieldType.optional()
});

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function matchesType(value: unknown, type: FieldType): boolean {
  switch (type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "boolean":
      return typeof value === "boolean";
    case "array":
      return Array.isArray(value);
    case "object":
      return isPlainObject(value);
  }
}

/** Doc C §3.2: does a defined value fit the field? `undefined` always does. */
export function matchesShape(value: unknown, expect: ShapeExpectation): boolean {
  if (value === undefined) return true;
  if (!matchesType(value, expect.type)) return false;
  if (expect.type === "array" && expect.item) return (value as unknown[]).every((item) => matchesType(item, expect.item!));
  if (expect.type === "object" && expect.values) {
    return Object.values(value as Record<string, unknown>).every((item) => matchesType(item, expect.values!));
  }
  return true;
}

/** A string holding `${{ }}` / `{{ }}` (partial templates included). */
export function isExpressionString(value: unknown): value is string {
  return typeof value === "string" && containsExpression(value);
}

export function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    return String(value);
  }
}

export const JSON_TAB_HINT = "请在 JSON 页签修改";

/**
 * Read-only view of a value the form cannot edit: expression text shown
 * as-is, anything else as JSON. Never writes, offers no clear action.
 */
export function RawNotice({ value, reason }: { value: unknown; reason: "expression" | "shape" }) {
  const text = reason === "expression" && typeof value === "string" ? value : safeJson(value);
  const message =
    reason === "expression" ? `该字段的值是表达式，表单无法编辑，${JSON_TAB_HINT}。` : `当前值与字段类型不符，以只读方式显示，${JSON_TAB_HINT}。`;
  return (
    <div className={`${NODEFORM}-raw`} data-nodeform-raw={reason}>
      <p className={`${NODEFORM}-raw-notice`} role="note">
        {message}
      </p>
      <pre className={`${NODEFORM}-raw-value`} tabIndex={0}>
        {text}
      </pre>
    </div>
  );
}

/** "默认：X" for an unset value with a defaultHint (display only). */
export function hintText(defaultHint: unknown, display: (value: unknown) => string = displayValue): string | undefined {
  return defaultHint === undefined ? undefined : `默认：${display(defaultHint)}`;
}

export function displayValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "开" : "关";
  if (typeof value === "number") return String(value);
  return JSON.stringify(value) ?? String(value);
}
