import type { WorkflowDef } from "./types";

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sortObjectKeys<T extends Record<string, unknown>>(obj: T): T {
  const sorted = Object.keys(obj)
    .sort()
    .reduce((acc, key) => {
      const val = obj[key];
      if (Array.isArray(val)) {
        acc[key] = val.map(normalizeValue) as unknown[];
      } else if (isPlainObject(val)) {
        acc[key] = sortObjectKeys(val);
      } else {
        acc[key] = normalizeValue(val);
      }
      return acc;
    }, {} as Record<string, unknown>);
  return sorted as T;
}

function normalizeValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeValue);
  }
  if (isPlainObject(value)) {
    return sortObjectKeys(value);
  }
  return value;
}

function removeUndefinedFields(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(removeUndefinedFields);
  }
  if (isPlainObject(value)) {
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const val = value[key];
      if (val !== undefined) {
        const cleaned = removeUndefinedFields(val);
        if (cleaned !== undefined) {
          result[key] = cleaned;
        }
      }
    }
    return result;
  }
  return value;
}

export function parseWorkflow(json: string): WorkflowDef {
  const parsed = JSON.parse(json) as unknown;
  if (!isPlainObject(parsed)) {
    throw new Error("Invalid workflow JSON: expected object");
  }
  return parsed as WorkflowDef;
}

export function normalizeWorkflow(def: WorkflowDef): WorkflowDef {
  const cleaned = removeUndefinedFields(def) as WorkflowDef;
  return sortObjectKeys(cleaned as Record<string, unknown>) as WorkflowDef;
}

export function serializeWorkflow(def: WorkflowDef): string {
  const normalized = normalizeWorkflow(def);
  return JSON.stringify(normalized, null, 2);
}

export function cloneWorkflow<T extends WorkflowDef | undefined>(def: T): T {
  if (def === undefined) {
    return def;
  }
  return JSON.parse(JSON.stringify(def)) as T;
}
