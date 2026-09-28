// PropsSchema adapter interface (Doc B §4) and the zod implementation.

import type { ZodType } from "zod";
import { formatPointer } from "./pointer";

export type PropsParseResult<P> =
  | { ok: true; value: P }
  | { ok: false; issues: { path: string; message: string }[] };

export interface PropsSchema<P = unknown> {
  parse(input: unknown): PropsParseResult<P>;
  /**
   * Optional: names of props whose string value references another
   * registered component type (e.g. KeyValue `valueType`). validateSpec
   * checks those references (Doc B §5 "组件组合").
   */
  readonly typeRefProps?: readonly string[];
}

/** Wraps a zod schema as a PropsSchema. */
export function zodProps<P>(schema: ZodType<P>, options: { typeRefProps?: readonly string[] } = {}): PropsSchema<P> {
  return {
    typeRefProps: options.typeRefProps,
    parse(input) {
      const result = schema.safeParse(input);
      if (result.success) return { ok: true, value: result.data };
      return {
        ok: false,
        issues: result.error.issues.map((issue) => ({
          path: formatPointer(issue.path.map((segment) => (typeof segment === "symbol" ? String(segment) : segment))),
          message: issue.message
        }))
      };
    }
  };
}

const parseCache = new WeakMap<PropsSchema<unknown>, WeakMap<object, PropsParseResult<unknown>>>();

/** Parses props, caching the result per (schema, props object). */
export function parseProps(schema: PropsSchema<unknown>, props: Record<string, unknown>): PropsParseResult<unknown> {
  let bySchema = parseCache.get(schema);
  if (!bySchema) {
    bySchema = new WeakMap();
    parseCache.set(schema, bySchema);
  }
  let result = bySchema.get(props);
  if (!result) {
    result = schema.parse(props);
    bySchema.set(props, result);
  }
  return result;
}
