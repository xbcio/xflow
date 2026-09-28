// RFC 6901 JSON Pointer helpers plus the small value utilities shared by the
// condition, check and write modules.

import type { JsonPointer } from "./types";

const parseCache = new Map<string, string[]>();

/** Parses a JSON Pointer into unescaped reference tokens. Throws on syntax errors. */
export function parsePointer(pointer: JsonPointer): string[] {
  const cached = parseCache.get(pointer);
  if (cached) return cached;
  if (pointer === "") return [];
  if (!pointer.startsWith("/")) {
    throw new Error(`invalid JSON Pointer ${JSON.stringify(pointer)}: must start with "/"`);
  }
  const tokens = pointer
    .slice(1)
    .split("/")
    .map((token) => {
      if (/~[^01]|~$/.test(token)) {
        throw new Error(`invalid JSON Pointer ${JSON.stringify(pointer)}: bad "~" escape`);
      }
      return token.replace(/~1/g, "/").replace(/~0/g, "~");
    });
  if (parseCache.size > 10_000) parseCache.clear();
  parseCache.set(pointer, tokens);
  return tokens;
}

export function isValidPointer(pointer: unknown): pointer is JsonPointer {
  if (typeof pointer !== "string") return false;
  try {
    parsePointer(pointer);
    return true;
  } catch {
    return false;
  }
}

export function escapeToken(token: string): string {
  return token.replace(/~/g, "~0").replace(/\//g, "~1");
}

export function formatPointer(tokens: readonly (string | number)[]): JsonPointer {
  return tokens.map((token) => "/" + escapeToken(String(token))).join("");
}

export function joinPointer(base: JsonPointer, ...tokens: (string | number)[]): JsonPointer {
  return base + formatPointer(tokens);
}

export function parentPointer(pointer: JsonPointer): JsonPointer | null {
  if (pointer === "") return null;
  return pointer.slice(0, pointer.lastIndexOf("/"));
}

/** True when `pointer` equals `ancestor` or lies below it. */
export function isWithin(pointer: JsonPointer, ancestor: JsonPointer): boolean {
  return pointer === ancestor || pointer.startsWith(ancestor + "/");
}

const ARRAY_INDEX = /^(0|[1-9][0-9]*)$/;

export function isArrayIndexToken(token: string): boolean {
  return ARRAY_INDEX.test(token);
}

/** Reads `tokens` from `root`; returns `undefined` for any missing segment. */
export function getIn(root: unknown, tokens: readonly string[]): unknown {
  let current: unknown = root;
  for (const token of tokens) {
    if (Array.isArray(current)) {
      if (!isArrayIndexToken(token)) return undefined;
      current = current[Number(token)];
    } else if (isPlainObject(current)) {
      if (!Object.prototype.hasOwnProperty.call(current, token)) return undefined;
      current = current[token];
    } else {
      return undefined;
    }
  }
  return current;
}

export function getPointer(root: unknown, pointer: JsonPointer): unknown {
  return getIn(root, parsePointer(pointer));
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Structural equality over JSON-like values; numbers compare numerically. */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true; // also makes 0 === -0
  if (typeof a === "number" && typeof b === "number") return Number.isNaN(a) && Number.isNaN(b);
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  if (isPlainObject(a)) {
    if (!isPlainObject(b)) return false;
    const aKeys = Object.keys(a);
    if (aKeys.length !== Object.keys(b).length) return false;
    for (const key of aKeys) {
      if (!Object.prototype.hasOwnProperty.call(b, key) || !deepEqual(a[key], b[key])) return false;
    }
    return true;
  }
  return false;
}

/** JavaScript truthiness: `[]`, `{}` and `"0"` are truthy (Doc A §2.2). */
export function jsTruthy(value: unknown): boolean {
  return Boolean(value);
}

/** "Set" per Doc A §2.2: present, not null, not "". */
export function isSet(value: unknown): boolean {
  return value !== undefined && value !== null && value !== "";
}
