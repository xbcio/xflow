// Mapping BindTargets to concrete JSON Pointers against the current value.

import { formatPointer, getPointer, parsePointer } from "./pointer";
import { rowUids } from "./rows";
import type { BindTarget, JsonPointer, RepeatInfo } from "./types";

export function fieldTokens(field: string): string[] {
  if (field === "") return [];
  return field.startsWith("/") ? parsePointer(field) : [field];
}

export function isPathTarget(target: BindTarget): target is { path: JsonPointer } {
  return "path" in target;
}

/**
 * Resolves a target to a concrete pointer in `value`. Row targets are looked
 * up by row uid in the *current* array; returns null when the row (or any
 * enclosing row) no longer exists.
 */
export function resolveTarget(
  target: BindTarget,
  value: unknown,
  repeats: Readonly<Record<string, RepeatInfo>>
): JsonPointer | null {
  if (isPathTarget(target)) return target.path;
  const info = repeats[target.repeat];
  if (!info) return null;
  const arrayPointer = resolveTarget(info.source, value, repeats);
  if (arrayPointer === null) return null;
  const rows = getPointer(value, arrayPointer);
  if (!Array.isArray(rows)) return null;
  const index = rowUids(rows, info.key).indexOf(target.row);
  if (index < 0) return null;
  return arrayPointer + formatPointer([index, ...fieldTokens(target.field)]);
}

export function sameTarget(a: BindTarget, b: BindTarget): boolean {
  if (isPathTarget(a)) return isPathTarget(b) && a.path === b.path;
  return !isPathTarget(b) && a.repeat === b.repeat && a.row === b.row && a.field === b.field;
}
