// Public contracts of composer/core (Doc B §3–§6). This module is type-only
// apart from a few constant tables; it must stay free of any UI dependency.

/** Major version identifier accepted by this engine. */
export const SPEC_MAJOR = "composer/v1";

/** Highest `minor` this engine understands (Doc B §3.1). */
export const SUPPORTED_MINOR = 0;

/** Element fields defined by composer/v1. */
export const ELEMENT_FIELDS = [
  "type",
  "props",
  "children",
  "slots",
  "visible",
  "repeat",
  "checks"
] as const;

/** Element fields reserved for later versions (Doc B §8); rejected in v1. */
export const RESERVED_ELEMENT_FIELDS = ["on", "watch", "actions", "data"] as const;

/** Top-level spec fields defined by composer/v1. */
export const SPEC_FIELDS = ["spec", "minor", "root", "elements"] as const;

/** Check fields defined by composer/v1. */
export const CHECK_FIELDS = ["type", "args", "message", "severity", "when", "targets"] as const;

/** Repeat fields defined by composer/v1. */
export const REPEAT_FIELDS = ["statePath", "key"] as const;

export type JsonPointer = string;

export type Severity = "error" | "warning";

// ---------------------------------------------------------------- conditions

export type CompareOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

export type Cond =
  | boolean
  | Cond[]
  | { $and: Cond[] }
  | { $or: Cond[] }
  | { $not: Cond }
  | ({ $state: JsonPointer } & CondLeafOp)
  | ({ $item: string } & CondLeafOp);

export type CondLeafOp =
  | { eq: unknown }
  | { neq: unknown }
  | { gt: number }
  | { gte: number }
  | { lt: number }
  | { lte: number }
  | { in: unknown[] }
  | { truthy: boolean };

// -------------------------------------------------------------------- checks

export interface CheckDef {
  type: string;
  args?: Record<string, unknown>;
  /** Required; may be an expression (Doc B §3.2). */
  message: unknown;
  severity?: Severity;
  when?: Cond;
  targets?: JsonPointer[];
}

// --------------------------------------------------------------------- spec

export interface RepeatDef {
  /** Absolute pointer, or `{ $item: field }` for nested repeats. */
  statePath: JsonPointer | { $item: string };
  /** Row field used as identity. Without it, object identity is used. */
  key?: string;
}

export interface ElementDef {
  type: string;
  props?: Record<string, unknown>;
  children?: string[];
  slots?: Record<string, string[]>;
  visible?: Cond;
  repeat?: RepeatDef;
  checks?: CheckDef[];
}

export interface ComposerSpec {
  spec: typeof SPEC_MAJOR;
  minor?: number;
  root: string;
  elements: Record<string, ElementDef>;
}

// -------------------------------------------------------------------- issues

export interface Issue {
  /** Absolute JSON Pointer of the value the issue is about, if any. */
  path?: JsonPointer;
  message: string;
  severity: Severity;
  /** Check type that produced the issue (absent for external issues). */
  check?: string;
  /** Instance id of the element whose check produced the issue. */
  source?: string;
  /** Optional check-provided detail, not meant as the primary message. */
  detail?: string;
}

export interface SpecError {
  /** JSON Pointer into the spec document, e.g. `/elements/f-mode/checks/0`. */
  path: JsonPointer;
  code: SpecErrorCode;
  message: string;
  severity: Severity;
}

export type SpecErrorCode =
  | "shape"
  | "major"
  | "minor"
  | "root"
  | "dangling"
  | "cycle"
  | "multi-parent"
  | "orphan"
  | "unknown-field"
  | "reserved-field"
  | "unknown-type"
  | "unknown-check"
  | "unknown-expression"
  | "invalid-cond"
  | "invalid-check";

export type ResolveErrorCode =
  | "unknown-type"
  | "props"
  | "expression"
  | "cond"
  | "repeat"
  | "cycle"
  | "unsupported"
  | "check";

export interface ResolveError {
  code: ResolveErrorCode;
  message: string;
  /** Props schema issues when `code === "props"`. */
  issues?: { path: string; message: string }[];
}

export interface Warning {
  code: string;
  message: string;
  path?: string;
}

// ------------------------------------------------------------------ binding

export type ValueKind = "scalar" | "array" | "object" | "none";

export type BindTarget =
  | { path: JsonPointer }
  | { repeat: string; row: string; field: string };

export interface ResolvedBinding {
  target: BindTarget;
  value: unknown;
  valueKind: ValueKind;
}

export interface ResolvedNode {
  id: string;
  type: string;
  props: Record<string, unknown>;
  bindings: Record<string, ResolvedBinding>;
  defaultHint?: unknown;
  issues: Issue[];
  error?: ResolveError;
  children: ResolvedNode[];
  slots: Record<string, ResolvedNode[]>;
}

/** Where a repeat instance reads its array from; used by `write` to map row uids. */
export interface RepeatInfo {
  source: BindTarget;
  key?: string;
}

export interface ResolvedTree {
  root: ResolvedNode | null;
  /** Spec-level diagnostics (from validateSpec). */
  specErrors: SpecError[];
  /** Every issue, including external ones, for `onValidate`. */
  issues: Issue[];
  warnings: Warning[];
  repeats: Record<string, RepeatInfo>;
  /** Targets of visible `valueKind: "object"` bindings (Doc B §6 rule 2). */
  objectTargets: BindTarget[];
}

export type Patch = { op: "set"; path: JsonPointer; value: unknown } | { op: "unset"; path: JsonPointer };

/** v2 data-source state; always empty in v1 (Doc B §8). */
export interface DataState {
  status: "idle" | "loading" | "ready" | "error";
  value?: unknown;
  error?: string;
}
