// Component and kernel contracts of composer/react (Doc B §5, §6, §7.1).
// Components depend only on these types; they never import a kernel.

import type { ReactNode } from "react";
import type {
  CheckRegistry,
  ComposerSpec,
  ExpressionRegistry,
  Issue,
  Patch,
  PropsSchema,
  ResolvedNode,
  ResolvedTree,
  SpecError,
  ValueKind,
  Warning
} from "../core";

// ------------------------------------------------------------- component

export interface ComposerComponent<P = unknown> {
  type: string;
  /** Props schema; `zodProps(z.object(...))` builds one. */
  props: PropsSchema<P>;
  /** Value kind of each bindable prop; defaults to `{ value: "scalar" }`. */
  bindings?: Record<string, ValueKind>;
  /**
   * Doc B §6 rule 1: `"keep"` writes `""` as a real value instead of
   * unsetting it. Applies to every binding of the component. Default "unset".
   */
  emptyAs?: "unset" | "keep";
  render(p: ComposerComponentProps<P>): ReactNode;
}

export interface ComposerBinding {
  value: unknown;
  onChange(next: unknown): void;
}

export interface ComposerRow {
  id: string;
  index: number;
  children: ReactNode;
}

export interface ComposerComponentProps<P = unknown> {
  node: { id: string; type: string };
  /** Evaluated and validated against the component's props schema. */
  props: P;
  bindings: Record<string, ComposerBinding>;
  /** Shorthand for `bindings.value?.value`. */
  value?: unknown;
  /** Shorthand for `bindings.value?.onChange`. */
  onChange?(next: unknown): void;
  /** Display only: show "default: X" while the value is unset; never write it. */
  defaultHint?: unknown;
  issues: Issue[];
  /** Default slot. */
  children?: ReactNode;
  /** Named slots. */
  slots: Record<string, ReactNode>;
  /** Present on repeat containers: one entry per `$row`. */
  rows?: ComposerRow[];
  readOnly: boolean;
  /** Reserved for v2 events (Doc B §8); v1 only logs a warning. */
  emit(event: string, payload?: unknown): void;
}

// -------------------------------------------------------------- registry

export interface Registry {
  get(type: string): ComposerComponent<unknown> | undefined;
  readonly types: readonly string[];
  /** Props schemas by type (the `schemas` input of core `resolve`). */
  readonly schemas: Readonly<Record<string, PropsSchema<unknown>>>;
  /** Binding value kinds by type (the `bindingKinds` input of core `resolve`). */
  readonly bindingKinds: Readonly<Record<string, Readonly<Record<string, ValueKind>>>>;
  readonly checks: CheckRegistry;
  readonly expressions: ExpressionRegistry;
}

// ---------------------------------------------------------------- kernel

export interface KernelContent {
  /**
   * Rendered default-slot children, in `node.children` order. Kernels pass
   * an array aligned 1:1 with `node.children` (or undefined when empty) so
   * that repeat containers can be split into `rows`.
   */
  children: ReactNode;
  slots: Record<string, ReactNode>;
}

export interface KernelEnv {
  renderNode(node: ResolvedNode, content: KernelContent): ReactNode;
}

export interface RenderKernel {
  readonly name: string;
  render(tree: ResolvedTree, env: KernelEnv): ReactNode;
}

// ------------------------------------------------------------- composer

export interface ValidationResult {
  /** Every issue (checks + externalIssues), both severities. */
  issues: Issue[];
  errors: Issue[];
  warnings: Issue[];
  specErrors: SpecError[];
  /** True when there is no error-severity issue or spec error. */
  valid: boolean;
}

export interface ComposerProps {
  spec: ComposerSpec;
  registry: Registry;
  /** Defaults to `defaultKernel` (kernel-json-render). */
  kernel?: RenderKernel;
  /** Controlled value. */
  value: object;
  /** Read-only host context, readable at `/$ctx`. */
  context?: Record<string, unknown>;
  /** Issues keyed by JSON Pointer (e.g. backend param_issues). */
  externalIssues?: Record<string, Issue[]>;
  readOnly?: boolean;
  onChange(patches: Patch[]): void;
  onValidate?(result: ValidationResult): void;
  /**
   * Receives engine warnings (dropped writes, overlay timeout, reserved
   * emit, ...). Defaults to `console.warn`.
   */
  onWarning?(warning: Warning): void;
}
