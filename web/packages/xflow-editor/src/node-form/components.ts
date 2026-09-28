// Component contract targeted by compileNodeForm (C1).
//
// Component TYPE NAMES follow Doc B §5 (engine components from composer/form)
// and Doc C §3 (host components registered by xflow-editor, ./components/).
// The PROP SHAPES below are the props compileNodeForm emits, reconciled with
// the real components: composer/form (B4) for the engine types, the C2 host
// components for the rest. Every props schema is strict (an unknown prop is
// an ElementError), so the compile tests resolve every fixture against the
// real registry (`createNodeFormRegistry`) instead of a stub.
//
// Conventions shared by every field component:
// - `value` carries the primary binding (`{$bindState: ptr}` / `{$bindItem: f}`);
//   core strips bindings and `defaultHint` before the props schema sees them.
// - `label` / `description` / `required` / `defaultHint` are the field chrome.
//   A control placed in an ExpressionInput `literal` slot gets only `label`
//   (+ `required`), kept for its accessible name and hidden visually: the
//   ExpressionInput renders label, description and issues for both states.
//   ObjectGroup in the literal slot gets no chrome (its title would repeat).
// - `required` is display-only (marker); enforcement is the `required` check.
// - `defaultHint` is display-only (Doc B §5, Doc C §5.2); never written.

import type { ExpressionModeName, FieldType } from "./schema";

/** A composer value expression (`$state`, `$bindState`, `$item`, `$bindItem`, `$cond`, …). */
export type Expr = Record<string, unknown>;
export type Bound<T> = T | Expr;

/** composer/form option (B4 `optionSchema`): primitive values only. */
export interface OptionItem {
  value: string | number | boolean;
  label: string;
}

/** Port list item; also the shape of `context.ports` (Doc C §4.3). */
export interface PortItem {
  name: string;
  display_name?: string;
}

/**
 * Runtime shape guard metadata (Doc C §3.2). Honoured by `ShapeGuard` and by
 * `ExpressionInput`. Given the current value `v`:
 * - `v === undefined`: render the control normally (unset is always fine).
 * - `v` is a string containing `${{ }}` / `{{ }}` and `type !== "string"`:
 *   template mode → ExpressionInput shows the fx state with the raw text;
 *   other modes → ShapeGuard shows the raw text read-only with
 *   "请在 JSON 页签修改".
 * - `v` does not match `type` (incl. `null`, arrays for objects, non-number
 *   strings for numbers), or an array element does not match `item`, or a
 *   free-map value does not match `values`: show a READ-ONLY JSON view with
 *   the same hint instead of the control.
 * - In every case the guard emits ZERO patches on mount and never coerces.
 */
export interface ShapeExpectation {
  type: FieldType;
  /** Element type for arrays, when known. */
  item?: FieldType;
  /** Value type of a free map (key-value widgets). */
  values?: FieldType;
}

/** Field chrome accepted by every composer/form field and every host field. */
interface FieldChrome {
  label?: string;
  description?: string;
  required?: Bound<boolean>;
  disabled?: boolean;
  defaultHint?: unknown;
}

interface ValueProps {
  value: Expr;
}

export interface NodeFormComponentProps {
  // ------------------------------------------------ engine (composer/form, B4)
  Form: { layout?: "vertical" | "horizontal"; labelWidth?: number; title?: string };
  FieldGroup: { title?: string; description?: string; collapsible?: boolean; defaultCollapsed?: boolean };
  Input: FieldChrome & ValueProps & { placeholder?: string };
  /** Also the `base64` widget (B4 has no base64 summary mode; see compile.ts). */
  TextArea: FieldChrome & ValueProps & { rows?: number };
  Password: FieldChrome & ValueProps;
  InputNumber: FieldChrome & ValueProps;
  Switch: FieldChrome & ValueProps;
  Select: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  Radio: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  MultiSelect: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  Tags: FieldChrome & ValueProps;
  /**
   * `valueType` names a registered component used as the value editor (a
   * type ref, Doc B §5); `valueProps` are that component's props.
   */
  KeyValue: FieldChrome & ValueProps & { valueType?: string; valueProps?: Record<string, unknown> };
  /** No `required` / `disabled` in B4; title comes from `label`. */
  ObjectGroup: { label?: string; description?: string; value: Expr; defaultHint?: unknown };
  ArrayTable: FieldChrome & ValueProps & { addText?: string };
  JsonEditor: FieldChrome & ValueProps;
  /** B4 has no highlighting, so `literal` mode needs no `highlightTemplates` flag. */
  CodeEditor: FieldChrome & ValueProps & { language?: string };

  // ------------------------------------------------------ host (xflow-editor, C2)
  /**
   * `mode: "template"` + `fxToggle: true`: literal state renders the
   * `literal` slot (the plain control, bound to the same path); expression
   * state renders the expression editor. The fx toggle flips the displayed
   * state only; it writes nothing until the user edits. A value containing a
   * template opens in the expression state.
   * `mode: "pure"`: always an expression editor, no toggle, no slot.
   */
  ExpressionInput: FieldChrome &
    ValueProps & { mode: "template" | "pure"; fxToggle?: boolean; expect?: ShapeExpectation; placeholder?: string };
  /** `unit`: "string" = Go duration text; "ns"/"ms" = integer of that unit on the wire. */
  DurationInput: FieldChrome & ValueProps & { unit: "string" | "ns" | "ms" };
  /** RFC 3339 text. */
  DateTimeInput: FieldChrome & ValueProps;
  CronInput: FieldChrome & ValueProps;
  /** `credentials` reads `context.credentials: string[]` (workflow credential names). */
  CredentialSelect: FieldChrome & ValueProps & { multiple: boolean; credentials?: Bound<string[]> };
  /** `ports` is static for fixed-port nodes, `{$state: "/$ctx/ports"}` for dynamic ones. */
  PortSelect: FieldChrome & ValueProps & { ports?: Bound<PortItem[]> };
  /**
   * Rename control for `/name` (Doc C §4.2). `value` is a READ (`$state`),
   * not a binding: the component emits no patch and calls the editor's
   * rename callback, closed over at registration, on blur.
   */
  NodeNameInput: { label?: string; description?: string; value: Expr };
  /** Wraps one non-template field; `observed` is a read of the guarded value. See ShapeExpectation. */
  ShapeGuard: { label?: string; observed: Expr; expect: ShapeExpectation; mode: ExpressionModeName };
  /** Static banner at the top of the form. */
  FormNotice: { tone: "info" | "warning"; code: NoticeCode; message: string; count?: number };
}

export type NoticeCode = "unregistered-type" | "undeclared-params" | "node-template";

export type NodeFormComponentType = keyof NodeFormComponentProps;

export const ENGINE_COMPONENT_TYPES = [
  "Form",
  "FieldGroup",
  "Input",
  "TextArea",
  "Password",
  "InputNumber",
  "Switch",
  "Select",
  "Radio",
  "MultiSelect",
  "Tags",
  "KeyValue",
  "ObjectGroup",
  "ArrayTable",
  "JsonEditor",
  "CodeEditor"
] as const satisfies readonly NodeFormComponentType[];

export const HOST_COMPONENT_TYPES = [
  "ExpressionInput",
  "DurationInput",
  "DateTimeInput",
  "CronInput",
  "CredentialSelect",
  "PortSelect",
  "NodeNameInput",
  "ShapeGuard",
  "FormNotice"
] as const satisfies readonly NodeFormComponentType[];

export const NODE_FORM_COMPONENT_TYPES: readonly NodeFormComponentType[] = [
  ...ENGINE_COMPONENT_TYPES,
  ...HOST_COMPONENT_TYPES
];

/** Named slot of ExpressionInput holding the plain control (template mode). */
export const EXPRESSION_LITERAL_SLOT = "literal";
