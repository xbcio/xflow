// Component contract targeted by compileNodeForm (C1).
//
// Component TYPE NAMES follow Doc B §5 (engine components from composer/form)
// and Doc C §3 (host components registered by xflow-editor). The PROP SHAPES
// below are C1's proposal, written before composer/form (B4) and the host
// components (C2) exist. C3 reconciles them with the real components: either
// the components accept these props, or compile.ts is adjusted to theirs.
// `NODE_FORM_COMPONENT_PROPS` is the runtime mirror of these interfaces; the
// C1 tests use it to prove every emitted prop is declared here.
//
// Conventions shared by every field component:
// - `value` carries the primary binding (`{$bindState: ptr}` / `{$bindItem: f}`).
// - `label` / `help` / `required` / `defaultHint` are the field chrome. A
//   control placed in an ExpressionInput `literal` slot gets NO chrome: the
//   ExpressionInput renders label, help and issues for both states.
// - `required` is display-only (asterisk); enforcement is the `required` check.
// - `defaultHint` is display-only (Doc B §5, Doc C §5.2); never written.

import type { ExpressionModeName, FieldType } from "./schema";

/** A composer value expression (`$state`, `$bindState`, `$item`, `$bindItem`, `$cond`, …). */
export type Expr = Record<string, unknown>;
export type Bound<T> = T | Expr;

export interface OptionItem {
  value: unknown;
  label: string;
  description?: string;
}

/** Port list item; also the shape of `context.ports` (Doc C §4.3). */
export interface PortItem {
  name: string;
  display_name?: string;
}

/**
 * Runtime shape guard metadata (Doc C §3.2). Honoured by `ShapeGuard` and by
 * template-mode `ExpressionInput`. Given the current value `v`:
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

interface FieldChrome {
  label?: string;
  help?: string;
  required?: Bound<boolean>;
  defaultHint?: unknown;
}

interface ValueProps {
  value: Expr;
}

export interface NodeFormComponentProps {
  // ------------------------------------------------ engine (composer/form, B4)
  Form: { layout: "vertical" | "horizontal" };
  FieldGroup: { title: string; description?: string; collapsed?: boolean; groupKey: string };
  Input: FieldChrome & ValueProps & { placeholder?: string };
  /** `encoding: "base64"` = read-only summary + replace (Doc C §3 `base64`). */
  TextArea: FieldChrome & ValueProps & { encoding?: "base64" };
  Password: FieldChrome & ValueProps;
  InputNumber: FieldChrome & ValueProps;
  Switch: FieldChrome & ValueProps;
  Select: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  Radio: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  MultiSelect: FieldChrome & ValueProps & { options: Bound<OptionItem[]> };
  Tags: FieldChrome & ValueProps;
  /** `valueType` names a registered component used as the value editor (a type ref, Doc B §5). */
  KeyValue: FieldChrome & ValueProps & { valueType?: string };
  ObjectGroup: FieldChrome & ValueProps;
  ArrayTable: FieldChrome & ValueProps & { addText?: string };
  JsonEditor: FieldChrome & ValueProps;
  /** `highlightTemplates: false` for host source (Doc C §4.1 `literal`). */
  CodeEditor: FieldChrome & ValueProps & { language?: string; highlightTemplates?: boolean };

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
    ValueProps & { mode: "template" | "pure"; fxToggle: boolean; expect?: ShapeExpectation };
  /** `unit`: "string" = Go duration text; "ns"/"ms" = integer of that unit on the wire. */
  DurationInput: FieldChrome & ValueProps & { unit: "string" | "ns" | "ms" };
  /** RFC 3339 text. */
  DateTimeInput: FieldChrome & ValueProps;
  CronInput: FieldChrome & ValueProps;
  /** `credentials` reads `context.credentials: string[]` (workflow credential names). */
  CredentialSelect: FieldChrome & ValueProps & { multiple: boolean; credentials: Bound<string[]> };
  /** `ports` is static for fixed-port nodes, `{$state: "/$ctx/ports"}` for dynamic ones. */
  PortSelect: FieldChrome & ValueProps & { ports: Bound<PortItem[]> };
  /**
   * Rename control for `/name` (Doc C §4.2). `value` is a READ (`$state`),
   * not a binding: the component emits no patch and calls the editor's
   * rename callback, closed over at registration, on blur.
   */
  NodeNameInput: { label?: string; help?: string; value: Expr };
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

const CHROME = ["label", "help", "required", "defaultHint"] as const;
const FIELD = [...CHROME, "value"] as const;

/** Runtime mirror of NodeFormComponentProps: the prop keys each type may receive. */
export const NODE_FORM_COMPONENT_PROPS: Readonly<Record<NodeFormComponentType, readonly string[]>> = {
  Form: ["layout"],
  FieldGroup: ["title", "description", "collapsed", "groupKey"],
  Input: [...FIELD, "placeholder"],
  TextArea: [...FIELD, "encoding"],
  Password: FIELD,
  InputNumber: FIELD,
  Switch: FIELD,
  Select: [...FIELD, "options"],
  Radio: [...FIELD, "options"],
  MultiSelect: [...FIELD, "options"],
  Tags: FIELD,
  KeyValue: [...FIELD, "valueType"],
  ObjectGroup: FIELD,
  ArrayTable: [...FIELD, "addText"],
  JsonEditor: FIELD,
  CodeEditor: [...FIELD, "language", "highlightTemplates"],
  ExpressionInput: [...FIELD, "mode", "fxToggle", "expect"],
  DurationInput: [...FIELD, "unit"],
  DateTimeInput: FIELD,
  CronInput: FIELD,
  CredentialSelect: [...FIELD, "multiple", "credentials"],
  PortSelect: [...FIELD, "ports"],
  NodeNameInput: ["label", "help", "value"],
  ShapeGuard: ["label", "observed", "expect", "mode"],
  FormNotice: ["tone", "code", "message", "count"]
};

/** Props that name another component type (validateSpec `typeRefProps`, Doc B §5). */
export const NODE_FORM_TYPE_REF_PROPS: Readonly<Partial<Record<NodeFormComponentType, readonly string[]>>> = {
  KeyValue: ["valueType"]
};

/**
 * Recommended `bindings` value kinds (Doc B §5) for the `value` prop, which
 * drive "clear means unset" and the empty-container collapse (Doc B §6 rules
 * 1–2). Undeclared types are "scalar". JsonEditor and CredentialSelect stay
 * scalar because their value's kind depends on the field; they must emit
 * `undefined` (not `{}` / `[]`) when cleared.
 */
export const NODE_FORM_BINDING_KINDS: Readonly<Partial<Record<NodeFormComponentType, Record<string, "scalar" | "array" | "object">>>> = {
  MultiSelect: { value: "array" },
  Tags: { value: "array" },
  KeyValue: { value: "object" },
  ObjectGroup: { value: "object" },
  ArrayTable: { value: "array" }
};

/** Named slot of ExpressionInput holding the plain control (template mode). */
export const EXPRESSION_LITERAL_SLOT = "literal";
