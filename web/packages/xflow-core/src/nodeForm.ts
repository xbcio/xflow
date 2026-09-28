// NodeFormSchema v1 (Doc C §2): the wire types of GET /v1/node-types. They
// live in @xflow/core so both the API client (@xflow/api, which may depend only
// on core) and the editor (@xflow/editor, which re-exports them from
// node-form/schema.ts) share one definition. Types only; no runtime code.
//
// The Go producer is service/apiserver/node_form.go; the checked-in fixture
// xflow-editor/src/node-form/testdata/node-types.generated.json is generated
// from it and compiled by the editor tests, so a drift fails there.
//
// Wire-format notes (the JSON must round-trip into these types):
// - Optional keys are omitted rather than written as null, with one exception:
//   `Condition.eq` is a clause only when the key is present. `eq: null` is a
//   real clause ("param is unset"), so the DTO omits `eq` when the Go
//   `Condition.Eq` is nil instead of writing `"eq": null`.
// - `Condition.in` / `any_of` are present whenever the Go slice is non-nil,
//   even when empty (a non-nil empty In or AnyOf never holds in Go).
// - Go `types.ParamBool` is spelled "bool"; the schema spells it "boolean".
//   The compiler accepts both (see `normalizeType` in the editor's node-form/compile.ts).
// - Inside an array element (`item.fields`), `path` is relative to the
//   element ("condition"); the compiler binds element fields by name.

export type NodeFormSpecVersion = "node-form/v1";

/** Mirrors Go types.NodeKind as projected (Doc C §2.1). */
export type NodeFormKind = "action" | "trigger" | "supply";

/** Field value types (Doc C §2.2). */
export type FieldType = "string" | "number" | "boolean" | "array" | "object";

/**
 * Result of the backend `graph.ExpressionMode(kind, type, path)` (Doc A §2.3,
 * Doc C §4.1). Served by A6 on every field; `expressionMode.ts` in the editor
 * derives a stand-in when it is absent.
 */
export type ExpressionModeName = "none" | "pure" | "template" | "literal";

/**
 * Widget names (Doc C §3). The list is open: a newer backend may name a widget
 * this frontend does not know, which degrades per Doc C §3.2.
 */
export type KnownWidget =
  | "text"
  | "textarea"
  | "password"
  | "number"
  | "switch"
  | "select"
  | "radio"
  | "multi-select"
  | "tags"
  | "key-value"
  | "key-expression"
  | "object-group"
  | "array-table"
  | "json"
  | "code"
  | "base64"
  | "duration"
  | "datetime"
  | "cron"
  | "expression"
  | "credential-select"
  | "port-select";

// `string & {}` keeps KnownWidget autocompletion while accepting any string.
export type WidgetName = KnownWidget | (string & {});

/**
 * Go types.Condition (Doc A §2.2). All present clauses must hold (implicit
 * AND); a condition with no clauses holds. `param` names a sibling param:
 * another top-level param, or another field of the same `fields` list.
 */
export interface Condition {
  param?: string;
  eq?: unknown;
  in?: unknown[];
  truthy?: boolean;
  all_of?: Condition[];
  any_of?: Condition[];
  not?: Condition;
}

export interface EnumOption {
  value: unknown;
  label?: string;
  description?: string;
}

/** Go types.ConditionalEnum; the first entry whose `when` holds wins. */
export interface ConditionalOptions {
  when: Condition;
  options: EnumOption[];
}

export type FieldRuleType =
  | "required"
  | "min"
  | "max"
  | "min_length"
  | "max_length"
  | "pattern"
  | "format"
  | "min_items"
  | "max_items"
  | "unique_items"
  | "json_parsable";

/**
 * One projected Constraints entry (Doc C §2.2). `value` carries the bound for
 * min/max/min_length/max_length/min_items/max_items, `pattern` an RE2 pattern,
 * `format` a Constraints.Format name. `advisory: true` means the backend does
 * not enforce the rule (Doc C §1 rule 2) and it compiles to a warning.
 */
export interface FieldRule {
  type: FieldRuleType;
  value?: number;
  pattern?: string;
  format?: string;
  advisory?: boolean;
  message?: string;
}

export interface NodeFormField {
  name: string;
  /** JSON Pointer from the WorkflowNode root, e.g. "/parameters/mode". */
  path: string;
  label?: string;
  /** "bool" is accepted as an alias of "boolean" (Go ParamType spelling). */
  type: FieldType | "bool";
  widget?: WidgetName;
  required?: boolean;
  required_when?: Condition | null;
  /** Descriptor.Default; display-only (defaultHint), never written (Doc C §5.2). */
  default?: unknown;
  /** Handler fallback for params without a Default (Doc A §2.4 whitelist); display-only. */
  fallback?: unknown;
  help?: string;
  group?: string;
  order?: number;
  secret?: boolean;
  deprecated?: string | null;
  expression?: { mode: ExpressionModeName } | null;
  rules?: FieldRule[];
  visible_when?: Condition | null;
  options?: EnumOption[] | null;
  options_when?: ConditionalOptions[] | null;
  /** Element schema, only for type=array. `name`/`path` are ignored on items. */
  item?: NodeFormItem | null;
  /** Known sub-fields, only for type=object. Absent/empty means a free map. */
  fields?: NodeFormField[] | null;
}

/** An array element schema: a Field without its own identity. */
export type NodeFormItem = Omit<NodeFormField, "name" | "path"> & { name?: string; path?: string };

export interface NodeFormPort {
  name: string;
  display_name?: string;
}

export interface NodeFormPorts {
  inputs?: NodeFormPort[];
  outputs?: NodeFormPort[];
  /** Switch-like nodes: output ports come from a parameter (Doc C §4.3). */
  dynamic_outputs?: { from: string } | null;
}

export interface NodeFormGroup {
  key: string;
  display_name?: string;
  description?: string;
  collapsed?: boolean;
}

export interface NodeFormOneOf {
  params: string[];
  mode?: "exactly" | "at_most" | "at_least";
}

export interface NodeFormSchema {
  spec: NodeFormSpecVersion;
  node_type: string;
  node_version: number;
  kind: NodeFormKind;
  display_name?: string;
  description?: string;
  docs?: string;
  capabilities?: string[];
  credentials?: string[];
  ports?: NodeFormPorts;
  groups?: NodeFormGroup[];
  one_of?: NodeFormOneOf[];
  fields: NodeFormField[];
}

/** Server-side param validation mode (Doc A §2.4, Doc C §1 rule 3). */
export type ParamValidationMode = "off" | "warn" | "enforce";

/** GET /v1/node-types response (Doc A §2.5). */
export interface NodeTypesResponse {
  param_validation_mode: ParamValidationMode;
  node_types: NodeFormSchema[];
}
