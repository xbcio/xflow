// compileNodeForm (Doc C, deliverable C1): NodeFormSchema → composer/v1 Spec.
//
// Pure TypeScript, no React. The output binds JSON Pointers from the
// WorkflowNode root, so the host mounts it as <Composer value={node}>.
// Component names and props are the contract in ./components.ts; host check
// types are in ./checks.ts and must be registered by the host.

import type { CheckDef, ComposerSpec, Cond, ElementDef } from "@xflow/composer/core";
import {
  EXPRESSION_LITERAL_SLOT,
  NODE_FORM_COMPONENT_TYPES,
  type Expr,
  type NodeFormComponentType,
  type NoticeCode,
  type OptionItem,
  type PortItem,
  type ShapeExpectation
} from "./components";
import {
  COMMON_WIDGET_DURATION_MS,
  COMMON_WIDGET_DURATION_NS,
  COMMON_WIDGET_NODE_NAME,
  commonSchema
} from "./commonSchema";
import { deriveExpressionMode } from "./expressionMode";
import type {
  Condition,
  EnumOption,
  ExpressionModeName,
  FieldRule,
  FieldType,
  NodeFormField,
  NodeFormItem,
  NodeFormSchema
} from "./schema";

export interface CompileNodeFormOptions {
  /** Common fields placed before the type schema (Doc C §4.2). Defaults to `commonSchema`; `null` omits them. */
  common?: NodeFormSchema | null;
  /** The node's current `parameters`, used only to count undeclared keys (Doc C §3.2). */
  parameters?: unknown;
  /** The node has `template`, or the workflow has `node_templates` (Doc C §5.3). */
  hasTemplate?: boolean;
  /**
   * Component types the host registry provides. Defaults to every type in
   * components.ts. A widget whose component is missing degrades (Doc C §3.2).
   */
  registeredTypes?: Iterable<string>;
}

export interface CompiledNodeForm {
  spec: ComposerSpec;
  warnings: string[];
}

export const ROOT_ID = "form";

/** Parameters rendered with JsonEditor on purpose in v1 (Doc C §3.1). */
export const JSON_EDITOR_PARAMS: Readonly<Record<string, readonly string[]>> = {
  "xflow.http": ["body", "options"],
  "xflow.grpc": ["request", "metadata", "options"],
  "xflow.database": ["where", "data"],
  "xflow.notification": ["data"],
  "xflow.function": ["params"],
  "xflow.browser.cdp": ["seed_cookies", "ttl", "plan", "harvest"],
  "xflow.transform.set": ["fields"],
  "xflow.map": ["body"]
};

const WIDGET_COMPONENTS: Readonly<Record<string, NodeFormComponentType>> = {
  text: "Input",
  textarea: "TextArea",
  password: "Password",
  number: "InputNumber",
  switch: "Switch",
  select: "Select",
  radio: "Radio",
  "multi-select": "MultiSelect",
  tags: "Tags",
  "key-value": "KeyValue",
  "key-expression": "KeyValue",
  "object-group": "ObjectGroup",
  "array-table": "ArrayTable",
  json: "JsonEditor",
  code: "CodeEditor",
  base64: "TextArea",
  duration: "DurationInput",
  datetime: "DateTimeInput",
  cron: "CronInput",
  expression: "ExpressionInput",
  "credential-select": "CredentialSelect",
  "port-select": "PortSelect",
  [COMMON_WIDGET_NODE_NAME]: "NodeNameInput",
  [COMMON_WIDGET_DURATION_NS]: "DurationInput",
  [COMMON_WIDGET_DURATION_MS]: "DurationInput"
};

const MSG = {
  required: "必填",
  requiredWhen: "当前配置下必填",
  notEvaluated: "此参数不会被求值，其中的 ${{ }} 会按原文使用",
  noTemplateInCode: "代码中的 ${{ }} 不会被渲染",
  secretLike: "值看起来像凭据，请改用凭据引用",
  unregistered: "该节点类型未在 server 注册，参数按 JSON 编辑",
  template: "节点模板（template / node_templates）目前不会被引擎解析"
} as const;

const FORMAT_MESSAGES: Readonly<Record<string, string>> = {
  duration: "需要 Go duration，例如 30s、5m",
  cron: "需要合法的 cron 表达式",
  expression: "需要表达式，例如 ${{ $input.x }}",
  "sha256-digest": "需要 sha256:<64 位十六进制>",
  url: "需要完整的 URL",
  "host-port": "需要 host:port",
  json: "需要合法 JSON"
};

/** Formats the backend does not enforce (Doc A §2.2); always warnings. */
const ADVISORY_FORMATS: ReadonlySet<string> = new Set(["url", "host-port", "code"]);
/** Formats composer core can check (core/formats.ts). */
const CORE_FORMATS: ReadonlySet<string> = new Set(["duration", "cron", "json", "expression", "sha256-digest", "url", "host-port"]);

// ------------------------------------------------------------------ scopes

/** Where sibling params live: an absolute object pointer, or a repeat row. */
type Scope =
  | { kind: "absolute"; base: string; segs: readonly string[]; idBase: string }
  | { kind: "row"; prefix: string; segs: readonly string[]; idBase: string };

function escapeSegment(segment: string): string {
  return segment.replace(/~/g, "~0").replace(/\//g, "~1");
}

function joinPointer(base: string, segment: string): string {
  return `${base}/${escapeSegment(segment)}`;
}

function pointerSegments(pointer: string): string[] {
  return pointer
    .split("/")
    .slice(1)
    .map((segment) => segment.replace(/~1/g, "/").replace(/~0/g, "~"));
}

function rowField(scope: Extract<Scope, { kind: "row" }>, name: string): string {
  return scope.prefix === "" ? name : `${scope.prefix}/${escapeSegment(name)}`;
}

function fieldPointer(field: { name?: string; path?: string }, scope: Extract<Scope, { kind: "absolute" }>): string {
  if (field.path && field.path.startsWith(`${scope.base}/`)) return field.path;
  return joinPointer(scope.base, field.name ?? "");
}

function readExpr(field: NodeFormField, scope: Scope): Expr {
  return scope.kind === "absolute" ? { $state: fieldPointer(field, scope) } : { $item: rowField(scope, field.name) };
}

function bindExpr(field: NodeFormField, scope: Scope): Expr {
  return scope.kind === "absolute" ? { $bindState: fieldPointer(field, scope) } : { $bindItem: rowField(scope, field.name) };
}

function siblingRef(scope: Scope, param: string): { $state: string } | { $item: string } {
  return scope.kind === "absolute" ? { $state: joinPointer(scope.base, param) } : { $item: rowField(scope, param) };
}

// -------------------------------------------------------------- conditions

function compileCondition(cond: Condition, scope: Scope, warn: (message: string) => void): Cond {
  const parts: Cond[] = [];
  const hasLeafClause = (cond.eq !== undefined) || cond.in !== undefined || cond.truthy !== undefined;
  if (cond.param !== undefined && cond.param !== "") {
    const leaf = siblingRef(scope, cond.param);
    // `eq: null` is a real clause (Doc B §3.3: missing compares as null).
    if (cond.eq !== undefined) parts.push({ ...leaf, eq: cond.eq } as Cond);
    if (cond.in !== undefined) parts.push({ ...leaf, in: cond.in } as Cond);
    if (cond.truthy !== undefined) parts.push({ ...leaf, truthy: cond.truthy } as Cond);
  } else if (hasLeafClause) {
    warn("condition has eq/in/truthy but no param; clause ignored");
  }
  if (cond.all_of && cond.all_of.length > 0) parts.push({ $and: cond.all_of.map((c) => compileCondition(c, scope, warn)) });
  if (cond.any_of && cond.any_of.length > 0) parts.push({ $or: cond.any_of.map((c) => compileCondition(c, scope, warn)) });
  if (cond.not) parts.push({ $not: compileCondition(cond.not, scope, warn) });
  if (parts.length === 0) return true; // Go: a Condition with no clauses holds.
  return parts.length === 1 ? parts[0] : { $and: parts };
}

/** Evaluates a condition that has no state leaves; undefined when it depends on values. */
function staticValue(cond: Cond): boolean | undefined {
  if (typeof cond === "boolean") return cond;
  const all = (list: Cond[]): boolean | undefined => {
    const values = list.map(staticValue);
    if (values.some((v) => v === false)) return false;
    return values.every((v) => v === true) ? true : undefined;
  };
  if (Array.isArray(cond)) return all(cond);
  if ("$and" in cond) return all(cond.$and);
  if ("$or" in cond) {
    const values = cond.$or.map(staticValue);
    if (values.some((v) => v === true)) return true;
    return values.every((v) => v === false) ? false : undefined;
  }
  if ("$not" in cond) {
    const inner = staticValue(cond.$not);
    return inner === undefined ? undefined : !inner;
  }
  return undefined;
}

// ----------------------------------------------------------------- helpers

function normalizeType(type: string, warn: (message: string) => void): FieldType {
  if (type === "bool") return "boolean";
  if (type === "string" || type === "number" || type === "boolean" || type === "array" || type === "object") return type;
  warn(`unknown field type ${JSON.stringify(type)}; treated as string`);
  return "string";
}

function humanize(name: string): string {
  return name.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());
}

function toOptions(options: readonly EnumOption[]): OptionItem[] {
  return options.map((option) => {
    const item: OptionItem = { value: option.value, label: option.label ?? String(option.value) };
    if (option.description) item.description = option.description;
    return item;
  });
}

function sortFields<T extends { order?: number }>(fields: readonly T[]): T[] {
  return fields
    .map((field, index) => ({ field, index }))
    .sort((a, b) => (a.field.order ?? 0) - (b.field.order ?? 0) || a.index - b.index)
    .map(({ field }) => field);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function sanitizeId(id: string): string {
  return id.replace(/[@#]/g, "_");
}

// ---------------------------------------------------------------- compiler

interface Target {
  kind: NodeFormSchema["kind"];
  nodeType: string;
  ports: PortItem[] | Expr;
}

class Compiler {
  readonly elements: Record<string, ElementDef> = {};
  readonly warnings: string[] = [];
  private readonly registered: ReadonlySet<string>;

  constructor(registered: ReadonlySet<string>) {
    this.registered = registered;
  }

  has(type: string): boolean {
    return this.registered.has(type);
  }

  warn(message: string): void {
    this.warnings.push(message);
  }

  add(base: string, element: ElementDef): string {
    let id = sanitizeId(base);
    for (let n = 2; Object.prototype.hasOwnProperty.call(this.elements, id); n++) id = sanitizeId(`${base}~${n}`);
    this.elements[id] = element;
    return id;
  }

  /** Compiles one field; returns the ids its parent lists as children (empty when statically hidden). */
  field(field: NodeFormField, scope: Scope, target: Target): string[] {
    const where = `${target.nodeType}:${scope.kind === "absolute" ? fieldPointer(field, scope) : `${scope.idBase}[].${field.name}`}`;
    const warn = (message: string) => this.warn(`${where}: ${message}`);
    const type = normalizeType(field.type, warn);
    const visible = field.visible_when ? compileCondition(field.visible_when, scope, warn) : undefined;
    if (visible !== undefined && staticValue(visible) === false) return [];

    const segs = scope.kind === "absolute" ? pointerSegments(fieldPointer(field, scope)) : [...scope.segs, field.name];
    const mode: ExpressionModeName =
      field.expression?.mode ?? (segs[0] === "parameters" ? deriveExpressionMode(target.kind, target.nodeType, segs.slice(1)) : "none");
    const idBase = scope.kind === "absolute" ? `f.${segs.join(".")}` : `${scope.idBase}.${field.name}`;

    const chrome = this.chrome(field, scope, warn);
    const hint = field.secret ? undefined : (field.default ?? field.fallback ?? undefined);
    const expect = this.expectation(field, type);
    const checks = this.checks(field, type, mode, scope, warn);

    // Doc C §3 note: script.code's widget follows `language`. ParamSpec has a
    // single Widget, so v1 special-cases it here: two elements bound to the
    // same path, switched by `visible` (0 patches on toggle, Doc B §7.3).
    if (target.nodeType === "xflow.script" && field.name === "code" && scope.kind === "absolute" && scope.base === "/parameters") {
      return this.scriptCode(field, scope, { idBase, chrome, hint, expect, checks, visible, mode, target });
    }

    let control = this.control(field, type, scope, target, idBase, warn);
    const controlChecks = [...checks];
    if (control.type === "KeyValue") controlChecks.push({ type: "secretLike", severity: "warning", message: MSG.secretLike });

    if (control.type === "NodeNameInput") {
      const { label, help } = chrome;
      control.props = { ...control.props, ...(label !== undefined && { label }), ...(help !== undefined && { help }) };
      return [this.add(idBase, withVisible(control, visible))];
    }

    // ---- expression mode (Doc C §4.1)
    if (mode === "pure") {
      const freeMap = type === "object" && control.type === "KeyValue";
      if (freeMap) {
        if (this.has("ExpressionInput")) control.props = { ...control.props, valueType: "ExpressionInput" };
        else warn("ExpressionInput is not registered; key-expression values edited as plain text");
      } else if (this.has("ExpressionInput")) {
        control = { type: "ExpressionInput", props: { value: bindExpr(field, scope), mode: "pure", fxToggle: false, expect } };
      } else {
        warn("ExpressionInput is not registered; pure expression edited as plain text");
      }
    } else if (mode === "literal") {
      const isCode = control.type === "CodeEditor" || (control.type === "TextArea" && control.props?.encoding === "base64");
      if (!isCode) control = this.degradable({ type: "CodeEditor", props: { value: bindExpr(field, scope) } }, type, field, scope, warn);
      if (control.type === "CodeEditor") control.props = { ...control.props, highlightTemplates: false };
      controlChecks.push({ type: "noTemplateInCode", severity: "warning", message: MSG.noTemplateInCode });
    } else if (mode === "none") {
      if (control.type === "ExpressionInput") {
        warn("expression widget on a param that is never evaluated; rendered as Input");
        control = this.degradable({ type: "Input", props: { value: bindExpr(field, scope) } }, type, field, scope, warn);
      }
      // Doc C §4.1 warns about templates in params; common node fields
      // (/notes, /timeout, …) are configuration, not params, so no warning.
      if (segs[0] === "parameters") controlChecks.push({ type: "notEvaluated", severity: "warning", message: MSG.notEvaluated });
    }

    if (control.type === "ExpressionInput") {
      // pure mode, or an `expression` widget in template mode: a single element.
      control.props = { ...control.props, ...chrome, ...(hint !== undefined && { defaultHint: hint }), mode: mode === "pure" ? "pure" : "template", fxToggle: false, expect };
      return [this.add(idBase, withVisible(withChecks(control, controlChecks), visible))];
    }

    if (mode === "template" && this.has("ExpressionInput")) {
      // The plain control sits in ExpressionInput's literal slot, bound to the
      // same path; ExpressionInput owns chrome, checks and the fx toggle.
      control.props = { ...control.props, ...(hint !== undefined && { defaultHint: hint }) };
      const literalId = this.add(`${idBase}:control`, control);
      const outer: ElementDef = {
        type: "ExpressionInput",
        props: { ...chrome, value: bindExpr(field, scope), mode: "template", fxToggle: true, expect },
        slots: { [EXPRESSION_LITERAL_SLOT]: [literalId] }
      };
      return [this.add(idBase, withVisible(withChecks(outer, controlChecks), visible))];
    }
    if (mode === "template") warn("ExpressionInput is not registered; template field has no fx toggle");

    control.props = { ...control.props, ...chrome, ...(hint !== undefined && { defaultHint: hint }) };
    return [this.guard(field, scope, idBase, withChecks(control, controlChecks), { expect, mode, visible, label: chrome.label })];
  }

  private scriptCode(
    field: NodeFormField,
    scope: Extract<Scope, { kind: "absolute" }>,
    c: {
      idBase: string;
      chrome: Record<string, unknown>;
      hint: unknown;
      expect: ShapeExpectation;
      checks: CheckDef[];
      visible: Cond | undefined;
      mode: ExpressionModeName;
      target: Target;
    }
  ): string[] {
    const language = joinPointer(scope.base, "language");
    const checks = [...c.checks];
    if (c.mode === "literal") checks.push({ type: "noTemplateInCode", severity: "warning", message: MSG.noTemplateInCode });
    const common = { ...c.chrome, ...(c.hint !== undefined && { defaultHint: c.hint }), value: bindExpr(field, scope) };
    const code: ElementDef = {
      type: "CodeEditor",
      props: { ...common, language: "javascript", highlightTemplates: false },
      visible: { $state: language, neq: "wasm" },
      ...(checks.length > 0 && { checks })
    };
    const base64: ElementDef = {
      type: "TextArea",
      props: { ...common, encoding: "base64" },
      visible: { $state: language, eq: "wasm" },
      ...(checks.length > 0 && { checks: checks.map((check) => ({ ...check })) })
    };
    for (const element of [code, base64]) {
      if (!this.has(element.type)) this.warn(`${c.target.nodeType}:${field.path}: ${element.type} is not registered`);
    }
    const ids = [this.add(`${c.idBase}:code`, code), this.add(`${c.idBase}:base64`, base64)];
    if (!this.has("ShapeGuard")) {
      if (c.visible !== undefined) {
        for (const id of ids) this.elements[id].visible = [c.visible, this.elements[id].visible as Cond];
      }
      return ids;
    }
    const guard: ElementDef = {
      type: "ShapeGuard",
      props: { ...(c.chrome.label !== undefined && { label: c.chrome.label }), observed: readExpr(field, scope), expect: c.expect, mode: c.mode },
      children: ids
    };
    return [this.add(c.idBase, withVisible(guard, c.visible))];
  }

  private guard(
    field: NodeFormField,
    scope: Scope,
    idBase: string,
    control: ElementDef,
    g: { expect: ShapeExpectation; mode: ExpressionModeName; visible: Cond | undefined; label: unknown }
  ): string {
    if (!this.has("ShapeGuard")) return this.add(idBase, withVisible(control, g.visible));
    const controlId = this.add(`${idBase}:control`, control);
    const guard: ElementDef = {
      type: "ShapeGuard",
      props: { ...(g.label !== undefined && { label: g.label }), observed: readExpr(field, scope), expect: g.expect, mode: g.mode },
      children: [controlId]
    };
    return this.add(idBase, withVisible(guard, g.visible));
  }

  private chrome(field: NodeFormField, scope: Scope, warn: (message: string) => void): Record<string, unknown> {
    const out: Record<string, unknown> = { label: field.label || humanize(field.name) };
    const help = [field.deprecated ? `已弃用：${field.deprecated}` : undefined, field.help || undefined].filter(Boolean).join("\n");
    if (help) out.help = help;
    if (field.required) out.required = true;
    else if (field.required_when) {
      out.required = { $cond: compileCondition(field.required_when, scope, warn), then: true, else: false };
    }
    return out;
  }

  private expectation(field: NodeFormField | NodeFormItem, type: FieldType): ShapeExpectation {
    const expect: ShapeExpectation = { type };
    if (type === "array" && field.item) expect.item = normalizeType(field.item.type, () => undefined);
    if (type === "object" && (field.widget === "key-value" || field.widget === "key-expression")) expect.values = "string";
    return expect;
  }

  private checks(field: NodeFormField, type: FieldType, mode: ExpressionModeName, scope: Scope, warn: (m: string) => void): CheckDef[] {
    const checks: CheckDef[] = [];
    let required = false;
    if (field.required) {
      checks.push({ type: "required", message: MSG.required });
      required = true;
    } else if (field.required_when) {
      checks.push({ type: "required", when: compileCondition(field.required_when, scope, warn), message: MSG.requiredWhen });
      required = true;
    }
    for (const rule of field.rules ?? []) {
      if (rule.type === "required") {
        if (!required) checks.push({ type: "required", message: rule.message ?? MSG.required, ...(rule.advisory && { severity: "warning" as const }) });
        required = true;
        continue;
      }
      const compiled = compileRule(rule, type, warn);
      if (!compiled) continue;
      if (mode !== "template") {
        checks.push(compiled);
      } else if (compiled.type === "format") {
        // A template string never has the target shape; the backend skips
        // shape checks on expressions (Doc A §2.4), so allow them outright.
        checks.push({ ...compiled, args: { ...compiled.args, allowExpression: true } });
      } else {
        // Doc C §4.1: once switched to an expression, only `required` stays;
        // other rules degrade to warnings.
        const wrapped = { check: compiled.type, args: compiled.args ?? {} };
        checks.push({ type: "literalOnly", args: wrapped, message: compiled.message, ...(compiled.severity && { severity: compiled.severity }) });
        checks.push({ type: "expressionOnly", args: wrapped, message: compiled.message, severity: "warning" });
      }
    }
    return checks;
  }

  /** Picks the control for a field (Doc C §3), before expression-mode wrapping. */
  private control(
    field: NodeFormField,
    type: FieldType,
    scope: Scope,
    target: Target,
    idBase: string,
    warn: (message: string) => void
  ): ElementDef {
    const value = bindExpr(field, scope);
    let widget = field.widget;
    if (widget !== undefined && !Object.prototype.hasOwnProperty.call(WIDGET_COMPONENTS, widget)) {
      warn(`unknown widget ${JSON.stringify(widget)}; degraded by type`);
      return this.fallback(type, value);
    }
    if (field.secret && type === "string") widget = "password"; // Doc C §4.5
    widget ??= inferWidget(field, type);

    const componentType = WIDGET_COMPONENTS[widget];
    const props: Record<string, unknown> = { value };
    const element: ElementDef = { type: componentType, props };

    switch (widget) {
      case "select":
      case "radio":
      case "multi-select": {
        const options = this.optionsProp(field, scope, warn);
        if (options === undefined) {
          warn(`${widget} widget without options; degraded by type`);
          return this.fallback(type, value);
        }
        props.options = options;
        break;
      }
      case "base64":
        props.encoding = "base64";
        break;
      case "key-expression":
        if (this.has("ExpressionInput")) props.valueType = "ExpressionInput";
        break;
      case "duration":
        props.unit = "string";
        break;
      case COMMON_WIDGET_DURATION_NS:
        props.unit = "ns";
        break;
      case COMMON_WIDGET_DURATION_MS:
        props.unit = "ms";
        break;
      case COMMON_WIDGET_NODE_NAME:
        // Read, not bind: renaming is not a patch (Doc C §4.2).
        props.value = readExpr(field, scope);
        break;
      case "credential-select":
        props.multiple = type === "array";
        props.credentials = { $state: "/$ctx/credentials" };
        break;
      case "port-select":
        props.ports = target.ports;
        break;
      case "object-group": {
        const fields = field.fields ?? [];
        if (type !== "object" || fields.length === 0) {
          warn("object-group widget without known fields; degraded by type");
          return this.fallback(type, value);
        }
        const child: Scope =
          scope.kind === "absolute"
            ? { kind: "absolute", base: fieldPointer(field, scope), segs: [], idBase }
            : { kind: "row", prefix: rowField(scope, field.name), segs: [...scope.segs, field.name], idBase };
        element.children = sortFields(fields).flatMap((sub) => this.field(sub, child, target));
        break;
      }
      case "array-table": {
        const fields = field.item?.fields ?? [];
        if (type !== "array" || fields.length === 0) {
          warn("array-table widget without item fields; degraded by type");
          return this.fallback(type, value);
        }
        props.addText = "添加";
        element.repeat = { statePath: scope.kind === "absolute" ? fieldPointer(field, scope) : { $item: rowField(scope, field.name) } };
        const parentSegs = scope.kind === "absolute" ? pointerSegments(fieldPointer(field, scope)) : [...scope.segs, field.name];
        const row: Scope = { kind: "row", prefix: "", segs: [...parentSegs, "*"], idBase: `${idBase}[]` };
        element.children = sortFields(fields).flatMap((sub) => this.field(sub, row, target));
        break;
      }
      default:
        break;
    }
    return this.degradable(element, type, field, scope, warn);
  }

  /** Doc C §3.2 compile-time degrade: an unregistered component falls back by type. */
  private degradable(element: ElementDef, type: FieldType, field: NodeFormField, scope: Scope, warn: (m: string) => void): ElementDef {
    if (this.has(element.type)) return element;
    warn(`component ${element.type} is not registered; degraded by type`);
    this.dropChildren(element);
    return this.fallback(type, bindExpr(field, scope));
  }

  private dropChildren(element: ElementDef): void {
    for (const id of element.children ?? []) {
      const child = this.elements[id];
      delete this.elements[id];
      if (child) this.dropChildren(child);
    }
    for (const list of Object.values(element.slots ?? {})) {
      for (const id of list) {
        const child = this.elements[id];
        delete this.elements[id];
        if (child) this.dropChildren(child);
      }
    }
  }

  private fallback(type: FieldType, value: Expr): ElementDef {
    const fallbackType = type === "object" || type === "array" ? "JsonEditor" : "Input";
    if (!this.has(fallbackType)) this.warn(`fallback component ${fallbackType} is not registered`);
    return { type: fallbackType, props: { value } };
  }

  private optionsProp(field: NodeFormField, scope: Scope, warn: (m: string) => void): OptionItem[] | Expr | undefined {
    const base = field.options ?? field.item?.options ?? undefined;
    const when = field.options_when ?? [];
    if (!base && when.length === 0) return undefined;
    // EnumWhen: the first matching entry wins; none matching falls back to Enum.
    let out: OptionItem[] | Expr = toOptions(base ?? []);
    for (const entry of [...when].reverse()) {
      out = { $cond: compileCondition(entry.when, scope, warn), then: toOptions(entry.options), else: out };
    }
    return out;
  }

  /** Compiles a schema's fields into FieldGroup elements; returns their ids. */
  groups(schema: NodeFormSchema, prefix: string, implicitTitle: string): { ids: string[]; hidden: Set<string> } {
    const target: Target = { kind: schema.kind, nodeType: schema.node_type, ports: portsFor(schema) };
    const declared = schema.groups ?? [];
    const known = new Set(declared.map((group) => group.key));
    const buckets = new Map<string, NodeFormField[]>([["", []], ...declared.map((group) => [group.key, []] as [string, NodeFormField[]])]);
    for (const field of schema.fields) {
      let key = field.group ?? "";
      if (key !== "" && !known.has(key)) {
        this.warn(`${schema.node_type}:${field.path}: unknown group ${JSON.stringify(key)}; placed in the default group`);
        key = "";
      }
      buckets.get(key)?.push(field);
    }
    const scope: Scope = { kind: "absolute", base: "/parameters", segs: [], idBase: "f" };
    const hidden = new Set<string>();
    const ids: string[] = [];
    const emit = (key: string, title: string, fields: NodeFormField[], extra: { description?: string; collapsed?: boolean }) => {
      const children: string[] = [];
      for (const field of sortFields(fields)) {
        const fieldScope: Scope = field.path.startsWith("/parameters/") ? scope : { kind: "absolute", base: "", segs: [], idBase: "f" };
        const out = this.field(field, fieldScope, target);
        if (out.length === 0) hidden.add(field.name);
        children.push(...out);
      }
      if (children.length === 0) return;
      const props: Record<string, unknown> = { title, groupKey: key };
      if (extra.description) props.description = extra.description;
      if (extra.collapsed) props.collapsed = true;
      ids.push(this.add(`g.${prefix}.${key || "default"}`, { type: "FieldGroup", props, children }));
    };
    emit("", implicitTitle, buckets.get("") ?? [], {});
    for (const group of declared) {
      emit(group.key, group.display_name || group.key, buckets.get(group.key) ?? [], {
        description: group.description,
        collapsed: group.collapsed
      });
    }
    return { ids, hidden };
  }

  notice(code: NoticeCode, tone: "info" | "warning", message: string, count?: number): string {
    return this.add(`n.${code}`, { type: "FormNotice", props: { tone, code, message, ...(count !== undefined && { count }) } });
  }
}

function withVisible(element: ElementDef, visible: Cond | undefined): ElementDef {
  return visible === undefined || visible === true ? element : { ...element, visible };
}

function withChecks(element: ElementDef, checks: CheckDef[]): ElementDef {
  return checks.length === 0 ? element : { ...element, checks: [...(element.checks ?? []), ...checks] };
}

function inferWidget(field: NodeFormField, type: FieldType): string {
  const hasOptions = (field.options?.length ?? 0) > 0 || (field.options_when?.length ?? 0) > 0;
  switch (type) {
    case "boolean":
      return "switch";
    case "number":
      return hasOptions ? "select" : "number";
    case "string":
      return hasOptions ? "select" : "text";
    case "array": {
      const item = field.item;
      if (hasOptions || (item?.options?.length ?? 0) > 0) return "multi-select";
      if ((item?.fields?.length ?? 0) > 0) return "array-table";
      if (item && (item.type === "string" || item.type === undefined)) return "tags";
      return "json";
    }
    case "object":
      return (field.fields?.length ?? 0) > 0 ? "object-group" : "json";
  }
}

function portsFor(schema: NodeFormSchema): PortItem[] | Expr {
  if (schema.ports?.dynamic_outputs) return { $state: "/$ctx/ports" };
  return (schema.ports?.outputs ?? []).map((port) => ({ name: port.name, ...(port.display_name && { display_name: port.display_name }) }));
}

const RULE_CHECKS: Readonly<Record<string, string>> = {
  min: "min",
  max: "max",
  min_length: "minLength",
  max_length: "maxLength",
  pattern: "pattern",
  format: "format",
  min_items: "minItems",
  max_items: "maxItems",
  unique_items: "uniqueItems",
  json_parsable: "json"
};

function compileRule(rule: FieldRule, _type: FieldType, warn: (message: string) => void): CheckDef | undefined {
  const checkType = RULE_CHECKS[rule.type];
  if (!checkType) {
    warn(`unknown rule ${JSON.stringify(rule.type)}; skipped`);
    return undefined;
  }
  let advisory = rule.advisory === true || rule.type === "json_parsable"; // json_parsable has no backend check (Doc C §2.2)
  let args: Record<string, unknown> | undefined;
  let message: string;
  switch (rule.type) {
    case "min":
    case "max":
    case "min_length":
    case "max_length":
    case "min_items":
    case "max_items": {
      if (typeof rule.value !== "number") {
        warn(`rule ${rule.type} without a numeric value; skipped`);
        return undefined;
      }
      args = { value: rule.value };
      message =
        {
          min: `不能小于 ${rule.value}`,
          max: `不能大于 ${rule.value}`,
          min_length: `长度不能少于 ${rule.value}`,
          max_length: `长度不能超过 ${rule.value}`,
          min_items: `至少 ${rule.value} 项`,
          max_items: `最多 ${rule.value} 项`
        }[rule.type];
      break;
    }
    case "pattern":
      if (typeof rule.pattern !== "string") {
        warn("pattern rule without a pattern; skipped");
        return undefined;
      }
      args = { pattern: rule.pattern };
      message = `需要匹配 ${rule.pattern}`;
      break;
    case "format": {
      const format = rule.format ?? "";
      if (ADVISORY_FORMATS.has(format)) advisory = true;
      if (!CORE_FORMATS.has(format)) {
        warn(`format ${JSON.stringify(format)} has no frontend check; skipped`);
        return undefined;
      }
      args = { format };
      message = FORMAT_MESSAGES[format] ?? `格式应为 ${format}`;
      break;
    }
    case "unique_items":
      message = "不能有重复项";
      break;
    case "json_parsable":
      message = FORMAT_MESSAGES.json;
      break;
    default:
      return undefined;
  }
  return {
    type: checkType,
    ...(args && { args }),
    message: rule.message ?? message,
    ...(advisory && { severity: "warning" as const })
  };
}

function oneOfMessage(labels: string[], mode: string): string {
  const list = labels.join("、");
  if (mode === "at_most") return `${list} 最多只能设置一个`;
  if (mode === "at_least") return `${list} 至少需要设置一个`;
  return `${list} 必须且只能设置一个`;
}

/**
 * Compiles a NodeFormSchema (or `null` for a type the server does not know,
 * Doc C §2.1) into a composer/v1 Spec rooted at the WorkflowNode.
 */
export function compileNodeForm(schema: NodeFormSchema | null | undefined, options: CompileNodeFormOptions = {}): CompiledNodeForm {
  const registered = new Set<string>(options.registeredTypes ?? NODE_FORM_COMPONENT_TYPES);
  const compiler = new Compiler(registered);
  if (!registered.has("Form") || !registered.has("FieldGroup")) compiler.warn("Form / FieldGroup are not registered");
  const children: string[] = [];

  if (!schema) {
    if (registered.has("FormNotice")) children.push(compiler.notice("unregistered-type", "warning", MSG.unregistered));
  } else {
    if (schema.spec !== "node-form/v1") compiler.warn(`${schema.node_type}: unexpected schema spec ${JSON.stringify(schema.spec)}`);
    if (options.hasTemplate && registered.has("FormNotice")) children.push(compiler.notice("node-template", "warning", MSG.template));
    if (isPlainObject(options.parameters) && registered.has("FormNotice")) {
      const declared = new Set(schema.fields.map((field) => field.name));
      const undeclared = Object.keys(options.parameters).filter((key) => !declared.has(key));
      if (undeclared.length > 0) {
        children.push(compiler.notice("undeclared-params", "info", `${undeclared.length} 个未声明参数，见 JSON 页签`, undeclared.length));
      }
    }
  }

  const common = options.common === undefined ? commonSchema : options.common;
  if (common) children.push(...compiler.groups(common, "common", "通用").ids);

  const rootChecks: CheckDef[] = [];
  if (schema) {
    const { ids, hidden } = compiler.groups(schema, "type", "参数");
    children.push(...ids);
    for (const group of schema.one_of ?? []) {
      const members = group.params.map((param) => {
        const field = schema.fields.find((candidate) => candidate.name === param);
        return { param, path: field?.path ?? joinPointer("/parameters", param), label: field?.label || param };
      });
      // Every member counts (hidden ones too, Doc A §2.2); issues land only on
      // members the panel can show (e.g. not script.__artifact_file_path).
      const paths = members.map((member) => member.path);
      const shown = members.filter((member) => !hidden.has(member.param));
      const targets = shown.map((member) => member.path);
      const labels = shown.map((member) => member.label);
      const mode = group.mode ?? "exactly";
      rootChecks.push({
        type: "oneOf",
        args: { paths, mode },
        ...(targets.length > 0 && { targets }),
        message: oneOfMessage(labels, mode)
      });
    }
  }

  const root: ElementDef = { type: "Form", props: { layout: "vertical" }, children };
  if (rootChecks.length > 0) root.checks = rootChecks;
  const elements = { [ROOT_ID]: root, ...compiler.elements };
  return { spec: { spec: "composer/v1", minor: 0, root: ROOT_ID, elements }, warnings: compiler.warnings };
}
