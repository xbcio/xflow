import { resolve, validateSpec, type ComposerSpec, type ElementDef, type ResolvedNode } from "@xflow/composer/core";
import { formComponents } from "@xflow/composer/form";
import { createRegistry, type Registry } from "@xflow/composer/react";
import { describe, expect, it } from "vitest";
import { nodeFormChecks } from "./checks";
import { commonSchema } from "./commonSchema";
import { compileNodeForm, JSON_EDITOR_PARAMS, ROOT_ID } from "./compile";
import { NODE_FORM_COMPONENT_TYPES } from "./components";
import { createNodeFormComponents, createNodeFormRegistry } from "./components/index";
import type { NodeFormSchema } from "./schema";
import {
  aggregateSchema,
  databaseSchema,
  fixtureSchemas,
  httpSchema,
  kafkaSchema,
  mapSchema,
  redisSchema,
  scriptSchema,
  switchSchema,
  waitSchema
} from "./testdata/nodeTypes";

// ------------------------------------------------------------------ harness

/** The real registry: composer/form built-ins + C2 host components + nodeFormChecks (strict props). */
const registry = createNodeFormRegistry({ onRename: () => {} });
const { schemas, checks, expressions } = registry;

/** A registry holding only some node-form component types (compile-time degrade tests). */
function subsetRegistry(types: readonly string[]): Registry {
  const wanted = new Set(types);
  const all = [...formComponents, ...createNodeFormComponents()];
  return createRegistry(
    all.filter((component) => wanted.has(component.type)),
    { checks: nodeFormChecks }
  );
}

function compile(schema: NodeFormSchema | null, options: Parameters<typeof compileNodeForm>[1] = {}) {
  return compileNodeForm(schema, { common: null, ...options });
}

/** Elements whose primary `value` binds (or reads, for NodeNameInput) the pointer. */
function bound(spec: ComposerSpec, pointer: string): [string, ElementDef][] {
  return Object.entries(spec.elements).filter(([, element]) => {
    const value = element.props?.value as Record<string, unknown> | undefined;
    return value?.$bindState === pointer || (element.type === "NodeNameInput" && value?.$state === pointer);
  });
}

/** Elements whose primary `value` binds the row field inside the given repeat container. */
function rowBound(spec: ComposerSpec, field: string): [string, ElementDef][] {
  return Object.entries(spec.elements).filter(([, element]) => (element.props?.value as Record<string, unknown> | undefined)?.$bindItem === field);
}

/** The outermost element for a field: the one its group or container lists. */
function outer(spec: ComposerSpec, pointer: string): ElementDef {
  const id = `f.${pointer.slice(1).split("/").join(".")}`;
  const element = spec.elements[id];
  if (!element) throw new Error(`no element ${id}`);
  return element;
}

function types(spec: ComposerSpec, pointer: string): string[] {
  return bound(spec, pointer)
    .map(([, element]) => element.type)
    .sort();
}

function walk(node: ResolvedNode | null, visit: (node: ResolvedNode) => void): void {
  if (!node) return;
  visit(node);
  node.children.forEach((child) => walk(child, visit));
  Object.values(node.slots).forEach((list) => list.forEach((child) => walk(child, visit)));
}

function resolveWith(spec: ComposerSpec, value: object) {
  return resolve({
    spec,
    value,
    context: { ports: [{ name: "a" }, { name: "b" }], credentials: ["db"] },
    schemas,
    bindingKinds: registry.bindingKinds,
    checks,
    expressions
  });
}

function visibleIds(spec: ComposerSpec, value: object): Set<string> {
  const ids = new Set<string>();
  walk(resolveWith(spec, value).root, (node) => ids.add(node.id));
  return ids;
}

function issuesAt(spec: ComposerSpec, value: object, pointer: string) {
  return resolveWith(spec, value).issues.filter((issue) => issue.path === pointer);
}

// --------------------------------------------------------- spec validity

describe("compileNodeForm: every fixture compiles to a valid Spec", () => {
  const cases: [string, NodeFormSchema | null][] = [...fixtureSchemas.map((s): [string, NodeFormSchema] => [s.node_type, s]), ["<no schema>", null]];

  it.each(cases)("%s passes validateSpec with the real node-form registry", (_name, schema) => {
    const { spec, warnings } = compileNodeForm(schema, { parameters: { extra: 1 }, hasTemplate: true });
    expect(warnings).toEqual([]);
    expect(validateSpec(spec, { schemas, checks, expressions })).toEqual([]);
  });

  it.each(cases)("%s resolves without element errors (props accepted by the strict real schemas)", (_name, schema) => {
    const { spec } = compileNodeForm(schema);
    const tree = resolveWith(spec, { name: "n1", parameters: {} });
    expect(tree.specErrors).toEqual([]);
    const errors: unknown[] = [];
    walk(tree.root, (node) => node.error && errors.push({ id: node.id, error: node.error }));
    expect(errors).toEqual([]);
  });

  it.each(fixtureSchemas.map((s) => [s.node_type, s] as const))("%s snapshot", (_name, schema) => {
    expect(compile(schema)).toMatchSnapshot();
  });

  it("commonSchema snapshot", () => {
    expect(compileNodeForm(null, { common: commonSchema })).toMatchSnapshot();
  });

  // Every builtin type: nodeForm.generated.test.ts compiles and snapshots the
  // A6-generated testdata/node-types.generated.json.

  it("registers every component type compileNodeForm targets", () => {
    expect(NODE_FORM_COMPONENT_TYPES.filter((type) => !registry.types.includes(type))).toEqual([]);
  });

  it("the real registry rejects an undeclared prop (strict schemas; harness self-test)", () => {
    const { spec } = compile(waitSchema);
    const id = "f.parameters.mode:control";
    const tampered: ComposerSpec = {
      ...spec,
      elements: { ...spec.elements, [id]: { ...spec.elements[id], props: { ...spec.elements[id].props, bogus: 1 } } }
    };
    const errors: string[] = [];
    walk(resolveWith(tampered, { parameters: {} }).root, (node) => node.error && errors.push(node.id));
    expect(errors).toEqual([id]);
  });
});

// ------------------------------------------------------ Doc C §3.1 JsonEditor

describe("JsonEditor appears exactly for the Doc C §3.1 list", () => {
  it("matches the list intersected with the fixtures", () => {
    const found: string[] = [];
    const expected: string[] = [];
    for (const schema of fixtureSchemas) {
      const { spec } = compile(schema);
      for (const element of Object.values(spec.elements)) {
        if (element.type !== "JsonEditor") continue;
        const value = element.props?.value as Record<string, string>;
        found.push(`${schema.node_type}${value.$bindState ?? `[].${value.$bindItem}`}`);
      }
      for (const param of JSON_EDITOR_PARAMS[schema.node_type] ?? []) expected.push(`${schema.node_type}/parameters/${param}`);
    }
    expect(found.sort()).toEqual(expected.sort());
    expect(expected).toEqual(
      expect.arrayContaining(["xflow.http/parameters/body", "xflow.http/parameters/options", "xflow.map/parameters/body"])
    );
  });
});

// ------------------------------------------------------------ widget mapping

describe("widget → component mapping (Doc C §3)", () => {
  it("maps the fixture widgets", () => {
    const wait = compile(waitSchema).spec;
    expect(types(wait, "/parameters/mode")).toEqual(["ExpressionInput", "Select"]);
    expect(types(wait, "/parameters/signals")).toEqual(["ExpressionInput", "Tags"]);
    expect(types(wait, "/parameters/quorum")).toEqual(["ExpressionInput", "InputNumber"]);
    expect(types(wait, "/parameters/timeout")).toEqual(["DurationInput", "ExpressionInput"]);
    expect(types(wait, "/parameters/until")).toEqual(["DateTimeInput", "ExpressionInput"]);

    const http = compile(httpSchema).spec;
    expect(types(http, "/parameters/headers")).toEqual(["ExpressionInput", "KeyValue"]);
    expect(types(http, "/parameters/authentication")).toEqual(["CredentialSelect", "ExpressionInput"]);
    expect(types(http, "/parameters/body_b64")).toEqual(["ExpressionInput", "TextArea"]);
    // B4 TextArea has no base64 summary mode: the base64 widget is a plain TextArea (strict props).
    expect(Object.keys(bound(http, "/parameters/body_b64").find(([, e]) => e.type === "TextArea")?.[1].props ?? {})).toEqual(["value", "label"]);

    const script = compile(scriptSchema).spec;
    const credentials = bound(script, "/parameters/credentials").find(([, e]) => e.type === "CredentialSelect")?.[1];
    expect(credentials?.props).toMatchObject({ multiple: true, credentials: { $state: "/$ctx/credentials" } });
    expect(types(script, "/parameters/roots")).toEqual(["ExpressionInput", "Tags"]);

    const kafka = compile(kafkaSchema).spec;
    // Trigger params are template mode (rendered at activation), so each is fx-wrapped.
    expect(types(kafka, "/parameters/aggregate")).toEqual(["ExpressionInput", "ObjectGroup"]);
    expect(types(kafka, "/parameters/brokers")).toEqual(["ExpressionInput", "Tags"]);
    expect(types(kafka, "/parameters/aggregate/enabled")).toEqual(["ExpressionInput", "Switch"]);
    expect(types(kafka, "/parameters/tuning/max_wait")).toEqual(["DurationInput", "ExpressionInput"]);
    expect(bound(kafka, "/parameters/tuning/max_wait").find(([, e]) => e.type === "DurationInput")?.[1].props?.unit).toBe("string");

    const aggregate = compile(aggregateSchema).spec;
    const table = bound(aggregate, "/parameters/operations").find(([, e]) => e.type === "ArrayTable")?.[1];
    expect(table?.repeat).toEqual({ statePath: "/parameters/operations" });
    expect(rowBound(aggregate, "kind").map(([, e]) => e.type).sort()).toEqual(["ExpressionInput", "Select"]);
  });

  it("port-select reads /$ctx/ports on dynamic-output nodes", () => {
    const { spec } = compile(switchSchema);
    const port = bound(spec, "/parameters/default_output").find(([, e]) => e.type === "PortSelect")?.[1];
    expect(port?.props?.ports).toEqual({ $state: "/$ctx/ports" });
    const rowPort = rowBound(spec, "output").find(([, e]) => e.type === "PortSelect")?.[1];
    expect(rowPort?.props?.ports).toEqual({ $state: "/$ctx/ports" });
  });

  it("port-select lists static outputs on fixed-port nodes", () => {
    const schema: NodeFormSchema = {
      ...waitSchema,
      fields: [{ name: "route", path: "/parameters/route", type: "string", widget: "port-select" }]
    };
    const port = bound(compile(schema).spec, "/parameters/route").find(([, e]) => e.type === "PortSelect")?.[1];
    expect(port?.props?.ports).toEqual([
      { name: "main", display_name: "Main" },
      { name: "timeout", display_name: "Timeout" },
      { name: "error", display_name: "Error" }
    ]);
  });

  it("secret forces Password and hides the default (Doc C §4.5)", () => {
    const schema: NodeFormSchema = {
      ...waitSchema,
      fields: [{ name: "token", path: "/parameters/token", type: "string", widget: "text", secret: true, default: "s3cret" }]
    };
    const { spec } = compile(schema);
    const password = bound(spec, "/parameters/token").find(([, e]) => e.type === "Password")?.[1];
    expect(password).toBeDefined();
    expect(JSON.stringify(spec)).not.toContain("s3cret");
  });

  it("default and fallback become defaultHint, never a value", () => {
    const wait = compile(waitSchema).spec;
    expect(bound(wait, "/parameters/mode").find(([, e]) => e.type === "Select")?.[1].props?.defaultHint).toBe("signal");
    const retry = compileNodeForm(null, { common: commonSchema }).spec;
    expect(bound(retry, "/retry/initial_interval")[0][1].props?.defaultHint).toBe(1000);
  });

  it("groups follow schema.groups order and fields follow `order`", () => {
    const { spec } = compile(kafkaSchema);
    expect(spec.elements[ROOT_ID].children).toEqual(["g.type.connection", "g.type.processing", "g.type.advanced"]);
    expect(spec.elements["g.type.advanced"].props).toMatchObject({ title: "Advanced", collapsible: true, defaultCollapsed: true });
    const schema: NodeFormSchema = {
      ...waitSchema,
      fields: [
        { name: "b", path: "/parameters/b", type: "string", order: 20 },
        { name: "a", path: "/parameters/a", type: "string", order: 10 },
        { name: "c", path: "/parameters/c", type: "string", order: 20 }
      ]
    };
    expect(compile(schema).spec.elements["g.type.default"].children).toEqual(["f.parameters.a", "f.parameters.b", "f.parameters.c"]);
  });
});

// ------------------------------------------------------------ degrade (§3.2)

describe("degrade rules (Doc C §3.2)", () => {
  const unknownWidget: NodeFormSchema = {
    ...waitSchema,
    fields: [
      { name: "s", path: "/parameters/s", type: "string", widget: "fancy-new" },
      { name: "o", path: "/parameters/o", type: "object", widget: "fancy-new" },
      { name: "a", path: "/parameters/a", type: "array", widget: "fancy-new" }
    ]
  };

  it("an unknown widget falls back by type with a warning", () => {
    const { spec, warnings } = compile(unknownWidget);
    expect(types(spec, "/parameters/s")).toEqual(["ExpressionInput", "Input"]);
    expect(types(spec, "/parameters/o")).toEqual(["ExpressionInput", "JsonEditor"]);
    expect(types(spec, "/parameters/a")).toEqual(["ExpressionInput", "JsonEditor"]);
    expect(warnings).toHaveLength(3);
    expect(warnings[0]).toContain('unknown widget "fancy-new"');
    expect(validateSpec(spec, { schemas, checks, expressions })).toEqual([]);
  });

  it("an unregistered component falls back by type", () => {
    const registeredTypes = NODE_FORM_COMPONENT_TYPES.filter((type) => type !== "DurationInput" && type !== "ObjectGroup");
    const { spec, warnings } = compile(kafkaSchema, { registeredTypes });
    expect(types(spec, "/parameters/tuning")).toEqual(["ExpressionInput", "JsonEditor"]);
    expect(bound(spec, "/parameters/tuning/max_wait")).toEqual([]); // sub-fields dropped with the group
    expect(warnings.some((warning) => warning.includes("ObjectGroup is not registered"))).toBe(true);
    const wait = compile(waitSchema, { registeredTypes }).spec;
    expect(types(wait, "/parameters/timeout")).toEqual(["ExpressionInput", "Input"]);
    const subset = subsetRegistry(registeredTypes);
    expect(validateSpec(spec, { schemas: subset.schemas, checks: subset.checks, expressions: subset.expressions })).toEqual([]);
  });

  it("widgets that need structure the field lacks degrade", () => {
    const schema: NodeFormSchema = {
      ...waitSchema,
      fields: [
        { name: "g", path: "/parameters/g", type: "object", widget: "object-group" },
        { name: "t", path: "/parameters/t", type: "array", widget: "array-table" },
        { name: "s", path: "/parameters/s", type: "string", widget: "select" }
      ]
    };
    const { spec, warnings } = compile(schema);
    expect(types(spec, "/parameters/g")).toEqual(["ExpressionInput", "JsonEditor"]);
    expect(types(spec, "/parameters/t")).toEqual(["ExpressionInput", "JsonEditor"]);
    expect(types(spec, "/parameters/s")).toEqual(["ExpressionInput", "Input"]);
    expect(warnings).toHaveLength(3);
  });

  it("every non-template field is wrapped in a ShapeGuard reading its value", () => {
    const body = outer(compile(mapSchema).spec, "/parameters/body");
    expect(body.type).toBe("ShapeGuard");
    expect(body.props).toMatchObject({ observed: { $state: "/parameters/body" }, expect: { type: "object" }, mode: "none" });
    const code = outer(compile(scriptSchema).spec, "/parameters/code");
    expect(code.type).toBe("ShapeGuard");
    expect(code.props).toMatchObject({ observed: { $state: "/parameters/code" }, expect: { type: "string" }, mode: "literal" });
  });

  it("template-mode ExpressionInput carries the shape expectation itself", () => {
    const { spec } = compile(httpSchema);
    expect(outer(spec, "/parameters/headers").props).toMatchObject({ mode: "template", expect: { type: "object", values: "string" } });
  });
});

// --------------------------------------------------------- expression modes

describe("expression modes (Doc C §4.1)", () => {
  it("template: action params get ExpressionInput with an fx toggle and the plain control in the literal slot", () => {
    const { spec } = compile(databaseSchema);
    const table = outer(spec, "/parameters/table");
    expect(table.type).toBe("ExpressionInput");
    expect(table.props).toMatchObject({ mode: "template", fxToggle: true, label: "Table", required: true });
    const [literal] = table.slots?.literal ?? [];
    expect(spec.elements[literal]).toMatchObject({ type: "Input", props: { value: { $bindState: "/parameters/table" } } });
    // The literal control keeps only its accessible name; description/issues live on the ExpressionInput.
    expect(spec.elements[literal].props).toEqual({ value: { $bindState: "/parameters/table" }, label: "Table", required: true });
  });

  it("template: required stays; other rules split into literalOnly (error) and expressionOnly (warning)", () => {
    const table = outer(compile(databaseSchema).spec, "/parameters/table");
    expect(table.checks?.map((check) => [check.type, check.severity ?? "error"])).toEqual([
      ["required", "error"],
      ["literalOnly", "error"],
      ["expressionOnly", "warning"]
    ]);
    const spec = compile(databaseSchema).spec;
    expect(issuesAt(spec, { parameters: { table: "1bad" } }, "/parameters/table").map((i) => i.severity)).toEqual(["error"]);
    expect(issuesAt(spec, { parameters: { table: "${{ $vars.t }}" } }, "/parameters/table").map((i) => i.severity)).toEqual([
      "warning"
    ]);
  });

  it("template: format rules allow expressions outright", () => {
    const timeout = outer(compile(waitSchema).spec, "/parameters/timeout");
    expect(timeout.checks).toEqual([{ type: "format", args: { format: "duration", allowExpression: true }, message: expect.any(String) }]);
  });

  it("pure: evaluable params and sub-fields are ExpressionInput without toggle", () => {
    const map = compile(mapSchema).spec;
    expect(outer(map, "/parameters/items")).toMatchObject({ type: "ExpressionInput", props: { mode: "pure", fxToggle: false } });
    expect(outer(map, "/parameters/expression").props?.mode).toBe("pure");
    const sw = compile(switchSchema).spec;
    expect(outer(sw, "/parameters/expression").props?.mode).toBe("pure");
    const [[, condition]] = rowBound(sw, "condition");
    expect(condition).toMatchObject({ type: "ExpressionInput", props: { mode: "pure", fxToggle: false } });
    const [, output] = rowBound(sw, "output").find(([, e]) => e.type === "ExpressionInput") ?? [];
    expect(output?.props).toMatchObject({ mode: "template", fxToggle: true }); // rules[].output is not evaluable
    expect(outer(compile(aggregateSchema).spec, "/parameters/items").props?.mode).toBe("pure");
  });

  it("pure: a free-map object becomes KeyValue with ExpressionInput values (key-expression)", () => {
    const schema: NodeFormSchema = {
      ...aggregateSchema,
      node_type: "xflow.transform.set",
      fields: [{ name: "expressions", path: "/parameters/expressions", type: "object", widget: "key-expression" }]
    };
    const { spec } = compile(schema);
    expect(types(spec, "/parameters/expressions")).toEqual(["KeyValue"]);
    expect(bound(spec, "/parameters/expressions")[0][1].props?.valueType).toBe("ExpressionInput");
  });

  it("template: trigger params get the fx toggle (rendered at activation against $config/$vars)", () => {
    const { spec } = compile(kafkaSchema);
    for (const pointer of ["/parameters/topic", "/parameters/aggregate/on_overflow", "/parameters/tuning/max_wait"]) {
      expect(types(spec, pointer)).toContain("ExpressionInput");
      expect(outer(spec, pointer)).toMatchObject({ type: "ExpressionInput", props: { mode: "template", fxToggle: true } });
    }
    expect(issuesAt(spec, { parameters: { topic: "${{ $config.t }}" } }, "/parameters/topic")).not.toContainEqual(
      expect.objectContaining({ check: "notEvaluated" })
    );
  });

  it("none: map.body is a plain JsonEditor", () => {
    const { spec } = compile(mapSchema);
    expect(outer(spec, "/parameters/body")).toMatchObject({ type: "ShapeGuard", props: { mode: "none" } });
    expect(types(spec, "/parameters/body")).toEqual(["JsonEditor"]);
    expect(bound(spec, "/parameters/body")[0][1].checks?.map((c) => c.type)).toContain("notEvaluated");
  });

  it("literal: script.code is CodeEditor (js) / base64 TextArea (wasm) with noTemplateInCode", () => {
    const { spec } = compile(scriptSchema);
    const elements = bound(spec, "/parameters/code");
    expect(elements.map(([, e]) => e.type).sort()).toEqual(["CodeEditor", "TextArea"]);
    const code = elements.find(([, e]) => e.type === "CodeEditor")?.[1];
    const b64 = elements.find(([, e]) => e.type === "TextArea")?.[1];
    expect(code?.props).toMatchObject({ language: "javascript" });
    expect(b64?.props).not.toHaveProperty("language");
    expect(code?.visible).toEqual({ $state: "/parameters/language", neq: "wasm" });
    expect(b64?.visible).toEqual({ $state: "/parameters/language", eq: "wasm" });
    for (const element of [code, b64]) {
      expect(element?.checks).toEqual([{ type: "noTemplateInCode", severity: "warning", message: expect.any(String) }]);
    }
    expect(outer(spec, "/parameters/code")).toMatchObject({ type: "ShapeGuard", props: { mode: "literal" } });

    const js = visibleIds(spec, { parameters: { language: "js" } });
    const wasm = visibleIds(spec, { parameters: { language: "wasm" } });
    const [codeId] = elements.find(([, e]) => e.type === "CodeEditor") ?? [];
    const [b64Id] = elements.find(([, e]) => e.type === "TextArea") ?? [];
    expect([js.has(codeId!), js.has(b64Id!)]).toEqual([true, false]);
    expect([wasm.has(codeId!), wasm.has(b64Id!)]).toEqual([false, true]);
  });

  it("an explicit expression.mode from the schema wins over the stub", () => {
    const schema: NodeFormSchema = {
      ...kafkaSchema,
      fields: [{ ...kafkaSchema.fields[1], expression: { mode: "template" } }]
    };
    expect(outer(compile(schema).spec, "/parameters/topic").props).toMatchObject({ mode: "template", fxToggle: true });
  });

  it("common fields are never evaluated (none)", () => {
    const { spec } = compileNodeForm(null, { common: commonSchema });
    expect(outer(spec, "/timeout").props?.mode).toBe("none");
  });
});

// ---------------------------------------------------- linkage (Doc C §4.4)

describe("conditional linkage matches the Go descriptors (Doc C §4.4)", () => {
  it("empty any_of never holds and empty all_of always holds, as in Go evalCondition", () => {
    const schema: NodeFormSchema = {
      ...waitSchema,
      fields: [
        { name: "never", path: "/parameters/never", type: "string", visible_when: { any_of: [] } },
        { name: "always", path: "/parameters/always", type: "string", visible_when: { all_of: [] } }
      ]
    };
    const { spec } = compile(schema);
    expect(bound(spec, "/parameters/never")).toEqual([]);
    expect(outer(spec, "/parameters/always").visible).toBeUndefined();
  });

  it("wait: mode drives signal_name/signals/duration/until; quorum needs signals; timeout always shown", () => {
    const { spec } = compile(waitSchema);
    const signalMode = { $state: "/parameters/mode", in: ["signal", null] };
    expect(outer(spec, "/parameters/signal_name").visible).toEqual(signalMode);
    expect(outer(spec, "/parameters/signals").visible).toEqual(signalMode);
    expect(outer(spec, "/parameters/quorum").visible).toEqual({ $and: [signalMode, { $state: "/parameters/signals", truthy: true }] });
    expect(outer(spec, "/parameters/duration").visible).toEqual({ $state: "/parameters/mode", eq: "timer" });
    expect(outer(spec, "/parameters/until").visible).toEqual({ $state: "/parameters/mode", eq: "timer" });
    expect(outer(spec, "/parameters/timeout").visible).toBeUndefined();
    expect(outer(spec, "/parameters/duration").checks?.[0]).toEqual({
      type: "required",
      when: { $and: [{ $state: "/parameters/mode", eq: "timer" }, { $state: "/parameters/until", truthy: false }] },
      message: expect.any(String)
    });

    const unset = visibleIds(spec, { parameters: {} });
    expect(unset.has("f.parameters.signal_name")).toBe(true);
    expect(unset.has("f.parameters.quorum")).toBe(false);
    expect(unset.has("f.parameters.timeout")).toBe(true);
    const timer = visibleIds(spec, { parameters: { mode: "timer" } });
    expect(timer.has("f.parameters.signal_name")).toBe(false);
    expect(timer.has("f.parameters.duration")).toBe(true);
    expect(timer.has("f.parameters.timeout")).toBe(true);
    expect(visibleIds(spec, { parameters: { signals: ["a"] } }).has("f.parameters.quorum")).toBe(true);
    expect(issuesAt(spec, { parameters: { mode: "timer" } }, "/parameters/duration")).toHaveLength(1);
    expect(issuesAt(spec, { parameters: { mode: "timer", until: "2026-01-01T00:00:00Z" } }, "/parameters/duration")).toHaveLength(0);
  });

  it("switch: mode drives rules and expression", () => {
    const { spec } = compile(switchSchema);
    expect(outer(spec, "/parameters/rules").visible).toEqual({ $state: "/parameters/mode", in: ["rules", null] });
    expect(outer(spec, "/parameters/expression").visible).toEqual({ $state: "/parameters/mode", eq: "expression" });
    expect(outer(spec, "/parameters/expression").checks?.[0]).toMatchObject({ type: "required", when: { $state: "/parameters/mode", eq: "expression" } });
  });

  it("script: language drives runtime options via $cond; OneOf exactly with targets excluding the SDK placeholder", () => {
    const { spec } = compile(scriptSchema);
    const runtime = bound(spec, "/parameters/runtime").find(([, e]) => e.type === "Select")?.[1];
    const options = runtime?.props?.options as Record<string, unknown>;
    expect(options.$cond).toEqual({ $state: "/parameters/language", eq: "js" });
    expect((options.then as { value: string }[]).map((o) => o.value)).toEqual(["goja", "qjs"]);
    const inner = options.else as Record<string, unknown>;
    expect(inner.$cond).toEqual({ $state: "/parameters/language", eq: "wasm" });
    expect((inner.then as { value: string }[]).map((o) => o.value)).toEqual(["wazero", "wazero-reactor"]);
    expect((inner.else as unknown[]).length).toBe(4);

    expect(spec.elements[ROOT_ID].checks).toEqual([
      {
        type: "oneOf",
        args: { paths: ["/parameters/code", "/parameters/artifact_digest", "/parameters/__artifact_file_path"], mode: "exactly" },
        targets: ["/parameters/code", "/parameters/artifact_digest"],
        message: expect.any(String)
      }
    ]);
    expect(bound(spec, "/parameters/__artifact_file_path")).toEqual([]);
    expect(issuesAt(spec, { parameters: {} }, "/parameters/code")).toEqual([expect.objectContaining({ check: "oneOf" })]);
    // A hidden member still counts (Doc A §2.2).
    expect(issuesAt(spec, { parameters: { __artifact_file_path: "x.js" } }, "/parameters/code")).toEqual([]);
  });

  it("http: mode drives body and body_b64", () => {
    const { spec } = compile(httpSchema);
    expect(outer(spec, "/parameters/body").visible).toEqual({ $state: "/parameters/mode", in: ["json", null] });
    expect(outer(spec, "/parameters/body_b64").visible).toEqual({ $state: "/parameters/mode", eq: "raw" });
    expect(outer(spec, "/parameters/url").checks?.find((c) => c.type === "format")?.severity).toBe("warning"); // url is advisory
    expect(bound(spec, "/parameters/headers").find(([, e]) => e.type === "KeyValue")).toBeDefined();
    expect(outer(spec, "/parameters/headers").checks?.map((c) => c.type)).toContain("secretLike");
  });

  it("database: operation drives where/data/columns/limit, where required for update/delete", () => {
    const { spec } = compile(databaseSchema);
    expect(outer(spec, "/parameters/where").visible).toEqual({ $state: "/parameters/operation", in: ["select", "update", "delete"] });
    expect(outer(spec, "/parameters/where").checks?.[0]).toMatchObject({ when: { $state: "/parameters/operation", in: ["update", "delete"] } });
    expect(outer(spec, "/parameters/data").visible).toEqual({ $state: "/parameters/operation", in: ["insert", "insert_many", "update"] });
    expect(outer(spec, "/parameters/columns").visible).toEqual({ $state: "/parameters/operation", eq: "select" });
    expect(outer(spec, "/parameters/limit").visible).toEqual({ $state: "/parameters/operation", eq: "select" });
  });

  it("trigger.redis: stream/group visible+required in stream-or-unset, channel in pubsub, tuning always visible", () => {
    const { spec } = compile(redisSchema);
    const streamOrUnset = { $state: "/parameters/mode", in: ["stream", null] };
    for (const name of ["stream", "group"]) {
      const element = outer(spec, `/parameters/${name}`);
      expect(element.visible).toEqual(streamOrUnset);
      expect(element.checks?.[0]).toEqual({ type: "required", when: streamOrUnset, message: expect.any(String) });
    }
    expect(outer(spec, "/parameters/channel").visible).toEqual({ $state: "/parameters/mode", eq: "pubsub" });
    expect(outer(spec, "/parameters/tuning").visible).toBeUndefined();
    expect(visibleIds(spec, { parameters: { mode: "pubsub" } }).has("f.parameters.tuning")).toBe(true);
    expect(issuesAt(spec, { parameters: {} }, "/parameters/group")).toHaveLength(1);
    expect(issuesAt(spec, { parameters: { mode: "pubsub" } }, "/parameters/group")).toHaveLength(0);
  });

  it("trigger.kafka: dead_letter_topic follows on_overflow / on_invalid inside their objects", () => {
    const { spec } = compile(kafkaSchema);
    const aggregateDlt = outer(spec, "/parameters/aggregate/dead_letter_topic");
    expect(aggregateDlt.visible).toEqual({ $state: "/parameters/aggregate/on_overflow", eq: "dead_letter" });
    expect(aggregateDlt.checks?.[0]).toMatchObject({
      type: "required",
      when: { $state: "/parameters/aggregate/on_overflow", eq: "dead_letter" }
    });
    expect(outer(spec, "/parameters/message_schema/dead_letter_topic").visible).toEqual({
      $state: "/parameters/message_schema/on_invalid",
      eq: "dead_letter"
    });
    expect(issuesAt(spec, { parameters: { aggregate: { on_overflow: "dead_letter" } } }, "/parameters/aggregate/dead_letter_topic")).toHaveLength(1);
  });

  it("map: OneOf{body, expression} exactly", () => {
    const { spec } = compile(mapSchema);
    expect(spec.elements[ROOT_ID].checks).toEqual([
      {
        type: "oneOf",
        args: { paths: ["/parameters/body", "/parameters/expression"], mode: "exactly" },
        targets: ["/parameters/body", "/parameters/expression"],
        message: expect.any(String)
      }
    ]);
  });

  it("transform.aggregate: row-scoped visibility uses $item", () => {
    const { spec } = compile(aggregateSchema);
    const [, field] = rowBound(spec, "field").find(([, e]) => e.type === "ExpressionInput") ?? [];
    expect(field?.visible).toEqual({ $item: "kind", in: ["sum", "avg", "average"] });
    const rows = { parameters: { items: "${{ $input }}", operations: [{ kind: "count", as: "n" }, { kind: "sum", as: "s" }] } };
    const ids = [...visibleIds(spec, rows)].filter((id) => id.startsWith("f.parameters.operations[].field@"));
    expect(ids).toHaveLength(1);
  });
});

// ---------------------------------------------------------- common + notices

describe("commonSchema (Doc C §4.2)", () => {
  const { spec } = compileNodeForm(null, { common: commonSchema });

  it("places common groups before the type groups", () => {
    const withType = compileNodeForm(waitSchema).spec;
    expect(withType.elements[ROOT_ID].children).toEqual(["g.common.general", "g.common.execution", "g.type.default"]);
  });

  it("/name is the host rename component reading, not binding, the name", () => {
    const [[, name]] = bound(spec, "/name");
    expect(name).toMatchObject({ type: "NodeNameInput", props: { value: { $state: "/name" } } });
    expect(JSON.stringify(spec)).not.toContain('"$bindState":"/name"');
  });

  it("timeout is DurationInput in ns, retry intervals in ms", () => {
    expect(bound(spec, "/timeout")[0][1]).toMatchObject({ type: "DurationInput", props: { unit: "ns" } });
    expect(bound(spec, "/retry/initial_interval")[0][1].props?.unit).toBe("ms");
    expect(bound(spec, "/retry/max_interval")[0][1].props?.unit).toBe("ms");
    expect(types(spec, "/retry")).toEqual(["ObjectGroup"]);
    expect(types(spec, "/retry/strategy")).toEqual(["Select"]);
  });

  it("on_error offers the four types.OnError values", () => {
    const select = bound(spec, "/on_error")[0][1];
    expect((select.props?.options as { value: string }[]).map((o) => o.value)).toEqual(["stop", "error_output", "main_output", "continue"]);
  });

  it("runner_selector exposes match_labels only (node-level mode is a compile error)", () => {
    expect(types(spec, "/runner_selector")).toEqual(["ObjectGroup"]);
    expect(types(spec, "/runner_selector/match_labels")).toEqual(["KeyValue"]);
    expect(bound(spec, "/runner_selector/mode")).toEqual([]);
    expect(types(spec, "/disabled")).toEqual(["Switch"]);
    expect(types(spec, "/notes")).toEqual(["TextArea"]);
  });
});

describe("notices", () => {
  it("a missing schema shows the unregistered-type notice and common fields only", () => {
    const { spec } = compileNodeForm(null);
    expect(spec.elements[ROOT_ID].children?.[0]).toBe("n.unregistered-type");
    expect(Object.keys(spec.elements).some((id) => id.startsWith("g.type."))).toBe(false);
  });

  it("counts undeclared parameters at compile time", () => {
    const { spec } = compile(waitSchema, { parameters: { mode: "signal", legacy: 1, other: true } });
    expect(spec.elements["n.undeclared-params"].props).toMatchObject({ code: "undeclared-params", count: 2 });
    expect(compile(waitSchema, { parameters: { mode: "signal" } }).spec.elements["n.undeclared-params"]).toBeUndefined();
    // script's hidden SDK placeholder is declared.
    expect(compile(scriptSchema, { parameters: { __artifact_file_path: "a.js" } }).spec.elements["n.undeclared-params"]).toBeUndefined();
  });

  it("shows the node-template notice", () => {
    expect(compile(waitSchema, { hasTemplate: true }).spec.elements["n.node-template"].props).toMatchObject({ tone: "warning" });
  });
});
