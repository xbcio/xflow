// C1 + C2 integration (Doc C §5.1 acceptance 1, §7 C2): every C1 fixture is
// compiled together with commonSchema, resolved against the REAL node-form
// registry and mounted through <Composer> under StrictMode with both kernels.
// For every value family — empty, full valid, templates (whole and partial),
// wrong shape, null, unknown keys — mounting writes zero patches, the value
// serializes identically, and no element renders an error placeholder.
// Then one field per fixture is edited and must produce exactly one patch.

import { fireEvent } from "@testing-library/react";
import type { Patch } from "@xflow/composer/core";
import { allPatches, mountComposer } from "@xflow/composer/testing";
import type { ValidationResult } from "@xflow/composer/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { compileNodeForm } from "./compile";
import { createNodeFormRegistry } from "./components/index";
import type { NodeFormField, NodeFormItem, NodeFormSchema } from "./schema";
import { change, fieldOf, kernels, one, pickOption } from "./testdata/testkit";
import { fixtureSchemas } from "./testdata/nodeTypes";

const context = { ports: [{ name: "a" }, { name: "b" }], credentials: ["db", "api"] };
const onRename = vi.fn();
const registry = createNodeFormRegistry({ onRename });

type Json = unknown;

function fieldType(field: NodeFormField | NodeFormItem): string {
  return field.type === "bool" ? "boolean" : field.type;
}

/** A shape-valid value for a field (Doc C §5.1 "完整合法值"). */
function validValue(field: NodeFormField | NodeFormItem): Json {
  const options = field.options ?? field.options_when?.[0]?.options ?? field.item?.options ?? null;
  switch (field.widget) {
    case "duration":
      return "30s";
    case "datetime":
      return "2026-01-01T00:00:00Z";
    case "cron":
      return "*/5 * * * *";
    case "expression":
      return "${{ $input.ok }}";
    case "credential-select":
      return fieldType(field) === "array" ? ["db"] : "db";
    case "port-select":
      return "a";
    case "code":
      return "return 1;";
    case "base64":
      return "AGFzbQ==";
    case "key-value":
    case "key-expression":
      return { k: "v" };
    default:
      break;
  }
  switch (fieldType(field)) {
    case "string":
      return options?.[0]?.value ?? "text";
    case "number":
      return (options?.[0]?.value as number | undefined) ?? 2;
    case "boolean":
      return true;
    case "array":
      if (options) return [options[0].value];
      if (field.item?.fields?.length) return [objectOf(field.item.fields)];
      return ["a", "b"];
    case "object":
      return field.fields?.length ? objectOf(field.fields) : { any: 1 };
    default:
      return "text";
  }
}

function objectOf(fields: readonly NodeFormField[]): Record<string, Json> {
  return Object.fromEntries(fields.map((sub) => [sub.name, validValue(sub)]));
}

function wrongValue(field: NodeFormField): Json {
  switch (fieldType(field)) {
    case "string":
      return 42;
    case "number":
      return "not-a-number";
    case "boolean":
      return "yes";
    case "array":
      return { not: "a list" };
    default:
      return [1, 2];
  }
}

function params(schema: NodeFormSchema, of: (field: NodeFormField) => Json): Record<string, Json> {
  return Object.fromEntries(schema.fields.map((field) => [field.name, of(field)]));
}

const commonFull = {
  disabled: false,
  notes: "备注",
  on_error: "error_output",
  timeout: 30e9,
  retry: { enabled: true, max_attempts: 3, strategy: "exponential", initial_interval: 1000, max_interval: 60000, multiplier: 2 },
  runner_selector: { match_labels: { zone: "a" } }
};

function valueFamilies(schema: NodeFormSchema): [string, Record<string, Json>][] {
  const base = { name: "n1", type: schema.node_type };
  return [
    ["empty", { ...base, parameters: {} }],
    ["full valid", { ...base, ...commonFull, parameters: params(schema, validValue) }],
    ["whole templates", { ...base, timeout: "${{ $vars.t }}", parameters: params(schema, (f) => `\${{ $vars.${f.name} }}`) }],
    ["partial templates", { ...base, notes: "x {{ y }}", parameters: params(schema, (f) => `pre-\${{ $vars.${f.name} }}-post`) }],
    ["wrong shape", { ...base, timeout: "30s", retry: [], disabled: "no", parameters: params(schema, wrongValue) }],
    ["null", { ...base, timeout: null, retry: null, on_error: null, parameters: params(schema, () => null) }],
    [
      "unknown keys",
      { ...base, extra_top: { a: 1 }, parameters: { ...params(schema, validValue), zz_unknown: [1, { deep: null }], __other: "x" } }
    ]
  ];
}

function compile(schema: NodeFormSchema, parameters: unknown) {
  const compiled = compileNodeForm(schema, { parameters, registeredTypes: registry.types });
  expect(compiled.warnings).toEqual([]);
  return compiled.spec;
}

let consoleError: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  consoleError = vi.spyOn(console, "error");
});
afterEach(() => consoleError.mockRestore());

for (const kernel of kernels) {
  describe(`node forms mount without writing (${kernel.name})`, () => {
    for (const schema of fixtureSchemas) {
      it.each(valueFamilies(schema))(`${schema.node_type}: %s`, async (family, value) => {
        const before = JSON.stringify(value);
        const spec = compile(schema, value.parameters);
        let validation: ValidationResult | undefined;
        const m = mountComposer({ kernel, registry, spec, value, context, onValidate: (result) => (validation = result) });
        await m.flush();
        await m.setValue((v) => ({ ...v })); // a host re-render must not write either
        expect(m.log).toEqual([]);
        expect(m.warnings).toEqual([]);
        expect(JSON.stringify(m.value)).toBe(before);
        expect(validation?.specErrors).toEqual([]);
        const placeholders = [...m.container.querySelectorAll("[data-composer-placeholder]")].map(
          (el) => `${el.getAttribute("data-composer-node")}: ${el.textContent}`
        );
        expect(placeholders).toEqual([]);
        if (family === "wrong shape" || family === "null") {
          expect(m.container.querySelectorAll("[data-nodeform-raw], [data-composer-raw]").length).toBeGreaterThan(0);
        }
        if (family.endsWith("templates")) {
          expect(m.container.querySelectorAll('.xflow-editor-nodeform-fx[aria-pressed="true"]').length).toBeGreaterThan(0);
        }
        expect(consoleError).not.toHaveBeenCalled();
        expect(onRename).not.toHaveBeenCalled();
        m.unmount();
      });
    }
  });

  interface EditCase {
    node: string;
    field: string;
    edit(field: HTMLElement): void;
    patch: Patch;
    value?: Record<string, Json>;
  }

  const typeInto = (text: string) => (field: HTMLElement) => change(one(field, "input"), text);
  const edits: Record<string, EditCase> = {
    "xflow.wait": {
      node: "xflow.wait",
      field: "f.parameters.signal_name:control",
      edit: typeInto("approved"),
      patch: { op: "set", path: "/parameters/signal_name", value: "approved" }
    },
    "xflow.switch": {
      node: "xflow.switch",
      field: "f.parameters.default_output:control",
      edit: (field) => pickOption(field, "b"),
      patch: { op: "set", path: "/parameters/default_output", value: "b" }
    },
    "xflow.map": {
      node: "xflow.map",
      field: "f.parameters.batch_size:control",
      edit: typeInto("5"),
      patch: { op: "set", path: "/parameters/batch_size", value: 5 }
    },
    "xflow.script": {
      node: "xflow.script",
      field: "f.parameters.code:code",
      edit: (field) => change(one(field, "textarea"), "return 2;"),
      patch: { op: "set", path: "/parameters/code", value: "return 2;" }
    },
    "xflow.http": {
      node: "xflow.http",
      field: "f.parameters.url:control",
      edit: typeInto("https://example.com/api"),
      patch: { op: "set", path: "/parameters/url", value: "https://example.com/api" }
    },
    "xflow.database": {
      node: "xflow.database",
      field: "f.parameters.table:control",
      edit: typeInto(""),
      patch: { op: "unset", path: "/parameters/table" },
      value: { name: "n1", parameters: { table: "users", operation: "select" } }
    },
    "xflow.trigger.kafka": {
      node: "xflow.trigger.kafka",
      field: "f.parameters.tuning.max_wait:control",
      edit: typeInto("250ms"),
      patch: { op: "set", path: "/parameters/tuning/max_wait", value: "250ms" }
    },
    "xflow.trigger.redis": {
      node: "xflow.trigger.redis",
      field: "f.parameters.stream:control",
      edit: typeInto("orders"),
      patch: { op: "set", path: "/parameters/stream", value: "orders" }
    },
    "xflow.transform.aggregate": {
      node: "xflow.transform.aggregate",
      field: "f.parameters.items",
      edit: (field) => change(one(field, "textarea"), "${{ $input.rows }}"),
      patch: { op: "set", path: "/parameters/items", value: "${{ $input.rows }}" }
    }
  };

  describe(`node forms: editing one field emits exactly its patch (${kernel.name})`, () => {
    it("covers every fixture", () => {
      expect(Object.keys(edits).sort()).toEqual(fixtureSchemas.map((schema) => schema.node_type).sort());
    });

    for (const schema of fixtureSchemas) {
      const c = edits[schema.node_type];
      it(`${schema.node_type}: ${c.field}`, async () => {
        const value = c.value ?? { name: "n1", parameters: {} };
        const m = mountComposer({ kernel, registry, spec: compile(schema, value.parameters), value, context });
        await m.flush();
        c.edit(fieldOf(m.container, c.field));
        await m.flush();
        expect(allPatches(m.log)).toEqual([c.patch]);
      });
    }

    it("common fields: /timeout in ns and rename through the callback, never a /name patch", async () => {
      const [wait] = fixtureSchemas;
      const m = mountComposer({ kernel, registry, spec: compile(wait, {}), value: { name: "n1", parameters: {} }, context });
      await m.flush();
      change(one(fieldOf(m.container, "f.timeout:control"), "input"), "45");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/timeout", value: 45e9 }]);
      const name = one<HTMLInputElement>(fieldOf(m.container, "f.name"), "input");
      change(name, "n2");
      fireEvent.blur(name);
      await m.flush();
      expect(onRename).toHaveBeenLastCalledWith("n2");
      expect(allPatches(m.log).some((patch) => patch.path === "/name")).toBe(false);
      onRename.mockClear();
    });
  });
}
