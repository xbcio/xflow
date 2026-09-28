import { describe, expect, it } from "vitest";
import { z } from "zod";
import { createCheckRegistry, defineCheck } from "./checks";
import { createExpressionRegistry, defineExpression } from "./expressions";
import { zodProps, type PropsSchema } from "./props";
import type { ComposerSpec, SpecError } from "./types";
import { validateSpec } from "./validate";

const anyProps: PropsSchema<unknown> = { parse: (input) => ({ ok: true, value: input }) };
const schemas = {
  Form: anyProps,
  Group: anyProps,
  Input: anyProps,
  Table: anyProps,
  KeyValue: zodProps(z.object({ valueType: z.string().optional() }).passthrough(), { typeRefProps: ["valueType"] })
};

function validSpec(): ComposerSpec {
  return {
    spec: "composer/v1",
    minor: 0,
    root: "form",
    elements: {
      form: { type: "Form", children: ["mode", "signal", "rules"], slots: { footer: ["kv"] } },
      mode: {
        type: "Input",
        props: { label: "Mode", value: { $bindState: "/p/mode" }, defaultHint: "signal" },
        checks: [{ type: "required", message: "必填" }]
      },
      signal: {
        type: "Input",
        props: {
          label: { $cond: { $state: "/p/mode", eq: "signal" }, then: "Signal", else: { $state: "/$ctx/label" } },
          value: { $bindState: "/p/signal" }
        },
        visible: { $state: "/p/mode", in: ["signal", null] },
        checks: [
          { type: "pattern", args: { pattern: "^[a-z]+$" }, message: { $state: "/$ctx/msg" }, severity: "warning" },
          { type: "oneOf", args: { paths: ["/p/a", "/p/b"], mode: "at_most" }, targets: ["/p/a"], message: "x" }
        ]
      },
      rules: {
        type: "Table",
        props: { value: { $bindState: "/p/rules" } },
        repeat: { statePath: "/p/rules", key: "id" },
        children: ["rule-name"]
      },
      "rule-name": {
        type: "Input",
        props: { value: { $bindItem: "name" }, label: { $index: true } },
        visible: { $item: "enabled", truthy: true }
      },
      kv: { type: "KeyValue", props: { valueType: "Input" } }
    }
  };
}

const codes = (errors: SpecError[]) => errors.map((e) => `${e.severity}:${e.code}@${e.path}`);

describe("validateSpec", () => {
  it("accepts a valid spec", () => {
    expect(validateSpec(validSpec(), { schemas })).toEqual([]);
  });

  it("rejects an unknown major and stops", () => {
    expect(codes(validateSpec({ ...validSpec(), spec: "composer/v2" }))).toEqual(["error:major@/spec"]);
    expect(codes(validateSpec(null))).toEqual(["error:shape@"]);
  });

  it("reports a missing root", () => {
    const spec = validSpec();
    spec.root = "nope";
    expect(codes(validateSpec(spec, { schemas }))).toContain("error:root@/root");
  });

  it("reports dangling references, cycles, multi-parent and orphans with pointers", () => {
    const spec = validSpec();
    spec.elements.form.children = ["mode", "ghost", "signal", "rules", "mode"];
    spec.elements.mode.children = ["form"];
    spec.elements.lonely = { type: "Input" };
    const result = codes(validateSpec(spec, { schemas }));
    expect(result).toContain("error:dangling@/elements/form/children/1");
    expect(result).toContain("error:multi-parent@/elements/form/children/4");
    expect(result).toContain("error:cycle@/elements/mode/children/0");
    expect(result).toContain("warning:orphan@/elements/lonely");
  });

  it("escapes ids in pointers", () => {
    const spec = validSpec();
    spec.elements.form.children!.push("a/b");
    spec.elements["a/b"] = { type: "Input", props: { value: { $bindState: "nope" } } };
    expect(codes(validateSpec(spec, { schemas }))).toContain("error:shape@/elements/a~1b/props/value");
  });

  it("reports unknown and reserved fields as errors at the current minor", () => {
    const spec = validSpec() as unknown as { elements: Record<string, Record<string, unknown>> } & Record<string, unknown>;
    spec.extra = 1;
    spec.elements.mode.on = { click: "x" };
    spec.elements.mode.tooltip = "x";
    (spec.elements.mode.checks as Record<string, unknown>[])[0].level = 1;
    const result = codes(validateSpec(spec, { schemas }));
    expect(result).toEqual(
      expect.arrayContaining([
        "error:unknown-field@/extra",
        "error:reserved-field@/elements/mode/on",
        "error:unknown-field@/elements/mode/tooltip",
        "error:unknown-field@/elements/mode/checks/0/level"
      ])
    );
  });

  it("reports unregistered type, check and expression", () => {
    const spec = validSpec();
    spec.elements.mode.type = "Fancy";
    spec.elements.mode.checks = [{ type: "noTemplate", message: { $t: "k" } }];
    spec.elements.kv.props = { valueType: "Missing" };
    const result = codes(validateSpec(spec, { schemas }));
    expect(result).toEqual(
      expect.arrayContaining([
        "error:unknown-type@/elements/mode/type",
        "error:unknown-check@/elements/mode/checks/0/type",
        "error:unknown-expression@/elements/mode/checks/0/message",
        "error:unknown-type@/elements/kv/props/valueType"
      ])
    );
  });

  it("accepts host-registered checks and expressions", () => {
    const spec = validSpec();
    spec.elements.mode.checks = [{ type: "noTemplate", message: { $t: "k" } }];
    const checks = createCheckRegistry([defineCheck("noTemplate", () => [])]);
    const expressions = createExpressionRegistry([defineExpression("$t", { resolve: (args) => args.$t })]);
    expect(validateSpec(spec, { schemas, checks, expressions })).toEqual([]);
  });

  it("degrades unknown features to warnings when minor is newer (§3.1)", () => {
    const spec = validSpec() as unknown as ComposerSpec & { elements: Record<string, Record<string, unknown>> };
    spec.minor = 3;
    spec.elements.mode.tooltip = "x";
    spec.elements.mode.on = {};
    spec.elements.mode.checks = [{ type: "future", message: { $t: "k" } }];
    const result = validateSpec(spec, { schemas });
    expect(result.every((e) => e.severity === "warning")).toBe(true);
    expect(codes(result)).toEqual(
      expect.arrayContaining([
        "warning:minor@/minor",
        "warning:unknown-field@/elements/mode/tooltip",
        "warning:reserved-field@/elements/mode/on",
        "warning:unknown-check@/elements/mode/checks/0/type",
        "warning:unknown-expression@/elements/mode/checks/0/message"
      ])
    );
  });

  it("keeps structural errors fatal even for a newer minor", () => {
    const spec = validSpec();
    spec.minor = 9;
    spec.elements.form.children = ["ghost"];
    expect(codes(validateSpec(spec, { schemas }))).toContain("error:dangling@/elements/form/children/0");
  });

  it("rejects an invalid minor", () => {
    expect(codes(validateSpec({ ...validSpec(), minor: -1 }, { schemas }))).toContain("error:shape@/minor");
  });

  it("validates conditions, repeat, check args and row-scoped expressions", () => {
    const spec = validSpec() as unknown as ComposerSpec;
    spec.elements.mode.visible = { $state: "/p/mode", like: "x" } as never;
    spec.elements.mode.checks = [
      { type: "format", args: { format: "nope" }, message: "m" },
      { type: "pattern", args: { pattern: "(" }, message: "m" },
      { type: "min", args: {}, message: "m" },
      { type: "oneOf", args: { paths: ["x"] }, message: "m" },
      { type: "required" } as never,
      { type: "required", severity: "fatal", when: { $item: "x", eq: 1 }, targets: ["bad"], message: "m" } as never
    ];
    spec.elements.signal.props = { label: { $item: "x" }, other: { $index: true } };
    spec.elements.rules.repeat = { statePath: "rules" } as never;
    const result = codes(validateSpec(spec, { schemas }));
    expect(result).toEqual(
      expect.arrayContaining([
        "error:invalid-cond@/elements/mode/visible/like",
        "error:invalid-check@/elements/mode/checks/0/args",
        "error:invalid-check@/elements/mode/checks/1/args",
        "error:invalid-check@/elements/mode/checks/2/args",
        "error:invalid-check@/elements/mode/checks/3/args",
        "error:invalid-check@/elements/mode/checks/4/message",
        "error:invalid-check@/elements/mode/checks/5/severity",
        "error:invalid-cond@/elements/mode/checks/5/when/$item",
        "error:invalid-check@/elements/mode/checks/5/targets",
        "error:shape@/elements/signal/props/label",
        "error:shape@/elements/signal/props/other",
        "error:shape@/elements/rules/repeat/statePath"
      ])
    );
  });

  it("validates nested $cond conditions", () => {
    const spec = validSpec();
    spec.elements.mode.props = { label: { $cond: { $state: "/x", gt: "1" }, then: { $state: "bad" } } };
    expect(codes(validateSpec(spec, { schemas }))).toEqual(
      expect.arrayContaining(["error:invalid-cond@/elements/mode/props/label/$cond/gt", "error:shape@/elements/mode/props/label/then"])
    );
  });

  it("accepts nested repeats with {$item} statePath only inside a repeat", () => {
    const spec = validSpec();
    spec.elements.rules.children = ["rule-name", "inner"];
    spec.elements.inner = { type: "Table", repeat: { statePath: { $item: "tags" } }, children: ["tag"] };
    spec.elements.tag = { type: "Input", props: { value: { $bindItem: "" } } };
    expect(validateSpec(spec, { schemas })).toEqual([]);
    spec.elements.form.children!.push("outer-item");
    spec.elements["outer-item"] = { type: "Table", repeat: { statePath: { $item: "x" } } };
    expect(codes(validateSpec(spec, { schemas }))).toContain("error:shape@/elements/outer-item/repeat/statePath");
  });
});
