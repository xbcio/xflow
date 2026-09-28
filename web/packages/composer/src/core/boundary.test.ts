import { describe, expect, it } from "vitest";
import { validateSpec } from "./validate";
import schema from "./spec.schema.json";
import { CHECK_FIELDS, ELEMENT_FIELDS, REPEAT_FIELDS, SPEC_FIELDS } from "./types";

type Glob = (pattern: string, options: { query: string; import: string; eager: true }) => Record<string, string>;
const sources = (import.meta as unknown as { glob: Glob }).glob("./**/*.ts", { query: "?raw", import: "default", eager: true });

describe("composer/core boundaries", () => {
  it("has no React (or other UI/kernel) imports", () => {
    const files = Object.entries(sources);
    expect(files.length).toBeGreaterThan(5);
    const forbidden = /from\s+["'](react|react-dom|antd|@json-render\/[^"']+|@xyflow\/react)(\/[^"']*)?["']|import\(\s*["']react/;
    for (const [file, text] of files) expect(forbidden.test(text), file).toBe(false);
  });

  it("only imports zod and relative modules", () => {
    for (const [file, text] of Object.entries(sources)) {
      if (file.endsWith(".test.ts")) continue;
      for (const match of text.matchAll(/from\s+["']([^"']+)["']/g)) {
        expect(match[1] === "zod" || match[1].startsWith("./"), `${file}: ${match[1]}`).toBe(true);
      }
    }
  });
});

describe("spec.schema.json", () => {
  it("lists exactly the fields known to the TS contract", () => {
    expect(Object.keys(schema.properties).sort()).toEqual([...SPEC_FIELDS].sort());
    expect(Object.keys(schema.$defs.element.properties).sort()).toEqual([...ELEMENT_FIELDS].sort());
    expect(Object.keys(schema.$defs.check.properties).sort()).toEqual([...CHECK_FIELDS].sort());
    expect(Object.keys(schema.$defs.repeat.properties).sort()).toEqual([...REPEAT_FIELDS].sort());
    expect(schema.properties.spec.const).toBe("composer/v1");
  });

  it("the Doc B §3.1 sample validates", () => {
    const sample = {
      spec: "composer/v1",
      minor: 0,
      root: "form",
      elements: {
        form: { type: "Form", props: { layout: "horizontal", labelWidth: 120 }, children: ["g-basic"] },
        "g-basic": { type: "FieldGroup", props: { title: "基础" }, children: ["f-mode", "f-signal"] },
        "f-mode": {
          type: "Select",
          props: {
            label: "Mode",
            options: [
              { value: "signal", label: "Signal" },
              { value: "timer", label: "Timer" }
            ],
            value: { $bindState: "/parameters/mode" },
            defaultHint: "signal"
          },
          checks: [{ type: "required", message: "必填" }]
        },
        "f-signal": {
          type: "Input",
          props: { label: "Signal Name", value: { $bindState: "/parameters/signal_name" } },
          visible: { $state: "/parameters/mode", in: ["signal", null] }
        }
      }
    };
    expect(validateSpec(sample)).toEqual([]);
  });
});
