// composer/form acceptance (Doc B §10 B4), run with both kernels under
// StrictMode through the real <Composer>: for every value component —
// controlled read/write, clear => unset, zero patches on mount (normal,
// unset, null and wrong-shape values), defaultHint, readOnly, props errors.

import { fireEvent, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { zodProps, type ComposerSpec, type ElementDef, type Patch } from "../core";
import { createRegistry, type ComposerComponent } from "../react";
import { allPatches, mountComposer } from "../testing/harness";
import { change, clickButton, clickClear, fieldOf, formSpec, kernels, one, pickOption, shownText } from "./form.testkit";
import { createFormRegistry, FieldLayoutContext, formComponents, Input } from "./index";

const registry = createFormRegistry();

/**
 * A slot that hides its children's labels the way ExpressionInput's literal
 * control and KeyValue's value editors do. It is test-only: no form component
 * owns this shape, but a container must still know which form it lives in.
 */
const InlineSlot: ComposerComponent<Record<string, never>> = {
  type: "InlineSlot",
  props: zodProps(z.strictObject({})),
  bindings: {},
  render: ({ children }) => <FieldLayoutContext.Provider value="inline">{children}</FieldLayoutContext.Provider>
};
const slotRegistry = createFormRegistry([InlineSlot]);

interface FieldCase {
  type: string;
  props?: Record<string, unknown>;
  /** Extra structure for containers (children / repeat), given the element id and bound path. */
  structure?(id: string, path: string): { element: Partial<ElementDef>; elements: Record<string, ElementDef> };
  normal: unknown;
  /** A defined value of the wrong shape; undefined when every value is valid (JsonEditor). */
  wrong?: unknown;
  /** Whether null is shown through the raw-value fallback (default true). */
  nullIsRaw?: boolean;
  /** Asserts the normal value is displayed. */
  shows(field: HTMLElement): void;
  edit(field: HTMLElement): void;
  edited: Patch[];
  clear(field: HTMLElement): void;
  hint?: { value: unknown; text: string };
  /** Asserts the control is not editable. */
  locked(field: HTMLElement): void;
}

const opts = [
  { value: "a", label: "A" },
  { value: "b", label: "B" }
];
const input = (field: HTMLElement) => one<HTMLInputElement>(field, "input");
const textarea = (field: HTMLElement) => one<HTMLTextAreaElement>(field, "textarea");
const set = (value: unknown, path = "/v"): Patch[] => [{ op: "set", path, value }];
const unset: Patch[] = [{ op: "unset", path: "/v" }];

const textCase = (type: string, get: (field: HTMLElement) => HTMLInputElement | HTMLTextAreaElement, props?: Record<string, unknown>): FieldCase => ({
  type,
  props,
  normal: "hello",
  wrong: 42,
  shows: (field) => expect(get(field).value).toBe("hello"),
  edit: (field) => change(get(field), "world"),
  edited: set("world"),
  clear: (field) => change(get(field), ""),
  hint: { value: "dflt", text: "默认：dflt" },
  locked: (field) => expect(get(field).readOnly).toBe(true)
});

const cases: FieldCase[] = [
  textCase("Input", input),
  textCase("TextArea", textarea),
  textCase("Password", (field) => one<HTMLInputElement>(field, 'input[type="password"]')),
  textCase("CodeEditor", textarea, { language: "python" }),
  {
    type: "InputNumber",
    normal: 3,
    wrong: "3",
    shows: (field) => expect(input(field).value).toBe("3"),
    edit: (field) => change(input(field), "7"),
    edited: set(7),
    clear: (field) => change(input(field), ""),
    hint: { value: 5, text: "默认：5" },
    locked: (field) => expect(input(field).readOnly).toBe(true)
  },
  {
    type: "Switch",
    normal: true,
    wrong: "yes",
    shows: (field) => expect(one(field, '[role="switch"]').getAttribute("aria-checked")).toBe("true"),
    edit: (field) => fireEvent.click(one(field, '[role="switch"]')),
    edited: set(false),
    clear: (field) => clickButton(field, /清除$/),
    hint: { value: true, text: "开（默认）" },
    locked: (field) => expect(one<HTMLButtonElement>(field, '[role="switch"]').disabled).toBe(true)
  },
  {
    type: "Select",
    props: { options: opts },
    normal: "a",
    wrong: { x: 1 },
    shows: (field) => expect(field.textContent).toContain("A"),
    edit: (field) => pickOption(field, "B"),
    edited: set("b"),
    clear: clickClear,
    hint: { value: "b", text: "默认：B" },
    locked: (field) => expect(one<HTMLInputElement>(field, '[role="combobox"]').disabled).toBe(true)
  },
  {
    type: "Radio",
    props: { options: [...opts, { value: 1, label: "One" }] },
    normal: "a",
    wrong: ["a"],
    shows: (field) => expect(field.querySelectorAll<HTMLInputElement>('input[type="radio"]')[0].checked).toBe(true),
    edit: (field) => fireEvent.click(field.querySelectorAll('input[type="radio"]')[2]),
    edited: set(1),
    clear: (field) => clickButton(field, /清除$/),
    hint: { value: 1, text: "默认：One" },
    locked: (field) =>
      field.querySelectorAll<HTMLInputElement>('input[type="radio"]').forEach((radio) => expect(radio.disabled).toBe(true))
  },
  {
    type: "MultiSelect",
    props: { options: opts },
    normal: ["a"],
    wrong: "a",
    shows: (field) => expect(field.textContent).toContain("A"),
    edit: (field) => pickOption(field, "B"),
    edited: set(["a", "b"]),
    clear: clickClear,
    hint: { value: ["b"], text: "默认：B" },
    locked: (field) => expect(one<HTMLInputElement>(field, '[role="combobox"]').disabled).toBe(true)
  },
  {
    type: "Tags",
    normal: ["x"],
    wrong: [1],
    shows: (field) => expect(field.textContent).toContain("x"),
    edit: (field) => {
      const search = one<HTMLInputElement>(field, '[role="combobox"]');
      change(search, "y");
      fireEvent.keyDown(search, { key: "Enter", keyCode: 13 });
    },
    edited: set(["x", "y"]),
    clear: clickClear,
    hint: { value: ["d"], text: "默认：d" },
    locked: (field) => expect(one<HTMLInputElement>(field, '[role="combobox"]').disabled).toBe(true)
  },
  {
    type: "KeyValue",
    normal: { a: "1" },
    wrong: { a: 1 },
    shows: (field) => {
      expect(within(field).getByLabelText("第 1 行的键")).toHaveProperty("value", "a");
      expect(within(field).getByLabelText("a 的值")).toHaveProperty("value", "1");
    },
    edit: (field) => change(within(field).getByLabelText("a 的值"), "2"),
    edited: set({ a: "2" }),
    clear: (field) => clickButton(field, "删除 a"),
    hint: { value: { h: "v" }, text: '默认：{"h":"v"}' },
    locked: (field) => {
      expect((within(field).getByLabelText("第 1 行的键") as HTMLInputElement).readOnly).toBe(true);
      expect(field.querySelector(".xflow-composer-add")).toBeNull();
    }
  },
  {
    type: "ObjectGroup",
    props: { clearable: true },
    structure: (id, path) => ({
      element: { children: [`${id}-x`] },
      elements: { [`${id}-x`]: { type: "Input", props: { label: "X", value: { $bindState: `${path}/x` } } } }
    }),
    normal: { x: "1" },
    wrong: "str",
    shows: (field) => expect(within(field).getByLabelText("X")).toHaveProperty("value", "1"),
    edit: (field) => change(within(field).getByLabelText("X"), "2"),
    edited: set("2", "/v/x"),
    clear: (field) => clickButton(field, "清空"),
    locked: (field) => {
      expect((within(field).getByLabelText("X") as HTMLInputElement).readOnly).toBe(true);
      expect(within(field).queryByText("清空")).toBeNull();
    }
  },
  {
    type: "ArrayTable",
    props: { columns: ["Name"] },
    structure: (id, path) => ({
      element: { repeat: { statePath: path }, children: [`${id}-name`] },
      elements: { [`${id}-name`]: { type: "Input", props: { label: "Name", value: { $bindItem: "name" } } } }
    }),
    normal: [{ name: "a" }],
    wrong: "str",
    nullIsRaw: false,
    shows: (field) => expect(within(field).getByLabelText("Name")).toHaveProperty("value", "a"),
    edit: (field) => clickButton(field, "添加"),
    edited: set([{ name: "a" }, {}]),
    clear: (field) => clickButton(field, "删除第 1 行"),
    hint: { value: [{ name: "d" }], text: '默认：[{"name":"d"}]' },
    locked: (field) => {
      expect(within(field).queryByText("添加")).toBeNull();
      expect(field.querySelector('[aria-label^="删除"]')).toBeNull();
    }
  },
  {
    type: "JsonEditor",
    normal: { a: 1 },
    nullIsRaw: false,
    shows: (field) => expect(JSON.parse(textarea(field).value)).toEqual({ a: 1 }),
    edit: (field) => change(textarea(field), '{"a":2}'),
    edited: set({ a: 2 }),
    clear: (field) => change(textarea(field), ""),
    hint: { value: { a: 0 }, text: '默认：{"a":0}' },
    locked: (field) => expect(textarea(field).readOnly).toBe(true)
  }
];

function fieldElements(c: FieldCase, id: string, path: string, extra: Record<string, unknown> = {}): Record<string, ElementDef> {
  const structure = c.structure?.(id, path);
  const element: ElementDef = {
    type: c.type,
    props: { label: "L", ...c.props, value: { $bindState: path }, ...extra },
    ...structure?.element
  };
  return { [id]: element, ...structure?.elements };
}

function single(c: FieldCase, extra?: Record<string, unknown>) {
  return formSpec(fieldElements(c, "f", "/v", extra), ["f"]);
}

let consoleError: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  consoleError = vi.spyOn(console, "error");
});
afterEach(() => consoleError.mockRestore());

for (const kernel of kernels) {
  describe(`composer/form with ${kernel.name}`, () => {
    for (const c of cases) {
      describe(c.type, () => {
        it("writes zero patches on mount for normal, unset, null and wrong-shape values", async () => {
          const elements = {
            ...fieldElements(c, "a", "/a"),
            ...fieldElements(c, "b", "/b"),
            ...fieldElements(c, "c", "/c"),
            ...(c.wrong !== undefined ? fieldElements(c, "d", "/d") : {})
          };
          const value: Record<string, unknown> = { a: c.normal, c: null };
          if (c.wrong !== undefined) value.d = c.wrong;
          const m = mountComposer({ kernel, registry, spec: formSpec(elements, Object.keys(elements).filter((k) => k.length === 1)), value });
          await m.flush();
          await m.setValue((v) => ({ ...v })); // a host re-render must not write either
          expect(m.log).toEqual([]);
          expect(m.warnings).toEqual([]);
          expect(consoleError).not.toHaveBeenCalled();
          c.shows(fieldOf(m.container, "a"));
          const nullField = fieldOf(m.container, "c");
          expect(nullField.querySelector("[data-composer-raw]") !== null).toBe(c.nullIsRaw !== false);
          if (c.wrong !== undefined) {
            const wrong = fieldOf(m.container, "d");
            const fallback = wrong.querySelector("[data-composer-raw]") ?? (wrong.matches("[data-composer-placeholder]") ? wrong : null);
            expect(fallback, "wrong-shape value is shown read-only").not.toBeNull();
            expect(wrong.textContent).toContain(JSON.stringify(c.wrong, null, 2).split("\n")[0]);
          }
        });

        it("reads and writes the controlled value", async () => {
          const m = mountComposer({ kernel, registry, spec: single(c), value: { v: c.normal } });
          await m.flush();
          c.shows(fieldOf(m.container, "f"));
          c.edit(fieldOf(m.container, "f"));
          await m.flush();
          expect(allPatches(m.log)).toEqual(c.edited);
        });

        it("clearing writes unset", async () => {
          const m = mountComposer({ kernel, registry, spec: single(c), value: { v: c.normal, keep: 1 } });
          await m.flush();
          c.clear(fieldOf(m.container, "f"));
          await m.flush();
          expect(allPatches(m.log)).toEqual(unset);
          expect(m.value).toEqual({ keep: 1 });
        });

        if (c.hint) {
          const hint = c.hint;
          it("shows defaultHint while unset without writing it", async () => {
            const m = mountComposer({ kernel, registry, spec: single(c, { defaultHint: hint.value }), value: {} });
            await m.flush();
            expect(shownText(fieldOf(m.container, "f"))).toContain(hint.text);
            expect(m.log).toEqual([]);
          });
        }

        it("respects readOnly", async () => {
          const m = mountComposer({ kernel, registry, spec: single(c), value: { v: c.normal }, readOnly: true });
          await m.flush();
          const field = fieldOf(m.container, "f");
          c.locked(field);
          try {
            c.edit(field);
          } catch {
            // the control offers no way to edit: fine
          }
          await m.flush();
          expect(m.log).toEqual([]);
        });

        it("renders the ElementError placeholder for invalid props", async () => {
          const m = mountComposer({ kernel, registry, spec: single(c, { bogus: 1 }), value: { v: c.normal } });
          await m.flush();
          const field = fieldOf(m.container, "f");
          expect(field.getAttribute("data-composer-placeholder")).toBe("error");
          expect(field.textContent).toContain("bogus");
          expect(m.log).toEqual([]);
        });

        if (c.wrong !== undefined && c.type !== "ArrayTable") {
          it("clears a wrong-shape value only on explicit request", async () => {
            const m = mountComposer({ kernel, registry, spec: single(c), value: { v: c.wrong } });
            await m.flush();
            clickButton(fieldOf(m.container, "f"), "清除此值");
            await m.flush();
            expect(allPatches(m.log)).toEqual(unset);
          });
        }
      });
    }

    describe("horizontal label placement (§6.4)", () => {
      const horizontalSpec = (elements: Record<string, ElementDef>, children: string[]): ComposerSpec => ({
        spec: "composer/v1",
        root: "form",
        elements: { form: { type: "Form", props: { layout: "horizontal" }, children }, ...elements }
      });
      const has = (container: HTMLElement, id: string, name: string) =>
        fieldOf(container, id).classList.contains(`xflow-composer-field--label-${name}`);

      it("defaults to the side column and keeps full-row controls on top", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            {
              input: { type: "Input", props: { label: "输入", value: { $bindState: "/input" } } },
              sw: { type: "Switch", props: { label: "开关", value: { $bindState: "/sw" } } },
              area: { type: "TextArea", props: { label: "备注", value: { $bindState: "/area" } } },
              code: { type: "CodeEditor", props: { label: "代码", value: { $bindState: "/code" } } },
              json: { type: "JsonEditor", props: { label: "JSON", value: { $bindState: "/json" } } },
              kv: { type: "KeyValue", props: { label: "映射", value: { $bindState: "/kv" } } },
              table: { type: "ArrayTable", props: { label: "规则", value: { $bindState: "/table" } } }
            },
            ["input", "sw", "area", "code", "json", "kv", "table"]
          ),
          value: {}
        });
        await m.flush();
        expect(has(m.container, "input", "side")).toBe(true);
        expect(has(m.container, "sw", "side")).toBe(true);
        for (const id of ["area", "code", "json", "kv", "table"]) {
          expect(has(m.container, id, "top"), id).toBe(true);
          expect(has(m.container, id, "side"), id).toBe(false);
        }
        expect(m.log).toEqual([]);
      });

      it("lets a field override either default", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            {
              input: { type: "Input", props: { label: "输入", labelPlacement: "top", value: { $bindState: "/input" } } },
              area: { type: "TextArea", props: { label: "备注", labelPlacement: "side", value: { $bindState: "/area" } } }
            },
            ["input", "area"]
          ),
          value: {}
        });
        await m.flush();
        expect(has(m.container, "input", "top")).toBe(true);
        expect(has(m.container, "input", "side")).toBe(false);
        expect(has(m.container, "area", "side")).toBe(true);
        expect(has(m.container, "area", "top")).toBe(false);
      });

      it("moves a Switch label into the left column", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            { sw: { type: "Switch", props: { label: "开关", value: { $bindState: "/sw" } } } },
            ["sw"]
          ),
          value: {}
        });
        await m.flush();
        const field = fieldOf(m.container, "sw");
        const direct = [...field.children].filter((el) => el.classList.contains("xflow-composer-field-label"));
        expect(direct).toHaveLength(1);
        const row = one<HTMLElement>(field, ".xflow-composer-switch-row");
        expect(row.querySelector(".xflow-composer-field-label")).toBeNull();
        expect(direct[0]!.getAttribute("for")).toBe(one<HTMLElement>(row, '[role="switch"]').id);
      });

      it("keeps the Switch label inside the row in a vertical form", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ sw: { type: "Switch", props: { label: "开关", value: { $bindState: "/sw" } } } }, ["sw"]),
          value: {}
        });
        await m.flush();
        const field = fieldOf(m.container, "sw");
        expect(field.classList.contains("xflow-composer-field--label-side")).toBe(false);
        const row = one<HTMLElement>(field, ".xflow-composer-switch-row");
        expect(row.querySelector(".xflow-composer-field-label")).not.toBeNull();
        expect([...field.children].some((el) => el.classList.contains("xflow-composer-field-label"))).toBe(false);
      });

      it("stacks a top-placed Switch label above its row", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            { sw: { type: "Switch", props: { label: "开关", labelPlacement: "top", value: { $bindState: "/sw" } } } },
            ["sw"]
          ),
          value: {}
        });
        await m.flush();
        const field = fieldOf(m.container, "sw");
        expect(has(m.container, "sw", "top")).toBe(true);
        expect(has(m.container, "sw", "side")).toBe(false);
        const direct = [...field.children].filter((el) => el.classList.contains("xflow-composer-field-label"));
        expect(direct).toHaveLength(1);
        const row = one<HTMLElement>(field, ".xflow-composer-switch-row");
        expect(row.querySelector(".xflow-composer-field-label")).toBeNull();
        expect(direct[0]!.getAttribute("for")).toBe(one<HTMLElement>(row, '[role="switch"]').id);
        expect(m.log).toEqual([]);
      });

      it("puts a Radio label in the left column and the group in the control column", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            {
              radio: {
                type: "Radio",
                props: { label: "审批模式", options: opts, value: { $bindState: "/radio" } }
              }
            },
            ["radio"]
          ),
          value: { radio: "a" }
        });
        await m.flush();
        expect(has(m.container, "radio", "side")).toBe(true);
        const field = fieldOf(m.container, "radio");
        const label = [...field.children].find((el) => el.classList.contains("xflow-composer-field-label")) as HTMLElement;
        const body = [...field.children].find((el) => el.classList.contains("xflow-composer-field-body")) as HTMLElement;
        expect(label).toBeTruthy();
        expect(body).toBeTruthy();
        expect(label.nextElementSibling).toBe(body);
        const row = one<HTMLElement>(body, ".xflow-composer-radio-row");
        expect(row.querySelector(".xflow-composer-field-label")).toBeNull();
        expect(one<HTMLElement>(row, '[role="radiogroup"]').getAttribute("aria-labelledby")).toBe(label.id);
        expect(m.log).toEqual([]);
      });

      it("keeps an unlabelled field in the control column", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec({ bare: { type: "Input", props: { value: { $bindState: "/bare" } } } }, ["bare"]),
          value: {}
        });
        await m.flush();
        const field = fieldOf(m.container, "bare");
        expect(field.classList.contains("xflow-composer-field--label-none")).toBe(true);
        expect(field.querySelector(".xflow-composer-field-label")).toBeNull();
        expect([...field.children].some((el) => el.classList.contains("xflow-composer-field-body"))).toBe(true);
      });

      it("keeps an ObjectGroup's fields in the enclosing horizontal layout", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: horizontalSpec(
            {
              og: {
                type: "ObjectGroup",
                props: { label: "对象", value: { $bindState: "/og" } },
                children: ["inner"]
              },
              inner: { type: "Input", props: { label: "内层", value: { $bindState: "/og/inner" } } }
            },
            ["og"]
          ),
          value: { og: { inner: "x" } }
        });
        await m.flush();
        const group = fieldOf(m.container, "og");
        expect(group.classList.contains("xflow-composer-object-group")).toBe(true);
        expect(group.classList.contains("xflow-composer-field")).toBe(false);
        expect(fieldOf(m.container, "inner").classList.contains("xflow-composer-field--label-side")).toBe(true);
        expect(m.log).toEqual([]);
      });

      // §6.4 (2026-11-06): an inline slot hides the label; it must not flatten
      // the form's direction. The real case is an ObjectGroup inside
      // ExpressionInput's literal control (e.g. kafka's aggregate/tuning).
      const slottedSpec = (layout: "horizontal" | "vertical"): ComposerSpec => ({
        spec: "composer/v1",
        root: "form",
        elements: {
          form: { type: "Form", props: { layout }, children: ["slot"] },
          slot: { type: "InlineSlot", children: ["og"] },
          og: { type: "ObjectGroup", props: { label: "对象", value: { $bindState: "/og" } }, children: ["inner"] },
          inner: { type: "Input", props: { label: "内层", value: { $bindState: "/og/inner" } } }
        }
      });

      for (const layout of ["horizontal", "vertical"] as const) {
        it(`keeps an inline-slot ObjectGroup's fields in the enclosing ${layout} layout`, async () => {
          const m = mountComposer({
            kernel,
            registry: slotRegistry,
            spec: slottedSpec(layout),
            value: { og: { inner: "x" } }
          });
          await m.flush();
          const inner = fieldOf(m.container, "inner");
          expect(inner.classList.contains("xflow-composer-field--label-side")).toBe(layout === "horizontal");
          expect(m.log).toEqual([]);
        });
      }

      it("keeps an inline-slot ObjectGroup's fields stacked inside an ArrayTable cell", async () => {
        const m = mountComposer({
          kernel,
          registry: slotRegistry,
          spec: {
            spec: "composer/v1",
            root: "form",
            elements: {
              form: { type: "Form", props: { layout: "horizontal" }, children: ["rows"] },
              rows: {
                type: "ArrayTable",
                props: { label: "规则", value: { $bindState: "/rows" }, newRow: {} },
                repeat: { statePath: "/rows" },
                children: ["slot"]
              },
              slot: { type: "InlineSlot", children: ["og"] },
              og: { type: "ObjectGroup", props: { label: "对象", value: { $bindItem: "og" } }, children: ["inner"] },
              inner: { type: "Input", props: { label: "内层", value: { $bindItem: "og/inner" } } }
            }
          },
          value: { rows: [{ og: { inner: "x" } }] }
        });
        await m.flush();
        // Repeat rows suffix the element id (@rows#rN), so match the prefix.
        const inner = m.container.querySelector<HTMLElement>('[data-composer-node^="inner@"]');
        expect(inner).not.toBeNull();
        expect(inner!.classList.contains("xflow-composer-field--label-side")).toBe(false);
        expect(m.log).toEqual([]);
      });
    });

    describe("structure and placeholders", () => {
      it("renders Form / FieldGroup with zero patches and labels associated", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: {
            spec: "composer/v1",
            root: "form",
            elements: {
              form: { type: "Form", props: { layout: "horizontal", labelWidth: 96, title: "节点" }, children: ["g"] },
              g: { type: "FieldGroup", props: { title: "基础", summary: "继承工作流 · 重试 2 次", collapsible: true }, children: ["name"] },
              name: {
                type: "Input",
                props: { label: "Name", required: true, description: "说明", value: { $bindState: "/name" } },
                checks: [{ type: "required", message: "必填" }]
              }
            }
          },
          value: {}
        });
        await m.flush();
        expect(m.log).toEqual([]);
        const form = fieldOf(m.container, "form");
        expect(form.getAttribute("style")).toContain("--xflow-composer-label-width: 96px");
        expect(within(m.container).getByRole("group", { name: "节点" })).toBe(form);
        // The section header carries a short right-aligned status line.
        expect(within(form).getByText("继承工作流 · 重试 2 次")).toBeTruthy();
        const control = within(form).getByLabelText(/^Name/) as HTMLInputElement;
        expect(control.tagName).toBe("INPUT");
        expect(fieldOf(m.container, "name").querySelector(".xflow-composer-field-required")).not.toBeNull();
        expect(control.getAttribute("aria-invalid")).toBe("true");
        const described = (control.getAttribute("aria-describedby") ?? "").split(" ").map((id) => document.getElementById(id)?.textContent);
        expect(described).toEqual(["说明", "必填"]);
        // Collapsing keeps the content mounted.
        clickButton(fieldOf(m.container, "g"), /基础/);
        expect(control.isConnected).toBe(true);
        expect(one<HTMLElement>(fieldOf(m.container, "g"), ".xflow-composer-section-body").hidden).toBe(true);
      });

      const describedSpec = (descriptionPlacement?: "below" | "tooltip") => ({
        spec: "composer/v1" as const,
        root: "form",
        elements: {
          form: { type: "Form", props: descriptionPlacement ? { descriptionPlacement } : {}, children: ["f", "bare"] },
          f: { type: "Input", props: { label: "Name", description: "说明", value: { $bindState: "/v" } } },
          // No label: nothing to put the help icon next to, so it stays below.
          bare: { type: "Input", props: { description: "无标签说明", value: { $bindState: "/w" } } }
        }
      });

      it('descriptionPlacement "below" (default) shows the description under the control', async () => {
        for (const placement of [undefined, "below"] as const) {
          const m = mountComposer({ kernel, registry, spec: describedSpec(placement), value: {} });
          await m.flush();
          const field = fieldOf(m.container, "f");
          const description = one<HTMLElement>(field, ".xflow-composer-field-description");
          expect(description.textContent).toBe("说明");
          expect(description.classList.contains("xflow-composer-visually-hidden")).toBe(false);
          expect(field.querySelector(".xflow-composer-field-help")).toBeNull();
          expect(m.log).toEqual([]);
          m.unmount();
        }
      });

      it('descriptionPlacement "tooltip" moves the description behind a help icon by the label', async () => {
        const m = mountComposer({ kernel, registry, spec: describedSpec("tooltip"), value: {} });
        await m.flush();
        expect(m.log).toEqual([]);
        const field = fieldOf(m.container, "f");
        const help = one<HTMLButtonElement>(field, ".xflow-composer-field-label-row > .xflow-composer-field-help");
        expect(help.getAttribute("aria-label")).toBe("Name说明");
        expect(help.type).toBe("button");
        // Not rendered visibly below the control, but still the control's description.
        const description = one<HTMLElement>(field, ".xflow-composer-field-description");
        expect(description.classList.contains("xflow-composer-visually-hidden")).toBe(true);
        const control = within(field).getByLabelText("Name");
        const described = (control.getAttribute("aria-describedby") ?? "").split(" ").map((id) => document.getElementById(id)?.textContent);
        expect(described).toEqual(["说明"]);
        fireEvent.mouseEnter(help);
        await waitFor(() => expect(document.querySelector(".xflow-composer-field-help-tooltip")?.textContent).toBe("说明"));
        // A label-less field keeps the description below.
        const bare = fieldOf(m.container, "bare");
        expect(bare.querySelector(".xflow-composer-field-help")).toBeNull();
        expect(one<HTMLElement>(bare, ".xflow-composer-field-description").classList.contains("xflow-composer-visually-hidden")).toBe(false);
      });

      it("renders warnings under the field", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({
            f: {
              type: "Input",
              props: { label: "L", value: { $bindState: "/v" } },
              checks: [{ type: "minLength", args: { value: 5 }, message: "太短", severity: "warning" }]
            }
          }),
          value: { v: "ab" }
        });
        await m.flush();
        const field = fieldOf(m.container, "f");
        expect(one(field, ".xflow-composer-issue--warning").textContent).toBe("太短");
        expect(field.classList.contains("is-warning")).toBe(true);
        expect(input(field).getAttribute("aria-invalid")).toBeNull();
        expect(input(field).getAttribute("aria-describedby")).toBe(one(field, ".xflow-composer-field-issues").id);
      });

      it("renders Unsupported for unknown types, keeping siblings", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({
            x: { type: "Nope", props: { value: { $bindState: "/v" } } },
            ok: { type: "Input", props: { label: "OK", value: { $bindState: "/ok" } } }
          }),
          value: { v: [1, 2], ok: "fine" }
        });
        await m.flush();
        const x = fieldOf(m.container, "x");
        expect(x.getAttribute("data-composer-placeholder")).toBe("unsupported");
        expect(x.textContent).toContain("Nope");
        expect(within(m.container).getByLabelText("OK")).toHaveProperty("value", "fine");
        expect(m.log).toEqual([]);
      });

      it("Switch shows 关（默认） for a false default", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ s: { type: "Switch", props: { label: "S", value: { $bindState: "/s" }, defaultHint: false } } }),
          value: {}
        });
        await m.flush();
        expect(fieldOf(m.container, "s").textContent).toContain("关（默认）");
        fireEvent.click(one(fieldOf(m.container, "s"), '[role="switch"]'));
        await m.flush();
        expect(allPatches(m.log)).toEqual([{ op: "set", path: "/s", value: true }]);
      });
    });
  });
}

describe("multiline textarea height (C12)", () => {
  for (const kernel of kernels) {
    it(`keeps an empty TextArea at the mockup's 64px floor (${kernel.name})`, async () => {
      const m = mountComposer({
        kernel,
        registry,
        spec: formSpec({ f: { type: "TextArea", props: { label: "备注", value: { $bindState: "/v" } } } }, ["f"]),
        value: { v: "" }
      });
      await m.flush();

      const area = textarea(fieldOf(m.container, "f"));
      // C12: the mockup's .ctl.area floor. It is inline because rc-textarea
      // merges its own autoSize.minHeight after the style prop; autoSize keeps
      // only the 16-row ceiling so the field still grows with its content.
      expect(area.style.minHeight).toBe("var(--xflow-composer-textarea-min-height, 64px)");
      expect(consoleError).not.toHaveBeenCalled();
    });
  }
});

describe("option descriptions", () => {
  const described = [
    { value: "a", label: "A", description: "first choice" },
    { value: "b", label: "B" }
  ];
  const mount = (type: string, kernel: (typeof kernels)[number]) =>
    mountComposer({
      kernel,
      registry,
      spec: formSpec({ f: { type, props: { label: "L", options: described, value: { $bindState: "/v" } } } }, ["f"]),
      value: { v: "a" }
    });

  for (const kernel of kernels) {
    it(`Select shows a described option as label plus a second line (${kernel.name})`, async () => {
      const m = mount("Select", kernel);
      await m.flush();
      const field = fieldOf(m.container, "f");
      // The selected value shows the label alone.
      expect(field.querySelector(".xflow-composer-option-description")).toBeNull();
      fireEvent.mouseDown(one(field, '[role="combobox"]'));
      const popup = [...document.querySelectorAll(".xflow-composer-select-popup")].at(-1);
      expect(popup?.querySelector(".xflow-composer-option-description")?.textContent).toBe("first choice");
      expect(popup?.querySelectorAll(".xflow-composer-option-description")).toHaveLength(1);
      pickOption(field, "B");
      await m.flush();
      expect(allPatches(m.log)).toEqual(set("b"));
      expect(consoleError).not.toHaveBeenCalled();
    });

    it(`Radio carries the description as the option tooltip (${kernel.name})`, async () => {
      const m = mount("Radio", kernel);
      await m.flush();
      const labels = [...fieldOf(m.container, "f").querySelectorAll("label.ant-radio-wrapper")];
      expect(labels.map((label) => label.getAttribute("title"))).toEqual(["first choice", null]);
      expect(consoleError).not.toHaveBeenCalled();
    });
  }
});

describe("registry", () => {
  it("formComponents covers every built-in type", () => {
    expect(formComponents.map((c) => c.type).sort()).toEqual(
      [
        "ArrayTable",
        "CodeEditor",
        "ElementError",
        "FieldGroup",
        "Form",
        "Input",
        "InputNumber",
        "JsonEditor",
        "KeyValue",
        "MultiSelect",
        "ObjectGroup",
        "Password",
        "Radio",
        "Select",
        "Switch",
        "Tags",
        "TextArea",
        "Unsupported"
      ].sort()
    );
    expect(createFormRegistry().types).toHaveLength(18);
  });

  it("createFormRegistry(extra) lets hosts add and override components", () => {
    const debug = vi.fn();
    const Custom: ComposerComponent = { type: "Custom", props: Input.props as ComposerComponent["props"], render: () => null };
    const MyInput: ComposerComponent = { ...Custom, type: "Input" };
    const reg = createFormRegistry([Custom, MyInput], { debug });
    expect(reg.get("Custom")).toBe(Custom);
    expect(reg.get("Input")).toBe(MyInput);
    expect(reg.get("Select")).toBe(formComponents.find((c) => c.type === "Select"));
    expect(debug).toHaveBeenCalledWith(expect.stringContaining("Input"));
    expect(() => createRegistry([Custom, Custom])).toThrow();
  });
});
