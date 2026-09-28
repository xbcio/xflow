// C2 host components (Doc C §7 C2), with both kernels under StrictMode
// through the real <Composer> and the real node-form registry: controlled
// read/write, clear => unset, zero patches on mount (normal, unset, null,
// wrong-shape and expression values), defaultHint shown and never written,
// readOnly respected, strict props.

import { fireEvent, within } from "@testing-library/react";
import type { ElementDef, Patch } from "@xflow/composer/core";
import { allPatches, mountComposer } from "@xflow/composer/testing";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { nodeFormChecks } from "../checks";
import { change, clickButton, fieldOf, formSpec, kernels, one, pickOption, shownText } from "../testdata/testkit";
import { createNodeFormRegistry, formatGoDuration, humanizeNs, isRfc3339, matchesShape, parseGoDuration } from "./index";

const registry = createNodeFormRegistry();

interface FieldCase {
  name: string;
  type: string;
  props?: Record<string, unknown>;
  structure?(id: string, path: string): { element: Partial<ElementDef>; elements: Record<string, ElementDef> };
  normal: unknown;
  wrong: unknown;
  /** Selector proving the wrong-shape / null value is shown read-only. */
  rawSelector: string;
  shows(field: HTMLElement): void;
  edit(field: HTMLElement): void;
  edited: Patch[];
  clear(field: HTMLElement): void;
  hint?: { value: unknown; text: string };
  locked(field: HTMLElement): void;
}

const input = (field: HTMLElement) => one<HTMLInputElement>(field, "input");
const textarea = (field: HTMLElement) => one<HTMLTextAreaElement>(field, "textarea");
const set = (value: unknown, path = "/v"): Patch[] => [{ op: "set", path, value }];
const unset: Patch[] = [{ op: "unset", path: "/v" }];
const clickClear = (field: HTMLElement) => fireEvent.mouseDown(one(field, ".xflow-editor-nodeform-clear-icon"));
const combobox = (field: HTMLElement) => one<HTMLInputElement>(field, '[role="combobox"]');
const COMPOSER_RAW = "[data-composer-raw]";
const NODEFORM_RAW = "[data-nodeform-raw]";

const textCase = (name: string, type: string, normal: string, edited: string, extra: Partial<FieldCase> = {}): FieldCase => ({
  name,
  type,
  normal,
  wrong: 42,
  rawSelector: COMPOSER_RAW,
  shows: (field) => expect(input(field).value).toBe(normal),
  edit: (field) => change(input(field), edited),
  edited: set(edited),
  clear: (field) => change(input(field), ""),
  locked: (field) => expect(input(field).readOnly).toBe(true),
  ...extra
});

const credentials = ["db", "api"];

const cases: FieldCase[] = [
  {
    name: "ExpressionInput (pure)",
    type: "ExpressionInput",
    props: { mode: "pure", expect: { type: "array" } },
    normal: "${{ $input.items }}",
    wrong: 42,
    rawSelector: NODEFORM_RAW,
    shows: (field) => expect(textarea(field).value).toBe("${{ $input.items }}"),
    edit: (field) => change(textarea(field), "${{ $input.list }}"),
    edited: set("${{ $input.list }}"),
    clear: (field) => change(textarea(field), ""),
    hint: { value: "${{ $input }}", text: "默认：${{ $input }}" },
    locked: (field) => expect(textarea(field).readOnly).toBe(true)
  },
  {
    name: "ExpressionInput (template, literal slot)",
    type: "ExpressionInput",
    props: { mode: "template", fxToggle: true, expect: { type: "string" } },
    structure: (id, path) => ({
      element: { slots: { literal: [`${id}-lit`] } },
      elements: { [`${id}-lit`]: { type: "Input", props: { label: "L", value: { $bindState: path } } } }
    }),
    normal: "hello",
    wrong: 42,
    rawSelector: NODEFORM_RAW,
    shows: (field) => expect(input(field).value).toBe("hello"),
    edit: (field) => change(input(field), "world"),
    edited: set("world"),
    clear: (field) => change(input(field), ""),
    locked: (field) => expect(input(field).readOnly).toBe(true)
  },
  textCase("DurationInput (string)", "DurationInput", "90s", "2m", {
    props: { unit: "string" },
    hint: { value: "30s", text: "默认：30s" },
    shows: (field) => {
      expect(input(field).value).toBe("90s"); // never reformatted to 1m30s
      expect(field.textContent).toContain("= 1 分 30 秒");
    }
  }),
  {
    name: "DurationInput (ns)",
    type: "DurationInput",
    props: { unit: "ns" },
    normal: 90e9,
    wrong: "30s",
    rawSelector: COMPOSER_RAW,
    shows: (field) => {
      expect(input(field).value).toBe("90");
      expect(field.textContent).toContain("秒");
      expect(field.textContent).toContain("= 1m30s（线上值 90000000000 纳秒）");
    },
    edit: (field) => change(input(field), "2"),
    edited: set(2e9),
    clear: (field) => change(input(field), ""),
    hint: { value: 1e9, text: "默认：1s" },
    locked: (field) => expect(input(field).readOnly).toBe(true)
  },
  {
    name: "DurationInput (ms)",
    type: "DurationInput",
    props: { unit: "ms" },
    normal: 1500,
    wrong: "1.5s",
    rawSelector: COMPOSER_RAW,
    shows: (field) => {
      expect(input(field).value).toBe("1500");
      expect(field.textContent).toContain("毫秒");
    },
    edit: (field) => change(input(field), "250"),
    edited: set(250),
    clear: (field) => change(input(field), ""),
    hint: { value: 1000, text: "默认：1s" },
    locked: (field) => expect(input(field).readOnly).toBe(true)
  },
  textCase("DateTimeInput", "DateTimeInput", "2026-01-01T00:00:00Z", "2026-02-01T08:00:00+08:00", {
    hint: { value: "2026-01-01T00:00:00Z", text: "默认：2026-01-01T00:00:00Z" }
  }),
  textCase("CronInput", "CronInput", "*/5 * * * *", "0 3 * * *", {
    hint: { value: "@daily", text: "默认：@daily" },
    shows: (field) => {
      expect(input(field).value).toBe("*/5 * * * *");
      expect(field.textContent).toContain("cron 表达式有效");
    }
  }),
  {
    name: "CredentialSelect (single)",
    type: "CredentialSelect",
    props: { multiple: false, credentials },
    normal: "db",
    wrong: ["db"],
    rawSelector: COMPOSER_RAW,
    shows: (field) => expect(field.textContent).toContain("db"),
    edit: (field) => pickOption(field, "api"),
    edited: set("api"),
    clear: clickClear,
    hint: { value: "db", text: "默认：db" },
    locked: (field) => expect(combobox(field).disabled).toBe(true)
  },
  {
    name: "CredentialSelect (multiple)",
    type: "CredentialSelect",
    props: { multiple: true, credentials },
    normal: ["db"],
    wrong: "db",
    rawSelector: COMPOSER_RAW,
    shows: (field) => expect(field.textContent).toContain("db"),
    edit: (field) => pickOption(field, "api"),
    edited: set(["db", "api"]),
    clear: clickClear,
    locked: (field) => expect(combobox(field).disabled).toBe(true)
  },
  {
    name: "PortSelect",
    type: "PortSelect",
    props: { ports: [{ name: "a", display_name: "Alpha" }, { name: "b" }] },
    normal: "a",
    wrong: 1,
    rawSelector: COMPOSER_RAW,
    shows: (field) => expect(field.textContent).toContain("Alpha（a）"),
    edit: (field) => pickOption(field, "b"),
    edited: set("b"),
    clear: clickClear,
    hint: { value: "b", text: "默认：b" },
    locked: (field) => expect(combobox(field).disabled).toBe(true)
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

const single = (c: FieldCase, extra?: Record<string, unknown>) => formSpec(fieldElements(c, "f", "/v", extra), ["f"]);

let consoleError: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  consoleError = vi.spyOn(console, "error");
});
afterEach(() => consoleError.mockRestore());

for (const kernel of kernels) {
  describe(`node-form host components with ${kernel.name}`, () => {
    for (const c of cases) {
      describe(c.name, () => {
        it("writes zero patches on mount for normal, unset, null, wrong-shape and expression values", async () => {
          const elements = {
            ...fieldElements(c, "a", "/a"),
            ...fieldElements(c, "b", "/b"),
            ...fieldElements(c, "c", "/c"),
            ...fieldElements(c, "d", "/d"),
            ...fieldElements(c, "e", "/e")
          };
          const value = { a: c.normal, c: null, d: c.wrong, e: "pre ${{ $vars.x }} post" };
          const m = mountComposer({ kernel, registry, spec: formSpec(elements, ["a", "b", "c", "d", "e"]), value });
          await m.flush();
          await m.setValue((v) => ({ ...v })); // a host re-render must not write either
          expect(m.log).toEqual([]);
          expect(m.warnings).toEqual([]);
          expect(consoleError).not.toHaveBeenCalled();
          expect(m.value).toEqual(value);
          c.shows(fieldOf(m.container, "a"));
          expect(fieldOf(m.container, "c").querySelector(c.rawSelector), "null is shown read-only").not.toBeNull();
          expect(fieldOf(m.container, "d").querySelector(c.rawSelector), "wrong shape is shown read-only").not.toBeNull();
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
            expect(m.value).toEqual({});
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

        it("rejects an unknown prop (strict props schema)", async () => {
          const m = mountComposer({ kernel, registry, spec: single(c, { bogus: 1 }), value: { v: c.normal } });
          await m.flush();
          const field = fieldOf(m.container, "f");
          expect(field.getAttribute("data-composer-placeholder")).toBe("error");
          expect(m.log).toEqual([]);
        });
      });
    }

    describe("ExpressionInput template mode", () => {
      const spec = (expectType: string, literal: ElementDef) =>
        formSpec(
          {
            f: {
              type: "ExpressionInput",
              props: { label: "Quorum", value: { $bindState: "/v" }, mode: "template", fxToggle: true, expect: { type: expectType } },
              slots: { literal: ["lit"] }
            },
            lit: literal
          },
          ["f"]
        );
      const numberSpec = spec("number", { type: "InputNumber", props: { label: "Quorum", value: { $bindState: "/v" }, defaultHint: 3 } });

      it("opens a (partial) template in the expression state", async () => {
        for (const v of ["${{ $input.n }}", "n = {{ n }}!"]) {
          const m = mountComposer({ kernel, registry, spec: numberSpec, value: { v } });
          await m.flush();
          const field = fieldOf(m.container, "f");
          expect(textarea(field).value).toBe(v);
          expect(one(field, "button[aria-pressed]").getAttribute("aria-pressed")).toBe("true");
          expect(m.log).toEqual([]);
          m.unmount();
        }
      });

      it("the fx toggle only flips the display and writes nothing", async () => {
        const m = mountComposer({ kernel, registry, spec: numberSpec, value: { v: 5 } });
        await m.flush();
        const field = fieldOf(m.container, "f");
        expect(input(field).value).toBe("5");
        clickButton(field, /切换为表达式$/);
        await m.flush();
        expect(field.querySelector("textarea")).not.toBeNull();
        expect(field.textContent).toContain("输入表达式后会替换它");
        clickButton(field, /切换为普通值$/);
        await m.flush();
        expect(input(field).value).toBe("5");
        expect(m.log).toEqual([]);
      });

      it("writes the typed expression and stays in the expression state", async () => {
        const m = mountComposer({ kernel, registry, spec: numberSpec, value: {} });
        await m.flush();
        const field = fieldOf(m.container, "f");
        expect(shownText(field)).toContain("默认：3"); // defaultHint of the literal control
        clickButton(field, /切换为表达式$/);
        await m.flush();
        change(textarea(field), "${{ $vars.q }}");
        await m.flush();
        change(textarea(field), "${{ $vars.q");
        await m.flush();
        expect(allPatches(m.log)).toEqual([...set("${{ $vars.q }}"), ...set("${{ $vars.q")]);
        expect(field.querySelector("textarea")).not.toBeNull();
      });

      it("shows a wrong-shape literal read-only with the JSON-tab hint, without a toggle", async () => {
        const m = mountComposer({ kernel, registry, spec: spec("object", { type: "KeyValue", props: { value: { $bindState: "/v" } } }), value: { v: ["x"] } });
        await m.flush();
        const field = fieldOf(m.container, "f");
        expect(field.textContent).toContain("请在 JSON 页签修改");
        expect(field.querySelector("button[aria-pressed]")).toBeNull();
        expect(m.log).toEqual([]);
      });

      it("labels the literal control through the ExpressionInput (single visible label)", async () => {
        const m = mountComposer({ kernel, registry, spec: numberSpec, value: { v: 5 } });
        await m.flush();
        const field = fieldOf(m.container, "f");
        // The literal control is named by its own (visually hidden) label; the frame names the group.
        const named = within(field).getAllByLabelText("Quorum");
        expect(named).toContain(input(field));
        expect(named.some((el) => el.getAttribute("role") === "group")).toBe(true);
        expect(one(field, ".xflow-editor-nodeform-expression-literal")).toBeTruthy();
      });
    });

    describe("DurationInput display unit", () => {
      it("switching the unit re-expresses the value without writing; typing then uses the unit", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ f: { type: "DurationInput", props: { label: "间隔", unit: "ms", value: { $bindState: "/v" } } } }),
          value: { v: 1500 }
        });
        await m.flush();
        const field = fieldOf(m.container, "f");
        pickOption(field, "秒");
        await m.flush();
        expect(input(field).value).toBe("1.5");
        expect(m.log).toEqual([]);
        change(input(field), "3");
        await m.flush();
        expect(allPatches(m.log)).toEqual(set(3000));
      });
    });

    describe("DateTimeInput / CronInput notes", () => {
      it("flags an invalid value without writing or coercing it", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({
            t: { type: "DateTimeInput", props: { value: { $bindState: "/t" } } },
            c: { type: "CronInput", props: { value: { $bindState: "/c" } } }
          }),
          value: { t: "tomorrow", c: "61 * * * *" }
        });
        await m.flush();
        expect(input(fieldOf(m.container, "t")).value).toBe("tomorrow");
        expect(fieldOf(m.container, "t").textContent).toContain("需要 RFC 3339 时间");
        expect(fieldOf(m.container, "c").textContent).toContain("cron 表达式无效");
        expect(m.log).toEqual([]);
      });

      it("DateTimeInput fills the current time only on request", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ t: { type: "DateTimeInput", props: { value: { $bindState: "/t" } } } }),
          value: {}
        });
        await m.flush();
        clickButton(fieldOf(m.container, "t"), "当前时间");
        await m.flush();
        const [patch] = allPatches(m.log);
        expect(patch.op).toBe("set");
        expect(isRfc3339((patch as { value: string }).value)).toBe(true);
      });
    });

    describe("CredentialSelect / PortSelect lists", () => {
      it("reads credentials from /$ctx and notes a value missing from the list", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({
            f: {
              type: "CredentialSelect",
              props: { multiple: false, credentials: { $state: "/$ctx/credentials" }, value: { $bindState: "/v" } }
            }
          }),
          context: { credentials: ["db"] },
          value: { v: "gone" }
        });
        await m.flush();
        const field = fieldOf(m.container, "f");
        expect(field.textContent).toContain("不在工作流凭据中：gone");
        expect(field.querySelector('input[type="password"]')).toBeNull(); // no plaintext entry
        pickOption(field, "db");
        await m.flush();
        expect(allPatches(m.log)).toEqual(set("db"));
      });

      it("PortSelect reads /$ctx/ports and notes a dangling port", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ f: { type: "PortSelect", props: { ports: { $state: "/$ctx/ports" }, value: { $bindState: "/v" } } } }),
          context: { ports: [{ name: "ok" }] },
          value: { v: "removed" }
        });
        await m.flush();
        expect(fieldOf(m.container, "f").textContent).toContain("端口不存在：removed");
        expect(m.log).toEqual([]);
      });
    });

    describe("NodeNameInput", () => {
      const nameSpec = formSpec({ n: { type: "NodeNameInput", props: { label: "名称", value: { $state: "/name" } } } });

      it("never patches; renames through the callback on blur / Enter; Escape restores", async () => {
        const onRename = vi.fn();
        const m = mountComposer({ kernel, registry: createNodeFormRegistry({ onRename }), spec: nameSpec, value: { name: "n1" } });
        await m.flush();
        const field = fieldOf(m.container, "n");
        expect(input(field).value).toBe("n1");
        change(input(field), "n2");
        await m.flush();
        expect(input(field).value).toBe("n2");
        fireEvent.blur(input(field));
        expect(onRename).toHaveBeenCalledWith("n2");
        change(input(field), "n3");
        fireEvent.keyDown(input(field), { key: "Enter" });
        expect(onRename).toHaveBeenLastCalledWith("n3");
        change(input(field), "n4");
        fireEvent.keyDown(input(field), { key: "Escape" });
        await m.flush();
        expect(input(field).value).toBe("n1");
        fireEvent.blur(input(field));
        expect(onRename).toHaveBeenCalledTimes(2);
        await m.setValue(() => ({ name: "renamed" })); // the host applied a rename
        expect(input(field).value).toBe("renamed");
        expect(m.log).toEqual([]);
      });

      it("is read-only without a callback or under readOnly", async () => {
        const onRename = vi.fn();
        for (const [reg, readOnly] of [
          [createNodeFormRegistry(), false],
          [createNodeFormRegistry({ onRename }), true]
        ] as const) {
          const m = mountComposer({ kernel, registry: reg, spec: nameSpec, value: { name: "n1" }, readOnly });
          await m.flush();
          expect(input(fieldOf(m.container, "n")).readOnly).toBe(true);
          m.unmount();
        }
        expect(onRename).not.toHaveBeenCalled();
      });
    });

    describe("ShapeGuard", () => {
      const guarded = (expect: Record<string, unknown>, mode = "none") =>
        formSpec(
          {
            g: { type: "ShapeGuard", props: { label: "Items", observed: { $state: "/v" }, expect, mode }, children: ["c"] },
            c: { type: "JsonEditor", props: { label: "Items", value: { $bindState: "/v" } } }
          },
          ["g"]
        );

      it("renders the children when the value fits, and nothing else", async () => {
        for (const v of [undefined, ["a"]]) {
          const m = mountComposer({ kernel, registry, spec: guarded({ type: "array", item: "string" }), value: v === undefined ? {} : { v } });
          await m.flush();
          expect(m.container.querySelector("textarea")).not.toBeNull();
          expect(m.container.querySelector(NODEFORM_RAW)).toBeNull();
          m.unmount();
        }
      });

      it("shows a mismatching value read-only (JSON or expression text) and never writes", async () => {
        const cases: [unknown, string][] = [
          [null, "shape"],
          [{ a: 1 }, "shape"],
          [[1], "shape"],
          ["${{ $input.list }}", "expression"]
        ];
        for (const [v, reason] of cases) {
          const m = mountComposer({ kernel, registry, spec: guarded({ type: "array", item: "string" }), value: { v } });
          await m.flush();
          const raw = one(m.container, NODEFORM_RAW);
          expect(raw.getAttribute("data-nodeform-raw")).toBe(reason);
          expect(raw.textContent).toContain("请在 JSON 页签修改");
          if (reason === "expression") expect(raw.textContent).toContain("${{ $input.list }}");
          expect(m.container.querySelector("textarea")).toBeNull();
          expect(m.container.querySelector("button")).toBeNull();
          expect(m.log).toEqual([]);
          m.unmount();
        }
      });
    });

    describe("FormNotice", () => {
      it("renders the message with its code and tone", async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: formSpec({ n: { type: "FormNotice", props: { tone: "info", code: "undeclared-params", message: "2 个未声明参数，见 JSON 页签", count: 2 } } }),
          value: {}
        });
        await m.flush();
        const notice = fieldOf(m.container, "n");
        expect(notice.getAttribute("data-notice-code")).toBe("undeclared-params");
        expect(notice.textContent).toContain("2 个未声明参数");
        expect(m.log).toEqual([]);
      });
    });
  });
}

describe("createNodeFormRegistry", () => {
  it("holds the composer/form built-ins, the host components and the node-form checks", () => {
    for (const type of ["Form", "Input", "KeyValue", "ExpressionInput", "DurationInput", "NodeNameInput", "ShapeGuard", "FormNotice"]) {
      expect(registry.types).toContain(type);
    }
    for (const check of nodeFormChecks) expect(registry.checks.get(check.type)).toBe(check);
    expect(registry.checks.get("required")).toBeDefined();
    expect(registry.bindingKinds.NodeNameInput).toEqual({});
    expect(registry.bindingKinds.ShapeGuard).toEqual({});
  });
});

describe("duration helpers", () => {
  it("parses Go durations", () => {
    expect(parseGoDuration("1h30m")).toBe(5.4e12);
    expect(parseGoDuration("1.5s")).toBe(1.5e9);
    expect(parseGoDuration("-250ms")).toBe(-2.5e8);
    expect(parseGoDuration("0")).toBe(0);
    expect(parseGoDuration("10")).toBeNull();
    expect(parseGoDuration("1x")).toBeNull();
  });

  it("formats nanoseconds for display", () => {
    expect(formatGoDuration(90e9)).toBe("1m30s");
    expect(formatGoDuration(3.6e12)).toBe("1h");
    expect(formatGoDuration(1.5e6)).toBe("1.5ms");
    expect(formatGoDuration(-1e9)).toBe("-1s");
    expect(humanizeNs(5.4e12 + 1.5e9)).toBe("1 小时 30 分 1.5 秒");
    expect(humanizeNs(500e6)).toBe("500 毫秒");
  });
});

describe("matchesShape", () => {
  it("follows Doc C §3.2", () => {
    expect(matchesShape(undefined, { type: "number" })).toBe(true);
    expect(matchesShape(null, { type: "object" })).toBe(false);
    expect(matchesShape([], { type: "object" })).toBe(false);
    expect(matchesShape("5", { type: "number" })).toBe(false);
    expect(matchesShape({ a: "x" }, { type: "object", values: "string" })).toBe(true);
    expect(matchesShape({ a: 1 }, { type: "object", values: "string" })).toBe(false);
    expect(matchesShape(["a", 1], { type: "array", item: "string" })).toBe(false);
  });
});
