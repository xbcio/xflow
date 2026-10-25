// Form-layer regressions: collapsed sections surface hidden errors, KeyValue
// reserved keys and row-numbered names, JsonEditor `""`, password autofill.

import { fireEvent, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ComposerSpec } from "../core";
import { allPatches, mountComposer } from "../testing/harness";
import { change, clickButton, fieldOf, formSpec, kernels, one } from "./form.testkit";
import { createFormRegistry } from "./index";

const registry = createFormRegistry();

for (const kernel of kernels) {
  describe(`collapsed FieldGroup with ${kernel.name}`, () => {
    const spec = formSpec(
      {
        g: { type: "FieldGroup", props: { title: "高级", collapsible: true, defaultCollapsed: true }, children: ["a", "b"] },
        a: {
          type: "Input",
          props: { label: "A", value: { $bindState: "/a" } },
          checks: [{ type: "required", message: "必填" }]
        },
        b: {
          type: "Input",
          props: { label: "B", value: { $bindState: "/b" } },
          checks: [{ type: "minLength", args: { value: 3 }, message: "太短" }]
        }
      },
      ["g"]
    );

    it("shows how many errors it hides and drops the count once they are fixed", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { b: "x" } });
      await m.flush();
      const group = fieldOf(m.container, "g");
      const toggle = within(group).getByRole("button", { name: /高级/ });
      expect(toggle.getAttribute("aria-expanded")).toBe("false");
      const badge = one<HTMLElement>(group, ".xflow-composer-section-badge");
      expect(badge.textContent).toBe("2 个错误");
      expect(toggle.getAttribute("aria-describedby")).toBe(badge.id);
      // Expanded: the field errors are visible in place, no badge.
      clickButton(group, /高级/);
      await m.flush();
      expect(group.querySelector(".xflow-composer-section-badge")).toBeNull();
      change(within(group).getByLabelText("B"), "long");
      await m.flush();
      clickButton(group, /高级/);
      await m.flush();
      expect(one<HTMLElement>(group, ".xflow-composer-section-badge").textContent).toBe("1 个错误");
      expect(m.log.length).toBeGreaterThan(0);
    });

    it("shows no badge when the hidden fields are valid", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { a: "ok", b: "long" } });
      await m.flush();
      expect(fieldOf(m.container, "g").querySelector(".xflow-composer-section-badge")).toBeNull();
      expect(m.log).toEqual([]);
    });

    it("marks the count and every issue row with a decorative icon", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { b: "x" } });
      await m.flush();
      const group = fieldOf(m.container, "g");
      const badge = one<HTMLElement>(group, ".xflow-composer-section-badge");
      expect(badge.querySelector("svg")).not.toBeNull();
      expect(badge.querySelector("[aria-hidden='true']")).not.toBeNull();
      // The icon carries no text: the count reads the same as before.
      expect(badge.textContent).toBe("2 个错误");
      clickButton(group, /高级/);
      await m.flush();
      const rows = [...group.querySelectorAll<HTMLElement>(".xflow-composer-issue--error")];
      expect(rows.map((row) => row.textContent).sort()).toEqual(["太短", "必填"]);
      for (const row of rows) {
        expect(row.querySelector("svg"), row.textContent ?? "").not.toBeNull();
        expect(row.querySelector("[aria-hidden='true']"), row.textContent ?? "").not.toBeNull();
      }
    });
    it("carries data-composer-path and reveals the first hidden error from the badge", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { b: "x" } });
      await m.flush();
      const group = fieldOf(m.container, "g");
      expect(fieldOf(m.container, "a").getAttribute("data-composer-path")).toBe("/a");
      expect(fieldOf(m.container, "b").getAttribute("data-composer-path")).toBe("/b");
      const badge = one<HTMLButtonElement>(group, ".xflow-composer-section-badge");
      // The count is a real button (P1-c S5): activatable by role, named for its job.
      expect(badge.tagName).toBe("BUTTON");
      expect(badge.getAttribute("type")).toBe("button");
      expect(badge.getAttribute("aria-label")).toBe("展开并定位第一个错误");
      const scrollIntoView = vi.fn();
      const original = Element.prototype.scrollIntoView;
      Element.prototype.scrollIntoView = scrollIntoView;
      try {
        fireEvent.click(badge);
        await m.flush();
        // The badge expands the section (its original job) and reveals the
        // first error among the frames below it ("a" reports before "b").
        expect(within(group).getByRole("button", { name: /高级/ }).getAttribute("aria-expanded")).toBe("true");
        expect(fieldOf(m.container, "a").contains(document.activeElement)).toBe(true);
        expect(scrollIntoView).toHaveBeenCalled();
      } finally {
        Element.prototype.scrollIntoView = original;
      }
    });

    it("locates the first hidden error in DOM order after repeat rows reorder", async () => {
      const rowSpec = formSpec({
        g: { type: "FieldGroup", props: { title: "Rows", collapsible: true, defaultCollapsed: true }, children: ["t"] },
        t: {
          type: "ArrayTable",
          props: { columns: ["Name"], value: { $bindState: "/rows" } },
          repeat: { statePath: "/rows" },
          children: ["cell"]
        },
        cell: { type: "Input", props: { label: "Name", value: { $bindItem: "name" } }, checks: [{ type: "required", message: "必填" }] }
      });
      const m = mountComposer({ kernel, registry, spec: rowSpec, value: { rows: [{}, {}, {}] } });
      await m.flush();
      const group = fieldOf(m.container, "g");
      expect(one<HTMLElement>(group, ".xflow-composer-section-badge").textContent).toBe("3 个错误");
      // Host reorder: the first two rows swap, so both get a new pointer and
      // re-report (appending themselves to the report map), while the third
      // row keeps its pointer and its old map position. The report order is
      // now [third, second, first] while the DOM is [second, first, third]:
      // the badge must follow the DOM, not the report order.
      await m.setValue((value) => {
        const rows = (value as { rows: unknown[] }).rows;
        return { ...value, rows: [rows[1], rows[0], rows[2]] };
      });
      fireEvent.click(one<HTMLButtonElement>(group, ".xflow-composer-section-badge"));
      await m.flush();
      const domRows = [...group.querySelectorAll<HTMLElement>("[data-composer-row]")];
      expect(domRows).toHaveLength(3);
      expect(domRows[0].contains(document.activeElement)).toBe(true);
      expect(domRows[2].contains(document.activeElement)).toBe(false);
    });
  });

  describe(`focusRequest with ${kernel.name}`, () => {
    it("focuses a field control, never the label row's description help button", async () => {
      const spec: ComposerSpec = {
        spec: "composer/v1",
        minor: 0,
        root: "form",
        elements: {
          form: { type: "Form", props: { descriptionPlacement: "tooltip" }, children: ["a"] },
          a: { type: "Input", props: { label: "A", description: "提示文本", value: { $bindState: "/a" } } }
        }
      };
      const m = mountComposer({ kernel, registry, spec, value: { a: "x" }, focusRequest: { path: "/a", nonce: 1 } });
      await m.flush();
      const field = fieldOf(m.container, "a");
      const help = within(field).getByRole("button", { name: "A说明" });
      const control = within(field).getByLabelText("A");
      // Guard the scenario: the help button really is the frame's first
      // focusable element, so the assertion below proves it was skipped.
      expect(help.compareDocumentPosition(control) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
      expect(document.activeElement).toBe(control);
      expect(document.activeElement).not.toBe(help);
    });
  });

  describe(`KeyValue rows with ${kernel.name}`, () => {
    const spec = formSpec({ kv: { type: "KeyValue", props: { label: "Headers", value: { $bindState: "/h" } } } });

    it("keeps a reserved key (__proto__ / constructor / prototype) as a draft with a row error", async () => {
      for (const key of ["__proto__", "constructor", "prototype"]) {
        const m = mountComposer({ kernel, registry, spec, value: { h: { a: "1" } } });
        await m.flush();
        const field = fieldOf(m.container, "kv");
        clickButton(field, "添加");
        await m.flush();
        const keyInput = within(field).getByLabelText("第 2 行的键") as HTMLInputElement;
        change(keyInput, key);
        await m.flush();
        change(within(field).getByLabelText(`${key} 的值`), "x");
        await m.flush();
        expect(m.log).toEqual([]); // nothing silently dropped or written
        expect(m.value).toEqual({ h: { a: "1" } });
        const error = document.getElementById(keyInput.getAttribute("aria-describedby") ?? "");
        expect(error?.textContent).toBe("第 2 行：保留键名，未保存");
        expect(keyInput.getAttribute("aria-invalid")).toBe("true");
        // Renaming the key commits the row.
        change(keyInput, "b");
        await m.flush();
        expect(allPatches(m.log)).toEqual([{ op: "set", path: "/h", value: { a: "1", b: "x" } }]);
        expect(keyInput.getAttribute("aria-describedby")).toBeNull();
        m.unmount();
      }
    });

    it("names key inputs by row number and ties duplicate errors to the key", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { h: { a: "1" } } });
      await m.flush();
      const field = fieldOf(m.container, "kv");
      clickButton(field, "添加");
      await m.flush();
      expect(within(field).getByLabelText("第 1 行的键")).toHaveProperty("value", "a");
      expect(within(field).getByLabelText("第 2 行的值")).toBeTruthy();
      const second = within(field).getByLabelText("第 2 行的键");
      change(second, "a");
      await m.flush();
      expect(document.getElementById(second.getAttribute("aria-describedby") ?? "")?.textContent).toBe("第 2 行：键重复，未保存");
      // Both rows of a duplicated key are in error, each pointing at its own message.
      const first = within(field).getByLabelText("第 1 行的键");
      expect(first.getAttribute("aria-describedby")).not.toBe(second.getAttribute("aria-describedby"));
      expect(document.getElementById(first.getAttribute("aria-describedby") ?? "")?.textContent).toBe("第 1 行：键重复，未保存");
    });

    it("lists one baseline error line per failing row under the rows and above the add button", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { h: { a: "1" } } });
      await m.flush();
      const field = fieldOf(m.container, "kv");
      clickButton(field, "添加");
      await m.flush();
      change(within(field).getByLabelText("第 2 行的键"), "a");
      await m.flush();
      const list = one<HTMLElement>(field, ".xflow-composer-key-value-list");
      const errors = [...list.querySelectorAll<HTMLElement>(".xflow-composer-row-error")];
      expect(errors.map((error) => error.textContent)).toEqual(["第 1 行：键重复，未保存", "第 2 行：键重复，未保存"]);
      const add = one<HTMLElement>(list, ".xflow-composer-add");
      for (const error of errors) {
        expect(error.getAttribute("role")).toBe("alert");
        expect(error.querySelector("svg")).not.toBeNull();
        expect(error.querySelector("[aria-hidden='true']")).not.toBeNull();
        // The lines follow every row and lead the add button.
        expect(error.compareDocumentPosition(add) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        expect(error.closest(".xflow-composer-key-value-row")).toBeNull();
      }
    });

    it("adds rows with a small default button and a plus icon, not a dashed one", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { h: {} } });
      await m.flush();
      const add = one<HTMLElement>(fieldOf(m.container, "kv"), ".xflow-composer-add");
      expect(add.classList.contains("ant-btn-dashed")).toBe(false);
      expect(add.classList.contains("ant-btn-sm")).toBe(true);
      expect(add.querySelector('[data-xflow-icon="plus"]')).not.toBeNull();
    });
  });

  describe(`Switch inline label with ${kernel.name}`, () => {
    const spec = (props: Record<string, unknown> = {}) =>
      formSpec({ s: { type: "Switch", props: { label: "启用", value: { $bindState: "/s" }, ...props } } });

    it("shows the label after the switch and keeps it the control's own label", async () => {
      const m = mountComposer({ kernel, registry, spec: spec(), value: { s: true } });
      await m.flush();
      const field = fieldOf(m.container, "s");
      const control = one<HTMLElement>(field, '[role="switch"]');
      const row = one<HTMLElement>(field, ".xflow-composer-switch-row");
      const label = one<HTMLLabelElement>(row, "label.xflow-composer-field-label");
      expect(within(field).getByLabelText("启用")).toBe(control);
      expect(label.htmlFor).toBe(control.id);
      // One label only: it moved into the row instead of sitting above it too.
      expect(field.querySelectorAll("label")).toHaveLength(1);
      expect(row.firstElementChild).toBe(control);
      expect(control.nextElementSibling).toBe(label);
    });

    it("greys the label while the switch is effectively off", async () => {
      for (const off of [false, undefined]) {
        const m = mountComposer({ kernel, registry, spec: spec(), value: off === undefined ? {} : { s: off } });
        await m.flush();
        const row = one<HTMLElement>(fieldOf(m.container, "s"), ".xflow-composer-switch-row");
        expect(row.classList.contains("is-off"), String(off)).toBe(true);
        m.unmount();
      }
      const on = mountComposer({ kernel, registry, spec: spec(), value: { s: true } });
      await on.flush();
      expect(one<HTMLElement>(fieldOf(on.container, "s"), ".xflow-composer-switch-row").classList.contains("is-off")).toBe(false);
      on.unmount();
    });

    it("keeps the （默认）state text after the label while unset", async () => {
      const m = mountComposer({ kernel, registry, spec: spec({ defaultHint: false }), value: {} });
      await m.flush();
      const field = fieldOf(m.container, "s");
      const row = one<HTMLElement>(field, ".xflow-composer-switch-row");
      const label = one<HTMLElement>(row, "label");
      const state = one<HTMLElement>(row, ".xflow-composer-switch-state");
      expect(state.textContent).toBe("关（默认）");
      expect(label.nextElementSibling).toBe(state);
      // The state line stays the switch's own description.
      expect(one<HTMLElement>(field, '[role="switch"]').getAttribute("aria-describedby")).toBe(state.id);
    });
  });

  describe(`JsonEditor empty string with ${kernel.name}`, () => {
    it('writes the text "" as the empty string; empty text still clears', async () => {
      const m = mountComposer({
        kernel,
        registry,
        spec: formSpec({ j: { type: "JsonEditor", props: { label: "J", value: { $bindState: "/j" } } } }),
        value: { j: 1 }
      });
      await m.flush();
      const area = within(m.container).getByLabelText("J") as HTMLTextAreaElement;
      change(area, '""');
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/j", value: "" }]);
      change(area, "");
      await m.flush();
      expect(allPatches(m.log).at(-1)).toEqual({ op: "unset", path: "/j" });
    });
  });

  describe(`Password with ${kernel.name}`, () => {
    it("opts out of saved-login autofill", async () => {
      const m = mountComposer({
        kernel,
        registry,
        spec: formSpec({ p: { type: "Password", props: { label: "Secret", value: { $bindState: "/p" } } } }),
        value: {}
      });
      await m.flush();
      expect((within(m.container).getByLabelText("Secret") as HTMLInputElement).getAttribute("autocomplete")).toBe("new-password");
    });
  });

  describe(`NumberField text draft with ${kernel.name}`, () => {
    const mountNumber = (props: Record<string, unknown> = {}, value: object = {}) =>
      mountComposer({
        kernel,
        registry,
        spec: formSpec({ n: { type: "InputNumber", props: { label: "重试次数", value: { $bindState: "/n" }, ...props } } }),
        value
      });
    type Shown = ReturnType<typeof mountNumber>;
    const numberInput = (m: Shown) => within(m.container).getByLabelText("重试次数") as HTMLInputElement;
    const issuesOf = (m: Shown) => one<HTMLElement>(fieldOf(m.container, "n"), ".xflow-composer-field-issues").textContent ?? "";
    const hintOf = (m: Shown) => one<HTMLElement>(fieldOf(m.container, "n"), ".xflow-composer-field-hint").textContent ?? "";

    it("keeps an unfinished number as an unwritten draft with the error and the hint", async () => {
      for (const text of ["-", "1.", "1e"]) {
        const m = mountNumber();
        await m.flush();
        change(numberInput(m), text);
        await m.flush();
        expect(m.log, text).toEqual([]);
        expect(m.value, text).toEqual({});
        expect(numberInput(m).getAttribute("aria-invalid"), text).toBe("true");
        expect(issuesOf(m), text).toContain("请输入完整数字");
        expect(hintOf(m), text).toBe("该值尚未写入定义，保存时会被忽略");
        expect(fieldOf(m.container, "n").querySelector(".xflow-composer-issue--error"), text).not.toBeNull();
        m.unmount();
      }
    });

    it("does not write an unfinished draft over an existing value", async () => {
      const m = mountNumber({}, { n: 7 });
      await m.flush();
      change(numberInput(m), "1e");
      await m.flush();
      expect(m.log).toEqual([]);
      expect(m.value).toEqual({ n: 7 });
      expect(hintOf(m)).toBe("该值尚未写入定义，保存时会被忽略");
    });

    it("clears the value when the draft is emptied", async () => {
      const m = mountNumber({}, { n: 7 });
      await m.flush();
      change(numberInput(m), "");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "unset", path: "/n" }]);
      expect(m.value).toEqual({});
    });

    it("keeps an out-of-range draft unwritten and names the bound", async () => {
      const cases: { props: Record<string, unknown>; text: string; message: string }[] = [
        { props: { min: 1, max: 5 }, text: "9", message: "超出范围：应在 1 到 5 之间" },
        { props: { min: 1, max: 5 }, text: "0", message: "超出范围：应在 1 到 5 之间" },
        { props: { min: 1 }, text: "0", message: "不能小于 1" },
        { props: { max: 5 }, text: "9", message: "不能大于 5" }
      ];
      for (const { props, text, message } of cases) {
        const m = mountNumber(props, { n: 3 });
        await m.flush();
        change(numberInput(m), text);
        await m.flush();
        expect(m.log, message).toEqual([]);
        expect(m.value, message).toEqual({ n: 3 });
        expect(issuesOf(m), message).toContain(message);
      }
    });

    it("drops the draft when the value changes from outside", async () => {
      const m = mountNumber({}, { n: 1 });
      await m.flush();
      const input = numberInput(m);
      change(input, "-");
      await m.flush();
      expect(input.value).toBe("-");
      await m.setValue((value) => ({ ...value, n: 5 }));
      await m.flush();
      expect(input.value).toBe("5");
      expect(issuesOf(m)).toBe("");
      expect(fieldOf(m.container, "n").querySelector(".xflow-composer-field-hint")).toBeNull();
      expect(m.log).toEqual([]);
    });

    it("writes a whole number once", async () => {
      const m = mountNumber();
      await m.flush();
      change(numberInput(m), "12");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/n", value: 12 }]);
      expect(m.value).toEqual({ n: 12 });
    });

    it("emits no patch when the draft equals the value", async () => {
      const m = mountNumber({}, { n: 12 });
      await m.flush();
      change(numberInput(m), "012");
      await m.flush();
      expect(m.log).toEqual([]);
      expect(m.value).toEqual({ n: 12 });
    });

    it("steps from the committed value while an unfinished draft is active", async () => {
      const m = mountNumber({}, { n: 7 });
      await m.flush();
      const input = numberInput(m);
      change(input, "1e");
      await m.flush();
      expect(m.log, "typing writes nothing").toEqual([]);
      fireEvent.keyDown(input, { key: "ArrowUp" });
      await m.flush();
      // Not rc's draft-derived 1: the step starts at the committed 7.
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/n", value: 8 }]);
      expect(m.value).toEqual({ n: 8 });
      expect(numberInput(m).value).toBe("8");
    });

    it("steps from the committed value while an out-of-range draft is active", async () => {
      const m = mountNumber({ min: 1, max: 5 }, { n: 3 });
      await m.flush();
      const input = numberInput(m);
      change(input, "9");
      await m.flush();
      expect(m.log).toEqual([]);
      const stepDown = one<HTMLElement>(fieldOf(m.container, "n"), '[aria-label="Decrease Value"]');
      fireEvent.mouseDown(stepDown);
      fireEvent.mouseUp(stepDown);
      await m.flush();
      // Not rc's draft-derived 8 clamped to 5: the step starts at the committed 3.
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/n", value: 2 }]);
      expect(m.value).toEqual({ n: 2 });
    });

    it("writes nothing until an IME composition ends", async () => {
      const m = mountNumber();
      await m.flush();
      const input = numberInput(m);
      fireEvent.compositionStart(input);
      change(input, "1");
      await m.flush();
      change(input, "12");
      await m.flush();
      expect(m.log, "mid-composition").toEqual([]);
      expect(m.value).toEqual({});
      fireEvent.compositionEnd(input);
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/n", value: 12 }]);
      expect(m.value).toEqual({ n: 12 });
    });

    it("writes the value rounded to the precision", async () => {
      const m = mountNumber({ precision: 2 }, { n: 1 });
      await m.flush();
      change(numberInput(m), "1.0059");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/n", value: 1.01 }]);
      expect(m.value).toEqual({ n: 1.01 });
    });
  });
}
