// Form-layer regressions: collapsed sections surface hidden errors, KeyValue
// reserved keys and row-numbered names, JsonEditor `""`, password autofill.

import { within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
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
        expect(error?.textContent).toBe("保留键名，未保存");
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
      expect(document.getElementById(second.getAttribute("aria-describedby") ?? "")?.textContent).toBe("键重复，未保存");
      // Both rows of a duplicated key are in error, each pointing at its own message.
      const first = within(field).getByLabelText("第 1 行的键");
      expect(first.getAttribute("aria-describedby")).not.toBe(second.getAttribute("aria-describedby"));
      expect(document.getElementById(first.getAttribute("aria-describedby") ?? "")?.textContent).toBe("键重复，未保存");
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
}
