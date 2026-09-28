// ArrayTable row identity, JsonEditor drafts and KeyValue drafts /
// valueType, with both kernels under StrictMode.

import { fireEvent, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ComposerSpec } from "../core";
import { allPatches, mountComposer } from "../testing/harness";
import { change, clickButton, fieldOf, formSpec, kernels, one } from "./form.testkit";
import { createFormRegistry } from "./index";

const registry = createFormRegistry();

type Row = { name: string; meta?: unknown };

const tableSpec = (key?: string): ComposerSpec =>
  formSpec(
    {
      rows: {
        type: "ArrayTable",
        props: { label: "Rules", columns: ["Name", "Meta"], value: { $bindState: "/rows" }, newRow: { name: "" } },
        repeat: key ? { statePath: "/rows", key } : { statePath: "/rows" },
        children: ["name", "meta"]
      },
      name: { type: "Input", props: { label: "Name", value: { $bindItem: "name" } } },
      meta: { type: "JsonEditor", props: { label: "Meta", value: { $bindItem: "meta" } } }
    },
    ["rows"]
  );

const rowEls = (container: HTMLElement) => [...container.querySelectorAll<HTMLElement>("[data-composer-row]")];
const nameInputs = (container: HTMLElement) => rowEls(container).map((row) => within(row).getByLabelText("Name") as HTMLInputElement);
const metaAreas = (container: HTMLElement) => rowEls(container).map((row) => within(row).getByLabelText("Meta") as HTMLTextAreaElement);

for (const kernel of kernels) {
  describe(`ArrayTable with ${kernel.name}`, () => {
    for (const key of [undefined, "name"]) {
      it(`add / remove / reorder keep the other rows' DOM and local state (${key ? "keyed" : "object identity"})`, async () => {
        const m = mountComposer({
          kernel,
          registry,
          spec: tableSpec(key),
          value: { rows: [{ name: "a" }, { name: "b" }, { name: "c" }] }
        });
        await m.flush();
        expect(m.log).toEqual([]);
        const table = fieldOf(m.container, "rows");
        const [, nameB, nameC] = nameInputs(m.container);
        const [, , metaC] = metaAreas(m.container);
        // Local state: an invalid JSON draft in row c and focus in row b.
        change(metaC, "{bad");
        nameB.focus();
        await m.flush();
        expect(m.log).toEqual([]);

        const assertKept = () => {
          expect(nameInputs(m.container)).toContain(nameB);
          expect(nameInputs(m.container)).toContain(nameC);
          expect(metaAreas(m.container)).toContain(metaC);
          expect(metaC.value).toBe("{bad");
          expect(document.activeElement).toBe(nameB);
        };
        const rows = () => (m.value as { rows: Row[] }).rows;

        // add (keyed rows need a distinct key, so name the new row via newRow's default then edit)
        clickButton(table, "添加");
        await m.flush();
        expect(allPatches(m.log).at(-1)).toEqual({ op: "set", path: "/rows", value: [{ name: "a" }, { name: "b" }, { name: "c" }, { name: "" }] });
        expect(rowEls(m.container)).toHaveLength(4);
        assertKept();

        // remove row a
        clickButton(table, "删除第 1 行");
        await m.flush();
        expect(rows().map((row) => row.name)).toEqual(["b", "c", ""]);
        expect(rowEls(m.container)).toHaveLength(3);
        assertKept();

        // move c up (now index 1 -> 0)
        clickButton(table, "上移第 2 行");
        await m.flush();
        expect(rows().map((row) => row.name)).toEqual(["c", "b", ""]);
        expect(nameInputs(m.container).slice(0, 2)).toEqual([nameC, nameB]);
        assertKept();

        // move b down past the new row
        clickButton(table, "下移第 2 行");
        await m.flush();
        expect(rows().map((row) => row.name)).toEqual(["c", "", "b"]);
        assertKept();

        // untouched rows keep their object references (row identity)
        expect(m.log.every((batch) => batch.every((patch) => patch.path === "/rows"))).toBe(true);
      });
    }

    it("edits a row field in place and removing the last row unsets the list", async () => {
      const m = mountComposer({ kernel, registry, spec: tableSpec(), value: { rows: [{ name: "a" }], other: 1 } });
      await m.flush();
      change(nameInputs(m.container)[0], "z");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/rows/0/name", value: "z" }]);
      clickButton(fieldOf(m.container, "rows"), "删除第 1 行");
      await m.flush();
      expect(allPatches(m.log).at(-1)).toEqual({ op: "unset", path: "/rows" });
      expect(m.value).toEqual({ other: 1 });
      expect(within(fieldOf(m.container, "rows")).getByText("暂无数据")).toBeTruthy();
    });

    it("respects maxItems / minItems and exposes a table to assistive technology", async () => {
      const spec = tableSpec();
      spec.elements.rows.props = { ...spec.elements.rows.props, minItems: 1, maxItems: 1 };
      const m = mountComposer({ kernel, registry, spec, value: { rows: [{ name: "a" }] } });
      await m.flush();
      const table = fieldOf(m.container, "rows");
      expect(within(table).queryByText("添加")).toBeNull();
      expect(one<HTMLButtonElement>(table, '[aria-label="删除第 1 行"]').disabled).toBe(true);
      const grid = within(table).getByRole("table");
      expect(within(grid).getAllByRole("columnheader").map((el) => el.textContent)).toEqual(["Name", "Meta", "操作"]);
      expect(within(grid).getAllByRole("row")).toHaveLength(2);
      expect(within(within(grid).getAllByRole("row")[1]).getAllByRole("cell")).toHaveLength(3);
    });
  });

  describe(`JsonEditor with ${kernel.name}`, () => {
    const spec = formSpec({
      j: { type: "JsonEditor", props: { label: "J", value: { $bindState: "/j" } } },
      other: { type: "Input", props: { label: "Other", value: { $bindState: "/other" } } }
    });

    it("keeps an invalid draft local, never writes it, and keeps it across re-renders", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { j: { a: 1 } } });
      await m.flush();
      const area = within(m.container).getByLabelText("J") as HTMLTextAreaElement;
      change(area, '{"a":');
      await m.flush();
      expect(m.log).toEqual([]);
      const field = fieldOf(m.container, "j");
      expect(field.textContent).toContain("JSON 无效，未保存");
      expect(area.getAttribute("aria-invalid")).toBe("true");
      expect(document.getElementById(area.getAttribute("aria-describedby")!.split(" ").at(-1)!)?.textContent).toContain("JSON 无效");

      // Another field's write and a host re-render with a new root object.
      change(within(m.container).getByLabelText("Other"), "x");
      await m.flush();
      await m.setValue((v) => ({ ...v }));
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/other", value: "x" }]);
      expect(area.value).toBe('{"a":');
      expect((m.value as { j: unknown }).j).toEqual({ a: 1 });

      // Fixing the draft writes it; the text as typed stays.
      change(area, '{"a": 2}');
      await m.flush();
      expect(allPatches(m.log).at(-1)).toEqual({ op: "set", path: "/j", value: { a: 2 } });
      expect(area.value).toBe('{"a": 2}');
      expect(field.textContent).not.toContain("JSON 无效");
    });

    it("an external change replaces the draft", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { j: { a: 1 } } });
      await m.flush();
      const area = within(m.container).getByLabelText("J") as HTMLTextAreaElement;
      change(area, "{oops");
      await m.setValue((v) => ({ ...v, j: [1, 2] }));
      expect(JSON.parse(area.value)).toEqual([1, 2]);
      expect(m.log).toEqual([]);
    });

    it("shows scalars, arrays and null without coercion", async () => {
      const m = mountComposer({ kernel, registry, spec, value: { j: null } });
      await m.flush();
      expect((within(m.container).getByLabelText("J") as HTMLTextAreaElement).value).toBe("null");
      await m.setValue((v) => ({ ...v, j: "text" }));
      expect((within(m.container).getByLabelText("J") as HTMLTextAreaElement).value).toBe('"text"');
      expect(m.log).toEqual([]);
    });
  });

  describe(`KeyValue with ${kernel.name}`, () => {
    it("keeps incomplete and duplicate rows as local drafts", async () => {
      const m = mountComposer({
        kernel,
        registry,
        spec: formSpec({ kv: { type: "KeyValue", props: { label: "Headers", value: { $bindState: "/h" } } } }),
        value: { h: { a: "1" } }
      });
      await m.flush();
      const field = fieldOf(m.container, "kv");
      clickButton(field, "添加");
      await m.flush();
      expect(m.log).toEqual([]);
      const keys = () => within(field).getAllByLabelText("键") as HTMLInputElement[];
      change(keys()[1], "a"); // duplicate key
      await m.flush();
      expect(m.log).toEqual([]);
      expect(field.textContent).toContain("键重复，未保存");
      change(keys()[1], "b");
      await m.flush();
      expect(m.log).toEqual([]); // key without value is still a draft
      change(within(field).getByLabelText("b 的值"), "2");
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/h", value: { a: "1", b: "2" } }]);
      expect(keys().map((el) => el.value)).toEqual(["a", "b"]);
    });

    it("uses a registered component as the value editor (valueType)", async () => {
      const m = mountComposer({
        kernel,
        registry,
        spec: formSpec({
          kv: { type: "KeyValue", props: { label: "Limits", valueType: "InputNumber", valueProps: { min: 0 }, value: { $bindState: "/l" } } },
          bad: { type: "KeyValue", props: { label: "Bad", valueType: "Missing", value: { $bindState: "/b" } } }
        }),
        value: { l: { cpu: 1 }, b: { x: 1 } }
      });
      await m.flush();
      expect(m.log).toEqual([]);
      const group = within(fieldOf(m.container, "kv")).getByRole("group", { name: "cpu 的值" });
      const number = one<HTMLInputElement>(group, "input");
      expect(number.value).toBe("1");
      fireEvent.change(number, { target: { value: "4" } });
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/l", value: { cpu: 4 } }]);
      // An unregistered valueType never silently falls back.
      const bad = m.container.querySelector('[data-composer-node="bad"]')!;
      expect(bad.textContent).toMatch(/Missing/);
    });
  });
}
