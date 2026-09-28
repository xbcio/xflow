// kernelConformance(kernel): the Doc B §7.3 suite every RenderKernel must
// pass before it may become the default. Registers vitest blocks; every
// case renders under React.StrictMode through the real <Composer>.

import { fireEvent } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RenderKernel } from "../react/contract";
import { createConformanceRegistry, createProbe } from "./components";
import { errorSpec, fieldsSpec, rowsSpec, slotSpec, toggleSpec, treeSpec } from "./fixtures";
import { allPatches, mountComposer, type MountOptions } from "./harness";

type Row = { name: string };

function inputOf(container: HTMLElement, nodeId: string): HTMLInputElement {
  const input = container.querySelector<HTMLInputElement>(`input[data-node="${nodeId.replace(/["\\]/g, "\\$&")}"]`);
  if (!input) throw new Error(`no input for ${nodeId}`);
  return input;
}

function rowInputs(container: HTMLElement): HTMLInputElement[] {
  return [...container.querySelectorAll<HTMLInputElement>("li input")];
}

function texts(container: HTMLElement): string[] {
  return [...container.querySelectorAll(".text")].map((el) => el.textContent ?? "");
}

export function kernelConformance(kernel: RenderKernel): void {
  describe(`kernel conformance: ${kernel.name}`, () => {
    let probe = createProbe();
    let registry = createConformanceRegistry(probe);
    let consoleError: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      probe = createProbe();
      registry = createConformanceRegistry(probe);
      // Error-boundary cases make React log the caught error; keep output clean.
      consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    });
    afterEach(() => consoleError.mockRestore());

    const mount = (options: Omit<MountOptions, "kernel" | "registry">) => mountComposer({ kernel, registry, ...options });

    it("renders the tree in order", async () => {
      const m = mount({ spec: treeSpec, value: { three: "three" }, context: { four: "four" } });
      await m.flush();
      expect(texts(m.container)).toEqual(["one", "two", "three", "four"]);
      const inner = m.container.querySelector('[data-node="inner"]')!;
      expect([...inner.querySelectorAll(".text")].map((el) => el.getAttribute("data-node"))).toEqual(["t2", "t3"]);
      expect(inner.parentElement?.getAttribute("data-node")).toBe("root");
    });

    it("renders named slots", async () => {
      const m = mount({ spec: slotSpec, value: {} });
      await m.flush();
      const root = m.container.querySelector('[data-node="root"]')!;
      expect([...root.querySelectorAll('[data-slot="header"] .text')].map((el) => el.textContent)).toEqual(["head-1", "head-2"]);
      expect([...root.querySelectorAll('[data-slot="footer"] .text')].map((el) => el.textContent)).toEqual(["foot"]);
      expect(texts(root as HTMLElement)).toEqual(["head-1", "head-2", "body", "foot"]);
    });

    it("groups repeat content by $row", async () => {
      const m = mount({ spec: rowsSpec("Input"), value: { rows: [{ name: "a" }, { name: "b" }] } });
      await m.flush();
      const items = [...m.container.querySelectorAll("li")];
      expect(items).toHaveLength(2);
      items.forEach((li, index) => {
        const row = ["a", "b"][index];
        expect(li.getAttribute("data-index")).toBe(String(index));
        expect(li.getAttribute("data-row")).toMatch(/^\$row@list#/);
        expect([...li.querySelectorAll(".text")].map((el) => el.textContent)).toEqual([row]);
        expect([...li.querySelectorAll("input")].map((el) => el.value)).toEqual([row]);
      });
    });

    for (const key of [undefined, "name"]) {
      it(`keeps repeat rows mounted across delete/insert/reorder (${key ? "keyed" : "object identity"})`, async () => {
        const m = mount({ spec: rowsSpec("LocalInput", key), value: { rows: [{ name: "a" }, { name: "b" }, { name: "c" }] } });
        await m.flush();
        const [, inputB, inputC] = rowInputs(m.container);
        inputC.focus();
        fireEvent.change(inputC, { target: { value: "c-draft" } });
        expect(inputC.value).toBe("c-draft");

        const rowsOf = (value: object) => (value as { rows: Row[] }).rows;
        const assertKept = () => {
          const current = rowInputs(m.container);
          expect(current).toContain(inputB);
          expect(current).toContain(inputC);
          expect(document.activeElement).toBe(inputC);
          expect(inputC.value).toBe("c-draft");
          expect(inputB.value).toBe("b");
        };

        await m.setValue((v) => ({ ...v, rows: rowsOf(v).slice(1) })); // delete "a"
        assertKept();
        expect(rowInputs(m.container)).toHaveLength(2);

        await m.setValue((v) => ({ ...v, rows: [{ name: "new" }, ...rowsOf(v)] })); // insert at front
        assertKept();
        expect(rowInputs(m.container).map((el) => el.value)).toEqual(["new", "b", "c-draft"]);

        await m.setValue((v) => ({ ...v, rows: [...rowsOf(v)].reverse() })); // reorder
        assertKept();
        const reordered = rowInputs(m.container);
        expect(reordered[0]).toBe(inputC);
        expect(reordered[1]).toBe(inputB);
        expect(allPatches(m.log)).toEqual([]);
      });
    }

    it("drops the unmount-blur write of a deleted row instead of hitting a neighbour", async () => {
      const m = mount({ spec: rowsSpec("LocalInput"), value: { rows: [{ name: "a" }, { name: "b" }, { name: "c" }] } });
      await m.flush();
      const [, inputB] = rowInputs(m.container);
      fireEvent.change(inputB, { target: { value: "dirty" } });
      await m.setValue((v) => ({ ...v, rows: (v as { rows: Row[] }).rows.filter((row) => row.name !== "b") }));
      await m.flush();
      expect(allPatches(m.log)).toEqual([]);
      expect((m.value as { rows: Row[] }).rows).toEqual([{ name: "a" }, { name: "c" }]);
      expect(rowInputs(m.container).map((el) => el.value)).toEqual(["a", "c"]);
      expect(m.warnings.map((w) => w.code)).toContain("row-gone");
    });

    it("renders a placeholder for unknown types and isolates/reset element errors", async () => {
      const m = mount({ spec: errorSpec, value: { fail: true } });
      await m.flush();
      expect(texts(m.container)).toEqual(["before", "after"]);
      const unsupported = m.container.querySelector('[data-composer-placeholder="unsupported"]');
      expect(unsupported?.getAttribute("data-composer-node")).toBe("unknown");
      const failed = m.container.querySelector('[data-composer-placeholder="error"]');
      expect(failed?.getAttribute("data-composer-node")).toBe("boom");
      expect(failed?.textContent).toContain("boom in boom");
      expect(m.container.querySelector(".boom")).toBeNull();

      await m.setValue((v) => ({ ...v, fail: false }));
      expect(m.container.querySelector('[data-composer-placeholder="error"]')).toBeNull();
      expect(m.container.querySelector(".boom")?.textContent).toBe("ok");
      expect(texts(m.container)).toEqual(["before", "after"]);
    });

    it("produces zero patches on mount", async () => {
      for (const [s, value] of [
        [treeSpec, { three: "3" }],
        [slotSpec, {}],
        [rowsSpec("Input"), { rows: [{ name: "a" }] }],
        [rowsSpec("LocalInput", "name"), { rows: [{ name: "a" }] }],
        [fieldsSpec, {}],
        [toggleSpec, { mode: "a" }],
        [errorSpec, { fail: false }]
      ] as const) {
        const m = mount({ spec: s, value });
        await m.flush();
        expect(m.log).toEqual([]);
        m.unmount();
      }
    });

    it("toggling two visible-exclusive elements bound to one path writes nothing for that path", async () => {
      const m = mount({ spec: toggleSpec, value: { mode: "a", x: "shared" } });
      await m.flush();
      expect(inputOf(m.container, "a").value).toBe("shared");
      const toggle = m.container.querySelector<HTMLButtonElement>('button[data-node="mode"]')!;
      fireEvent.click(toggle);
      await m.flush();
      expect(m.container.querySelector('input[data-node="a"]')).toBeNull();
      expect(inputOf(m.container, "b").value).toBe("shared");
      fireEvent.click(toggle);
      await m.flush();
      expect(inputOf(m.container, "a").value).toBe("shared");
      expect(allPatches(m.log)).toEqual([
        { op: "set", path: "/mode", value: "b" },
        { op: "set", path: "/mode", value: "a" }
      ]);
      expect(m.value).toEqual({ mode: "a", x: "shared" });

      // Host-driven toggles produce no patches at all.
      const before = m.log.length;
      await m.setValue((v) => ({ ...v, mode: "b" }));
      await m.setValue((v) => ({ ...v, mode: "a" }));
      await m.flush();
      expect(m.log.length).toBe(before);
    });

    it("re-renders only the edited leaf on a keystroke", async () => {
      const m = mount({ spec: fieldsSpec, value: { f0: "zero" } });
      await m.flush();
      probe.reset();
      fireEvent.change(inputOf(m.container, "f3"), { target: { value: "x" } });
      await m.flush();
      expect(m.log).toEqual([[{ op: "set", path: "/g/f3", value: "x" }]]);
      expect(new Set(probe.renders)).toEqual(new Set(["f3"]));
      expect(inputOf(m.container, "f3").value).toBe("x");
    });

    it("keeps IME composition and fast typing intact through batching", async () => {
      const m = mount({ spec: fieldsSpec, value: {} });
      await m.flush();
      const input = inputOf(m.container, "f0");
      input.focus();
      fireEvent.compositionStart(input);
      for (const step of ["n", "ni", "你"]) {
        fireEvent.change(input, { target: { value: step } });
        expect(input.value).toBe(step);
      }
      fireEvent.compositionEnd(input, { data: "你" });
      expect(input.value).toBe("你");
      for (const step of ["你a", "你ab", "你abc"]) {
        fireEvent.change(input, { target: { value: step } });
        expect(input.value).toBe(step);
      }
      await m.flush();
      expect(m.log).toEqual([[{ op: "set", path: "/f0", value: "你abc" }]]);
      expect(m.value).toEqual({ f0: "你abc" });
      expect(input.value).toBe("你abc");
      expect(document.activeElement).toBe(input);
    });

    it("keeps the overlay while a slow host has not acknowledged", async () => {
      const m = mount({ spec: fieldsSpec, value: {}, ackDelayMs: 30 });
      await m.flush();
      const input = inputOf(m.container, "f0");
      for (const step of ["a", "ab", "abc"]) {
        fireEvent.change(input, { target: { value: step } });
        await m.flush(); // each keystroke is its own batch; the host is still behind
        expect(input.value).toBe(step);
      }
      expect(allPatches(m.log).map((p) => (p.op === "set" ? p.value : null))).toEqual(["a", "ab", "abc"]);
      await new Promise((resolve) => setTimeout(resolve, 80));
      await m.flush();
      expect(m.value).toEqual({ f0: "abc" });
      expect(input.value).toBe("abc");
    });
  });
}
