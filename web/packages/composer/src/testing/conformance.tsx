// kernelConformance(kernel): the Doc B §7.3 suite every RenderKernel must
// pass before it may become the default. Registers vitest blocks; every
// case renders under React.StrictMode through the real <Composer>.

import { act, fireEvent, render } from "@testing-library/react";
import { StrictMode, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { applyPatches, type Patch, type Warning } from "../core";
import { Composer } from "../react/Composer";
import type { Registry, RenderKernel } from "../react/contract";
import { OVERLAY_TIMEOUT_MS } from "../react/overlay";
import { createConformanceRegistry, createProbe } from "./components";
import { errorSpec, fieldsSpec, revealSpec, rowsSpec, slotSpec, toggleSpec, treeSpec, wideSpec } from "./fixtures";
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

type Commit = (update: (value: object) => object) => void;

/**
 * Mounts <Composer> under a host whose onChange policy is supplied by the
 * test (debounce, normalization, ...). `commit` replaces the host value.
 */
function mountHost(kernel: RenderKernel, registry: Registry, initial: object, onPatches: (patches: Patch[], commit: Commit) => void) {
  const warnings: Warning[] = [];
  let current = initial;
  let setHost: ((update: (value: object) => object) => void) | undefined;
  const commit: Commit = (update) => act(() => setHost!(update));
  function Host() {
    const [value, setValue] = useState<object>(initial);
    current = value;
    setHost = setValue;
    return (
      <Composer
        spec={fieldsSpec}
        registry={registry}
        kernel={kernel}
        value={value}
        onChange={(patches) => onPatches(patches, commit)}
        onWarning={(warning) => warnings.push(warning)}
      />
    );
  }
  const utils = render(
    <StrictMode>
      <Host />
    </StrictMode>
  );
  return {
    container: utils.container,
    warnings,
    get value() {
      return current;
    }
  };
}

const advance = (ms: number) => act(async () => vi.advanceTimersByTime(ms));

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
    afterEach(() => {
      consoleError.mockRestore();
      vi.useRealTimers();
    });

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

    describe("focusRequest (P1-c S5)", () => {
      it("expands collapsed ancestors and focuses the named field", async () => {
        const scrollIntoView = vi.fn();
        const original = Element.prototype.scrollIntoView;
        Element.prototype.scrollIntoView = scrollIntoView;
        try {
          const m = mount({ spec: revealSpec, value: { a: { b: "bee", c: "cee" } }, focusRequest: { path: "/a/b", nonce: 1 } });
          await m.flush();
          const target = inputOf(m.container, "f");
          expect(target.getAttribute("data-composer-path")).toBe("/a/b");
          expect(document.activeElement).toBe(target);
          // The collapsed Group pair around the target is open again.
          expect([...m.container.querySelectorAll("[aria-expanded]")].map((el) => el.getAttribute("aria-expanded"))).toEqual(["true", "true"]);
          expect(m.container.querySelector("[hidden]")).toBeNull();
          expect(scrollIntoView).toHaveBeenCalled();
          expect(m.warnings).toEqual([]);
        } finally {
          Element.prototype.scrollIntoView = original;
        }
      });

      it("falls back to the nearest ancestor path that has a field", async () => {
        const m = mount({ spec: revealSpec, value: { a: { b: "bee", c: "cee" } }, focusRequest: { path: "/a/b/deeper", nonce: 1 } });
        await m.flush();
        expect(document.activeElement).toBe(inputOf(m.container, "f"));
        expect(m.warnings).toEqual([]);
      });

      it("executes a nonce once, and a new nonce again", async () => {
        const m = mount({ spec: revealSpec, value: { a: { b: "bee", c: "cee" } }, focusRequest: { path: "/a/b", nonce: 1 } });
        await m.flush();
        const target = inputOf(m.container, "f");
        expect(document.activeElement).toBe(target);
        target.blur();
        await m.setFocusRequest({ path: "/a/b", nonce: 1 });
        expect(document.activeElement).not.toBe(target);
        await m.setFocusRequest({ path: "/a/b", nonce: 2 });
        expect(document.activeElement).toBe(target);
      });

      it("reports each executed nonce to the host exactly once", async () => {
        // The host clears its request on this signal; the provider's own
        // de-duplication is instance-local and cannot survive a remount, so
        // the report is what keeps a remount from replaying the reveal.
        const handled = vi.fn();
        const m = mount({
          spec: revealSpec,
          value: { a: { b: "bee", c: "cee" } },
          focusRequest: { path: "/a/b", nonce: 1 },
          onFocusRequestHandled: handled
        });
        await m.flush();
        expect(handled.mock.calls).toEqual([[1]]);
        inputOf(m.container, "f").blur();
        await m.setFocusRequest({ path: "/a/b", nonce: 1 });
        expect(handled.mock.calls).toEqual([[1]]);
        await m.setFocusRequest({ path: "/a/b", nonce: 2 });
        expect(handled.mock.calls).toEqual([[1], [2]]);
      });

      it("reports a nonce that only produced the not-found warning", async () => {
        const handled = vi.fn();
        const m = mount({
          spec: revealSpec,
          value: { a: { b: "bee", c: "cee" } },
          focusRequest: { path: "/absent", nonce: 7 },
          onFocusRequestHandled: handled
        });
        await m.flush();
        expect(m.warnings.map((warning) => warning.code)).toEqual(["focus-target-not-found"]);
        expect(handled.mock.calls).toEqual([[7]]);
      });

      it("warns once, without throwing, when no field answers the path", async () => {
        const m = mount({ spec: revealSpec, value: { a: { b: "bee", c: "cee" } }, focusRequest: { path: "/absent", nonce: 1 } });
        await m.flush();
        expect(m.warnings.map((warning) => warning.code)).toEqual(["focus-target-not-found"]);
        expect(m.warnings[0]?.path).toBe("/absent");
        // Re-issuing the same nonce neither focuses nor warns again.
        await m.setFocusRequest({ path: "/absent", nonce: 1 });
        expect(m.warnings).toHaveLength(1);
      });
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

    it("re-renders the edited path and no other leaf on a keystroke", async () => {
      const m = mount({ spec: fieldsSpec, value: { f0: "zero" } });
      await m.flush();
      probe.reset();
      fireEvent.change(inputOf(m.container, "f3"), { target: { value: "x" } });
      await m.flush();
      expect(m.log).toEqual([[{ op: "set", path: "/g/f3", value: "x" }]]);
      // "root" and "grp" are f3's ancestor boxes: React re-renders the edited
      // path. No sibling leaf may re-render.
      const rendered = new Set(probe.renders);
      expect(rendered.has("f3")).toBe(true);
      expect(["f0", "f1", "f2", "f4", "t"].filter((id) => rendered.has(id))).toEqual([]);
      expect(inputOf(m.container, "f3").value).toBe("x");
    });

    it("skips an unrelated container subtree on a keystroke", async () => {
      const m = mount({ spec: wideSpec, value: {} });
      await m.flush();
      probe.reset();
      fireEvent.change(inputOf(m.container, "f3"), { target: { value: "x" } });
      await m.flush();
      expect(allPatches(m.log)).toEqual([{ op: "set", path: "/g/f3", value: "x" }]);
      // The whole "side" subtree is unrelated: neither its container render
      // function nor any of its components may run again.
      const rendered = new Set(probe.renders);
      expect(rendered.has("f3")).toBe(true);
      expect(["side", "s1", "s2", "s3"].filter((id) => rendered.has(id))).toEqual([]);
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

    it("keeps newer keystrokes while a lagging host acknowledges older batches", async () => {
      vi.useFakeTimers();
      const m = mount({ spec: fieldsSpec, value: {}, ackDelayMs: 30 });
      await m.flush();
      const input = inputOf(m.container, "f0");
      for (const step of ["a", "ab", "abc"]) {
        fireEvent.change(input, { target: { value: step } });
        await m.flush();
        await advance(10);
      }
      expect(m.log).toHaveLength(3);
      // t=30: the host has applied batch 1 only; batches 2 and 3 are in flight.
      expect(m.value).toEqual({ f0: "a" });
      expect(input.value).toBe("abc");
      await advance(10);
      expect(m.value).toEqual({ f0: "ab" });
      expect(input.value).toBe("abc");
      await advance(10);
      expect(m.value).toEqual({ f0: "abc" });
      expect(input.value).toBe("abc");
      await advance(OVERLAY_TIMEOUT_MS * 2);
      expect(input.value).toBe("abc");
      expect(m.warnings).toEqual([]);
    });

    it("reverts to the host value with a warning when the host never applies", async () => {
      vi.useFakeTimers();
      const m = mount({ spec: fieldsSpec, value: { f0: "a" }, ignoreChanges: true });
      await m.flush();
      const input = inputOf(m.container, "f0");
      fireEvent.change(input, { target: { value: "ab" } });
      await m.flush();
      await advance(OVERLAY_TIMEOUT_MS - 10);
      expect(input.value).toBe("ab");
      expect(m.warnings).toEqual([]);
      await advance(20);
      expect(input.value).toBe("a");
      expect(m.warnings.map((w) => w.code)).toEqual(["overlay-timeout"]);
    });

    it("measures the timeout from the latest flush during continuous typing", async () => {
      vi.useFakeTimers();
      // A debouncing host: applies everything 800ms after the last onChange.
      let pending: Patch[] = [];
      let timer: ReturnType<typeof setTimeout> | undefined;
      const h = mountHost(kernel, registry, {}, (patches, commit) => {
        pending.push(...patches);
        clearTimeout(timer);
        timer = setTimeout(() => {
          const batch = pending;
          pending = [];
          commit((value) => applyPatches(value, batch));
        }, 800);
      });
      await act(async () => {});
      const input = inputOf(h.container, "f0");
      let text = "";
      for (let t = 0; t <= 3000; t += 600) {
        text += "x";
        fireEvent.change(input, { target: { value: text } });
        await act(async () => {});
        expect(input.value).toBe(text);
        await advance(600);
      }
      // The host catches up 800ms after the last keystroke (t=3800).
      await advance(200);
      expect(h.value).toEqual({ f0: text });
      await advance(OVERLAY_TIMEOUT_MS * 2);
      expect(input.value).toBe(text);
      expect(h.warnings).toEqual([]);
    });

    it("lets a normalizing host win once its value is acknowledged or the timeout passes", async () => {
      vi.useFakeTimers();
      const trim = (patches: Patch[]) => patches.map((p) => (p.op === "set" && typeof p.value === "string" ? { ...p, value: p.value.trim() } : p));
      const h = mountHost(kernel, registry, {}, (patches, commit) => commit((value) => applyPatches(value, trim(patches))));
      await act(async () => {});
      const input = inputOf(h.container, "f0");

      // A later write the host reflects acknowledges the earlier normalized one.
      fireEvent.change(input, { target: { value: "x " } });
      await act(async () => {});
      expect(h.value).toEqual({ f0: "x" });
      expect(input.value).toBe("x ");
      fireEvent.change(input, { target: { value: "x y" } });
      await act(async () => {});
      expect(input.value).toBe("x y");

      // A normalized write nothing reflects: the host value wins at the timeout.
      fireEvent.change(input, { target: { value: "x y " } });
      await act(async () => {});
      expect(h.value).toEqual({ f0: "x y" });
      expect(input.value).toBe("x y ");
      await advance(OVERLAY_TIMEOUT_MS + 10);
      expect(input.value).toBe("x y");
      expect(h.warnings.map((w) => w.code)).toEqual(["overlay-timeout"]);
    });
  });
}
