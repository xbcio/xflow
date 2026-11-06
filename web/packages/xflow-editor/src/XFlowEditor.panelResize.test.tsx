import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { XFlowEditor } from "./index";
import { PANEL_WIDTH_STORAGE_KEY } from "./panels/panelWidths";

const workflow = {
  id: "wf-resize",
  name: "Resize",
  nodes: [
    { name: "fetch", type: "xflow.http" as const },
    { name: "finish", type: "xflow.end" as const }
  ],
  connections: {}
};

/**
 * jsdom has no localStorage here (Node 26 ships a bare global), so this file
 * installs the same Map-backed Storage the panel tests use. The editor reads
 * and writes inside try/catch, which is the fallback the last cases pin.
 */
class MemoryStorage {
  #items = new Map<string, string>();
  get length(): number {
    return this.#items.size;
  }
  clear(): void {
    this.#items.clear();
  }
  getItem(key: string): string | null {
    return this.#items.get(key) ?? null;
  }
  key(index: number): string | null {
    return [...this.#items.keys()][index] ?? null;
  }
  removeItem(key: string): void {
    this.#items.delete(key);
  }
  setItem(key: string, value: string): void {
    this.#items.set(key, String(value));
  }
}

const storage = new MemoryStorage();
Object.defineProperty(window, "localStorage", { configurable: true, value: storage });

const leftHandle = () => screen.getByRole("separator", { name: "调整节点面板宽度" });
const rightHandle = () => screen.getByRole("separator", { name: "调整属性面板宽度" });
const valueOf = (handle: HTMLElement) => Number(handle.getAttribute("aria-valuenow"));
const stored = () => JSON.parse(storage.getItem(PANEL_WIDTH_STORAGE_KEY) ?? "null") as unknown;

beforeEach(() => storage.clear());

describe("XFlowEditor panel resize (§6.0)", () => {
  it("exposes both docked edges as ARIA window separators with the §6.0 ranges", () => {
    render(<XFlowEditor value={workflow} />);

    const left = leftHandle();
    expect(left.getAttribute("aria-orientation")).toBe("vertical");
    expect(left.getAttribute("aria-valuemin")).toBe("200");
    expect(left.getAttribute("aria-valuemax")).toBe("400");
    expect(left.getAttribute("aria-valuenow")).toBe("240");
    expect(left.tabIndex).toBe(0);

    const right = rightHandle();
    expect(right.getAttribute("aria-valuemin")).toBe("320");
    expect(right.getAttribute("aria-valuemax")).toBe("520");
    expect(right.getAttribute("aria-valuenow")).toBe("320");
  });

  it("steps the left content 16px per arrow, with → widening it", () => {
    render(<XFlowEditor value={workflow} />);
    const handle = leftHandle();

    fireEvent.keyDown(handle, { key: "ArrowRight" });
    expect(valueOf(leftHandle())).toBe(256);
    fireEvent.keyDown(leftHandle(), { key: "ArrowLeft" });
    expect(valueOf(leftHandle())).toBe(240);
    fireEvent.keyDown(leftHandle(), { key: "ArrowLeft" });
    expect(valueOf(leftHandle())).toBe(224);
  });

  it("steps the inspector 16px per arrow, with ← widening it", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.keyDown(rightHandle(), { key: "ArrowLeft" });
    expect(valueOf(rightHandle())).toBe(336);
    fireEvent.keyDown(rightHandle(), { key: "ArrowRight" });
    expect(valueOf(rightHandle())).toBe(320);
    fireEvent.keyDown(rightHandle(), { key: "ArrowRight" });
    expect(valueOf(rightHandle())).toBe(320);
  });

  it("jumps to the range ends with Home/End and never leaves the range", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.keyDown(leftHandle(), { key: "End" });
    expect(valueOf(leftHandle())).toBe(400);
    fireEvent.keyDown(leftHandle(), { key: "ArrowRight" });
    expect(valueOf(leftHandle())).toBe(400);
    fireEvent.keyDown(leftHandle(), { key: "Home" });
    expect(valueOf(leftHandle())).toBe(200);
    fireEvent.keyDown(leftHandle(), { key: "ArrowLeft" });
    expect(valueOf(leftHandle())).toBe(200);

    fireEvent.keyDown(rightHandle(), { key: "End" });
    expect(valueOf(rightHandle())).toBe(520);
    fireEvent.keyDown(rightHandle(), { key: "Home" });
    expect(valueOf(rightHandle())).toBe(320);
  });

  it("resets to the default on Enter and on double-click", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.keyDown(leftHandle(), { key: "End" });
    fireEvent.keyDown(leftHandle(), { key: "Enter" });
    expect(valueOf(leftHandle())).toBe(240);

    fireEvent.keyDown(rightHandle(), { key: "End" });
    fireEvent.doubleClick(rightHandle());
    expect(valueOf(rightHandle())).toBe(320);
  });

  it("persists the shared width and restores it on the next mount", () => {
    const first = render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(leftHandle(), { key: "End" });
    expect(stored()).toEqual({ left: 400, right: 320 });
    first.unmount();

    render(<XFlowEditor value={workflow} />);
    expect(valueOf(leftHandle())).toBe(400);
    expect(valueOf(rightHandle())).toBe(320);
  });

  it("clamps an out-of-range stored width to the §6.0 range", () => {
    storage.setItem(PANEL_WIDTH_STORAGE_KEY, JSON.stringify({ left: 9999, right: 40 }));
    render(<XFlowEditor value={workflow} />);

    expect(valueOf(leftHandle())).toBe(400);
    expect(valueOf(rightHandle())).toBe(320);
  });

  it("keeps the width across a collapse and expand", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(leftHandle(), { key: "End" });

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("separator", { name: "调整节点面板宽度" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(valueOf(leftHandle())).toBe(400);
  });

  it("renders no handle in the compact layout and uses the stored width when docked again", () => {
    const original = globalThis.ResizeObserver;
    const observers: { callback: ResizeObserverCallback; targets: Set<Element> }[] = [];
    class WidthObserver {
      readonly targets = new Set<Element>();
      readonly callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
        observers.push(this);
      }
      observe(target: Element) {
        this.targets.add(target);
      }
      unobserve(target: Element) {
        this.targets.delete(target);
      }
      disconnect() {
        this.targets.clear();
      }
    }
    const setWidth = (width: number) => {
      for (const observer of [...observers]) {
        for (const target of [...observer.targets]) {
          if (!(target instanceof HTMLElement) || !target.classList.contains("xflow-editor")) continue;
          observer.callback(
            [{
              target,
              borderBoxSize: [{ blockSize: 800, inlineSize: width }],
              contentRect: { width, height: 800 }
            } as unknown as ResizeObserverEntry],
            observer as unknown as ResizeObserver
          );
        }
      }
    };
    globalThis.ResizeObserver = WidthObserver as unknown as typeof ResizeObserver;
    try {
      storage.setItem(PANEL_WIDTH_STORAGE_KEY, JSON.stringify({ left: 384, right: 320 }));
      render(<XFlowEditor value={workflow} />);
      act(() => setWidth(1280));
      expect(screen.queryByRole("separator", { name: "调整节点面板宽度" })).toBeNull();
      expect(screen.queryByRole("separator", { name: "调整属性面板宽度" })).toBeNull();

      act(() => setWidth(1600));
      expect(valueOf(leftHandle())).toBe(384);
    } finally {
      globalThis.ResizeObserver = original;
    }
  });

  it("still resizes when storage denies reads and writes", () => {
    const getItem = vi.spyOn(storage, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    const setItem = vi.spyOn(storage, "setItem").mockImplementation(() => {
      throw new Error("denied");
    });
    try {
      render(<XFlowEditor value={workflow} />);
      expect(valueOf(leftHandle())).toBe(240);
      fireEvent.keyDown(leftHandle(), { key: "ArrowRight" });
      expect(valueOf(leftHandle())).toBe(256);
    } finally {
      getItem.mockRestore();
      setItem.mockRestore();
    }
  });
});
