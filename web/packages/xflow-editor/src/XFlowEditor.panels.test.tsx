import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { XFlowEditor } from "./index";

/** The canvas node's own select button (the canvas is the list). */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const workflow = {
  id: "wf-panels",
  name: "Panels",
  nodes: [
    { name: "fetch", type: "xflow.http" as const },
    { name: "finish", type: "xflow.end" as const }
  ],
  connections: {}
};

const panelStorage = (id: string) => `xflow-editor:panels:${id}`;

/**
 * This jsdom build leaves `window.localStorage` undefined (Node 26 ships its own
 * bare localStorage global), so the tests install a Map-backed Storage. The
 * editor reads `window.localStorage` inside a try/catch, which is exactly the
 * silent-fallback path the last case pins.
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
/** Storage's own API: `Object.keys` on the mock would miss the prototype methods. */
const storedKeys = () => Array.from({ length: storage.length }, (_, index) => storage.key(index) ?? "");

beforeEach(() => storage.clear());

describe("XFlowEditor panel collapse (§6.0)", () => {
  it("collapses and expands from the title-bar keys and the left rail", () => {
    render(<XFlowEditor value={workflow} />);

    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");

    // The title-bar key reports the pane it controls as expanded (it only
    // renders while the column is open), not a pressed toggle.
    expect(screen.getByRole("button", { name: "收起左栏" }).getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByRole("button", { name: "收起左栏" }).hasAttribute("aria-pressed")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    // The rail survives at width 0 and reports the collapsed panel.
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");
    expect(screen.getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("false");

    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");

    // Pressing the current section's key again collapses it.
    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    // A different section's key expands the column onto that section.
    fireEvent.click(screen.getByRole("button", { name: "变量" }));
    expect(screen.getByRole("region", { name: "变量" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
  });

  it("toggles both panels from the keyboard but not from a text field", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.keyDown(window, { key: "b", metaKey: true });
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    fireEvent.keyDown(window, { key: "b", ctrlKey: true });
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();

    // macOS reports ⌥B as the composed "∫"; only `code` still identifies the
    // physical key, so the real-platform event must toggle the inspector.
    fireEvent.keyDown(window, { key: "∫", code: "KeyB", metaKey: true, altKey: true });
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    fireEvent.keyDown(window, { key: "b", code: "KeyB", ctrlKey: true, altKey: true });
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();

    // Canvas-scope shortcuts defer to a text field (the same rule as ⌘B).
    const library = screen.getByRole("region", { name: "节点" });
    fireEvent.click(within(library).getByRole("button", { name: "搜索节点" }));
    const search = within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    fireEvent.keyDown(search, { key: "b", metaKey: true });
    fireEvent.keyDown(search, { key: "∫", code: "KeyB", metaKey: true, altKey: true });
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
  });

  it("names the selected node in the reopen chip and expands on click", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    const reopen = screen.getByRole("button", { name: "展开属性面板" });
    expect(reopen.textContent).toContain("属性 · fetch");
    // The canvas chip's own hook (12px inset, 28px wrapper, 6px radius).
    expect(reopen.classList.contains("xflow-editor-canvas-reopen")).toBe(true);
    // The chip's glyph is the baseline's #i-side-right, not AntD's menu-unfold.
    const glyph = reopen.querySelector<SVGSVGElement>('[data-xflow-icon="side-right"]');
    expect(glyph?.getAttribute("aria-hidden")).toBe("true");
    expect(glyph?.getAttribute("viewBox")).toBe("0 0 24 24");
    expect(glyph?.querySelector("path")?.getAttribute("d")).toBe("M14.5 5v14");
    expect(reopen.querySelector(".anticon")).toBeNull();

    // A single click selects (and renames the chip) without expanding.
    fireEvent.click(canvasNode("finish"));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    expect(screen.getByRole("button", { name: "展开属性面板" }).textContent).toContain("属性 · finish");

    fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector).getByText("finish", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("expands the inspector on a node double-click but not on a single click", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));

    fireEvent.click(canvasNode("finish"));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();

    fireEvent.doubleClick(canvasNode("finish"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector).getByText("finish", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("locates a node from the search box without expanding the inspector", async () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));

    fireEvent.keyDown(window, { key: "k", metaKey: true });
    const search = screen.getByRole("combobox", { name: "定位节点" });
    fireEvent.change(search, { target: { value: "finish" } });
    fireEvent.click(await screen.findByText("finish", { selector: ".xflow-editor-node-search__option strong" }));

    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    expect(screen.getByRole("button", { name: "展开属性面板" }).textContent).toContain("属性 · finish");
  });

  it("shows the bare 属性 chip when there is no selected node", () => {
    render(<XFlowEditor value={{ id: "wf-empty", name: "Empty", nodes: [], connections: {} }} />);
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));

    const reopen = screen.getByRole("button", { name: "展开属性面板" });
    expect(reopen.textContent).toBe("属性");
  });

  it("collapses the active 节点与大纲 key on the first read-only click and keeps the collapse entry", () => {
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" />);

    // The read-only viewer opens on the outline alone; the library zone is not drawn.
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("false");
    // The preview notice replaces the inspector body, not its title-bar key.
    expect(screen.getByRole("button", { name: "收起属性面板" })).toBeTruthy();
    // The outline is the only zone, so it carries the left collapse entry.
    expect(within(screen.getByRole("region", { name: "大纲" })).getByRole("button", { name: "收起左栏" })).toBeTruthy();

    // The section on screen is the one whose key must collapse on the first click.
    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.queryByRole("region", { name: "大纲" })).toBeNull();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");
  });

  it("adopts the first assigned id without resetting the chosen collapse state", async () => {
    const draft = { name: "Draft", nodes: workflow.nodes, connections: {} };
    const { rerender } = render(<XFlowEditor value={draft} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    // The admin create flow saves the draft and sets its id on the same
    // mounted editor: that is adoption, not a workflow swap.
    rerender(<XFlowEditor value={{ ...draft, id: "wf-created" }} />);
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    await waitFor(() =>
      expect(JSON.parse(window.localStorage.getItem(panelStorage("wf-created")) ?? "null")).toEqual({
        leftCollapsed: true,
        rightCollapsed: false
      })
    );
  });

  it("remembers the collapse state per workflow and only for saved workflows", async () => {
    const { unmount } = render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    await waitFor(() =>
      expect(JSON.parse(window.localStorage.getItem(panelStorage("wf-panels")) ?? "null")).toEqual({
        leftCollapsed: true,
        rightCollapsed: true
      })
    );
    unmount();

    // A fresh mount of the same workflow restores both panels.
    const { unmount: unmountRestored } = render(<XFlowEditor value={workflow} />);
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    unmountRestored();

    // Another workflow keeps its own (default) state.
    render(<XFlowEditor value={{ ...workflow, id: "wf-other" }} />);
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
    await waitFor(() =>
      expect(JSON.parse(window.localStorage.getItem(panelStorage("wf-other")) ?? "null")).toEqual({
        leftCollapsed: false,
        rightCollapsed: false
      })
    );
  });

  it("does not persist an unsaved workflow and survives storage failures", () => {
    const withoutId = { name: "Draft", nodes: workflow.nodes, connections: {} };
    const { unmount } = render(<XFlowEditor value={withoutId} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(storedKeys().filter((key) => key.startsWith("xflow-editor:panels:"))).toEqual([]);
    unmount();

    // Denied storage must silently fall back to the expanded default.
    const getItem = vi.spyOn(storage, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    const setItem = vi.spyOn(storage, "setItem").mockImplementation(() => {
      throw new Error("denied");
    });
    try {
      render(<XFlowEditor value={workflow} />);
      expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
      expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
      fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
      expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    } finally {
      getItem.mockRestore();
      setItem.mockRestore();
    }
  });
});

afterEach(() => vi.restoreAllMocks());
