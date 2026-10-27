import * as React from "react";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, WorkflowDef } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";
import { READABLE_FIT_ZOOM } from "@xflow/preview";
import { XFlowEditor } from "./index";
import { RailKey } from "./panels/rail";

// E5a (spec §6.0.1, §6.5): the left rail, the dual-zone shell (节点 over 大纲)
// and the node-library rows. The outline tree itself is read in
// XFlowEditor.outline.test.tsx; here the outline is its title bar and actions.

// React Flow never initialises in jsdom, so the preview's view handle would
// never arrive. This pass-through wrapper hands the editor a spy handle the way
// the real preview does once it is live.
const preview = vi.hoisted(() => ({
  handle: { fitView: vi.fn(), fitNode: vi.fn() }
}));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    React.useEffect(() => {
      props.onReady?.(preview.handle);
    }, [props.onReady]);
    return React.createElement(actual.XFlowPreview, props);
  };
  return { ...actual, XFlowPreview: Wrapped };
});

const workflow: WorkflowDef = {
  id: "wf-left-column",
  name: "Left column",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "fetch", type: "xflow.http" },
    { name: "finish", type: "xflow.end" }
  ],
  connections: { start: { main: [{ node: "fetch" }] }, fetch: { main: [{ node: "finish" }] } }
};

/** Stands in for the ResizeObserver that measures the editor root (jsdom has no layout). */
const mockEditorWidth = () => {
  const observers: { callback: ResizeObserverCallback; targets: Set<Element> }[] = [];
  class MockResizeObserver {
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
      const index = observers.indexOf(this);
      if (index >= 0) observers.splice(index, 1);
    }
  }
  globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
  return {
    setWidth(width: number) {
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
    }
  };
};

const originalResizeObserver = globalThis.ResizeObserver;

beforeEach(() => {
  preview.handle.fitView.mockClear();
  preview.handle.fitNode.mockClear();
  // Panel collapse is persisted per workflow id (§6.0); a case that ends with
  // the rail collapsed must not hand that state to the next case.
  window.localStorage?.clear();
});

afterEach(() => {
  globalThis.ResizeObserver = originalResizeObserver;
  vi.unstubAllEnvs();
});

const railNames = (rail: HTMLElement) => within(rail).getAllByRole("button").map((button) => button.getAttribute("aria-label"));
const desktopRail = () => screen.getByRole("navigation", { name: "工作台导航" });
const zones = () => screen.getByRole("complementary", { name: "节点与大纲" });
const countOf = (region: HTMLElement) => region.querySelector(".xflow-editor-panel-title__count")?.textContent;
/** ED-14 folds every group but the first by default; row tests open the rest. */
const expandAllGroups = () => {
  const library = screen.getByRole("region", { name: "节点" });
  for (const group of library.querySelectorAll<HTMLButtonElement>(".xflow-editor-node-group__title[aria-expanded='false']")) {
    fireEvent.click(group);
  }
};
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};
/** The 未接入 notes currently showing: AntD keeps a closed tooltip mounted but hidden. */
const openNotes = () => screen.queryAllByText("未接入").filter((note) => !note.closest(".ant-tooltip-hidden"));

/**
 * A rail key carrying the 未接入 note. The editor no longer uses `note` (P3e
 * wires 执行), but RailKey still owns the hover/keyboard contract the cases
 * below pin, so they exercise the component directly instead of a placeholder
 * key that no longer exists.
 */
const renderNoteKey = (onClick = vi.fn()): HTMLButtonElement => {
  render(<RailKey icon={<span />} label="执行记录" note="未接入" text="执行" onClick={onClick} />);
  return screen.getByRole("button", { name: "执行记录" }) as HTMLButtonElement;
};

describe("left rail (§6.5)", () => {
  it("lists 节点与大纲, 执行记录, 变量 and then 帮助, in the baseline order", () => {
    render(<XFlowEditor value={workflow} />);

    expect(railNames(desktopRail())).toEqual(["节点与大纲", "执行记录", "变量", "快捷键与帮助"]);
    // The short caption under each icon.
    expect(within(desktopRail()).getAllByRole("button").map((button) => button.textContent)).toEqual([
      "节点",
      "执行",
      "变量",
      "帮助"
    ]);
    expect(within(desktopRail()).getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");
    expect(within(desktopRail()).getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("false");
  });

  it("draws 执行 as a live entry: no 未接入 note and a switch to 运行记录", async () => {
    render(<XFlowEditor value={workflow} />);
    const run = within(desktopRail()).getByRole("button", { name: "执行记录" });

    // P3e wired the key: it is a toggle now, never a described placeholder.
    expect(run.getAttribute("aria-description")).toBeNull();
    expect(run.getAttribute("aria-pressed")).toBe("false");

    fireEvent.mouseEnter(run);
    await waitFor(() => expect(openNotes()).toHaveLength(0));

    fireEvent.click(run);
    expect(run.getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
    expect(within(desktopRail()).getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");
  });

  it("shows the 未接入 note when the key takes keyboard focus, not only on hover", async () => {
    const run = renderNoteKey();
    expect(openNotes()).toHaveLength(0);

    act(() => run.focus());

    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
  });

  it("opens the 未接入 note to the right of the rail, never above it over the key", async () => {
    const run = renderNoteKey();

    act(() => run.focus());
    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));

    // A top-placed note would cover the key right above the note's owner.
    const root = openNotes()[0].closest(".xflow-editor-tooltip");
    expect(root?.className).toContain("placement-right");
    expect(root?.className).not.toContain("placement-top");
  });

  it("closes the 未接入 note on a mouse click and does not reopen it from the click's focus", async () => {
    const run = renderNoteKey();

    fireEvent.mouseEnter(run);
    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));

    // A real click is press, focus, release, click.
    fireEvent.mouseDown(run);
    act(() => run.focus());
    fireEvent.mouseUp(run);
    fireEvent.click(run);

    await waitFor(() => expect(openNotes()).toHaveLength(0));
    expect(document.activeElement).toBe(run);
  });

  it("does not open the note from the focus a mouse press gives, only from a keyboard focus", async () => {
    const run = renderNoteKey();

    fireEvent.mouseDown(run);
    act(() => run.focus());
    expect(openNotes()).toHaveLength(0);

    // Leaving and coming back by keyboard opens it.
    act(() => run.blur());
    act(() => run.focus());
    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
  });

  it("does not swallow a later keyboard focus after a press that gave no focus", async () => {
    const run = renderNoteKey();

    // Safari and Firefox on macOS do not focus a button on click.
    fireEvent.mouseDown(run);
    fireEvent.mouseUp(run);
    fireEvent.click(run);
    act(() => run.focus());

    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
  });

  it("drops a press that leaves the key before release, so the next focus is a keyboard one", async () => {
    const run = renderNoteKey();

    fireEvent.mouseDown(run);
    fireEvent.mouseLeave(run);
    act(() => run.focus());

    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
  });

  it("closes a keyboard-opened note on Esc", async () => {
    const run = renderNoteKey();

    act(() => run.focus());
    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
    fireEvent.keyDown(run, { key: "Escape" });

    await waitFor(() => expect(openNotes()).toHaveLength(0));
  });

  it("closes the note when its key loses focus", async () => {
    const run = renderNoteKey();

    act(() => run.focus());
    await waitFor(() => expect(openNotes().length).toBeGreaterThan(0));
    act(() => run.blur());

    await waitFor(() => expect(openNotes()).toHaveLength(0));
  });

  it("moves focus to the 节点 rail key when 收起左栏 unmounts the key that had it", () => {
    render(<XFlowEditor value={workflow} />);
    const collapse = screen.getByRole("button", { name: "收起左栏" });
    act(() => collapse.focus());

    fireEvent.click(collapse);

    expect(document.activeElement).toBe(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
  });

  it("moves focus to the 节点 rail key when ⌘B collapses the column under a focused library row", () => {
    render(<XFlowEditor value={workflow} />);
    const tile = document.querySelector<HTMLElement>(".xflow-editor-node-tile");
    if (!tile) throw new Error("no library row");
    act(() => tile.focus());

    fireEvent.keyDown(tile, { key: "b", code: "KeyB", metaKey: true });

    expect(screen.queryByRole("complementary", { name: "节点与大纲" })).toBeNull();
    expect(document.activeElement).toBe(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
  });

  it("leaves focus alone when the column collapses while it is somewhere else", () => {
    render(<XFlowEditor value={workflow} />);
    const card = canvasNode("fetch");
    act(() => card.focus());

    fireEvent.keyDown(card, { key: "b", code: "KeyB", metaKey: true });

    expect(screen.queryByRole("complementary", { name: "节点与大纲" })).toBeNull();
    expect(document.activeElement).toBe(card);
  });

  it("keeps the note closed when a click lands right after the pointer enters", async () => {
    const run = renderNoteKey();

    // Click before any enter delay could have opened the note: a pending hover
    // timer must not reopen it over the panel afterwards.
    fireEvent.mouseEnter(run);
    fireEvent.mouseDown(run);
    act(() => run.focus());
    fireEvent.mouseUp(run);
    fireEvent.click(run);
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 400));
    });

    expect(openNotes()).toHaveLength(0);
  });

  it("keeps the rail icons at 16px and only the caption at 11px", () => {
    render(<XFlowEditor value={workflow} />);
    const run = within(desktopRail()).getByRole("button", { name: "执行记录" });

    // The caption is the one span that is not the glyph, so the 11px rule can
    // no longer reach the icon (jsdom has no cascade; the class is the hook).
    expect(run.querySelector("svg[data-xflow-icon]")?.classList.contains("xflow-editor-rail__text")).toBe(false);
    expect(run.querySelector(".xflow-editor-rail__text")?.textContent).toBe("执行");
    expect(run.querySelectorAll(".xflow-editor-rail__text")).toHaveLength(1);
  });

  it("survives the left column collapsing, at the rail width only", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));

    expect(screen.queryByRole("complementary", { name: "节点与大纲" })).toBeNull();
    expect(railNames(desktopRail())).toEqual(["节点与大纲", "执行记录", "变量", "快捷键与帮助"]);
    expect(document.querySelector(".xflow-editor-main")?.getAttribute("data-left-collapsed")).toBe("true");
    expect(within(desktopRail()).getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");

    // Clicking the 节点 key again opens it, and clicking it once more collapses.
    fireEvent.click(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
    expect(zones()).toBeTruthy();
    fireEvent.click(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
    expect(screen.queryByRole("complementary", { name: "节点与大纲" })).toBeNull();
  });

  it("hands focus to the compact 节点 key when shrinking the editor unmounts the focused outline row", () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1400));
    const row = document.querySelector<HTMLElement>('[role="treeitem"]');
    act(() => row?.focus());
    expect(document.activeElement).toBe(row);

    act(() => container.setWidth(1100));
    const compactRail = screen.getByRole("complementary", { name: "紧凑导航" });
    expect(document.activeElement).toBe(within(compactRail).getByRole("button", { name: "节点与大纲" }));
  });

  it("hands focus to the desktop 节点 key when widening the editor closes the drawer holding focus", () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1100));
    const compactRail = screen.getByRole("complementary", { name: "紧凑导航" });
    fireEvent.click(within(compactRail).getByRole("button", { name: "节点与大纲" }));
    const row = document.querySelector<HTMLElement>('.xflow-editor-compact-drawer [role="treeitem"]');
    expect(row).toBeTruthy();
    act(() => row?.focus());
    expect(document.activeElement).toBe(row);

    act(() => container.setWidth(1400));
    expect(document.activeElement).not.toBe(document.body);
    expect(document.activeElement).toBe(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
  });

  it("puts 属性 between the spacer and 帮助 at every compact width", () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    const compactRail = () => screen.getByRole("complementary", { name: "紧凑导航" });

    // <1280 (§6.0): no docked inspector, so the rail carries the 属性 key.
    act(() => container.setWidth(1279));
    expect(railNames(compactRail())).toEqual(["节点与大纲", "执行记录", "变量", "属性面板", "快捷键与帮助"]);

    // The same single rail below 1024.
    act(() => container.setWidth(1023));
    expect(railNames(compactRail())).toEqual(["节点与大纲", "执行记录", "变量", "属性面板", "快捷键与帮助"]);
    const run = within(compactRail()).getByRole("button", { name: "执行记录" });
    // P3e wired the key: it is the 运行记录 toggle in the compact rail too.
    expect(run.getAttribute("aria-description")).toBeNull();
    expect(run.getAttribute("aria-pressed")).toBe("false");
  });
});

describe("dual-zone left column (§6.0.1)", () => {
  it("is one aside named 节点与大纲 holding the 节点 and 大纲 regions with their counts", () => {
    render(<XFlowEditor value={workflow} />);
    expandAllGroups();

    const aside = zones();
    const library = within(aside).getByRole("region", { name: "节点" });
    const outline = within(aside).getByRole("region", { name: "大纲" });
    expect(aside.getAttribute("data-zones")).toBe("dual");

    // 节点 comes first (upper zone), 大纲 second (lower zone).
    expect(library.compareDocumentPosition(outline) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(library.querySelector("h3")?.textContent).toBe("节点");
    expect(outline.querySelector("h3")?.textContent).toBe("大纲");
    // 节点 counts the types in the library; 大纲 counts the workflow's nodes.
    expect(countOf(library)).toBe(String(library.querySelectorAll("button[data-node-type]").length));
    expect(Number(countOf(library))).toBeGreaterThan(0);
    expect(countOf(outline)).toBe("3");
    // The title-bar keys: the collapse action stays on the upper zone only.
    expect(within(library).getByRole("button", { name: "收起左栏" })).toBeTruthy();
    expect(within(outline).queryByRole("button", { name: "收起左栏" })).toBeNull();
  });

  it("draws the baseline's side-left sprite glyph on the 收起左栏 key", () => {
    render(<XFlowEditor value={workflow} />);
    const collapse = within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "收起左栏" });
    // The title-bar key hook the stylesheet sizes (32x24, 6px radius).
    expect(collapse.classList.contains("xflow-editor-panel-collapse")).toBe(true);
    expect(collapse.getAttribute("aria-expanded")).toBe("true");

    // The sprite's own geometry, ported verbatim: panel frame plus the divider
    // near the left edge (#i-side-left in the baseline mockup).
    const glyph = collapse.querySelector<SVGSVGElement>('[data-xflow-icon="side-left"]');
    expect(glyph).toBeTruthy();
    expect(glyph?.getAttribute("aria-hidden")).toBe("true");
    expect(glyph?.getAttribute("viewBox")).toBe("0 0 24 24");
    expect(glyph?.getAttribute("width")).toBe("16");
    expect(glyph?.getAttribute("height")).toBe("16");
    expect(glyph?.getAttribute("fill")).toBe("none");
    expect(glyph?.getAttribute("stroke")).toBe("currentColor");
    expect(glyph?.getAttribute("stroke-width")).toBe("1.75");
    expect(glyph?.querySelector("rect")?.getAttribute("x")).toBe("3.5");
    expect(glyph?.querySelector("path")?.getAttribute("d")).toBe("M9.5 5v14");
    // The AntD menu glyph this replaced is gone.
    expect(collapse.querySelector(".anticon")).toBeNull();
  });

  it("locates the selected node on the canvas, and is disabled with nothing selected", () => {
    const { unmount } = render(<XFlowEditor value={{ ...workflow, nodes: [], connections: {} }} />);
    const idle = within(screen.getByRole("region", { name: "大纲" })).getByRole("button", { name: "定位选中节点" }) as HTMLButtonElement;
    expect(idle.disabled).toBe(true);
    fireEvent.click(idle);
    expect(preview.handle.fitNode).not.toHaveBeenCalled();
    unmount();

    render(<XFlowEditor value={workflow} />);
    const locate = () => within(screen.getByRole("region", { name: "大纲" })).getByRole("button", { name: "定位选中节点" }) as HTMLButtonElement;
    expect(locate().disabled).toBe(false);

    fireEvent.click(locate());
    expect(preview.handle.fitNode).toHaveBeenCalledTimes(1);
    const first = preview.handle.fitNode.mock.calls[0][0];
    expect(typeof first).toBe("string");

    // A different selection fits a different node.
    fireEvent.click(canvasNode("finish"));
    fireEvent.click(locate());
    expect(preview.handle.fitNode).toHaveBeenCalledTimes(2);
    expect(preview.handle.fitNode.mock.calls[1][0]).not.toBe(first);
  });

  it("turns 搜索大纲 off when the workflow has no node to find", () => {
    render(<XFlowEditor value={{ ...workflow, nodes: [], connections: {} }} />);

    const search = within(screen.getByRole("region", { name: "大纲" })).getByRole("button", { name: "搜索大纲" }) as HTMLButtonElement;
    expect(search.disabled).toBe(true);
  });

  it("opens the ⌘K node search from 搜索大纲", async () => {
    render(<XFlowEditor value={workflow} />);
    expect(screen.queryByRole("combobox", { name: "定位节点" })).toBeNull();

    fireEvent.click(within(screen.getByRole("region", { name: "大纲" })).getByRole("button", { name: "搜索大纲" }));

    expect(await screen.findByRole("combobox", { name: "定位节点" })).toBeTruthy();
  });

  it("shows only the outline in a read-only viewer, with 变量 still one rail key away", () => {
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" />);

    const aside = zones();
    expect(aside.getAttribute("data-zones")).toBe("single");
    expect(within(aside).queryByRole("region", { name: "节点" })).toBeNull();
    const outline = within(aside).getByRole("region", { name: "大纲" });
    expect(countOf(outline)).toBe("3");
    // With no library zone above it, the outline carries the collapse action.
    expect(within(outline).getByRole("button", { name: "收起左栏" })).toBeTruthy();
    expect(within(desktopRail()).getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(within(desktopRail()).getByRole("button", { name: "变量" }));
    expect(screen.getByRole("region", { name: "变量" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "大纲" })).toBeNull();
  });

  it("replaces both zones with the variables panel, and leaves the draft alone", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();

    fireEvent.click(within(desktopRail()).getByRole("button", { name: "变量" }));

    expect(screen.getByRole("region", { name: "变量" })).toBeTruthy();
    expect(screen.queryByRole("complementary", { name: "节点与大纲" })).toBeNull();
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.queryByRole("region", { name: "大纲" })).toBeNull();
    expect(within(desktopRail()).getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("true");
    expect(within(desktopRail()).getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");
    // No route change and no write: switching sections is view state only.
    expect(handleChange).not.toHaveBeenCalled();

    fireEvent.click(within(desktopRail()).getByRole("button", { name: "节点与大纲" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
  });

  it("opens the compact drawer as the same two zones", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "节点与大纲" }));

    const dialog = await screen.findByRole("dialog", { name: "节点与大纲" });
    expect(within(dialog).getByRole("region", { name: "节点" })).toBeTruthy();
    expect(within(dialog).getByRole("region", { name: "大纲" })).toBeTruthy();
  });

  it("draws the compact drawer inside the workbench body, with no header of its own", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "节点与大纲" }));

    const dialog = await screen.findByRole("dialog", { name: "节点与大纲" });
    // Under the top bar and above the status bar, so neither is covered (baseline).
    expect(dialog.closest(".xflow-editor-main")).not.toBeNull();
    expect(dialog.closest("header")).toBeNull();
    // The panel title bar names the panel; an empty header would only waste 55px.
    expect(document.querySelector(".xflow-editor-compact-drawer__header")).toBeNull();
    expect(document.querySelector(".xflow-editor-compact-drawer__close")).toBeNull();
  });

  it("marks the left drawer wrapper with its canvas-side modifier", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "节点与大纲" }));

    const dialog = await screen.findByRole("dialog", { name: "节点与大纲" });
    const wrapper = dialog.closest(".xflow-editor-compact-drawer__wrapper");
    expect(wrapper?.classList.contains("xflow-editor-compact-drawer__wrapper--start")).toBe(true);
    expect(wrapper?.classList.contains("xflow-editor-compact-drawer__wrapper--end")).toBe(false);
  });

  it("hands focus back to the 节点与大纲 key when Escape closes the compact left drawer", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    const key = within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "节点与大纲" });
    key.focus();
    fireEvent.click(key);
    const dialog = await screen.findByRole("dialog", { name: "节点与大纲" });
    await waitFor(() => expect(dialog.contains(document.activeElement)).toBe(true));

    fireEvent.keyDown(document.activeElement ?? dialog, { key: "Escape" });

    await waitFor(() => expect(screen.queryByRole("dialog", { name: "节点与大纲" })).toBeNull());
    expect(document.activeElement).toBe(key);
  });

  it("hands focus back to the 属性面板 key when Escape closes the compact right drawer", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    const key = within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "属性面板" });
    key.focus();
    fireEvent.click(key);
    const dialog = await screen.findByRole("dialog", { name: "属性" });

    fireEvent.keyDown(document.activeElement ?? dialog, { key: "Escape" });

    await waitFor(() => expect(screen.queryByRole("dialog", { name: "属性" })).toBeNull());
    expect(document.activeElement).toBe(key);
  });

  it("names the compact right drawer close button 关闭", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "属性面板" }));

    const dialog = await screen.findByRole("dialog", { name: "属性" });
    expect(within(dialog).getByRole("button", { name: "关闭" })).toBeTruthy();
    expect(within(dialog).queryByRole("button", { name: "Close" })).toBeNull();
  });

  it("draws the compact right drawer inside the workbench body too, so both start under the top bar", async () => {
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));

    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "属性面板" }));

    const dialog = await screen.findByRole("dialog", { name: "属性" });
    expect(dialog.closest(".xflow-editor-main")).not.toBeNull();
    expect(dialog.closest("header")).toBeNull();
  });

  it("draws the library group head label, then the chevron, then the count last", () => {
    render(<XFlowEditor value={workflow} />);
    const head = document.querySelector<HTMLElement>(".xflow-editor-node-group__title")!;
    const parts = Array.from(head.querySelector(".xflow-editor-node-group__heading")!.children).map((child) =>
      child.classList.contains("xflow-editor-node-group__chevron")
        ? "chevron"
        : child.classList.contains("xflow-editor-node-group__count")
          ? "count"
          : "label"
    );
    expect(parts).toEqual(["label", "chevron", "count"]);
    // The chevron is decoration: it must not leak into the toggle's accessible name.
    expect(head.querySelector(".xflow-editor-node-group__chevron")?.getAttribute("aria-hidden")).toBe("true");
    expect(head.parentElement?.querySelector(":scope > .xflow-editor-node-group__title")).toBe(head);
  });

  it("closes the locate modal first when Esc is pressed over the compact drawer", async () => {
    // rc hands every overlay the constant id "test-id" when NODE_ENV is "test",
    // which folds the drawer and the modal into one entry of its Esc stack. A
    // real build gives each its own id, and the stack then closes the top one.
    vi.stubEnv("NODE_ENV", "development");
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1279));
    fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "节点与大纲" }));
    const dialog = await screen.findByRole("dialog", { name: "节点与大纲" });

    fireEvent.click(within(dialog).getByRole("button", { name: "搜索大纲" }));
    const search = await screen.findByRole("combobox", { name: "定位节点" });
    fireEvent.keyDown(search, { key: "Escape", code: "Escape", keyCode: 27 });

    // The modal goes, the drawer under it stays.
    await waitFor(() => expect(screen.queryByRole("combobox", { name: "定位节点" })).toBeNull());
    expect(screen.getByRole("dialog", { name: "节点与大纲" })).toBeTruthy();
  });
});

describe("node-library rows (§6.0.1)", () => {
  const schema = (node_type: string, extra: Record<string, unknown> = {}) => ({
    spec: "node-form/v1",
    node_type,
    node_version: 1,
    kind: "action",
    fields: [],
    ...extra
  });
  const nodeTypes = {
    node_types: [
      schema("xflow.http", { display_name: "HTTP", description: "调用 HTTP 接口并返回响应" }),
      schema("acme.plain", { display_name: "Plain" }),
      schema("xflow.trigger.webhook", { kind: "trigger", display_name: "Webhook" }),
      schema("xflow.if", { display_name: "If" })
    ]
  } as unknown as NodeTypesResponse;
  const tile = (type: string) => {
    const found = screen.getByRole("region", { name: "节点" }).querySelector<HTMLButtonElement>(`button[data-node-type="${type}"]`);
    if (!found) throw new Error(`no library tile for ${type}`);
    return found;
  };
  const hint = (type: string) => tile(type).querySelector<HTMLElement>(".xflow-editor-node-tile__hint");

  it("puts the type's description on the second line, ellipsised with a title", () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} />);
    expandAllGroups();

    expect(hint("xflow.http")?.textContent).toBe("调用 HTTP 接口并返回响应");
    expect(hint("xflow.http")?.getAttribute("title")).toBe("调用 HTTP 接口并返回响应");
    expect(tile("xflow.http").querySelector("strong")?.textContent).toBe("HTTP");
  });

  it("falls back to the type id when there is no description, and keeps the id searchable", () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} />);
    expandAllGroups();

    expect(hint("acme.plain")?.textContent).toBe("acme.plain");
    expect(hint("acme.plain")?.getAttribute("title")).toBe("acme.plain");

    const library = screen.getByRole("region", { name: "节点" });
    fireEvent.click(within(library).getByRole("button", { name: "搜索节点" }));
    fireEvent.change(within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…"), { target: { value: "acme." } });
    expect(library.querySelector('button[data-node-type="acme.plain"]')).toBeTruthy();
    expect(library.querySelector('button[data-node-type="xflow.http"]')).toBeNull();
  });

  it("draws a category icon block whose data-cat comes from the type", () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} />);
    expandAllGroups();

    const cat = (type: string) => tile(type).querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat");
    expect(cat("xflow.http")).toBe("action");
    expect(cat("xflow.trigger.webhook")).toBe("trigger");
    expect(cat("xflow.if")).toBe("control");
    expect(cat("acme.plain")).toBe("unknown");
    // The old tone-driven block and the trailing + are gone.
    expect(tile("xflow.http").querySelector(".xflow-editor-node-tile__icon")).toBeNull();
    expect(tile("xflow.http").hasAttribute("data-tone")).toBe(false);
    expect(tile("xflow.http").textContent).not.toContain("＋");
  });

  it("hides the search box by default and opens it from the title-bar key", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });

    // Hidden and out of the layout until asked for; the key points at the box.
    expect(within(library).queryByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeNull();
    expect(library.querySelector(".xflow-editor-library__search")?.hasAttribute("hidden")).toBe(true);
    expect(key.getAttribute("aria-expanded")).toBe("false");
    expect(key.getAttribute("aria-controls")).toBe(library.querySelector(".xflow-editor-library__search")?.id);
    expect(key.hasAttribute("data-active")).toBe(false);
    // 32x24 ghost, the same title-bar action as the outline's search.
    expect(key.classList.contains("xflow-editor-panel-collapse")).toBe(true);

    fireEvent.click(key);

    const search = within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    expect(document.activeElement).toBe(search);
    expect(key.getAttribute("aria-expanded")).toBe("true");
    expect(key.getAttribute("data-active")).toBe("true");
    expect(search.closest(".xflow-editor-library-search")?.querySelector("kbd")?.textContent).toBe("/");
  });

  it("opens the search from / and keeps the key pressed while a filter is set", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });
    expect(document.activeElement).not.toBe(key);

    fireEvent.keyDown(window, { key: "/" });
    const search = within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    expect(document.activeElement).toBe(search);

    fireEvent.change(search, { target: { value: "http" } });
    expect(key.getAttribute("data-active")).toBe("true");
    // The filter narrows the list.
    expect(within(library).getByRole("button", { name: "HTTP" })).toBeTruthy();
    expect(within(library).queryByRole("button", { name: "Kafka" })).toBeNull();
  });

  it("closes the search on Esc, clearing the query and returning focus to the key", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });

    fireEvent.click(key);
    const search = within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    fireEvent.change(search, { target: { value: "http" } });
    fireEvent.keyDown(search, { key: "Escape" });

    expect(within(library).queryByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeNull();
    expect(document.activeElement).toBe(key);
    expect(key.getAttribute("aria-expanded")).toBe("false");

    // Reopening starts from an empty filter.
    fireEvent.click(key);
    expect((within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…") as HTMLInputElement).value).toBe("");
  });

  it("folds an empty search away on blur but keeps a keyword on screen", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });
    const open = () => {
      fireEvent.click(key);
      return within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    };

    fireEvent.blur(open());
    expect(within(library).queryByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeNull();

    const filtered = open();
    fireEvent.change(filtered, { target: { value: "http" } });
    fireEvent.blur(filtered);
    // The list stays filtered, so its filter must stay visible.
    expect(within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeTruthy();
    expect(key.getAttribute("aria-expanded")).toBe("true");
  });

  it("closes and clears the search when its key is clicked again", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });

    fireEvent.click(key);
    fireEvent.change(within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…"), { target: { value: "http" } });
    fireEvent.click(key);

    expect(within(library).queryByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeNull();
    expect(key.getAttribute("aria-expanded")).toBe("false");
    expect(key.hasAttribute("data-active")).toBe(false);
  });

  it("advertises / on the search key and closes the search from the key itself", () => {
    render(<XFlowEditor value={workflow} />);
    const library = screen.getByRole("region", { name: "节点" });
    const key = within(library).getByRole("button", { name: "搜索节点" });
    expect(key.getAttribute("aria-keyshortcuts")).toBe("/");

    fireEvent.keyDown(window, { key: "/" });
    expect(within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeTruthy();
    expect(key.getAttribute("aria-expanded")).toBe("true");

    // Esc from anywhere (here: the key, not the input) closes and restores focus.
    key.focus();
    fireEvent.keyDown(key, { key: "Escape" });
    expect(within(library).queryByPlaceholderText("搜索 Webhook / If / HTTP…")).toBeNull();
    expect(document.activeElement).toBe(key);
  });

  it("reopens a collapsed column and focuses the search on /", async () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    fireEvent.keyDown(window, { key: "/" });

    await waitFor(() => {
      const search = within(screen.getByRole("region", { name: "节点" })).getByPlaceholderText("搜索 Webhook / If / HTTP…");
      expect(document.activeElement).toBe(search);
    });
  });
});

describe("canvas fit (§6.2, ED-15)", () => {
  it("fits the whole graph at the readable floor from the pane menu", () => {
    const { container } = render(<XFlowEditor value={workflow} />);
    preview.handle.fitView.mockClear();

    fireEvent.contextMenu(container.querySelector(".react-flow__pane") as Element, { clientX: 640, clientY: 380 });
    fireEvent.click(screen.getByRole("menuitem", { name: /适应视图/ }));

    expect(preview.handle.fitView).toHaveBeenCalledWith({ minZoom: READABLE_FIT_ZOOM });
  });
});
