import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";

/** The canvas node's own select button: the canvas cards stay a selectable list beside the outline tree. */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};
/** Deselects the node (Esc) so the Inspector shows the workflow settings. */
const workflowSettings = (): HTMLElement => {
  const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
  // A selected node shows its head; Esc deselects it (the spec's path back to the workflow settings).
  if (inspector.querySelector(".xflow-editor-inspector-head")) fireEvent.keyDown(window, { key: "Escape" });
  return screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
};
/** Opens the left column's 变量 panel from the desktop rail. */
const variablesPanel = (): HTMLElement => {
  fireEvent.click(screen.getByRole("button", { name: "变量" }));
  return screen.getByRole("region", { name: "变量" });
};

describe("XFlowEditor", () => {
  const originalMatchMedia = window.matchMedia;
  const workflow = {
    id: "wf-purchase-approval",
    name: "Purchase approval",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" as const },
      {
        name: "route_by_amount",
        type: "xflow.switch",
        notes: "Route purchase requests by amount"
      },
      { name: "l1_manager", type: "xflow.approval" }
    ],
    connections: {
      start: {
        main: [{ node: "route_by_amount" }]
      },
      route_by_amount: {
        level_1: [{ node: "l1_manager" }]
      }
    }
  };

  const mockSystemColorScheme = (matches: boolean) => {
    const createMediaQuery = (media: string, initialMatches: boolean) => {
      let currentMatches = initialMatches;
      const listeners = new Set<(event: MediaQueryListEvent) => void>();
      const addEventListener = vi.fn(
        (type: string, listener: EventListenerOrEventListenerObject | null) => {
          if (type === "change" && typeof listener === "function") {
            listeners.add(listener as (event: MediaQueryListEvent) => void);
          }
        }
      );
      const removeEventListener = vi.fn(
        (type: string, listener: EventListenerOrEventListenerObject | null) => {
          if (type === "change" && typeof listener === "function") {
            listeners.delete(listener as (event: MediaQueryListEvent) => void);
          }
        }
      );
      const mediaQuery = {
        get matches() {
          return currentMatches;
        },
        media,
        onchange: null,
        addListener: vi.fn((listener: ((event: MediaQueryListEvent) => void) | null) => {
          if (listener) listeners.add(listener);
        }),
        removeListener: vi.fn((listener: ((event: MediaQueryListEvent) => void) | null) => {
          if (listener) listeners.delete(listener);
        }),
        addEventListener,
        removeEventListener,
        dispatchEvent: vi.fn(() => false)
      } as unknown as MediaQueryList;

      return {
        mediaQuery,
        addEventListener,
        removeEventListener,
        listenerCount: () => listeners.size,
        setMatches: (nextMatches: boolean) => {
          currentMatches = nextMatches;
          const event = { matches: currentMatches, media } as MediaQueryListEvent;
          [...listeners].forEach((listener) => listener(event));
        }
      };
    };
    const colorScheme = createMediaQuery("(prefers-color-scheme: dark)", matches);
    // Width is no longer a media query: the editor measures its own container.
    // Any other query falls back to the color-scheme list.
    const matchMedia = vi.fn((_query: string) => colorScheme.mediaQuery);
    window.matchMedia = matchMedia;

    return Object.assign(matchMedia, {
      addEventListener: colorScheme.addEventListener,
      removeEventListener: colorScheme.removeEventListener,
      listenerCount: colorScheme.listenerCount,
      setMatches: colorScheme.setMatches
    });
  };

  /**
   * Stands in for the ResizeObserver that measures the editor root. jsdom has
   * no layout, so tests report the inline size explicitly. Only the editor's
   * own `.xflow-editor` root is answered; AntD's internal observers observe
   * other elements and must not receive these fake dimensions.
   */
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

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    globalThis.ResizeObserver = originalResizeObserver;
  });

  it("only shows the replay notice in 运行记录", () => {
    render(<XFlowEditor value={workflow} />);

    expect(screen.queryByText("可编辑节点与连接配置")).toBeNull();
    expect(screen.queryByText("只读回放")).toBeNull();

    fireEvent.click(screen.getByRole("radio", { name: /运行记录/ }));

    expect(screen.getByText("只读回放")).toBeTruthy();
    expect(screen.getByText("· 节点不可拖动、不可连线")).toBeTruthy();
  });

  it("drives the canvas tool from shortcuts and keeps history in the toolbar", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    const tools = screen.getByRole("group", { name: "常用画布与历史工具" });
    const undo = within(tools).getByRole("button", { name: "撤销" }) as HTMLButtonElement;
    const redo = within(tools).getByRole("button", { name: "重做" }) as HTMLButtonElement;

    // §6.2 removed the select/connect mode buttons; V / L are the bindings.
    expect(within(tools).getAllByRole("button")).toHaveLength(2);
    expect(screen.queryByRole("button", { name: "选择工具" })).toBeNull();
    expect(screen.queryByRole("button", { name: "连线工具" })).toBeNull();
    expect(editor.getAttribute("data-canvas-tool")).toBe("select");
    expect(undo.disabled).toBe(true);
    expect(redo.disabled).toBe(true);

    fireEvent.keyDown(window, { key: "l" });
    expect(editor.getAttribute("data-canvas-tool")).toBe("connect");
    fireEvent.keyDown(window, { key: "v" });
    expect(editor.getAttribute("data-canvas-tool")).toBe("select");

    fireEvent.click(within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "HTTP" }));
    expect(undo.disabled).toBe(false);
    fireEvent.click(undo);
    expect(handleChange).toHaveBeenLastCalledWith(workflow);
    expect(redo.disabled).toBe(false);
    fireEvent.click(redo);
    expect(handleChange.mock.calls.at(-1)?.[0]?.nodes).toHaveLength(4);
  });

  it("collapses node-library groups without removing their discovery affordance", () => {
    render(<XFlowEditor value={workflow} />);

    const library = screen.getByRole("region", { name: "节点" });
    const triggers = within(library).getByRole("button", { name: /触发器 4/ });
    expect(triggers.getAttribute("aria-expanded")).toBe("true");
    expect(within(library).getByRole("button", { name: "Webhook" })).toBeTruthy();

    fireEvent.click(triggers);
    expect(triggers.getAttribute("aria-expanded")).toBe("false");
    expect(within(library).queryByRole("button", { name: "Webhook" })).toBeNull();

    fireEvent.click(triggers);
    expect(triggers.getAttribute("aria-expanded")).toBe("true");
  });

  it("starts a library drag with the node type the canvas reads back on drop", () => {
    render(<XFlowEditor value={workflow} />);

    const tile = within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "HTTP" });
    expect(tile.getAttribute("draggable")).toBe("true");

    const setData = vi.fn();
    fireEvent.dragStart(tile, { dataTransfer: { setData, effectAllowed: "" } });

    // Literal, not the shared constant: this string is the cross-package
    // contract with the preview's drop reader, so a rename must fail here.
    expect(setData).toHaveBeenCalledWith("application/xflow-node-type", "xflow.http");
  });

  it("renders the editable workbench around the preview canvas", async () => {
    render(
      <XFlowEditor
        value={workflow}
        runtime={{
          status: "running",
          nodes: {
            route_by_amount: { status: "running", attempts: 2, durationMs: 1840 }
          }
        }}
      />
    );

    expect(screen.getByRole("region", { name: "Purchase approval editor" })).toBeTruthy();
    expect(screen.getByRole("toolbar", { name: "编辑器工具栏" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    // §6.0.1: the left column is two zones, the library over the outline tree.
    expect(screen.getByRole("complementary", { name: "节点与大纲" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
    // §6.3 (E2b): the top bar has no search field; ⌘K opens a locate panel and
    // the canvas float carries the magnifier instead.
    expect(screen.queryByRole("combobox", { name: "定位节点" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "定位节点 ⌘K" }));
    expect(await screen.findByRole("combobox", { name: "定位节点" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "画布" })).toBeTruthy();
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
    // Collapsed: only the 28px status strip is there, named 展开诊断台; the
    // drawer (tabs, summary) mounts on open and is named by its own heading.
    expect(screen.getByRole("region", { name: "诊断台" })).toBeTruthy();
    expect(screen.queryByRole("tab", { name: "运行日志" })).toBeNull();
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起左栏" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起属性面板" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByRole("region", { name: "诊断台" })).toBeTruthy();
    // Opening moves focus to this tab, and rc-tabs prefixes the focused tab's name with "Tab 1 of 4".
    expect(screen.getByRole("tab", { name: /问题 \d+$/ })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "运行日志" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "输入" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "输出" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起诊断台" })).toBeTruthy();
  });

  it("exposes the new node statuses on the hooks the token rules target", () => {
    // The fixture's `route_by_amount` is a switch node, which is what the
    // editor selects by default, so one runtime entry drives both the canvas
    // node and the inspector's run panel.
    const { rerender } = render(
      <XFlowEditor value={workflow} runtime={{ nodes: { route_by_amount: { status: "committing" } } }} />
    );

    const canvasCard = () => canvasNode("route_by_amount").closest(".xflow-preview-node");
    expect(canvasCard()?.getAttribute("data-status")).toBe("committing");

    fireEvent.click(screen.getByRole("tab", { name: "运行" }));
    expect(
      screen.getByRole("tabpanel").querySelector(".xflow-editor-chip")?.getAttribute("data-status")
    ).toBe("committing");

    // Both statuses carry data-status rather than a preset color name, which is
    // what lets the editor-scoped rules tone them from the shared
    // --xflow-status-* tokens: no preset tells `committing` from `running`, or
    // `expired` from a status the editor does not recognize.
    rerender(
      <XFlowEditor value={workflow} runtime={{ nodes: { route_by_amount: { status: "expired" } } }} />
    );

    expect(canvasCard()?.getAttribute("data-status")).toBe("expired");
    expect(
      screen.getByRole("tabpanel").querySelector(".xflow-editor-chip")?.getAttribute("data-status")
    ).toBe("expired");
  });

  it("defaults the editor appearance to the system scheme", () => {
    mockSystemColorScheme(true);
    const dark = render(<XFlowEditor value={workflow} />);

    const darkEditor = screen.getByRole("region", { name: "Purchase approval editor" });
    expect(darkEditor.getAttribute("data-appearance")).toBe("system");
    expect(darkEditor.getAttribute("data-theme")).toBe("dark");

    dark.unmount();
    mockSystemColorScheme(false);
    render(<XFlowEditor value={workflow} />);

    const lightEditor = screen.getByRole("region", { name: "Purchase approval editor" });
    expect(lightEditor.getAttribute("data-appearance")).toBe("system");
    expect(lightEditor.getAttribute("data-theme")).toBe("light");
  });

  it("marks a definition without an id as unsaved until its first save", () => {
    render(<XFlowEditor value={{ name: "未命名工作流", spec: "1.0", nodes: [], connections: {} }} />);

    const chip = document.querySelector(".xflow-editor-chip");
    expect(chip?.textContent).toBe("未保存");
    expect(chip?.getAttribute("data-tone")).toBe("warning");
    // §6.3: 保存 keeps the quiet default appearance; dirty never turns it solid primary.
    const save = screen.getByRole("button", { name: "保存" });
    expect(save.className).not.toContain("ant-btn-color-primary");
    expect(save.className).not.toContain("ant-btn-variant-solid");
    expect(save.querySelector("kbd.xflow-editor-kbd")?.textContent).toBe("⌘S");
  });

  it("disables 保存 and makes ⌘S a no-op until an edit dirties the draft (baseline clean state)", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={handleSave} />);
    const save = () => screen.getByRole("button", { name: "保存" }) as HTMLButtonElement;
    // The saved definition matches its baseline: the key is drawn but unusable.
    expect(save().disabled).toBe(true);
    fireEvent.keyDown(document.body, { key: "s", metaKey: true });
    fireEvent.click(save());
    expect(handleSave).not.toHaveBeenCalled();

    // The first real edit enables both paths; a successful save disables them again.
    fireEvent.change(within(workflowSettings()).getByLabelText("工作流名称"), {
      target: { value: "Purchase approval edited" }
    });
    expect(save().disabled).toBe(false);
    fireEvent.click(save());
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(save().disabled).toBe(true));
  });

  it("reports the persisted version on the chip and keeps 已保存 without one", () => {
    const { rerender } = render(<XFlowEditor value={{ ...workflow, version: "2.1.0" }} />);
    const chip = () => document.querySelector(".xflow-editor-chip") as HTMLElement;
    expect(chip().textContent).toBe("线上 v2.1.0");
    expect(chip().getAttribute("data-tone")).toBe("success");

    rerender(<XFlowEditor value={{ ...workflow }} />);
    expect(chip().textContent).toBe("已保存");
    expect(chip().getAttribute("data-tone")).toBe("success");
  });

  it("counts edits against the saved version and returns to success after undo", () => {
    render(<XFlowEditor value={{ ...workflow, version: "2.1.0" }} onSave={vi.fn(async (next) => next)} />);
    const chip = () => document.querySelector(".xflow-editor-chip") as HTMLElement;
    expect(chip().textContent).toBe("线上 v2.1.0");

    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Renamed workflow" }
    });
    expect(chip().textContent).toBe("未保存 1 处 · 基于 v2.1.0");
    expect(chip().getAttribute("data-tone")).toBe("warning");

    fireEvent.click(within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" }));
    expect(chip().textContent).toBe("线上 v2.1.0");
    expect(chip().getAttribute("data-tone")).toBe("success");
  });

  it("names the baseline version, not an edited version field, in the dirty chip", () => {
    render(<XFlowEditor value={{ ...workflow, version: "2.1.0" }} />);
    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流版本"), {
      target: { value: "3.0.0" }
    });

    const chip = document.querySelector(".xflow-editor-chip") as HTMLElement;
    expect(chip.textContent).toBe("未保存 1 处 · 基于 v2.1.0");
    expect(chip.getAttribute("data-tone")).toBe("warning");
  });

  it("adopts a saved definition that adds an id and version as the new baseline", async () => {
    const draft = { name: "未命名工作流", spec: "1.0", nodes: [], connections: {} };
    const handleSave = vi.fn(async (next: typeof draft) => ({ ...next, id: "wf-new", version: "1.0.0" }));
    render(<XFlowEditor value={draft} onSave={handleSave} />);
    const chip = () => document.querySelector(".xflow-editor-chip") as HTMLElement;
    expect(chip().textContent).toBe("未保存");

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(chip().textContent).toBe("线上 v1.0.0"));
    expect(chip().getAttribute("data-tone")).toBe("success");
  });

  it("adopts a save result that changes the definition while edits are in flight", async () => {
    let resolveSave!: (saved: typeof workflow) => void;
    const pendingSave = new Promise<typeof workflow>((resolve) => {
      resolveSave = resolve;
    });
    const handleSave = vi.fn(() => pendingSave);
    const versioned = { ...workflow, version: "1.0.0" };
    render(<XFlowEditor value={versioned} onChange={vi.fn()} onSave={handleSave} />);
    const chip = () => document.querySelector(".xflow-editor-chip") as HTMLElement;

    // A clean draft cannot be saved (baseline clean state); dirty it first.
    const saved = { ...versioned, name: "Saved before pending" };
    fireEvent.change(within(workflowSettings()).getByLabelText("工作流名称"), {
      target: { value: saved.name }
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledWith(saved));

    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Edited while save is pending" }
    });

    await act(async () => {
      resolveSave({ ...saved, version: "1.1.0" });
      await pendingSave;
    });

    expect(within(inspector).getByDisplayValue("Edited while save is pending")).toBeTruthy();
    // The server snapshot is the baseline even though the draft kept the edit:
    // name (draft) and version (stale draft) both differ from it.
    expect(chip().textContent).toBe("未保存 2 处 · 基于 v1.1.0");
    expect(chip().getAttribute("data-tone")).toBe("warning");
  });

  it("renders the back button only when the host supplies onBack", () => {
    const handleBack = vi.fn();
    const { rerender } = render(<XFlowEditor value={workflow} onBack={handleBack} />);

    fireEvent.click(screen.getByRole("button", { name: "返回工作流列表" }));
    expect(handleBack).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "返回工作流列表" }).querySelector(".anticon")).toBeTruthy();

    rerender(<XFlowEditor value={workflow} />);
    expect(screen.queryByRole("button", { name: "返回工作流列表" })).toBeNull();
  });

  it("disables 新建工作流 in the ⋯ menu while in 运行记录 and keeps 沉浸布局 available", async () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("radio", { name: /运行记录/ }));

    // §6.3: the two former top-bar icon buttons live in ⋯ now, not beside it.
    expect(screen.queryByRole("button", { name: "进入沉浸布局" })).toBeNull();
    expect(screen.queryByRole("button", { name: "新建工作流" })).toBeNull();

    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    const newItem = await screen.findByRole("menuitem", { name: "新建工作流" });
    expect(newItem.getAttribute("aria-disabled")).toBe("true");
    expect(newItem.getAttribute("title")).toBe("运行记录模式为只读，请切换到编辑模式。");
    const layoutItem = screen.getByRole("menuitem", { name: "进入沉浸布局" });
    expect(layoutItem.getAttribute("aria-disabled")).toBeNull();
  });

  it("opens the run options placeholder with its single disabled item", async () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "运行选项" }));
    const items = await screen.findAllByRole("menuitem");
    expect(items).toHaveLength(1);
    expect(items[0].textContent).toBe("更多运行方式（未接入）");
    expect(items[0].getAttribute("aria-disabled")).toBe("true");
  });

  it("uses the controlled light and dark appearances on the editor root", () => {
    const { rerender } = render(<XFlowEditor value={workflow} appearance="light" />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    expect(editor.getAttribute("data-theme")).toBe("light");

    rerender(<XFlowEditor value={workflow} appearance="dark" />);

    expect(editor.getAttribute("data-theme")).toBe("dark");
  });

  it("resolves the system appearance from matchMedia", () => {
    const lightMatchMedia = mockSystemColorScheme(false);
    const lightEditor = render(<XFlowEditor value={workflow} appearance="system" />);

    expect(screen.getByRole("region", { name: "Purchase approval editor" }).getAttribute("data-theme")).toBe("light");
    expect(lightMatchMedia).toHaveBeenCalledWith("(prefers-color-scheme: dark)");

    lightEditor.unmount();

    const darkMatchMedia = mockSystemColorScheme(true);
    render(<XFlowEditor value={workflow} appearance="system" />);

    expect(screen.getByRole("region", { name: "Purchase approval editor" }).getAttribute("data-theme")).toBe("dark");
    expect(darkMatchMedia).toHaveBeenCalledWith("(prefers-color-scheme: dark)");
  });

  const openThemeSettings = async () => {
    fireEvent.click(screen.getByRole("button", { name: "主题设置" }));

    return {
      appearance: await screen.findByLabelText("颜色模式")
    };
  };

  it("opens compact theme settings with a labeled appearance control and no workbench-style choice", async () => {
    render(<XFlowEditor value={workflow} appearance="dark" />);

    expect(screen.queryByLabelText("颜色模式")).toBeNull();

    const { appearance } = await openThemeSettings();

    expect(within(appearance).getByText("浅色")).toBeTruthy();
    expect(within(appearance).getByText("深色")).toBeTruthy();
    expect(within(appearance).getByText("跟随系统")).toBeTruthy();
    expect(screen.queryByLabelText("工作台风格")).toBeNull();
  });

  it("updates the uncontrolled editor root after selecting an appearance", async () => {
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    const { appearance } = await openThemeSettings();

    fireEvent.click(within(appearance).getByText("浅色"));

    expect(editor.getAttribute("data-appearance")).toBe("light");
    expect(editor.getAttribute("data-theme")).toBe("light");

    fireEvent.click(within(appearance).getByText("深色"));

    expect(editor.getAttribute("data-appearance")).toBe("dark");
    expect(editor.getAttribute("data-theme")).toBe("dark");
  });

  it("updates the resolved system theme and cleans its media-query listener", () => {
    const systemColorScheme = mockSystemColorScheme(false);
    const { unmount } = render(<XFlowEditor value={workflow} appearance="system" />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    expect(editor.getAttribute("data-appearance")).toBe("system");
    expect(editor.getAttribute("data-theme")).toBe("light");
    expect(systemColorScheme.addEventListener).toHaveBeenCalledWith("change", expect.any(Function));

    const registeredListener = systemColorScheme.addEventListener.mock.calls[0]?.[1];
    expect(registeredListener).toEqual(expect.any(Function));

    act(() => {
      systemColorScheme.setMatches(true);
    });
    expect(editor.getAttribute("data-theme")).toBe("dark");

    act(() => {
      systemColorScheme.setMatches(false);
    });
    expect(editor.getAttribute("data-theme")).toBe("light");

    unmount();

    expect(systemColorScheme.removeEventListener).toHaveBeenCalledWith("change", registeredListener);
    expect(systemColorScheme.listenerCount()).toBe(0);
  });

  it.each([
    { current: "dark", label: "浅色", next: "light" },
    { current: "light", label: "深色", next: "dark" },
    { current: "dark", label: "跟随系统", next: "system" }
  ] as const)(
    "delegates the controlled $next color-mode selection to its callback",
    async ({ current, label, next }) => {
      const onAppearanceChange = vi.fn();
      render(
        <XFlowEditor
          value={workflow}
          appearance={current}
          onAppearanceChange={onAppearanceChange}
        />
      );

      const editor = screen.getByRole("region", { name: "Purchase approval editor" });
      const { appearance } = await openThemeSettings();
      fireEvent.click(within(appearance).getByText(label));

      expect(onAppearanceChange).toHaveBeenCalledTimes(1);
      expect(onAppearanceChange).toHaveBeenCalledWith(next);
      expect(editor.getAttribute("data-appearance")).toBe(current);
    }
  );

  it("collapses the left panel at compact and both panes at narrow, keeping drawers mutually exclusive", async () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    expect(editor.getAttribute("data-layout")).toBe("a");

    // 1024-1280 (ui-preview COMPACT_LEFT): the left panel is a rail, the
    // inspector keeps its docked column.
    act(() => container.setWidth(1279));
    expect(editor.getAttribute("data-layout")).toBe("b");
    expect(editor.getAttribute("data-layout-policy")).toBe("auto");
    expect(screen.getByRole("complementary", { name: "紧凑导航" })).toBeTruthy();
    // The inspector is docked here, so the rail has no 属性 key.
    expect(within(screen.getByRole("complementary", { name: "紧凑导航" })).queryByRole("button", { name: "属性面板" })).toBeNull();
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();

    // The compact left rail in the same band opens its drawer.
    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "节点" })).toBeTruthy());
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");
    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("region", { name: "节点" })).toBeNull());

    // Below 1024 both panes are rails with drawers.
    act(() => container.setWidth(1023));
    expect(editor.getAttribute("data-layout")).toBe("c");
    expect(screen.getByRole("complementary", { name: "紧凑导航" })).toBeTruthy();
    // One rail below `narrow`: the 属性 key joins the left rail.
    expect(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "属性面板" })).toBeTruthy();

    const nodesRail = screen.getByRole("button", { name: "节点与大纲" });
    fireEvent.click(nodesRail);

    await waitFor(() => {
      expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
      expect(screen.queryByRole("tab", { name: "节点库" })).toBeNull();
    });
    expect(nodesRail.getAttribute("aria-pressed")).toBe("true");
    // The drawer shows no title of its own (the panel's title bar names it) but
    // the dialog keeps an accessible name.
    expect(screen.getByRole("dialog", { name: "节点与大纲" })).toBeTruthy();
    const search = screen.getByPlaceholderText("搜索 Webhook / If / HTTP…");
    await waitFor(() => {
      expect(document.activeElement).toBe(search);
    });

    const variablesRail = screen.getByRole("button", { name: "变量" });
    fireEvent.click(variablesRail);
    await waitFor(() => {
      expect(screen.getByRole("region", { name: "变量" })).toBeTruthy();
      expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
      expect(screen.getByRole("button", { name: "添加输入参数" })).toBeTruthy();
    });
    expect(nodesRail.getAttribute("aria-pressed")).toBe("false");
    expect(variablesRail.getAttribute("aria-pressed")).toBe("true");

    // Pressing the active rail key again closes its drawer.
    fireEvent.click(variablesRail);
    await waitFor(() => expect(screen.queryByRole("region", { name: "变量" })).toBeNull());
    expect(variablesRail.getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(variablesRail);
    await waitFor(() => expect(screen.getByRole("region", { name: "变量" })).toBeTruthy());

    const propertyRail = screen.getByRole("button", { name: "属性面板" });
    fireEvent.click(propertyRail);
    await waitFor(() => {
      expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();
      expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    });
    expect(nodesRail.className).not.toContain("active");
    expect(propertyRail.className).toContain("active");
    expect(propertyRail.getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("dialog", { name: "属性" })).toBeTruthy();

    fireEvent.click(propertyRail);
    await waitFor(() => expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull());
    expect(propertyRail.getAttribute("aria-pressed")).toBe("false");
  });

  it("keeps the desktop collapse preference when a compact action opens the node library", async () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    // At 1024-1280 the canvas "添加节点" action opens the compact drawer; that
    // must not rewrite the persisted desktop collapse flag.
    act(() => container.setWidth(1279));
    expect(screen.getByRole("complementary", { name: "紧凑导航" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "添加节点 (A)" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "节点" })).toBeTruthy());

    // Back on the desktop layout the panel is still collapsed.
    act(() => container.setWidth(1600));
    expect(screen.getByRole("region", { name: "Purchase approval editor" }).getAttribute("data-layout")).toBe("a");
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    try {
      window.localStorage.removeItem("xflow-editor:panels:wf-purchase-approval");
    } catch {
      // This jsdom build may not provide storage at all.
    }
  });

  it("keeps the compact 节点 rail key live in a read-only viewer and opens the outline alone", async () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" />);
    act(() => container.setWidth(1279));

    const nodesRail = screen.getByRole("button", { name: "节点与大纲" }) as HTMLButtonElement;
    // The library is an edit-mode surface, the outline is not: the key stays
    // live and the drawer shows the outline without a library zone.
    expect(nodesRail.disabled).toBe(false);

    fireEvent.click(nodesRail);
    await waitFor(() => expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy());
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
  });

  it("preserves the canvas while manually entering and leaving the compact layout", async () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    act(() => container.setWidth(1600));

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    const preview = screen.getByRole("region", { name: "Purchase approval preview" });
    expect(editor.getAttribute("data-layout")).toBe("a");

    // 沉浸布局 / 恢复自动布局 live in the ⋯ menu (user ruling 2026-10-17).
    const chooseFromMoreMenu = async (name: string) => {
      fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
      fireEvent.click(await screen.findByRole("menuitem", { name }));
    };

    await chooseFromMoreMenu("进入沉浸布局");
    expect(editor.getAttribute("data-layout-policy")).toBe("compact");
    expect(editor.getAttribute("data-layout")).toBe("c");
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBe(preview);

    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    await waitFor(() => {
      expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
      expect(screen.queryByRole("tab", { name: "节点库" })).toBeNull();
    });

    // A wide container does not eject the manually chosen compact layout.
    act(() => container.setWidth(1200));
    expect(editor.getAttribute("data-layout")).toBe("c");
    act(() => container.setWidth(1600));
    expect(editor.getAttribute("data-layout")).toBe("c");

    await chooseFromMoreMenu("恢复自动布局");
    await waitFor(() => {
      expect(editor.getAttribute("data-layout-policy")).toBe("auto");
      expect(editor.getAttribute("data-layout")).toBe("a");
      expect(screen.queryByRole("region", { name: "节点" })).toBeTruthy();
    });
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBe(preview);
  });

  it("switches the layout from the container width, not the window", () => {
    const media = mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    act(() => container.setWidth(1279));
    expect(editor.getAttribute("data-layout")).toBe("b");

    // A window resize leaves the measured container untouched: no switch. The
    // window is deliberately wider than compact, so a window listener would
    // flip the layout here.
    const innerWidthDescriptor = Object.getOwnPropertyDescriptor(window, "innerWidth");
    try {
      Object.defineProperty(window, "innerWidth", { configurable: true, value: 1600 });
      act(() => {
        window.dispatchEvent(new Event("resize"));
      });
      expect(editor.getAttribute("data-layout")).toBe("b");
    } finally {
      // `delete` would remove the property outright (jsdom has no prototype
      // fallback), leaving every later test with an undefined window width.
      if (innerWidthDescriptor) Object.defineProperty(window, "innerWidth", innerWidthDescriptor);
    }

    act(() => container.setWidth(1281));
    expect(editor.getAttribute("data-layout")).toBe("a");
    expect(media).not.toHaveBeenCalledWith("(max-width: 1399px)");
  });

  it("maps the compact and narrow bands from unrounded widths", () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    act(() => container.setWidth(1023));
    expect(editor.getAttribute("data-layout")).toBe("c");

    act(() => container.setWidth(1024));
    expect(editor.getAttribute("data-layout")).toBe("b");

    // 1279.6 must not be rounded up to 1280: the @container query compares the
    // raw content width, so the JS tier must use the same unrounded value.
    act(() => container.setWidth(1279.6));
    expect(editor.getAttribute("data-layout")).toBe("b");

    act(() => container.setWidth(1280));
    expect(editor.getAttribute("data-layout")).toBe("a");
  });

  it("keeps 撤销 / 重做 / 校验 / 主题 independent at wide and folds them into ⋯ when compact", async () => {
    mockSystemColorScheme(false);
    const container = mockEditorWidth();
    render(<XFlowEditor value={workflow} />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    // Layout a: each secondary action keeps its own top-bar button.
    for (const name of ["撤销", "重做", "校验", "主题设置"]) {
      expect(screen.getByRole("button", { name })).toBeTruthy();
    }

    act(() => container.setWidth(1279));
    // Layout b: those buttons are gone; only the ⋯ menu carries them.
    for (const name of ["撤销", "重做", "校验", "主题设置"]) {
      expect(screen.queryByRole("button", { name })).toBeNull();
    }

    const openMenu = async () => {
      if (!screen.queryByRole("menu")) fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
      return within(await screen.findByRole("menu"));
    };
    const choose = async (name: string) => {
      const menu = await openMenu();
      fireEvent.click(menu.getByRole("menuitem", { name }));
      await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
    };

    // Empty history: both entries exist, disabled, with the independent
    // buttons' shortcut hint; the row order is the §6.3 order.
    const menu = await openMenu();
    expect(menu.getByRole("menuitem", { name: "撤销" }).getAttribute("aria-disabled")).toBe("true");
    expect(menu.getByRole("menuitem", { name: "重做" }).getAttribute("aria-disabled")).toBe("true");
    expect(menu.getByRole("menuitem", { name: "撤销" }).getAttribute("title")).toContain("Ctrl + Z");
    expect(menu.getAllByRole("menuitem").map((item) => item.textContent)).toEqual([
      "撤销",
      "重做",
      "校验",
      "浅色",
      "深色",
      "跟随系统（当前）",
      "进入沉浸布局",
      "新建工作流"
    ]);

    // 撤销 really undoes the edit the menu offers it for.
    const settings = workflowSettings();
    fireEvent.change(within(settings).getByLabelText("工作流名称"), { target: { value: "Narrow edit" } });
    expect(document.querySelector(".xflow-editor-chip")?.textContent).toBe("未保存 1 处");
    await choose("撤销");
    expect(document.querySelector(".xflow-editor-chip")?.textContent).toBe("已保存");

    // 重做 re-applies the very edit 撤销 took away.
    await choose("重做");
    expect(document.querySelector(".xflow-editor-chip")?.textContent).toBe("未保存 1 处");

    // 校验 really validates: the live region reports the verdict, panel opens.
    await choose("校验");
    expect(document.querySelector(".xflow-editor-sr-only")?.textContent).toContain("校验通过");
    expect(screen.getByRole("button", { name: "收起诊断台" })).toBeTruthy();

    // 主题 is a three-way radio group; choosing one updates data-theme. rc-menu
    // exposes no aria-checked, so the active row names itself “（当前）”.
    const themeMenu = await openMenu();
    expect(themeMenu.getByRole("menuitem", { name: "浅色" })).toBeTruthy();
    expect(themeMenu.getByRole("menuitem", { name: /跟随系统.*当前/ })).toBeTruthy();
    // Every theme row keeps a check-icon slot, active or not, so the labels
    // share one text column (the review measured a 21px indent on the active row).
    for (const name of ["浅色", "深色", /跟随系统.*当前/]) {
      expect(themeMenu.getByRole("menuitem", { name }).querySelector(".anticon-check")).toBeTruthy();
    }
    // Inactive slots are hidden, the active one is not, so only the selected
    // row shows a check while all three labels stay aligned.
    expect(
      themeMenu.getByRole("menuitem", { name: "浅色" }).querySelector(".xflow-editor-menu-check-inactive")
    ).toBeTruthy();
    expect(
      themeMenu.getByRole("menuitem", { name: /跟随系统.*当前/ }).querySelector(".xflow-editor-menu-check-inactive")
    ).toBeNull();
    fireEvent.click(themeMenu.getByRole("menuitem", { name: "深色" }));
    await waitFor(() => expect(editor.getAttribute("data-theme")).toBe("dark"));
    await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
    const themedMenu = await openMenu();
    expect(themedMenu.getByRole("menuitem", { name: /深色.*当前/ })).toBeTruthy();
    // Close the ⋯ menu before switching tiers.
    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());

    // Layout c (narrow tier and manual immersive) folds the same four actions,
    // and its menu items stay live.
    act(() => container.setWidth(1023));
    expect(editor.getAttribute("data-layout")).toBe("c");
    for (const name of ["撤销", "重做", "校验", "主题设置"]) {
      expect(screen.queryByRole("button", { name })).toBeNull();
    }
    const narrowMenu = await openMenu();
    for (const name of ["撤销", "重做", "校验", "浅色"]) {
      expect(narrowMenu.getByRole("menuitem", { name })).toBeTruthy();
    }
    fireEvent.click(narrowMenu.getByRole("menuitem", { name: "浅色" }));
    await waitFor(() => expect(editor.getAttribute("data-theme")).toBe("light"));
  });

  it("seeds the first frame from the measured container before the observer fires", () => {
    mockSystemColorScheme(false);
    class SilentResizeObserver {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
    globalThis.ResizeObserver = SilentResizeObserver as unknown as typeof ResizeObserver;
    const narrowRect = {
      bottom: 800,
      height: 800,
      left: 0,
      right: 1200,
      top: 0,
      width: 1200,
      x: 0,
      y: 0
    } as DOMRect;
    const originalRect = HTMLElement.prototype.getBoundingClientRect;
    const measure = vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
      return this.classList.contains("xflow-editor") ? narrowRect : originalRect.call(this);
    });
    try {
      render(<XFlowEditor value={workflow} />);
      expect(
        screen.getByRole("region", { name: "Purchase approval editor" }).getAttribute("data-layout")
      ).toBe("b");
    } finally {
      measure.mockRestore();
    }
  });

  it("locates a node from the ⌘K node search without dirtying the workflow", async () => {
    const handleChange = vi.fn();
    render(
      <XFlowEditor
        value={workflow}
        onChange={handleChange}
        onSave={async (nextWorkflow) => nextWorkflow}
        runtime={{
          status: "running",
          nodes: { route_by_amount: { status: "running", attempts: 2 } }
        }}
      />
    );

    // The canvas and the outline both carry status and selection.
    expect(canvasNode("route_by_amount").closest(".xflow-preview-node")?.getAttribute("data-status")).toBe("running");
    expect(canvasNode("route_by_amount").closest(".xflow-preview-node")?.getAttribute("data-selected")).toBe("true");

    fireEvent.keyDown(window, { key: "k", metaKey: true });
    const search = screen.getByRole("combobox", { name: "定位节点" });
    await waitFor(() => expect(document.activeElement).toBe(search));
    fireEvent.change(search, { target: { value: "manager" } });
    const option = await screen.findByText("l1_manager", { selector: ".xflow-editor-node-search__option strong" });
    expect(screen.queryByText("start", { selector: ".xflow-editor-node-search__option strong" })).toBeNull();
    fireEvent.click(option);

    await waitFor(() =>
      expect(canvasNode("l1_manager").closest(".xflow-preview-node")?.getAttribute("data-selected")).toBe("true")
    );
    // Picking a row is a locate, then the panel closes (E2b).
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "定位节点" })).toBeNull());
    // Reopening starts an empty query instead of re-showing the old filter.
    fireEvent.keyDown(window, { key: "k", metaKey: true });
    const reopened = await screen.findByRole("combobox", { name: "定位节点" });
    expect((reopened as HTMLInputElement).value).toBe("");
    fireEvent.keyDown(reopened, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "定位节点" })).toBeNull());
    expect(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByDisplayValue("l1_manager")).toBeTruthy();
    expect(handleChange).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "保存" }).className).not.toContain("ant-btn-variant-solid");
  });

  it("opens the locate panel from the canvas magnifier and closes it on Esc, restoring focus", async () => {
    render(<XFlowEditor value={workflow} />);

    const trigger = screen.getByRole("button", { name: "定位节点 ⌘K" });
    trigger.focus();
    fireEvent.click(trigger);

    const panel = await screen.findByRole("dialog", { name: "定位节点" });
    const search = within(panel).getByRole("combobox", { name: "定位节点" });
    await waitFor(() => expect(document.activeElement).toBe(search));

    // No matches: the message is not an invalid direct child of the listbox.
    fireEvent.change(search, { target: { value: "zzz" } });
    expect(within(panel).queryByRole("listbox")).toBeNull();
    expect(within(panel).getByText("没有匹配节点")).toBeTruthy();

    fireEvent.keyDown(search, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "定位节点" })).toBeNull());
    await waitFor(() => expect(document.activeElement).toBe(trigger));
  });

  it("keeps the locate panel shut when there is no node to locate", () => {
    render(<XFlowEditor value={{ id: "wf-empty", name: "Empty", nodes: [], connections: {} }} />);

    fireEvent.keyDown(window, { key: "k", metaKey: true });
    // Query the dialog's semantic hook directly: a role query would ignore the
    // panel while its enter animation keeps it inaccessible, letting a
    // regression that does open it pass the negative assertion.
    expect(document.querySelector(".xflow-editor-locate")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "定位节点 ⌘K" }));
    expect(document.querySelector(".xflow-editor-locate")).toBeNull();
  });

  it("scrolls the active locate option into view as the arrow keys move it", async () => {
    const scrollIntoView = vi.fn();
    const original = Element.prototype.scrollIntoView;
    Element.prototype.scrollIntoView = scrollIntoView;
    try {
      render(<XFlowEditor value={workflow} />);
      fireEvent.keyDown(window, { key: "k", metaKey: true });
      const search = await screen.findByRole("combobox", { name: "定位节点" });
      scrollIntoView.mockClear();
      fireEvent.keyDown(search, { key: "ArrowDown" });
      expect(scrollIntoView).toHaveBeenCalledWith({ block: "nearest" });
      // The row that scrolled is the newly active one, not the input's container.
      expect(scrollIntoView.mock.instances.at(-1)).toBe(
        document.getElementById("xflow-editor-locate-option-1")
      );
    } finally {
      Element.prototype.scrollIntoView = original;
    }
  });

  it("changes inspector tabs through their tablist keyboard contract", () => {
    render(<XFlowEditor value={workflow} />);

    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    const parameters = within(inspector).getByRole("tab", { name: "参数" });
    const settings = within(inspector).getByRole("tab", { name: "设置" });
    const connections = within(inspector).getByRole("tab", { name: "连接" });
    const run = within(inspector).getByRole("tab", { name: "运行" });
    expect(parameters.getAttribute("aria-controls")).toBe("xflow-editor-inspector-panel-parameters");
    expect(within(inspector).getAllByRole("tabpanel")[0].getAttribute("aria-labelledby")).toBe(parameters.id);

    parameters.focus();
    fireEvent.keyDown(parameters, { key: "ArrowRight", code: "ArrowRight" });
    expect(document.activeElement).toBe(settings);
    expect(settings.getAttribute("aria-selected")).toBe("true");
    expect(within(inspector).getAllByRole("tabpanel")[0].getAttribute("aria-labelledby")).toBe(settings.id);

    fireEvent.keyDown(settings, { key: "ArrowRight", code: "ArrowRight" });
    expect(document.activeElement).toBe(connections);
    expect(connections.getAttribute("aria-selected")).toBe("true");

    fireEvent.keyDown(connections, { key: "End", code: "End" });
    expect(document.activeElement).toBe(run);
    expect(run.getAttribute("aria-selected")).toBe("true");
    fireEvent.keyDown(run, { key: "Home", code: "Home" });
    expect(document.activeElement).toBe(parameters);
    expect(parameters.getAttribute("aria-selected")).toBe("true");
  });

  it("keeps matching parent echoes dirty and lets diagnostics locate a node without editing", async () => {
    const handleChange = vi.fn();
    const handleSave = vi.fn(async (nextWorkflow) => nextWorkflow);
    const invalidWorkflow = {
      ...workflow,
      connections: {
        ...workflow.connections,
        route_by_amount: {
          ...workflow.connections.route_by_amount,
          main: [{ node: "missing_target" }]
        }
      }
    };
    const { rerender } = render(
      <XFlowEditor value={invalidWorkflow} onChange={handleChange} onSave={handleSave} />
    );

    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Locally dirty workflow" }
    });
    const echoedWorkflow = handleChange.mock.calls.at(-1)?.[0];
    expect(echoedWorkflow).toBeTruthy();
    const save = screen.getByRole("button", { name: "保存" });
    expect(save.className).not.toContain("ant-btn-color-primary");
    expect(save.className).not.toContain("ant-btn-variant-solid");

    rerender(<XFlowEditor value={echoedWorkflow} onChange={handleChange} onSave={handleSave} />);
    expect(screen.getByText("未保存 1 处")).toBeTruthy();
    expect(save.className).not.toContain("ant-btn-color-primary");
    expect(save.className).not.toContain("ant-btn-variant-solid");

    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    const locate = await screen.findByRole("button", { name: "定位节点 route_by_amount" });
    fireEvent.click(locate);

    expect(
      within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByText("route_by_amount · xflow.switch", {
        selector: ".xflow-editor-inspector-head__type"
      })
    ).toBeTruthy();
    expect(handleChange).toHaveBeenCalledTimes(1);
    // The drawer's 版本 row repeats the wording, so pin the top-bar chip.
    expect(screen.getByText("未保存 1 处", { selector: ".xflow-editor-chip span" })).toBeTruthy();
  });

  it("shows the node search always, with the / key hint, and filters by it", () => {
    render(<XFlowEditor value={workflow} />);

    const library = screen.getByRole("region", { name: "节点" });
    expect(library.querySelector(".xflow-editor-panel-title__icon")?.getAttribute("aria-hidden")).toBe("true");
    // No toggle any more: the baseline's search box is on screen from the start.
    expect(within(library).queryByRole("button", { name: "搜索节点" })).toBeNull();

    const searchInput = within(library).getByPlaceholderText("搜索 Webhook / If / HTTP…");
    expect(searchInput).toBeTruthy();
    expect(searchInput.className).toContain("xflow-editor-field__control");
    expect(library.querySelector(".xflow-editor-library-search")).toBeTruthy();
    expect(library.querySelector(".xflow-editor-library-search kbd")?.textContent).toBe("/");
    expect(within(library).queryByRole("button", { name: "search" })).toBeNull();

    fireEvent.change(searchInput, { target: { value: "http" } });
    expect(within(library).getByRole("button", { name: "HTTP" })).toBeTruthy();
    expect(within(library).queryByRole("button", { name: "Kafka" })).toBeNull();
  });

  it("collapses and expands the editor panels", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    // The rail survives the collapse; its key is now unpressed and re-expands.
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    expect(screen.getByRole("button", { name: "展开属性面板" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
    expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByRole("complementary", { name: "摘要" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "收起诊断台" }));
    expect(screen.queryByRole("complementary", { name: "摘要" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByRole("complementary", { name: "摘要" })).toBeTruthy();
  });

  it("selects a canvas node and exposes the editable node configuration", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(canvasNode("route_by_amount"));

    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector).getByText("属性")).toBeTruthy();
    expect(within(inspector).getAllByText(/route_by_amount/).length).toBeGreaterThan(0);
    expect(within(inspector).queryByText("running")).toBeNull();
    expect(within(inspector).getByText("route_by_amount · xflow.switch", { selector: ".xflow-editor-inspector-head__type" })).toBeTruthy();
    expect(within(inspector).getByLabelText("节点名称").className).toContain("xflow-editor-nodeform-node-name-control");
    expect(within(inspector).getByRole("tab", { name: "参数" }).getAttribute("aria-selected")).toBe("true");
    expect(within(inspector).getByRole("tab", { name: "设置" })).toBeTruthy();
    expect(within(inspector).getByRole("tab", { name: "连接" })).toBeTruthy();
    expect(within(inspector).getByRole("tab", { name: "运行" })).toBeTruthy();
    expect(within(inspector).getByDisplayValue("Route purchase requests by amount")).toBeTruthy();

  });

  it("renames structured references without rewriting free-form expressions", () => {
    const handleChange = vi.fn();
    const referencedWorkflow = {
      name: "Reference-safe rename",
      nodes: [
        { name: "rules", type: "xflow.supply.external", kind: "supply" as const },
        {
          name: "worker",
          type: "xflow.function",
          notes: "Keep $nodes['worker'].output as a user-authored expression"
        },
        { name: "consumer", type: "xflow.http" }
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency" as const,
            targets: [{ node: "worker" }]
          }
        },
        worker: {
          main: {
            targets: [{ node: "consumer" }]
          }
        }
      },
      pin_data: {
        worker: { fixture: "worker-output" }
      },
      groups: [{ name: "workers", members: ["worker"] }],
      dependency_edges: [{ node: "worker", supply: "rules" }]
    };

    render(<XFlowEditor value={referencedWorkflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("worker"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    const nodeName = within(inspector).getByLabelText("节点名称");
    fireEvent.change(nodeName, { target: { value: "executor" } });
    fireEvent.blur(nodeName);

    expect(handleChange).toHaveBeenLastCalledWith({
      ...referencedWorkflow,
      nodes: [
        referencedWorkflow.nodes[0],
        { ...referencedWorkflow.nodes[1], name: "executor" },
        referencedWorkflow.nodes[2]
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency",
            targets: [{ node: "executor" }]
          }
        },
        executor: {
          main: {
            targets: [{ node: "consumer" }]
          }
        }
      },
      pin_data: {
        executor: { fixture: "worker-output" }
      },
      groups: [{ name: "workers", members: ["executor"] }],
      dependency_edges: [{ node: "executor", supply: "rules" }]
    });
    expect(handleChange.mock.calls.at(-1)?.[0].nodes[1].notes).toBe(
      "Keep $nodes['worker'].output as a user-authored expression"
    );
    expect(within(inspector).getByText(/自由文本/)).toBeTruthy();
  });

  it("does not submit blank or duplicate node names", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("route_by_amount"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    const nodeName = within(inspector).getByLabelText("节点名称");

    fireEvent.change(nodeName, { target: { value: "   " } });
    fireEvent.blur(nodeName);
    expect(handleChange).not.toHaveBeenCalled();
    expect(within(inspector).getByText("节点名称不能为空")).toBeTruthy();

    fireEvent.change(nodeName, { target: { value: "start" } });
    fireEvent.blur(nodeName);
    expect(handleChange).not.toHaveBeenCalled();
    expect(within(inspector).getByText("节点名称已存在: start")).toBeTruthy();
  });

  it("edits workflow metadata from the property panel", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "purchase-approval-v2" }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      name: "purchase-approval-v2"
    });
  });

  it("replaces an unsaved draft when the parent supplies a new controlled value", async () => {
    const handleChange = vi.fn();
    const parentReplacement = {
      id: "wf-parent-replacement",
      name: "Parent replacement",
      version: "2.0.0",
      nodes: [{ name: "external_start", type: "xflow.start", kind: "trigger" as const }],
      connections: {}
    };
    const { rerender } = render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = workflowSettings();
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Local unsaved draft" }
    });
    expect(within(inspector).getByDisplayValue("Local unsaved draft")).toBeTruthy();
    expect(handleChange).toHaveBeenLastCalledWith({ ...workflow, name: "Local unsaved draft" });

    rerender(<XFlowEditor value={parentReplacement} onChange={handleChange} />);

    await waitFor(() => {
      expect(screen.getByRole("region", { name: "Parent replacement editor" })).toBeTruthy();
      expect(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByDisplayValue("Parent replacement")).toBeTruthy();
      expect(canvasNode("external_start")).toBeTruthy();
    });
    expect(document.querySelector('[aria-label^="route_by_amount node "]')).toBeNull();
    expect(handleChange).toHaveBeenCalledTimes(1);
  });

  it("edits workflow-level DSL fields from the property panel", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = workflowSettings();
    // Runner is a structured row now; the JSON box is behind its `</>` toggle.
    fireEvent.click(within(inspector).getByRole("button", { name: "以 JSON 编辑 Runner 选择" }));
    fireEvent.change(within(inspector).getByLabelText("工作流 Runner 选择 JSON"), {
      target: {
        value: '{ "mode": "required", "match_labels": { "env": "prod", "region": "cn" } }'
      }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      runner_selector: {
        mode: "required",
        match_labels: {
          env: "prod",
          region: "cn"
        }
      }
    });

    const variables = variablesPanel();
    fireEvent.click(within(variables).getByRole("button", { name: "以 JSON 编辑输入参数" }));
    fireEvent.change(within(variables).getByLabelText("工作流输入参数 JSON"), {
      target: {
        value: '{ "amount": { "type": "number", "required": true, "display_name": "金额" } }'
      }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      runner_selector: {
        mode: "required",
        match_labels: {
          env: "prod",
          region: "cn"
        }
      },
      params: {
        amount: {
          type: "number",
          required: true,
          display_name: "金额"
        }
      }
    });
  });

  it("edits node-level runner selector overrides through the node form", async () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("l1_manager"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    // The form owns /runner_selector (Doc C §4.2); node-level `mode` is a
    // compile error, so only match_labels is offered. The legacy JSON box is gone.
    expect(within(inspector).queryByLabelText("节点 Runner 选择 JSON")).toBeNull();
    fireEvent.click(within(inspector).getByRole("tab", { name: "设置" }));
    fireEvent.click(within(inspector).getByRole("button", { name: "执行策略" }));
    const labels = within(inspector).getByRole("group", { name: "匹配标签" });
    fireEvent.click(within(labels).getByRole("button", { name: /添加/ }));
    fireEvent.change(within(labels).getByLabelText("第 1 行的键"), { target: { value: "env" } });
    await act(async () => {
      fireEvent.change(within(labels).getByLabelText("env 的值"), { target: { value: "prod" } });
    });

    expect(handleChange).toHaveBeenLastCalledWith({
      ...workflow,
      nodes: [
        workflow.nodes[0],
        workflow.nodes[1],
        { ...workflow.nodes[2], runner_selector: { match_labels: { env: "prod" } } }
      ]
    });
  });

  it("edits node parameters in the JSON source view and reports invalid JSON", async () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("route_by_amount"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("button", { name: "以 JSON 编辑参数" }));
    const parameters = within(inspector).getByLabelText("参数 JSON");

    fireEvent.change(parameters, {
      target: { value: '{ "threshold": 5000 }' }
    });
    // Parseable input is debounced (Doc C §6.2): nothing commits per keystroke.
    expect(handleChange).not.toHaveBeenCalled();
    fireEvent.blur(parameters);

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        workflow.nodes[0],
        {
          ...workflow.nodes[1],
          parameters: { threshold: 5000 }
        },
        workflow.nodes[2]
      ]
    });

    fireEvent.change(parameters, {
      target: { value: "{ broken" }
    });
    expect(within(inspector).getByText("参数 JSON 格式错误")).toBeTruthy();
  });

  it("adds nodes from the node library into the editable workflow", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const library = screen.getByRole("region", { name: "节点" });
    fireEvent.click(within(library).getByRole("button", { name: "HTTP" }));

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        ...workflow.nodes,
        {
          name: "http_1",
          type: "xflow.http",
          kind: "action",
          position: { x: 620, y: 220 },
          ui: { label: "HTTP" }
        }
      ],
      connections: {
        ...workflow.connections,
        route_by_amount: {
          ...workflow.connections.route_by_amount,
          main: [{ node: "http_1" }]
        }
      }
    });
    expect(canvasNode("http_1")).toBeTruthy();
  });

  it("adds trigger nodes without creating invalid data connections", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const library = screen.getByRole("region", { name: "节点" });
    fireEvent.click(within(library).getByRole("button", { name: "Webhook" }));

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        ...workflow.nodes,
        {
          name: "trigger_webhook_1",
          type: "xflow.trigger.webhook",
          kind: "trigger",
          position: { x: 620, y: 220 },
          ui: { label: "Webhook" }
        }
      ],
      connections: workflow.connections
    });
  });

  it("saves and runs the current editable workflow", async () => {
    const handleSave = vi.fn(async (nextWorkflow) => nextWorkflow);
    const handleRun = vi.fn(async () => ({
      status: "success" as const,
      nodes: {
        start: { status: "success" as const, durationMs: 12 },
        route_by_amount: { status: "success" as const, durationMs: 24 },
        l1_manager: { status: "success" as const, durationMs: 36 }
      }
    }));

    render(<XFlowEditor value={workflow} onSave={handleSave} onRun={handleRun} />);

    // A clean draft cannot be saved (baseline clean state); dirty it first.
    const saved = { ...workflow, name: "Saved purchase approval" };
    fireEvent.change(within(workflowSettings()).getByLabelText("工作流名称"), {
      target: { value: saved.name }
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledWith(saved);
    });
    expect(screen.getByText("已保存", { selector: ".xflow-editor-chip span" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    await waitFor(() => {
      expect(handleRun).toHaveBeenCalledWith(saved);
    });
    expect(screen.getByText("运行完成")).toBeTruthy();
    // The status strip names the run by the shared status table, not a raw enum.
    // A run opens the drawer, so the strip now reads as its summary.
    expect(screen.getByRole("button", { name: "诊断台摘要" }).textContent).toContain("最近运行 成功");
    expect(screen.queryAllByText("success")).toHaveLength(0);
    expect(within(screen.getByRole("complementary", { name: "摘要" })).getByText("成功")).toBeTruthy();
  });

  it("does not announce 运行完成 when the run handler returns a still-running snapshot", async () => {
    const handleRun = vi.fn(async () => ({
      status: "running" as const,
      nodes: {
        start: { status: "success" as const, durationMs: 12 },
        route_by_amount: { status: "running" as const }
      }
    }));

    render(<XFlowEditor value={workflow} onRun={handleRun} />);

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    await waitFor(() => {
      expect(handleRun).toHaveBeenCalledWith(workflow);
    });
    await waitFor(() => {
      // The operation result is a visually hidden live region now (§6.3).
      expect(document.querySelector(".xflow-editor-sr-only")?.textContent).toContain("运行中");
    });
    expect(screen.queryByText("运行完成")).toBeNull();
  });

  it("keeps edits made while a save request is pending dirty after the old save resolves", async () => {
    let resolveSave!: (savedWorkflow: typeof workflow) => void;
    const pendingSave = new Promise<typeof workflow>((resolve) => {
      resolveSave = resolve;
    });
    const handleChange = vi.fn();
    const handleSave = vi.fn(() => pendingSave);

    render(<XFlowEditor value={workflow} onChange={handleChange} onSave={handleSave} />);

    // A clean draft cannot be saved (baseline clean state); dirty it first.
    const inspector = workflowSettings();
    const saved = { ...workflow, name: "Saved before pending" };
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: saved.name }
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledWith(saved);
    });

    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Edited while save is pending" }
    });
    expect(handleChange).toHaveBeenLastCalledWith({
      ...workflow,
      name: "Edited while save is pending"
    });

    await act(async () => {
      resolveSave(saved);
      await pendingSave;
    });

    expect(within(inspector).getByDisplayValue("Edited while save is pending")).toBeTruthy();
    expect(screen.getByText("未保存 1 处")).toBeTruthy();
    expect(screen.queryByText("已保存")).toBeNull();
  });

  it("disables run instead of synthesizing a successful runtime without onRun", () => {
    render(<XFlowEditor value={workflow} />);

    const runButton = screen.getByRole("button", { name: "运行工作流" }) as HTMLButtonElement;
    expect(runButton.disabled).toBe(true);

    fireEvent.click(runButton);

    expect(screen.queryByText("运行完成")).toBeNull();
    expect(screen.queryByText("success")).toBeNull();
  });

  it("adds a main connection from the selected node in the connections tab", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("route_by_amount"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));
    fireEvent.click(within(inspector).getByRole("button", { name: "连接到 l1_manager" }));

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      connections: {
        ...workflow.connections,
        route_by_amount: {
          ...workflow.connections.route_by_amount,
          main: [{ node: "l1_manager" }]
        }
      }
    });
  });

  it("preserves object-form data connections when adding and deleting targets", () => {
    const handleChange = vi.fn();
    const objectConnectionWorkflow = {
      name: "Object connections",
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" as const },
        { name: "worker", type: "xflow.function" }
      ],
      connections: {
        start: {
          main: {
            targets: []
          }
        }
      }
    };

    render(<XFlowEditor value={objectConnectionWorkflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("start"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));
    fireEvent.click(within(inspector).getByRole("button", { name: "连接到 worker" }));

    expect(handleChange).toHaveBeenLastCalledWith({
      ...objectConnectionWorkflow,
      connections: {
        start: {
          main: {
            targets: [{ node: "worker" }]
          }
        }
      }
    });

    fireEvent.click(
      within(inspector).getByRole("button", { name: "删除连接 start main 到 worker main" })
    );

    expect(handleChange).toHaveBeenLastCalledWith({
      ...objectConnectionWorkflow,
      connections: {
        start: {
          main: {
            targets: []
          }
        }
      }
    });
  });

  it("removes a selected connection from the connections tab", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("route_by_amount"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));
    fireEvent.click(within(inspector).getByRole("button", { name: "删除连接 route_by_amount level_1 到 l1_manager main" }));

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      connections: {
        ...workflow.connections,
        route_by_amount: {
          level_1: []
        }
      }
    });
  });

  it("shows dependency connections in the connections UI and preserves their type after deletion", () => {
    const handleChange = vi.fn();
    const dependencyWorkflow = {
      name: "Supply dependencies",
      nodes: [
        { name: "rules", type: "xflow.supply.external", kind: "supply" as const },
        { name: "worker", type: "xflow.function" }
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency" as const,
            targets: [{ node: "worker" }]
          }
        }
      }
    };

    render(<XFlowEditor value={dependencyWorkflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("rules"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));

    expect(within(inspector).getByText(/dependency|依赖/)).toBeTruthy();
    const deleteButton = within(inspector).getByRole("button", {
      name: /删除连接 rules supply 到 worker/
    });
    fireEvent.click(deleteButton);

    expect(handleChange).toHaveBeenLastCalledWith({
      ...dependencyWorkflow,
      connections: {
        rules: {
          supply: {
            type: "dependency",
            targets: []
          }
        }
      }
    });
  });

  it("deletes matching legacy dependency edges with the typed dependency connection", () => {
    const handleChange = vi.fn();
    const dualRepresentationWorkflow = {
      name: "Dual dependency representations",
      nodes: [
        { name: "rules", type: "xflow.supply.external", kind: "supply" as const },
        { name: "worker", type: "xflow.function" }
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency" as const,
            targets: [{ node: "worker" }]
          }
        }
      },
      dependency_edges: [{ node: "worker", supply: "rules" }]
    };

    render(<XFlowEditor value={dualRepresentationWorkflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("rules"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));
    expect(within(inspector).queryByText("dependency (legacy)")).toBeNull();

    fireEvent.click(
      within(inspector).getByRole("button", { name: "删除连接 rules supply 到 worker main" })
    );

    expect(handleChange).toHaveBeenLastCalledWith({
      ...dualRepresentationWorkflow,
      connections: {
        rules: {
          supply: {
            type: "dependency",
            targets: []
          }
        }
      },
      dependency_edges: []
    });
  });

  it("shows and deletes a legacy-only dependency edge", () => {
    const handleChange = vi.fn();
    const legacyOnlyDependencyWorkflow = {
      name: "Legacy dependency",
      nodes: [
        { name: "rules", type: "xflow.supply.external", kind: "supply" as const },
        { name: "worker", type: "xflow.function" }
      ],
      dependency_edges: [{ node: "worker", supply: "rules" }]
    };

    render(<XFlowEditor value={legacyOnlyDependencyWorkflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("worker"));
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspector).getByRole("tab", { name: "连接" }));
    expect(within(inspector).getByText("dependency (legacy)")).toBeTruthy();

    fireEvent.click(
      within(inspector).getByRole("button", { name: "删除连接 rules supply 到 worker main" })
    );

    expect(handleChange).toHaveBeenLastCalledWith({
      ...legacyOnlyDependencyWorkflow,
      dependency_edges: []
    });
  });

  it("deletes the selected node and removes dangling connections", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("l1_manager"));
    fireEvent.click(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByRole("button", { name: "更多操作" }));
    fireEvent.click(screen.getByRole("menuitem", { name: /删除/ }));

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [workflow.nodes[0], workflow.nodes[1]],
      connections: {
        start: {
          main: [{ node: "route_by_amount" }]
        },
        route_by_amount: {
          level_1: []
        }
      }
    });
    expect(document.querySelector('[aria-label^="l1_manager node "]')).toBeNull();
  });

  it("cleans pin data, groups, dependency edges, and both connection directions when deleting a node", () => {
    const handleChange = vi.fn();
    const workflowWithReferences = {
      name: "Delete references",
      nodes: [
        { name: "rules", type: "xflow.supply.external", kind: "supply" as const },
        { name: "worker", type: "xflow.function" },
        { name: "downstream", type: "xflow.http" }
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency" as const,
            targets: [{ node: "worker" }]
          }
        },
        worker: {
          main: [{ node: "downstream" }]
        },
        downstream: {
          retry: [{ node: "worker" }]
        }
      },
      pin_data: {
        worker: { fixture: "remove-me" },
        downstream: { fixture: "keep-me" }
      },
      groups: [
        { name: "only-worker", members: ["worker"] },
        { name: "mixed", members: ["worker", "downstream"] }
      ],
      dependency_edges: [
        { node: "worker", supply: "rules" },
        { node: "downstream", supply: "worker" },
        { node: "downstream", supply: "rules" }
      ]
    };

    render(<XFlowEditor value={workflowWithReferences} onChange={handleChange} />);

    fireEvent.click(canvasNode("worker"));
    fireEvent.click(
      within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByRole("button", {
        name: "更多操作"
      })
    );
    fireEvent.click(screen.getByRole("menuitem", { name: /删除/ }));

    expect(handleChange).toHaveBeenLastCalledWith({
      ...workflowWithReferences,
      nodes: [workflowWithReferences.nodes[0], workflowWithReferences.nodes[2]],
      connections: {
        rules: {
          supply: {
            type: "dependency",
            targets: []
          }
        },
        downstream: {
          retry: []
        }
      },
      pin_data: {
        downstream: { fixture: "keep-me" }
      },
      groups: [{ name: "mixed", members: ["downstream"] }],
      dependency_edges: [{ node: "downstream", supply: "rules" }]
    });
  });

  it("reports invalid workflow diagnostics for missing connection targets", () => {
    render(
      <XFlowEditor
        value={{
          name: "Broken workflow",
          nodes: [{ name: "start", type: "xflow.start" }],
          connections: {
            start: {
              main: [{ node: "missing_target" }]
            }
          }
        }}
      />
    );

    expect(screen.getByRole("button", { name: "展开诊断台" }).textContent).toContain("1 错误");
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    // The finding is plain Chinese; the internal target id stays out of the text.
    expect(screen.getByText("start.main 连接目标不存在: missing_target")).toBeTruthy();
    expect(screen.queryByText("DSL invalid")).toBeNull();
    expect(within(screen.getByRole("region", { name: "诊断台" })).queryByText("missing_target")).toBeNull();
  });

  it("creates a new workflow draft from the toolbar", async () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "新建工作流" }));

    expect(handleChange).toHaveBeenCalledWith({
      name: "未命名工作流",
      spec: "1.0",
      nodes: [],
      connections: {}
    });
    // The fresh draft is its own baseline: nothing to compare, so no count (§6.3).
    expect(document.querySelector(".xflow-editor-chip")?.textContent).toBe("未保存");
    // §6.2 empty canvas: the trigger card replaces the preview's default note.
    expect(screen.getByRole("heading", { name: "添加触发器开始" })).toBeTruthy();
    expect(screen.queryByText("No nodes to preview")).toBeNull();
  });

  it("offers the empty canvas card's trigger pick and marks DSL import unavailable", () => {
    render(<XFlowEditor value={{ name: "Empty", spec: "1.0", nodes: [], connections: {} }} />);
    const canvas = screen.getByRole("region", { name: "画布" });

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    fireEvent.click(within(canvas).getByRole("button", { name: "选择触发器" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect((within(canvas).getByRole("button", { name: "导入 DSL" }) as HTMLButtonElement).disabled).toBe(true);
    // No nodes, no rulers (§6.2).
    expect(canvas.querySelector(".xflow-preview-ruler")).toBeNull();
  });

  it("draws the preview's rulers and zoom bar instead of the old static ruler", () => {
    render(<XFlowEditor value={workflow} />);
    const canvas = screen.getByRole("region", { name: "画布" });

    expect(canvas.querySelectorAll(".xflow-preview-ruler")).toHaveLength(2);
    expect(within(canvas).getByRole("toolbar", { name: "画布缩放" })).toBeTruthy();
    expect(canvas.querySelector(".xflow-editor-canvas__ruler")).toBeNull();
  });

  it("puts 添加节点 (A) at the canvas's top left and opens the library from it or from A", () => {
    render(<XFlowEditor value={workflow} />);
    const canvas = screen.getByRole("region", { name: "画布" });
    const add = within(canvas).getByRole("button", { name: "添加节点 (A)" });
    expect(add.closest(".xflow-preview-canvas-float")).toBeTruthy();
    // The magnifier shares that one float shell, as the baseline's add-node pair.
    const locate = within(canvas).getByRole("button", { name: "定位节点 ⌘K" });
    expect(locate.closest(".xflow-preview-canvas-float")).toBe(add.closest(".xflow-preview-canvas-float"));

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    fireEvent.click(add);
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    fireEvent.keyDown(window, { key: "a" });
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
  });

  it("drops 添加节点 and 自动布局 from the read-only canvas but keeps locate", async () => {
    const { rerender } = render(<XFlowEditor value={workflow} />);
    const canvas = screen.getByRole("region", { name: "画布" });
    expect(within(canvas).getByRole("button", { name: "自动布局" })).toBeTruthy();

    rerender(<XFlowEditor value={workflow} readOnlyReason="没有写权限" />);

    expect(within(canvas).queryByRole("button", { name: "添加节点 (A)" })).toBeNull();
    expect(within(canvas).queryByRole("button", { name: "自动布局" })).toBeNull();

    // A read-only viewer still needs locate: the magnifier is the only float.
    const locate = within(canvas).getByRole("button", { name: "定位节点 ⌘K" });
    expect(locate.closest(".xflow-preview-canvas-float")).toBeTruthy();
    fireEvent.click(locate);
    expect(await screen.findByRole("dialog", { name: "定位节点" })).toBeTruthy();
  });

  it("shows save and run failures inside diagnostics", async () => {
    const handleRun = vi.fn(async () => {
      throw new Error("runner offline");
    });

    render(<XFlowEditor value={workflow} onRun={handleRun} />);

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    await waitFor(() => {
      expect(screen.getByText("运行失败")).toBeTruthy();
    });
    expect(screen.getByText("runner offline")).toBeTruthy();
  });

  it("drives history from the keyboard, not only from the toolbar", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "HTTP" }));
    const addedCount = handleChange.mock.calls.at(-1)?.[0]?.nodes.length;
    expect(addedCount).toBe(4);

    fireEvent.keyDown(window, { key: "z", metaKey: true });
    expect(handleChange.mock.calls.at(-1)?.[0]).toEqual(workflow);

    fireEvent.keyDown(window, { key: "y", ctrlKey: true });
    expect(handleChange.mock.calls.at(-1)?.[0]?.nodes).toHaveLength(4);

    fireEvent.keyDown(window, { key: "z", metaKey: true });
    expect(handleChange.mock.calls.at(-1)?.[0]).toEqual(workflow);

    fireEvent.keyDown(window, { key: "z", metaKey: true, shiftKey: true });
    expect(handleChange.mock.calls.at(-1)?.[0]?.nodes).toHaveLength(4);
  });

  it("deletes the selected node from the keyboard and takes its connections with it", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    // The fixture's switch node is the default selection, and starting a node is
    // wired into it, so this also covers the dangling-connection cleanup.
    fireEvent.keyDown(window, { key: "Delete" });

    const next = handleChange.mock.calls.at(-1)?.[0];
    expect(next.nodes.map((node: { name: string }) => node.name)).toEqual(["start", "l1_manager"]);
    expect(JSON.stringify(next.connections)).not.toContain("route_by_amount");
  });

  it("leaves node deletion alone in a read-only viewer", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" onChange={handleChange} />);

    fireEvent.keyDown(window, { key: "Delete" });
    fireEvent.keyDown(window, { key: "Backspace" });

    expect(handleChange).not.toHaveBeenCalled();
  });

  it("duplicates the selected node under a fresh name at an offset", () => {
    const handleChange = vi.fn();
    const positioned = {
      ...workflow,
      nodes: [
        { name: "start", type: "xflow.start", position: { x: 100, y: 200 } },
        { name: "route_by_amount", type: "xflow.switch", position: { x: 300, y: 240 } },
        { name: "route_by_amount_1", type: "xflow.switch" }
      ]
    };
    render(<XFlowEditor value={positioned} onChange={handleChange} />);

    fireEvent.keyDown(window, { key: "d", metaKey: true });

    const next = handleChange.mock.calls.at(-1)?.[0];
    const duplicated = next.nodes.at(-1);
    expect(next.nodes).toHaveLength(4);
    // `route_by_amount_1` is taken, so the suffix skips past it.
    expect(duplicated.name).toBe("route_by_amount_2");
    expect(duplicated.type).toBe("xflow.switch");
    expect(duplicated.position).toEqual({ x: 336, y: 276 });
    // A duplicate is added beside the original, never spliced into its slot.
    expect(next.connections).toEqual(positioned.connections);
  });

  it("saves with the primary modifier and runs with the primary modifier plus Enter", async () => {
    const handleSave = vi.fn(async (nextWorkflow: typeof workflow) => nextWorkflow);
    const handleRun = vi.fn(async () => ({
      status: "success" as const,
      nodes: { start: { status: "success" as const, durationMs: 12 } }
    }));
    render(<XFlowEditor value={workflow} onSave={handleSave} onRun={handleRun} />);

    // A clean draft cannot be saved (baseline clean state); dirty it first.
    const saved = { ...workflow, name: "Saved with the shortcut" };
    fireEvent.change(within(workflowSettings()).getByLabelText("工作流名称"), {
      target: { value: saved.name }
    });
    fireEvent.keyDown(window, { key: "s", metaKey: true });
    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledWith(saved);
    });

    fireEvent.keyDown(window, { key: "Enter", ctrlKey: true });
    await waitFor(() => {
      expect(handleRun).toHaveBeenCalledWith(saved);
    });
  });

  it("keeps global shortcuts alive in a text field while canvas ones defer to it", async () => {
    const handleChange = vi.fn();
    const handleSave = vi.fn(async (nextWorkflow: typeof workflow) => nextWorkflow);
    render(<XFlowEditor value={workflow} onChange={handleChange} onSave={handleSave} />);

    fireEvent.click(within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "HTTP" }));
    const changeCountBefore = handleChange.mock.calls.length;

    const nameField = within(workflowSettings()).getByLabelText("工作流名称");
    fireEvent.keyDown(nameField, { key: "z", metaKey: true });
    expect(handleChange.mock.calls).toHaveLength(changeCountBefore);

    // Saving is app-level: the browser's own ⌘S is no use to the user here.
    fireEvent.keyDown(nameField, { key: "s", metaKey: true });
    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledTimes(1);
    });
  });

  it("collapses and restores the left panel from the keyboard", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.keyDown(window, { key: "b", metaKey: true });
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.getByRole("button", { name: "节点与大纲" }).getAttribute("aria-pressed")).toBe("false");

    fireEvent.keyDown(window, { key: "b", ctrlKey: true });
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起左栏" })).toBeTruthy();
  });

  it("opens the keyboard reference from ? and from the rail help button", () => {
    const { unmount } = render(<XFlowEditor value={workflow} />);
    expect(screen.queryByRole("dialog", { name: /快捷键/ })).toBeNull();

    fireEvent.keyDown(window, { key: "?" });

    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText("复制选中节点")).toBeTruthy();
    expect(within(dialog).getByText("⌘ / Ctrl + S")).toBeTruthy();
    unmount();

    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "快捷键与帮助" }));
    expect(screen.getByRole("dialog")).toBeTruthy();
  });
});
