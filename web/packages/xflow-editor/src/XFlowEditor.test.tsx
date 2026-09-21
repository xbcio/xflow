import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { XFlowEditor } from "./index";

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

  const mockSystemColorScheme = (matches: boolean, compactMatches = false) => {
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
    const compactViewport = createMediaQuery("(max-width: 1399px)", compactMatches);
    const matchMedia = vi.fn((query: string) => (
      query === "(max-width: 1399px)" ? compactViewport.mediaQuery : colorScheme.mediaQuery
    ));
    window.matchMedia = matchMedia;

    return Object.assign(matchMedia, {
      addEventListener: colorScheme.addEventListener,
      removeEventListener: colorScheme.removeEventListener,
      listenerCount: colorScheme.listenerCount,
      setMatches: colorScheme.setMatches,
      compactListenerCount: compactViewport.listenerCount,
      setCompactMatches: compactViewport.setMatches
    });
  };

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
  });

  it("only shows the canvas mode notice while previewing", () => {
    render(<XFlowEditor value={workflow} />);

    expect(screen.queryByText("可编辑节点与连接配置")).toBeNull();
    expect(screen.queryByText("画布为只读；切换到编辑模式后才能修改工作流")).toBeNull();

    fireEvent.click(screen.getByRole("radio", { name: /预览/ }));

    expect(screen.getByText("画布为只读；切换到编辑模式后才能修改工作流")).toBeTruthy();
  });

  it("keeps four icon-only canvas and history controls centered and functional", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const tools = screen.getByRole("group", { name: "常用画布与历史工具" });
    const select = within(tools).getByRole("button", { name: "选择工具" });
    const connect = within(tools).getByRole("button", { name: "连线工具" });
    const undo = within(tools).getByRole("button", { name: "撤销" }) as HTMLButtonElement;
    const redo = within(tools).getByRole("button", { name: "重做" }) as HTMLButtonElement;

    expect(within(tools).getAllByRole("button")).toHaveLength(4);
    expect(select.getAttribute("aria-pressed")).toBe("true");
    expect(connect.getAttribute("aria-pressed")).toBe("false");
    expect(undo.disabled).toBe(true);
    expect(redo.disabled).toBe(true);

    fireEvent.click(connect);
    expect(connect.getAttribute("aria-pressed")).toBe("true");
    expect(select.getAttribute("aria-pressed")).toBe("false");
    fireEvent.keyDown(window, { key: "v" });
    expect(select.getAttribute("aria-pressed")).toBe("true");

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

  it("renders the editable workbench around the preview canvas", () => {
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
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "画布" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "属性" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "诊断台" })).toBeTruthy();
    expect(screen.getByText("问题")).toBeTruthy();
    expect(screen.getByRole("button", { name: "运行日志" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "输入" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "输出" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起左侧面板" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起属性面板" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "展开诊断台" })).toBeTruthy();
  });

  it("defaults the editor theme to dark graphite", () => {
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    expect(editor.getAttribute("data-theme")).toBe("dark");
    expect(editor.getAttribute("data-theme-variant")).toBe("graphite");
  });

  it("marks a definition without an id as unsaved until its first save", () => {
    render(<XFlowEditor value={{ name: "未命名工作流", spec: "1.0", nodes: [], connections: {} }} />);

    expect(screen.getByText("未保存")).toBeTruthy();
    expect(screen.getByRole("button", { name: "保存" }).className).toContain("ant-btn-variant-solid");
  });

  it("uses the controlled light and dark appearances on the editor root", () => {
    const { rerender } = render(<XFlowEditor value={workflow} appearance="light" />);
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    expect(editor.getAttribute("data-theme")).toBe("light");

    rerender(<XFlowEditor value={workflow} appearance="dark" />);

    expect(editor.getAttribute("data-theme")).toBe("dark");
  });

  it("uses the controlled graphite and blueprint theme variants on the editor root", () => {
    const { rerender } = render(
      <XFlowEditor value={workflow} appearance="dark" themeVariant="graphite" />
    );
    const editor = screen.getByRole("region", { name: "Purchase approval editor" });

    expect(editor.getAttribute("data-theme-variant")).toBe("graphite");

    rerender(<XFlowEditor value={workflow} appearance="dark" themeVariant="blueprint" />);

    expect(editor.getAttribute("data-theme-variant")).toBe("blueprint");
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
      appearance: await screen.findByLabelText("颜色模式"),
      themeVariant: screen.getByLabelText("工作台风格")
    };
  };

  it("opens compact theme settings with labeled appearance and workbench controls", async () => {
    render(<XFlowEditor value={workflow} appearance="dark" themeVariant="graphite" />);

    expect(screen.queryByLabelText("颜色模式")).toBeNull();
    expect(screen.queryByLabelText("工作台风格")).toBeNull();

    const { appearance, themeVariant } = await openThemeSettings();

    expect(within(appearance).getByText("浅色")).toBeTruthy();
    expect(within(appearance).getByText("深色")).toBeTruthy();
    expect(within(appearance).getByText("跟随系统")).toBeTruthy();
    expect(within(themeVariant).getByText("石墨")).toBeTruthy();
    expect(within(themeVariant).getByText("蓝图")).toBeTruthy();
  });

  it("updates the uncontrolled editor root after selecting appearance and workbench style", async () => {
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    const { appearance, themeVariant } = await openThemeSettings();

    fireEvent.click(within(appearance).getByText("浅色"));

    expect(editor.getAttribute("data-appearance")).toBe("light");
    expect(editor.getAttribute("data-theme")).toBe("light");
    expect(editor.getAttribute("data-theme-variant")).toBe("graphite");

    fireEvent.click(within(themeVariant).getByText("蓝图"));

    expect(editor.getAttribute("data-appearance")).toBe("light");
    expect(editor.getAttribute("data-theme")).toBe("light");
    expect(editor.getAttribute("data-theme-variant")).toBe("blueprint");
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
          themeVariant="graphite"
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

  it.each([
    { current: "graphite", label: "蓝图", next: "blueprint" },
    { current: "blueprint", label: "石墨", next: "graphite" }
  ] as const)(
    "delegates the controlled $next workbench-style selection to its callback",
    async ({ current, label, next }) => {
      const onThemeVariantChange = vi.fn();
      render(
        <XFlowEditor
          value={workflow}
          appearance="dark"
          themeVariant={current}
          onThemeVariantChange={onThemeVariantChange}
        />
      );

      const editor = screen.getByRole("region", { name: "Purchase approval editor" });
      const { themeVariant } = await openThemeSettings();
      fireEvent.click(within(themeVariant).getByText(label));

      expect(onThemeVariantChange).toHaveBeenCalledTimes(1);
      expect(onThemeVariantChange).toHaveBeenCalledWith(next);
      expect(editor.getAttribute("data-theme-variant")).toBe(current);
    }
  );

  it("uses compact rails at the breakpoint and keeps the navigation drawers mutually exclusive", async () => {
    const media = mockSystemColorScheme(false, true);
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    expect(editor.getAttribute("data-layout")).toBe("c");
    expect(editor.getAttribute("data-layout-policy")).toBe("auto");
    expect(media.compactListenerCount()).toBe(1);
    expect(screen.getByRole("complementary", { name: "紧凑导航" })).toBeTruthy();
    expect(screen.getByRole("complementary", { name: "紧凑属性" })).toBeTruthy();

    const nodesRail = screen.getByRole("button", { name: "节点" });
    fireEvent.click(nodesRail);

    await waitFor(() => {
      expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
      expect(screen.queryByRole("tab", { name: "节点库" })).toBeNull();
    });
    const search = screen.getByPlaceholderText("搜索 Start / Switch / HTTP");
    await waitFor(() => {
      expect(document.activeElement).toBe(search);
    });

    fireEvent.click(screen.getByRole("button", { name: "大纲" }));
    await waitFor(() => {
      expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
      expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
      expect(screen.getByRole("button", { name: "选择节点 route_by_amount" })).toBeTruthy();
    });

    const propertyRail = screen.getByRole("button", { name: "属性" });
    fireEvent.click(propertyRail);
    await waitFor(() => {
      expect(screen.getByRole("region", { name: "属性" })).toBeTruthy();
      expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    });
    expect(nodesRail.className).not.toContain("active");
    expect(propertyRail.className).toContain("active");
  });

  it("preserves the canvas while manually entering and leaving the compact layout", async () => {
    const media = mockSystemColorScheme(false);
    render(<XFlowEditor value={workflow} />);

    const editor = screen.getByRole("region", { name: "Purchase approval editor" });
    const preview = screen.getByRole("region", { name: "Purchase approval preview" });
    expect(editor.getAttribute("data-layout")).toBe("a");

    fireEvent.click(screen.getByRole("button", { name: "进入沉浸布局" }));
    expect(editor.getAttribute("data-layout-policy")).toBe("compact");
    expect(editor.getAttribute("data-layout")).toBe("c");
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBe(preview);

    fireEvent.click(screen.getByRole("button", { name: "节点" }));
    await waitFor(() => {
      expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
      expect(screen.queryByRole("tab", { name: "节点库" })).toBeNull();
    });

    act(() => {
      media.setCompactMatches(false);
    });
    expect(editor.getAttribute("data-layout")).toBe("c");

    fireEvent.click(screen.getByRole("button", { name: "恢复自动布局" }));
    await waitFor(() => {
      expect(editor.getAttribute("data-layout-policy")).toBe("auto");
      expect(editor.getAttribute("data-layout")).toBe("a");
      expect(screen.queryByRole("region", { name: "节点" })).toBeTruthy();
    });
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBe(preview);
  });

  it("provides outline metadata and keyboard roving selection without dirtying the workflow", () => {
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

    const outline = screen.getByRole("region", { name: "大纲" });
    const start = within(outline).getByRole("button", { name: "选择节点 start" });
    const route = within(outline).getByRole("button", { name: "选择节点 route_by_amount" });
    const manager = within(outline).getByRole("button", { name: "选择节点 l1_manager" });
    expect(within(route).getByText("xflow.switch")).toBeTruthy();
    expect(within(route).getByText("running")).toBeTruthy();
    expect(within(route).getByText("入 1 / 出 1")).toBeTruthy();
    expect(within(route).getByText("诊断 0")).toBeTruthy();
    expect(start.tabIndex).toBe(-1);
    expect(route.tabIndex).toBe(0);

    start.focus();
    fireEvent.keyDown(start, { key: "ArrowDown" });
    expect(document.activeElement).toBe(route);
    expect(route.tabIndex).toBe(0);

    fireEvent.keyDown(route, { key: "End" });
    expect(document.activeElement).toBe(manager);
    fireEvent.keyDown(manager, { key: "Home" });
    expect(document.activeElement).toBe(start);
    fireEvent.keyDown(start, { key: "r" });
    expect(document.activeElement).toBe(route);

    expect(handleChange).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "保存" }).className).not.toContain("ant-btn-variant-solid");
  });

  it("changes inspector tabs through their tablist keyboard contract", () => {
    render(<XFlowEditor value={workflow} />);

    const inspector = screen.getByRole("region", { name: "属性" });
    const config = within(inspector).getByRole("tab", { name: "配置" });
    const connections = within(inspector).getByRole("tab", { name: "连接" });
    const run = within(inspector).getByRole("tab", { name: "运行" });
    expect(config.getAttribute("aria-controls")).toBe("xflow-editor-inspector-panel-config");
    expect(within(inspector).getByRole("tabpanel").getAttribute("aria-labelledby")).toBe(config.id);

    config.focus();
    fireEvent.keyDown(config, { key: "ArrowRight" });
    expect(document.activeElement).toBe(connections);
    expect(connections.getAttribute("aria-selected")).toBe("true");
    expect(within(inspector).getByRole("tabpanel").getAttribute("aria-labelledby")).toBe(connections.id);

    fireEvent.keyDown(connections, { key: "End" });
    expect(document.activeElement).toBe(run);
    expect(run.getAttribute("aria-selected")).toBe("true");
    fireEvent.keyDown(run, { key: "Home" });
    expect(document.activeElement).toBe(config);
    expect(config.getAttribute("aria-selected")).toBe("true");
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

    const inspector = screen.getByRole("region", { name: "属性" });
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Locally dirty workflow" }
    });
    const echoedWorkflow = handleChange.mock.calls.at(-1)?.[0];
    expect(echoedWorkflow).toBeTruthy();
    expect(screen.getByRole("button", { name: "保存" }).className).toContain("ant-btn-color-primary");
    expect(screen.getByRole("button", { name: "保存" }).className).toContain("ant-btn-variant-solid");

    rerender(<XFlowEditor value={echoedWorkflow} onChange={handleChange} onSave={handleSave} />);
    expect(screen.getByText("未保存")).toBeTruthy();
    expect(screen.getByRole("button", { name: "保存" }).className).toContain("ant-btn-color-primary");
    expect(screen.getByRole("button", { name: "保存" }).className).toContain("ant-btn-variant-solid");

    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    const locate = await screen.findByRole("button", { name: "定位节点 route_by_amount" });
    fireEvent.click(locate);

    expect(within(screen.getByRole("region", { name: "属性" })).getByDisplayValue("xflow.switch")).toBeTruthy();
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(screen.getByText("未保存")).toBeTruthy();
  });

  it("reveals node search from the node library header", () => {
    render(<XFlowEditor value={workflow} />);

    const library = screen.getByRole("region", { name: "节点" });
    expect(within(library).getByLabelText("添加节点图标")).toBeTruthy();
    expect(within(library).queryByPlaceholderText("搜索 Start / Switch / HTTP")).toBeNull();

    fireEvent.click(within(library).getByRole("button", { name: "搜索节点" }));

    const searchInput = within(library).getByPlaceholderText("搜索 Start / Switch / HTTP");
    expect(searchInput).toBeTruthy();
    expect(searchInput.className).toContain("ant-input-sm");
    expect(within(library).queryByRole("button", { name: "search" })).toBeNull();

    fireEvent.change(searchInput, { target: { value: "http" } });
    expect(within(library).getByRole("button", { name: "HTTP" })).toBeTruthy();
    expect(within(library).queryByRole("button", { name: "Kafka" })).toBeNull();
  });

  it("collapses and expands the editor panels", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "收起左侧面板" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开左侧面板" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    expect(screen.queryByRole("region", { name: "属性" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
    expect(screen.getByRole("region", { name: "属性" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByText("DSL 摘要")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "收起诊断台" }));
    expect(screen.queryByText("DSL 摘要")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByText("DSL 摘要")).toBeTruthy();
  });

  it("selects outline nodes and exposes the editable node configuration", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));

    const inspector = screen.getByRole("region", { name: "属性" });
    expect(within(inspector).getByText("属性")).toBeTruthy();
    expect(within(inspector).getAllByText(/route_by_amount/).length).toBeGreaterThan(0);
    expect(within(inspector).queryByText("running")).toBeNull();
    expect(within(inspector).getByDisplayValue("xflow.switch")).toBeTruthy();
    expect(within(inspector).getByLabelText("节点名称").className).toContain("ant-input-sm");
    expect(within(inspector).getByRole("tab", { name: "配置" }).getAttribute("aria-selected")).toBe("true");
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 worker" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    const inspector = screen.getByRole("region", { name: "属性" });
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

    const inspector = screen.getByRole("region", { name: "属性" });
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Local unsaved draft" }
    });
    expect(within(inspector).getByDisplayValue("Local unsaved draft")).toBeTruthy();
    expect(handleChange).toHaveBeenLastCalledWith({ ...workflow, name: "Local unsaved draft" });

    rerender(<XFlowEditor value={parentReplacement} onChange={handleChange} />);

    await waitFor(() => {
      expect(screen.getByRole("region", { name: "Parent replacement editor" })).toBeTruthy();
      expect(within(screen.getByRole("region", { name: "属性" })).getByDisplayValue("Parent replacement")).toBeTruthy();
      expect(screen.getByRole("button", { name: "选择节点 external_start" })).toBeTruthy();
    });
    expect(screen.queryByRole("button", { name: "选择节点 route_by_amount" })).toBeNull();
    expect(handleChange).toHaveBeenCalledTimes(1);
  });

  it("edits workflow-level DSL fields from the property panel", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.change(within(inspector).getByLabelText("工作流输入参数 JSON"), {
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

  it("edits node-level runner selector overrides", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 l1_manager" }));
    const inspector = screen.getByRole("region", { name: "属性" });
    fireEvent.change(within(inspector).getByLabelText("节点 Runner 选择 JSON"), {
      target: {
        value: '{ "mode": "default", "match_labels": { "mode": "local", "env": "prod" } }'
      }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        workflow.nodes[0],
        workflow.nodes[1],
        {
          ...workflow.nodes[2],
          runner_selector: {
            mode: "default",
            match_labels: {
              mode: "local",
              env: "prod"
            }
          }
        }
      ]
    });
  });

  it("edits node parameters as JSON and reports invalid JSON", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性" });
    const parameters = within(inspector).getByLabelText("参数 JSON");

    fireEvent.change(parameters, {
      target: { value: '{ "threshold": 5000 }' }
    });

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
    expect(screen.getByRole("button", { name: "选择节点 http_1" })).toBeTruthy();
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

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledWith(workflow);
    });
    expect(screen.getByText("已保存")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    await waitFor(() => {
      expect(handleRun).toHaveBeenCalledWith(workflow);
    });
    expect(screen.getByText("运行完成")).toBeTruthy();
    expect(screen.getByText("last run 0.0s")).toBeTruthy();
    expect(screen.getAllByText("success").length).toBeGreaterThan(0);
  });

  it("keeps edits made while a save request is pending dirty after the old save resolves", async () => {
    let resolveSave!: (savedWorkflow: typeof workflow) => void;
    const pendingSave = new Promise<typeof workflow>((resolve) => {
      resolveSave = resolve;
    });
    const handleChange = vi.fn();
    const handleSave = vi.fn(() => pendingSave);

    render(<XFlowEditor value={workflow} onChange={handleChange} onSave={handleSave} />);

    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => {
      expect(handleSave).toHaveBeenCalledWith(workflow);
    });

    const inspector = screen.getByRole("region", { name: "属性" });
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "Edited while save is pending" }
    });
    expect(handleChange).toHaveBeenLastCalledWith({
      ...workflow,
      name: "Edited while save is pending"
    });

    await act(async () => {
      resolveSave(workflow);
      await pendingSave;
    });

    expect(within(inspector).getByDisplayValue("Edited while save is pending")).toBeTruthy();
    expect(screen.getByText("未保存")).toBeTruthy();
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 start" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 rules" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 rules" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 worker" }));
    const inspector = screen.getByRole("region", { name: "属性" });
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 l1_manager" }));
    fireEvent.click(within(screen.getByRole("region", { name: "属性" })).getByRole("button", { name: "删除节点" }));

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
    expect(screen.queryByRole("button", { name: "选择节点 l1_manager" })).toBeNull();
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

    fireEvent.click(screen.getByRole("button", { name: "选择节点 worker" }));
    fireEvent.click(
      within(screen.getByRole("region", { name: "属性" })).getByRole("button", {
        name: "删除节点"
      })
    );

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

    expect(screen.getByText("DSL invalid")).toBeTruthy();
    expect(screen.getByText("missing_target")).toBeTruthy();
  });

  it("creates a new workflow draft from the toolbar", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "新建工作流" }));

    expect(handleChange).toHaveBeenCalledWith({
      name: "未命名工作流",
      spec: "1.0",
      nodes: [],
      connections: {}
    });
    expect(screen.getByText("等待真实工作流数据")).toBeTruthy();
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
});
