import * as React from "react";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, ParamIssue, RuntimeSnapshot, WorkflowDef } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";
import type { XFlowExecution } from "./panels/runBar";
import { XFlowEditor, type XFlowEditorSaveResult } from "./index";
import generated from "./node-form/testdata/node-types.generated.json";

// E4a: the 28px status strip and the 256px diagnostics drawer (spec §6.5).
// E4b: the three dock states, ⌘J, focus hand-over and 定位.

// React Flow never initialises in jsdom, so the preview's view handle would
// never arrive. This pass-through wrapper hands the editor a spy handle the way
// the real preview does once it is live, and keeps the last props for assertions.
const preview = vi.hoisted(() => ({
  props: undefined as undefined | Record<string, unknown>,
  mounts: 0,
  handle: { fitView: vi.fn(), fitNode: vi.fn() }
}));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    preview.props = props as unknown as Record<string, unknown>;
    React.useEffect(() => {
      preview.mounts += 1;
    }, []);
    React.useEffect(() => {
      props.onReady?.(preview.handle);
    }, [props.onReady]);
    return React.createElement(actual.XFlowPreview, props);
  };
  return { ...actual, XFlowPreview: Wrapped };
});

beforeEach(() => {
  preview.handle.fitView.mockClear();
  preview.handle.fitNode.mockClear();
  // Panel collapse is persisted per workflow id (§6.0); a case that ends with
  // the inspector collapsed must not hand that state to the next case.
  window.localStorage?.clear();
});

const workflow: WorkflowDef = {
  id: "wf-diagnostics",
  name: "Diagnostics",
  version: "1.2.0",
  credentials: { crm_token: { type: "bearer" } },
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function" },
    { name: "tail", type: "xflow.http" }
  ],
  connections: { start: { main: [{ node: "worker" }] }, worker: { main: [{ node: "tail" }] } }
};

/** worker → ghost: one dangling target, so exactly one error attributed to `worker`. */
const broken: WorkflowDef = {
  ...workflow,
  connections: { start: { main: [{ node: "worker" }] }, worker: { main: [{ node: "ghost" }] } }
};

/**
 * Defect A: a trigger entry edge into `xflow.start`. The backend's
 * `validateEdgeTarget` accepts it — an edge target only constrains a node that
 * DECLARES inputs — so the diagnostics must not call `start.main` invalid just
 * because the editor's connect affordances omit entry nodes.
 */
const timerEntry: WorkflowDef = {
  id: "wf-timer-entry",
  name: "Timer entry",
  version: "1.0.0",
  credentials: { crm_token: { type: "bearer" } },
  nodes: [
    { name: "every_minute", type: "xflow.trigger.timer" },
    { name: "start", type: "xflow.start" },
    { name: "worker", type: "xflow.function" }
  ],
  connections: {
    every_minute: { main: [{ node: "start" }] },
    start: { main: [{ node: "worker" }] }
  }
};

/** The same shape with an input declaration that does not include `main`. */
const declaredInputs: WorkflowDef = {
  ...timerEntry,
  nodes: [
    { name: "every_minute", type: "xflow.trigger.timer" },
    { name: "start", type: "xflow.start" },
    { name: "worker", type: "xflow.function", inputs: [{ name: "payload" }] }
  ],
  connections: {
    every_minute: { main: [{ node: "start" }] },
    start: { main: [{ node: "worker" }] }
  }
};

/** The same graph with no credential declared: one extra, workflow-level warning. */
const withoutCredentials = (def: WorkflowDef): WorkflowDef => ({ ...def, credentials: undefined });

const strip = (name: "展开诊断台" | "诊断台摘要" = "展开诊断台") => screen.getByRole("button", { name });
const openDrawer = () => fireEvent.click(strip("展开诊断台"));
const drawer = () => screen.getByRole("region", { name: "诊断台" });
const summary = () => screen.getByRole("complementary", { name: "摘要" });
const rowOf = (text: string | RegExp): HTMLElement => {
  const row = screen.getByText(text).closest("li");
  if (!row) throw new Error("issue row not found");
  return row;
};

describe("status strip (§6.5)", () => {
  it("is one button named 展开诊断台 while closed and 诊断台摘要 while open", () => {
    render(<XFlowEditor value={workflow} />);

    expect(strip().getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("button", { name: "诊断台摘要" })).toBeNull();
    expect(screen.queryByRole("button", { name: "收起诊断台" })).toBeNull();
    // The shortcut lives on the title and aria-keyshortcuts, never in the name.
    expect(strip().getAttribute("title")).toContain("⌘J");
    expect(strip().getAttribute("aria-keyshortcuts")).toContain("Meta+J");

    openDrawer();
    expect(strip("诊断台摘要").getAttribute("aria-expanded")).toBe("true");
    expect(screen.queryByRole("button", { name: "展开诊断台" })).toBeNull();
    // Exactly one control closes the drawer by name.
    expect(screen.getAllByRole("button", { name: "收起诊断台" })).toHaveLength(1);
  });

  it("reads 校验通过 with no warning chip when there is nothing to report", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);

    const text = strip().textContent ?? "";
    expect(text).toContain("校验通过");
    expect(text).not.toContain("警告");
    expect(text).not.toContain("错误");
    expect(text).not.toContain("最近运行");
  });

  it("counts errors and warnings from the findings, and drops the pass mark on an error", () => {
    render(<XFlowEditor value={withoutCredentials(broken)} />);

    const text = strip().textContent ?? "";
    expect(text).toContain("1 错误");
    expect(text).not.toContain("校验通过");
    // No credentials declared: that is the one warning.
    expect(text).toContain("1 警告");
  });

  it("reports node and edge counts read from the workflow arrays, and follows an edit", () => {
    const { rerender } = render(<XFlowEditor value={workflow} />);
    const stats = () => strip().querySelector("[data-graph-stats]");

    expect(stats()?.textContent).toBe("3 节点 · 2 连线");
    expect(stats()?.getAttribute("data-graph-stats")).toBe("3/2");

    rerender(
      <XFlowEditor
        value={{ ...workflow, nodes: workflow.nodes?.slice(0, 2), connections: { start: { main: [{ node: "worker" }] } } }}
      />
    );
    expect(stats()?.textContent).toBe("2 节点 · 1 连线");
  });

  it("names the last run from the shared status table, and omits the segment before any run", () => {
    const runtime: RuntimeSnapshot = { status: "failed", nodes: { start: { status: "success" } } };
    const { rerender } = render(<XFlowEditor value={workflow} />);
    expect(strip().textContent).not.toContain("最近运行");

    rerender(<XFlowEditor value={workflow} runtime={runtime} />);
    expect(strip().textContent).toContain("最近运行 失败");
    expect(strip().textContent).not.toMatch(/failed|success/);
  });

  it("counts local unsaved changes on the strip, and hides the count once the draft is clean again (WB-09)", async () => {
    render(<XFlowEditor value={workflow} />);
    expect(strip().textContent).not.toContain("本地");

    const inspectorRoot = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspectorRoot).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "禁用" }));

    // One changed node: the same count the version chip and the publish diff use.
    expect(strip().textContent).toContain("本地 1 处未保存");

    fireEvent.keyDown(window, { key: "z", metaKey: true });
    expect(strip().textContent).not.toContain("本地");
  });
});

describe("issue list (§6.5)", () => {
  it("gives every row an icon, a target, a Chinese sentence and a locate action, with no internal ids", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();

    const row = rowOf("worker.main 连接目标不存在: ghost");
    expect(row.getAttribute("data-tone")).toBe("danger");
    expect(within(row).getByRole("img", { name: "错误" })).toBeTruthy();
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("worker");
    expect(within(row).getByRole("button", { name: "定位节点 worker" }).textContent).toBe("定位节点");

    const visible = drawer().textContent ?? "";
    expect(visible).not.toMatch(/\b(pass|warn|error|waiting|errors|warnings)\b/);
    expect(visible).not.toMatch(/\b(edges|nodes|DSL valid|DSL invalid|credentials|runtime)\b/);
    // The internal area key rides on a data attribute, not on text.
    expect(row.getAttribute("data-area")).toBe("edges");
  });

  it("targets 工作流 for a finding about the workflow itself, with no node to locate", () => {
    render(<XFlowEditor value={withoutCredentials(workflow)} />);
    openDrawer();

    const row = rowOf("未声明凭证引用；涉及外部资源的节点运行前需要配置");
    expect(row.getAttribute("data-tone")).toBe("warning");
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("工作流");
    expect(within(row).queryByRole("button")).toBeNull();
  });

  it("selects the node on 定位节点 and does not select anything on its own when a finding appears", () => {
    render(<XFlowEditor value={broken} />);
    // Nothing is selected for the user just because the drawer lists a failure.
    openDrawer();
    expect(screen.queryByText("xflow.function", { selector: ".xflow-editor-inspector-head__type" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(
      within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByText("xflow.function", {
        selector: ".xflow-editor-inspector-head__type"
      })
    ).toBeTruthy();
  });

  it("folds the passed checks into one line that expands to each item and folds back", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);
    openDrawer();

    const line = screen.getByText(/^\d+ 项检查通过：/);
    expect(line.textContent).toContain("DSL 结构");
    const toggle = within(line.closest("li") as HTMLElement).getByRole("button", { name: "展开通过项" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByText("2 条数据连接和 0 条依赖连接已解析")).toBeNull();

    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByText("2 条数据连接和 0 条依赖连接已解析")).toBeTruthy();
    expect(screen.getAllByRole("img", { name: "通过" }).length).toBeGreaterThan(1);

    fireEvent.click(toggle);
    expect(screen.queryByText("2 条数据连接和 0 条依赖连接已解析")).toBeNull();
  });

  it("leaves only the pass line when nothing is wrong", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);
    openDrawer();

    expect(drawer().querySelectorAll(".xflow-editor-issue")).toHaveLength(0);
    expect(drawer().querySelectorAll(".xflow-editor-issues__pass")).toHaveLength(1);
    // The focused tab carries rc-tabs' "Tab 1 of 4" live region in its name.
    expect(screen.getByRole("tab", { name: /问题 0$/ })).toBeTruthy();
  });

  it("accepts an entry edge into xflow.start, which declares no input ports", () => {
    render(<XFlowEditor value={timerEntry} />);

    expect(strip().textContent).not.toContain("错误");
    openDrawer();
    expect(screen.queryByText(/不是有效输入端口/)).toBeNull();
    expect(drawer().querySelectorAll(".xflow-editor-issue")).toHaveLength(0);
  });

  it("still rejects an undeclared input port on a node that declares inputs", () => {
    render(<XFlowEditor value={declaredInputs} />);

    openDrawer();
    expect(rowOf("worker.main 不是有效输入端口")).toBeTruthy();
  });
});

describe("node-form findings in the issue list (WB-13)", () => {
  const formTypes = generated as unknown as NodeTypesResponse;
  const formWorkflow: WorkflowDef = {
    // No id: a draft that was never persisted keeps 保存 usable, so the save
    // button's issue count and the drawer can be read in the same render.
    name: "form",
    credentials: { crm_token: { type: "bearer" } },
    nodes: [{ name: "api", type: "xflow.http", parameters: { method: "GET" } }],
    connections: {}
  };
  const withUrl = (url: string): WorkflowDef => ({
    ...formWorkflow,
    nodes: [{ name: "api", type: "xflow.http", parameters: { method: "GET", url } }]
  });
  const field = () => document.querySelector<HTMLElement>('[data-composer-path="/parameters/url"]');
  const inspectorRoot = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
  /** The drawer's row for a message; the Inspector's composer renders the same sentence. */
  const issueRowOf = (text: string | RegExp): HTMLElement => {
    const row = within(drawer()).getByText(text).closest("li");
    if (!row) throw new Error("issue row not found");
    return row;
  };

  it("lists a missing required parameter with its node and path, counted once", () => {
    render(<XFlowEditor nodeTypes={formTypes} value={formWorkflow} />);

    // The strip and the save button agree with the one row the drawer shows.
    expect(strip().textContent).toContain("1 错误");
    expect(screen.getByTestId("save-issue-count").textContent).toBe("1");

    openDrawer();
    expect(screen.getByRole("tab", { name: /问题 1$/ })).toBeTruthy();
    const row = issueRowOf("必填");
    expect(row.getAttribute("data-tone")).toBe("danger");
    expect(row.getAttribute("data-area")).toBe("parameters");
    expect(row.getAttribute("data-source")).toBe("local");
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("api");
    expect(within(row).getByRole("button", { name: "定位字段 api /parameters/url" })).toBeTruthy();
    expect(drawer().querySelectorAll('.xflow-editor-issue[data-tone="danger"]')).toHaveLength(1);
  });

  it("keeps a warning-severity finding a warning row", () => {
    render(<XFlowEditor nodeTypes={formTypes} value={withUrl("not-a-url")} />);

    const text = strip().textContent ?? "";
    expect(text).toContain("1 警告");
    expect(text).not.toContain("错误");

    openDrawer();
    const row = issueRowOf("需要完整的 URL");
    expect(row.getAttribute("data-tone")).toBe("warning");
    expect(within(row).getByRole("img", { name: "警告" })).toBeTruthy();
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("api");
    expect(within(row).getByRole("button", { name: "定位字段 api /parameters/url" })).toBeTruthy();
  });

  it("locates the node and focuses its field from a local finding", async () => {
    render(<XFlowEditor nodeTypes={formTypes} value={formWorkflow} />);
    openDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位字段 api /parameters/url" }));
    await waitFor(() => {
      expect(within(inspectorRoot()).getByText(/^xflow\.http/, { selector: ".xflow-editor-inspector-head__type" })).toBeTruthy();
      expect(within(inspectorRoot()).getByRole("tab", { name: "配置" }).getAttribute("aria-selected")).toBe("true");
      expect(field()?.contains(document.activeElement)).toBe(true);
    });
  });

  it("shows a local finding once when the server repeats the same node, path and severity", async () => {
    const repeated: ParamIssue = {
      node: "api",
      path: "/parameters/url",
      code: "required",
      message: "url is required",
      severity: "error"
    };
    const serverOnly: ParamIssue = {
      node: "api",
      path: "/parameters/method",
      code: "enum",
      message: "server rejects method",
      severity: "error"
    };
    const handleSave = vi.fn(
      async (next: WorkflowDef): Promise<XFlowEditorSaveResult> => ({ workflow: next, paramIssues: [repeated, serverOnly] })
    );
    render(<XFlowEditor nodeTypes={formTypes} value={formWorkflow} onSave={handleSave} />);
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));

    openDrawer();
    // The local row wins and the server duplicate is dropped: one row, one locator.
    expect(within(drawer()).queryByText(/url is required/)).toBeNull();
    expect(screen.getAllByRole("button", { name: "定位字段 api /parameters/url" })).toHaveLength(1);
    expect(issueRowOf("必填").getAttribute("data-source")).toBe("local");
    // A server-only finding still shows, marked as coming from the server.
    const serverRow = issueRowOf(/server rejects method/);
    expect(serverRow.getAttribute("data-source")).toBe("server");
    expect(serverRow.textContent).toContain("服务端");
    expect(screen.getByRole("tab", { name: /问题 2$/ })).toBeTruthy();
  });

  it("keeps the empty state when every form is valid", () => {
    render(<XFlowEditor nodeTypes={formTypes} value={withUrl("https://example.com")} />);
    expect(strip().textContent).toContain("校验通过");

    openDrawer();
    expect(drawer().querySelectorAll(".xflow-editor-issue")).toHaveLength(0);
    expect(screen.getByRole("tab", { name: /问题 0$/ })).toBeTruthy();
  });

  it("accepts a bare pure expression instead of demanding ${{ }}", () => {
    const ifWorkflow: WorkflowDef = {
      ...formWorkflow,
      nodes: [{ name: "branch", type: "xflow.if", parameters: { condition: "$input.amount >= 100" } }]
    };
    render(<XFlowEditor nodeTypes={formTypes} value={ifWorkflow} />);

    // formIssueSummary.errors is 0: no error count on the save button, no
    // error/warning chip in the strip, and the drawer holds only the pass line.
    expect(screen.queryByTestId("save-issue-count")).toBeNull();
    expect(strip().textContent).toContain("校验通过");
    openDrawer();
    expect(within(drawer()).queryByText(/需要表达式/)).toBeNull();
    expect(drawer().querySelectorAll(".xflow-editor-issue")).toHaveLength(0);
    expect(screen.getByRole("tab", { name: /问题 0$/ })).toBeTruthy();
  });
});

describe("drawer head and tabs (§6.0 / §6.5)", () => {
  it("is a region named by its 诊断台 heading, with the issue count tab and every other tab wired", () => {
    render(<XFlowEditor value={withoutCredentials(broken)} />);
    openDrawer();

    const heading = within(drawer()).getByRole("heading", { name: "诊断台" });
    expect(drawer().getAttribute("aria-labelledby")).toBe(heading.id);
    expect(drawer().getAttribute("aria-label")).toBeNull();

    const tabs = within(drawer()).getAllByRole("tab");
    expect(tabs.map((tab) => tab.textContent?.replace(/^Tab \d+ of \d+/, ""))).toEqual(["问题 2", "运行日志", "输入", "输出"]);
    expect(tabs[0].getAttribute("aria-selected")).toBe("true");
    // Every tab has a real panel behind it (WB-04/WB-05/WB-06): no placeholder
    // note, and each tab points its aria-controls at its own panel.
    for (const tab of tabs) {
      expect(tab.hasAttribute("aria-description")).toBe(false);
      const panel = document.getElementById(tab.getAttribute("aria-controls") ?? "");
      expect(panel?.getAttribute("role")).toBe("tabpanel");
    }
    // Only the active panel is exposed; the other three are hidden. (AntD
    // renders an empty internal tabpanel of its own, so the assertion is
    // scoped to the tab's own aria-controls target.)
    const exposed = within(drawer())
      .getAllByRole("tabpanel")
      .filter((panel) => panel.id === tabs[0].getAttribute("aria-controls"));
    expect(exposed).toHaveLength(1);
    expect(exposed[0].getAttribute("aria-labelledby")).toBe(tabs[0].id);
    for (const selector of [".xflow-editor-timeline", ".xflow-editor-input", ".xflow-editor-output"]) {
      expect((drawer().querySelector(selector) as HTMLElement).closest("[hidden]")).not.toBeNull();
    }
  });

  it("reveals each tab's own panel and carries no 未接入 placeholder", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const panels = [
      ["运行日志", ".xflow-editor-timeline"],
      ["输入", ".xflow-editor-input"],
      ["输出", ".xflow-editor-output"],
      ["问题 0", ".xflow-editor-issues__problems"]
    ] as const;

    for (const [name, activeSelector] of panels) {
      fireEvent.click(within(drawer()).getByRole("tab", { name: new RegExp(`${name}$`) }));
      for (const [, selector] of panels) {
        const panel = drawer().querySelector<HTMLElement>(selector);
        expect(panel).not.toBeNull();
        // Only the chosen panel and its ancestors are exposed.
        expect(panel?.closest("[hidden]") === null).toBe(selector === activeSelector);
      }
      expect(within(drawer()).queryByText("未接入")).toBeNull();
    }
  });

  it("closes from the drawer's own button and hands focus back to the strip", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(screen.queryByRole("tab", { name: "运行日志" })).toBeNull();
    expect(strip().getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(strip());
  });

  it("offers 扩展到画布高度 as a labelled 24px-class action beside 收起诊断台", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const expand = within(drawer()).getByRole("button", { name: "扩展到画布高度" });
    expect(expand.className).toContain("xflow-editor-panel-collapse");
    expect(within(drawer()).getByRole("button", { name: "收起诊断台" })).toBeTruthy();
  });

  it("names 扩展到画布高度 in its tooltip, and 还原诊断台高度 once expanded", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.mouseEnter(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
    expect(await screen.findByRole("tooltip", { name: "扩展到画布高度" })).toBeTruthy();
    fireEvent.mouseLeave(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));

    fireEvent.click(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
    fireEvent.mouseEnter(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(await screen.findByRole("tooltip", { name: "还原诊断台高度" })).toBeTruthy();
  });

  it("groups the heading icon and title 8px apart inside the head", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const group = within(drawer()).getByRole("heading", { name: "诊断台" }).parentElement;
    expect(group?.className).toBe("xflow-editor-drawer__title");
    expect(group?.querySelector(".xflow-editor-panel-title__icon")).toBeTruthy();
  });
});

describe("summary column (§6.5)", () => {
  it("lists 定义摘要 from the draft: node mix, edge count, credentials and the top bar's version wording", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const side = within(summary());
    expect(side.getByRole("heading", { name: "定义摘要" })).toBeTruthy();
    expect(side.getByRole("heading", { name: "最近运行" })).toBeTruthy();

    const dd = (label: string) => side.getByText(label, { selector: "dt" }).nextElementSibling as HTMLElement;
    expect(dd("节点").textContent).toBe("3（流程控制 1 · 动作 1 · 代码 1）");
    // Each category is its own nowrap span, so a wrap can only fall on a `·`.
    expect(dd("节点").querySelectorAll(".xflow-editor-kvlist__mix")).toHaveLength(3);
    expect(dd("连线").textContent).toBe("2");
    expect(dd("凭证").textContent).toBe("crm_token");
    expect(dd("版本").textContent).toBe(document.querySelector(".xflow-editor-chip")?.textContent);
    expect(dd("版本").textContent).toBe("线上 v1.2.0");
  });

  it("draws a dash where there is no data source, and the running node when there is one", () => {
    const empty = render(<XFlowEditor value={{ ...workflow, credentials: undefined }} />);
    openDrawer();
    const dd = (label: string) => within(summary()).getByText(label, { selector: "dt" }).nextElementSibling as HTMLElement;
    expect(dd("凭证").textContent).toBe("—");
    expect(dd("执行").textContent).toBe("—");
    // The status row names the latest run's state, not an execution id.
    expect(dd("状态").textContent).toBe("—");
    expect(dd("当前节点").textContent).toBe("—");
    expect(dd("命中分支").textContent).toBe("—");
    expect(dd("耗时").textContent).toBe("—");
    empty.unmount();

    const runtime: RuntimeSnapshot = { status: "running", nodes: { worker: { status: "running" } } };
    render(<XFlowEditor value={workflow} runtime={runtime} />);
    openDrawer();
    expect(dd("执行").textContent).toBe("—");
    expect(dd("状态").textContent).toBe("运行中");
    expect(dd("当前节点").textContent).toBe("worker");
    // No execution timestamps and no terminal state: only "in progress" is honest.
    expect(dd("耗时").textContent).toBe("进行中");
  });

  it("carries none of the retired English dump", () => {
    render(<XFlowEditor value={workflow} runtime={{ status: "success" }} />);
    openDrawer();

    expect(screen.queryByLabelText("DSL 预览")).toBeNull();
    expect(drawer().querySelector("pre, meter, progress, .xflow-editor-diagnostics-meter")).toBeNull();
    expect(drawer().textContent).not.toMatch(/last run|valid|execution-local|errors|warnings/);
  });
});

// ---- WB-04: the 输出 tab -----------------------------------------------------

const outputTab = () => within(drawer()).getByRole("tab", { name: /输出$/ });
const outputPanel = () => drawer().querySelector<HTMLElement>(".xflow-editor-output") as HTMLElement;
const openOutput = () => fireEvent.click(outputTab());

describe("输出 tab (WB-04)", () => {
  it("renders the selected node's output as read-only JSON in the shared code block", () => {
    const runtime: RuntimeSnapshot = {
      status: "success",
      nodes: { start: { status: "success", output: { order_id: "SO-1", total: 42 } } }
    };
    render(<XFlowEditor value={workflow} runtime={runtime} />);
    openDrawer();
    openOutput();

    const panel = outputPanel();
    expect(panel.getAttribute("role")).toBe("tabpanel");
    expect(panel.hasAttribute("hidden")).toBe(false);
    expect(outputTab().getAttribute("aria-controls")).toBe(panel.id);
    expect(outputTab().hasAttribute("aria-description")).toBe(false);
    expect(panel.textContent).toContain("start 的输出");
    const code = panel.querySelector("pre.xflow-editor-run-code") as HTMLElement;
    expect(code).not.toBeNull();
    expect(code.textContent).toContain('"order_id": "SO-1"');
    expect(code.textContent).toContain('"total": 42');
    // Read-only: the drawer never grows an edit surface.
    expect(panel.querySelector("input, textarea, [contenteditable]")).toBeNull();
    expect(code.getAttribute("contenteditable")).toBeNull();
    expect(within(panel).getByRole("button", { name: "复制输出" })).toBeTruthy();
  });

  it("asks for a selection when nothing is selected", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "Escape" });
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("选择一个节点后查看它的输出。");
  });

  it("says a node has not run yet when it has no runtime state", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("节点尚未运行，暂无输出。");
    expect(outputPanel().querySelector("pre")).toBeNull();
  });

  it("says the run finished without output when the output map is empty", () => {
    render(
      <XFlowEditor
        value={workflow}
        runtime={{
          status: "success",
          nodes: { start: { status: "success", output: {} } }
        }}
      />
    );
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("节点运行完成，但没有输出。");
    expect(outputPanel().querySelector("pre")).toBeNull();
  });

  it("marks a node declared private 已遮蔽 and never prints a raw payload", () => {
    const privateWorkflow: WorkflowDef = {
      ...workflow,
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger", output: { private: true } },
        ...(workflow.nodes ?? []).slice(1)
      ]
    };
    render(
      <XFlowEditor
        value={privateWorkflow}
        runtime={{
          status: "success",
          nodes: { start: { status: "success", output: { secret: "top-secret-value" } } }
        }}
      />
    );
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("输出已遮蔽");
    // The host may still hand over an unredacted payload; the drawer must not print it.
    expect(document.body.textContent).not.toContain("top-secret-value");
    expect(outputPanel().querySelector("pre")).toBeNull();
  });

  it("treats a missing public output on a run node as 已遮蔽", () => {
    // engine/inspect.go returns no output at all for a fail-closed node: a node
    // that ran without a public output is the same verdict, not "no output".
    render(<XFlowEditor value={workflow} runtime={{ status: "success", nodes: { start: { status: "success" } } }} />);
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("输出已遮蔽");
  });

  it("honours an explicit redacted marker on the payload", () => {
    render(
      <XFlowEditor
        value={workflow}
        runtime={{
          status: "success",
          nodes: { start: { status: "success", output: { redacted: true, secret: "another-secret" } } }
        }}
      />
    );
    openDrawer();
    openOutput();
    expect(outputPanel().textContent).toContain("输出已遮蔽");
    expect(document.body.textContent).not.toContain("another-secret");
  });
});

// ---- WB-05: the 输入 tab -----------------------------------------------------

const openDiagnosticsTab = (name: string | RegExp) =>
  fireEvent.click(within(drawer()).getByRole("tab", { name }));
const selectNode = (name: string) =>
  fireEvent.click(document.querySelector(`.xflow-preview-node__select[aria-label^="${name} node "]`) as HTMLElement);
const inputPanel = () => drawer().querySelector<HTMLElement>(".xflow-editor-input") as HTMLElement;
const logsPanel = () => drawer().querySelector<HTMLElement>(".xflow-editor-timeline") as HTMLElement;

describe("输入 tab (WB-05)", () => {
  // Worker carries parameters a runner might resolve: the tab must never read
  // them, so their values are the leak sentinel for every case below.
  const inputWorkflow: WorkflowDef = {
    ...workflow,
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "worker", type: "xflow.function", parameters: { password: "hunter2", token: "tok-secret" } },
      { name: "tail", type: "xflow.http" }
    ]
  };

  it("lists the selected node's incoming edges with the upstream public output and hit port", () => {
    const runtime: RuntimeSnapshot = {
      status: "success",
      nodes: {
        start: { status: "success", port: "main", output: { order_id: "SO-1", total: 42 } }
      }
    };
    render(<XFlowEditor value={inputWorkflow} runtime={runtime} />);
    selectNode("worker");
    openDrawer();
    openDiagnosticsTab(/输入$/);

    const panel = inputPanel();
    expect(panel.hasAttribute("hidden")).toBe(false);
    expect(panel.textContent).toContain("求值后的参数不保留，这里只显示上游节点的公开输出。");
    const row = panel.querySelector<HTMLElement>('.xflow-editor-input__row[data-source="start"]');
    expect(row).not.toBeNull();
    expect(row?.textContent).toContain("start");
    // Source port → target port, and the port the upstream node actually took.
    expect(row?.querySelector(".xflow-editor-input__ports")?.textContent).toBe("main → main");
    expect(row?.textContent).toContain("命中 main");
    const code = row?.querySelector("pre.xflow-editor-run-code") as HTMLElement;
    expect(code.textContent).toContain('"order_id": "SO-1"');
    expect(code.textContent).toContain('"total": 42');

    // Node parameters are never consulted, so their values are nowhere in the
    // drawer — the tab shows upstream outputs, not the evaluated input.
    expect(drawer().textContent).not.toContain("hunter2");
    expect(drawer().textContent).not.toContain("tok-secret");
  });

  it("copies an upstream output", () => {
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    try {
      render(
        <XFlowEditor
          value={inputWorkflow}
          runtime={{ status: "success", nodes: { start: { status: "success", output: { order_id: "SO-1" } } } }}
        />
      );
      selectNode("worker");
      openDrawer();
      openDiagnosticsTab(/输入$/);

      fireEvent.click(within(inputPanel()).getByRole("button", { name: "复制 start 的输出" }));
      expect(writeText).toHaveBeenCalledTimes(1);
      expect(writeText.mock.calls[0][0]).toContain('"order_id": "SO-1"');
    } finally {
      Reflect.deleteProperty(navigator, "clipboard");
    }
  });

  it("marks a private upstream output 已遮蔽 and never falls back to its parameters", () => {
    const privateWorkflow: WorkflowDef = {
      ...inputWorkflow,
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger", output: { private: true } },
        ...(inputWorkflow.nodes ?? []).slice(1)
      ]
    };
    render(
      <XFlowEditor
        value={privateWorkflow}
        runtime={{ status: "success", nodes: { start: { status: "success", output: { secret: "upstream-secret" } } } }}
      />
    );
    selectNode("worker");
    openDrawer();
    openDiagnosticsTab(/输入$/);

    const panel = inputPanel();
    expect(panel.textContent).toContain("输出已遮蔽");
    expect(panel.querySelector("pre")).toBeNull();
    expect(drawer().textContent).not.toContain("upstream-secret");
    expect(drawer().textContent).not.toContain("hunter2");
  });

  it("treats a missing public output on a node that ran as 已遮蔽 (fail-closed)", () => {
    render(<XFlowEditor value={inputWorkflow} runtime={{ status: "success", nodes: { start: { status: "success" } } }} />);
    selectNode("worker");
    openDrawer();
    openDiagnosticsTab(/输入$/);

    expect(inputPanel().textContent).toContain("输出已遮蔽");
    expect(inputPanel().querySelector("pre")).toBeNull();
  });

  it("says an upstream node has not run when it has no runtime state", () => {
    render(<XFlowEditor value={inputWorkflow} />);
    selectNode("worker");
    openDrawer();
    openDiagnosticsTab(/输入$/);

    expect(inputPanel().textContent).toContain("节点尚未运行，暂无输出。");
    expect(inputPanel().querySelector("pre")).toBeNull();
  });

  it("asks for a selection when nothing is selected, and reports a node with no inbound edge", () => {
    const first = render(<XFlowEditor value={inputWorkflow} />);
    fireEvent.keyDown(window, { key: "Escape" });
    openDrawer();
    openDiagnosticsTab(/输入$/);
    expect(inputPanel().textContent).toContain("选择一个节点后查看它的输入。");
    first.unmount();

    // A fresh editor starts on `start`, the graph's entry node: no inbound edge.
    render(<XFlowEditor value={inputWorkflow} />);
    openDrawer();
    openDiagnosticsTab(/输入$/);
    expect(inputPanel().textContent).toContain("该节点没有入边");
  });
});

// ---- WB-06: the 运行日志 tab -------------------------------------------------

describe("运行日志 tab (WB-06)", () => {
  const traceId = "0123456789abcdef0123456789abcdef";

  it("draws a node event timeline ordered by startedAt and says it is not a raw log", () => {
    const execution = {
      executionId: "exec-1",
      status: "failed",
      nodes: [
        {
          name: "start",
          status: "success",
          attempt: 1,
          startedAt: "2026-10-21T12:00:02.000Z",
          finishedAt: "2026-10-21T12:00:03.200Z"
        },
        {
          name: "worker",
          status: "failed",
          attempt: 2,
          error: "boom\nstack line 2",
          startedAt: "2026-10-21T12:00:00.000Z",
          finishedAt: "2026-10-21T12:00:01.000Z"
        }
      ]
    } as unknown as XFlowExecution;
    render(
      <XFlowEditor
        value={workflow}
        execution={execution}
        runtime={{ status: "failed", nodes: { start: { status: "success" }, worker: { status: "failed" } } }}
      />
    );
    openDrawer();
    openDiagnosticsTab(/运行日志$/);

    const panel = logsPanel();
    expect(panel.textContent).toContain("这是节点事件时间线，不是原始运行日志。");
    const rows = Array.from(panel.querySelectorAll<HTMLElement>(".xflow-editor-timeline__row"));
    // startedAt first (worker at :00, start at :02), then graph order for the
    // node that never started (tail).
    expect(rows.map((row) => row.getAttribute("data-node"))).toEqual(["worker", "start", "tail"]);

    const workerRow = rows[0];
    expect(workerRow.textContent).toContain("失败");
    expect(workerRow.textContent).toContain("尝试 2");
    expect(workerRow.textContent).toContain("1.0s");
    // Only the first line is rendered; the full text stays on the title.
    expect(workerRow.querySelector(".xflow-editor-timeline__error")?.textContent).toBe("boom");
    expect(workerRow.textContent).not.toContain("stack line 2");
    expect(workerRow.querySelector(".xflow-editor-timeline__error")?.getAttribute("title")).toBe("boom\nstack line 2");
  });

  it("shows and copies the trace id, and links out only when the host supplies traceHref", () => {
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    try {
      const runtime: RuntimeSnapshot = { status: "running", traceId, nodes: { start: { status: "running" } } };
      const { rerender } = render(<XFlowEditor value={workflow} runtime={runtime} />);
      openDrawer();
      openDiagnosticsTab(/运行日志$/);

      expect(logsPanel().textContent).toContain(traceId);
      // No host URL factory: the id is shown, the external link is not invented.
      expect(within(logsPanel()).queryByRole("link", { name: "在追踪系统打开" })).toBeNull();

      fireEvent.click(within(logsPanel()).getByRole("button", { name: "复制 trace_id" }));
      expect(writeText).toHaveBeenCalledWith(traceId);

      rerender(
        <XFlowEditor
          value={workflow}
          runtime={runtime}
          traceHref={(id) => `https://trace.example/traces/${id}`}
        />
      );
      const link = within(logsPanel()).getByRole("link", { name: "在追踪系统打开" });
      expect(link.getAttribute("href")).toBe(`https://trace.example/traces/${traceId}`);
      expect(link.getAttribute("target")).toBe("_blank");
      expect(link.getAttribute("rel")).toContain("noopener");
    } finally {
      Reflect.deleteProperty(navigator, "clipboard");
    }
  });

  it("reports a missing trace id instead of offering a copy or a link", () => {
    render(
      <XFlowEditor
        value={workflow}
        execution={{ executionId: "exec-1", status: "running" } as XFlowExecution}
        runtime={{ status: "running", nodes: { start: { status: "running" } } }}
        traceHref={(id) => `https://trace.example/${id}`}
      />
    );
    openDrawer();
    openDiagnosticsTab(/运行日志$/);

    expect(logsPanel().textContent).toContain("未记录");
    expect((within(logsPanel()).getByRole("button", { name: "复制 trace_id" }) as HTMLButtonElement).disabled).toBe(true);
    expect(within(logsPanel()).queryByRole("link", { name: "在追踪系统打开" })).toBeNull();
  });

  it("says there is nothing to show before a run, and when a run has no node snapshots", () => {
    const { rerender } = render(<XFlowEditor value={workflow} />);
    openDrawer();
    openDiagnosticsTab(/运行日志$/);
    expect(logsPanel().textContent).toContain("运行工作流后，这里显示节点事件时间线。");

    rerender(
      <XFlowEditor value={workflow} execution={{ executionId: "exec-1", status: "running" } as XFlowExecution} />
    );
    expect(logsPanel().textContent).toContain("没有节点快照，无法生成事件时间线。");
  });
});

// ---- WB-10 / WB-11 / WB-12: the 最近运行 card --------------------------------

const runDd = (label: string) =>
  within(summary()).getByText(label, { selector: "dt" }).nextElementSibling as HTMLElement;

describe("最近运行 card (WB-10 / WB-11 / WB-12)", () => {
  it("shows the execution id, the status, the hit port and a human duration", () => {
    // The host's ExecutionDetail carries per-node started_at / finished_at; the
    // editor's structural XFlowExecution slice does not declare them yet, so the
    // fixture widens it with the same fields the API sends.
    const execution = {
      executionId: "exec-8432",
      status: "failed",
      createdAt: "2026-10-21T12:00:00.000Z",
      updatedAt: "2026-10-21T12:05:00.000Z",
      nodes: [
        {
          name: "start",
          status: "success",
          port: "true",
          startedAt: "2026-10-21T12:00:00.000Z",
          finishedAt: "2026-10-21T12:00:01.200Z"
        }
      ]
    } as unknown as XFlowExecution;
    const { rerender } = render(
      <XFlowEditor
        value={workflow}
        execution={execution}
        runtime={{ status: "failed", nodes: { start: { status: "success", port: "true" } } }}
      />
    );
    openDrawer();

    expect(runDd("执行").textContent).toBe("exec-8432");
    expect(runDd("状态").textContent).toBe("失败");
    expect(runDd("命中分支").textContent).toBe("命中 true");
    // Node stamps win over the execution's created_at / updated_at pair.
    expect(runDd("耗时").textContent).toBe("1.2s");

    // No node stamps: fall back to created_at -> updated_at (2m 3s).
    const fallback = {
      executionId: "exec-8432",
      status: "failed",
      createdAt: "2026-10-21T12:00:00.000Z",
      updatedAt: "2026-10-21T12:02:03.000Z"
    } as XFlowExecution;
    rerender(
      <XFlowEditor
        value={workflow}
        execution={fallback}
        runtime={{ status: "failed", nodes: { start: { status: "success" } } }}
      />
    );
    expect(runDd("耗时").textContent).toBe("2m 3s");
    expect(runDd("命中分支").textContent).toBe("—");
  });

  it("reads 进行中 for a run that has not finished", () => {
    render(
      <XFlowEditor
        value={workflow}
        execution={{ executionId: "exec-1", status: "running" } as XFlowExecution}
        runtime={{ status: "running", nodes: { worker: { status: "running" } } }}
      />
    );
    openDrawer();

    expect(runDd("状态").textContent).toBe("运行中");
    expect(runDd("耗时").textContent).toBe("进行中");
  });
});

describe("empty canvas (§6.5.1)", () => {
  it("waits for nodes instead of reporting a pass, and disables 导入 DSL with 未接入", async () => {
    render(<XFlowEditor value={{ name: "Empty", spec: "1.0", nodes: [], connections: {} }} />);

    expect(strip().textContent).toContain("等待节点");
    expect(strip().textContent).not.toContain("校验通过");
    expect(strip().querySelector("[data-graph-stats]")?.textContent).toBe("0 节点 · 0 连线");

    const importDsl = within(screen.getByRole("region", { name: "画布" })).getByRole("button", { name: "导入 DSL" });
    expect((importDsl as HTMLButtonElement).disabled).toBe(true);
    fireEvent.mouseEnter(importDsl.parentElement as HTMLElement);
    expect((await screen.findAllByText("未接入")).length).toBeGreaterThan(0);
    expect(screen.queryByText("DSL 导入尚未接入。")).toBeNull();
  });
});

// ---- E4b: dock states, ⌘J, focus, 定位 ---------------------------------------

const dockState = () => screen.getByRole("region", { name: "诊断台" }).getAttribute("data-dock");
const pressJ = (init: KeyboardEventInit = {}, target: EventTarget = window) =>
  fireEvent.keyDown(target, { key: "j", code: "KeyJ", metaKey: true, ...init });
const inspected = () => document.querySelector(".xflow-editor-inspector-head__type")?.textContent;
/** C03: the header's primary line names an unlabelled node; the type line no longer repeats it. */
const inspectedName = () => document.querySelector(".xflow-editor-inspector-head__name")?.textContent;
const expandButton = () => within(drawer()).getByRole("button", { name: /^(扩展到画布高度|还原诊断台高度)$/ });
const expandDrawer = () => fireEvent.click(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
const activeTab = () => within(drawer()).getAllByRole("tab").find((tab) => tab.getAttribute("aria-selected") === "true");

/** `worker` answers to the id `node-worker`, so a name-for-id mix-up cannot pass by accident. */
const withIds: WorkflowDef = {
  ...broken,
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { id: "node-worker", name: "worker", type: "xflow.function" },
    { name: "tail", type: "xflow.http" }
  ]
};

describe("dock states (E4b)", () => {
  it("starts collapsed and walks collapsed -> docked -> expanded -> docked -> collapsed", () => {
    render(<XFlowEditor value={workflow} />);
    expect(dockState()).toBe("collapsed");

    openDrawer();
    expect(dockState()).toBe("docked");

    expandDrawer();
    expect(dockState()).toBe("expanded");
    expect(drawer().querySelector(".xflow-editor-drawer")?.getAttribute("data-dock")).toBe("expanded");

    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(dockState()).toBe("docked");
    expect(drawer().querySelector(".xflow-editor-drawer")?.getAttribute("data-dock")).toBe("docked");

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(dockState()).toBe("collapsed");
  });

  it("collapses straight from expanded with the drawer's own button", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(dockState()).toBe("collapsed");
    expect(screen.queryByRole("tab", { name: "运行日志" })).toBeNull();
  });

  it("turns one dock button between two labels and icons, pressed only while expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const docked = expandButton();
    expect(docked.getAttribute("aria-label")).toBe("扩展到画布高度");
    expect(docked.getAttribute("aria-pressed")).toBe("false");
    expect(docked.querySelector('[data-xflow-icon="dock-max"]')).toBeTruthy();

    fireEvent.click(docked);
    const restore = expandButton();
    expect(restore.getAttribute("aria-label")).toBe("还原诊断台高度");
    expect(restore.getAttribute("aria-pressed")).toBe("true");
    expect(restore.querySelector('[data-xflow-icon="dock-restore"]')).toBeTruthy();
    // The collapse button never changes, and the name is not duplicated.
    expect(within(drawer()).getAllByRole("button", { name: "收起诊断台" })).toHaveLength(1);
    expect(
      within(drawer()).getByRole("button", { name: "收起诊断台" }).querySelector('[data-xflow-icon="dock-down"]')
    ).toBeTruthy();
  });

  it("keeps the drawer inside the editor root and the strip in the tab order while expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();

    const root = document.querySelector(".xflow-editor") as HTMLElement;
    expect(root.contains(drawer())).toBe(true);
    expect(strip("诊断台摘要").hasAttribute("inert")).toBe(true);
    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(strip("诊断台摘要").hasAttribute("inert")).toBe(false);
  });

  it("does not refit or re-mount the canvas when the drawer expands and restores", () => {
    render(<XFlowEditor value={workflow} />);
    // The preview's own section and a node card inside it: a remount of the
    // preview or of React Flow would replace both elements.
    const section = document.querySelector(".xflow-editor-preview-frame > *");
    const card = document.querySelector(".xflow-preview-node__select");
    expect(section).not.toBeNull();
    expect(card).not.toBeNull();
    const mounts = preview.mounts;
    preview.handle.fitView.mockClear();

    openDrawer();
    expandDrawer();
    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));

    expect(preview.handle.fitView).not.toHaveBeenCalled();
    expect(preview.handle.fitNode).not.toHaveBeenCalled();
    expect(preview.mounts).toBe(mounts);
    expect(document.querySelector(".xflow-editor-preview-frame > *")).toBe(section);
    expect(document.querySelector(".xflow-preview-node__select")).toBe(card);
  });

  it("takes the covered workbench out of the tab order while expanded, and only then", () => {
    render(<XFlowEditor value={workflow} />);
    const main = document.querySelector(".xflow-editor-main") as HTMLElement;
    expect(main.hasAttribute("inert")).toBe(false);

    openDrawer();
    expect(main.hasAttribute("inert")).toBe(false);
    expandDrawer();
    expect(main.hasAttribute("inert")).toBe(true);
    // The toolbar stays live: the drawer stops short of it.
    expect((document.querySelector(".xflow-editor-toolbar") as HTMLElement).closest("[inert]")).toBeNull();
    expect(drawer().closest("[inert]")).toBeNull();

    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(main.hasAttribute("inert")).toBe(false);
    expandDrawer();
    pressJ();
    expect(main.hasAttribute("inert")).toBe(false);
  });
});

describe("auto-open points (E4b)", () => {
  it("opens a collapsed dock to docked on 校验 and never selects a node for the user", () => {
    render(<XFlowEditor value={broken} />);
    const before = inspected();
    expect(before).toContain("start");

    fireEvent.click(screen.getByRole("button", { name: "校验" }));
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);
  });

  it("does not press an expanded drawer back down when 校验 runs again", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "校验" }));
    expect(dockState()).toBe("expanded");
  });

  it("opens on a failed save and keeps the selection and an expanded drawer as they were", async () => {
    const onSave = vi.fn().mockRejectedValue(new Error("boom"));
    render(<XFlowEditor value={broken} onSave={onSave} />);
    const before = inspected();

    // A clean draft keeps ⌘S a no-op (baseline clean state): dirty the selected
    // node without changing which node/type the Inspector shows.
    const inspectorRoot = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspectorRoot).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "禁用" }));

    fireEvent.keyDown(window, { key: "s", metaKey: true });
    expect(await screen.findAllByText("boom")).not.toHaveLength(0);
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "s", metaKey: true });
    await vi.waitFor(() => expect(onSave).toHaveBeenCalledTimes(2));
    await screen.findAllByText("boom");
    expect(dockState()).toBe("expanded");
  });

  it("opens on a failed run without selecting the failing node, and stays expanded once expanded", async () => {
    const onRun = vi.fn().mockRejectedValue(new Error("run exploded"));
    render(<XFlowEditor value={workflow} onRun={onRun} />);
    const before = inspected();

    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(onRun).toHaveBeenCalledTimes(2));
    expect(dockState()).toBe("expanded");
    expect(inspected()).toBe(before);
  });
});

describe("⌘J / Ctrl+J (E4b)", () => {
  it("toggles collapsed <-> docked with either modifier and prevents the browser default", () => {
    render(<XFlowEditor value={workflow} />);

    expect(pressJ()).toBe(false);
    expect(dockState()).toBe("docked");
    expect(pressJ({ metaKey: false, ctrlKey: true })).toBe(false);
    expect(dockState()).toBe("collapsed");
  });

  it("closes an expanded drawer outright", () => {
    render(<XFlowEditor value={workflow} />);
    pressJ();
    expandDrawer();

    pressJ();
    expect(dockState()).toBe("collapsed");
  });

  it("goes by the physical key, ignores Shift / Alt chords and a bare J", () => {
    render(<XFlowEditor value={workflow} />);

    // A layout that produces another letter for the J key still toggles.
    pressJ({ key: "о", metaKey: false, ctrlKey: true });
    expect(dockState()).toBe("docked");
    // Each chord on its own, so two stray toggles cannot cancel each other out.
    pressJ({ shiftKey: true });
    expect(dockState()).toBe("docked");
    pressJ({ altKey: true });
    expect(dockState()).toBe("docked");
    fireEvent.keyDown(window, { key: "j", code: "KeyJ" });
    expect(dockState()).toBe("docked");
    fireEvent.keyDown(window, { key: "j", code: "KeyK", metaKey: true });
    expect(dockState()).toBe("docked");
  });

  it("fires from inside a text field, like the other global shortcuts", () => {
    render(<XFlowEditor value={workflow} />);
    const field = document.body.appendChild(document.createElement("input"));
    field.focus();

    pressJ({}, field);
    expect(dockState()).toBe("docked");
    field.remove();
  });

  it("is listed in the shortcut reference", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "?" });

    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText("收起 / 展开诊断台")).toBeTruthy();
    expect(within(dialog).getByText("⌘ / Ctrl + J")).toBeTruthy();
  });
});

describe("⇧P summary / expanded toggle (CP-02)", () => {
  const pressShiftP = (init: KeyboardEventInit = {}, target: EventTarget = window) =>
    fireEvent.keyDown(target, { key: "P", code: "KeyP", shiftKey: true, ...init });

  it("walks collapsed -> docked -> expanded -> docked and names the chord on the strip", () => {
    render(<XFlowEditor value={workflow} />);
    expect(strip().getAttribute("title")).toContain("⇧P");

    expect(pressShiftP()).toBe(false);
    expect(dockState()).toBe("docked");
    pressShiftP();
    expect(dockState()).toBe("expanded");
    pressShiftP();
    expect(dockState()).toBe("docked");
    // The open strip keeps the same chord in its own title.
    expect(strip("诊断台摘要").getAttribute("title")).toContain("⇧P");
  });

  it("ignores text fields and events the host page owns, leaving the browser default alone", () => {
    render(<XFlowEditor value={workflow} />);

    const outsideInput = document.body.appendChild(document.createElement("input"));
    outsideInput.focus();
    expect(pressShiftP({}, outsideInput)).toBe(true);
    expect(dockState()).toBe("collapsed");
    outsideInput.remove();

    // A non-text control outside the editor is not the editor's shortcut either.
    const outsideButton = document.body.appendChild(document.createElement("button"));
    outsideButton.focus();
    expect(pressShiftP({}, outsideButton)).toBe(true);
    expect(dockState()).toBe("collapsed");
    outsideButton.remove();

    // A textarea inside the editor is still a text field.
    const insideArea = document.createElement("textarea");
    document.querySelector(".xflow-editor")?.appendChild(insideArea);
    insideArea.focus();
    expect(pressShiftP({}, insideArea)).toBe(true);
    expect(dockState()).toBe("collapsed");
    insideArea.remove();
  });

  it("is listed in the shortcut reference", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "?" });

    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText("切换诊断台摘要 / 展开")).toBeTruthy();
    expect(within(dialog).getByText("⇧ + P")).toBeTruthy();
  });
});

describe("focus hand-over (E4b)", () => {
  it("moves focus to the active tab when the strip opens the drawer", () => {
    render(<XFlowEditor value={workflow} />);
    strip().focus();

    openDrawer();
    expect(document.activeElement).toBe(activeTab());
    expect(activeTab()?.textContent).toMatch(/问题 \d+$/);
  });

  it("moves focus to the active tab when ⌘J opens it", () => {
    render(<XFlowEditor value={workflow} />);

    pressJ();
    expect(document.activeElement).toBe(activeTab());
  });

  it("does not take focus when a failure opens the drawer on its own", () => {
    render(<XFlowEditor value={broken} />);
    const validate = screen.getByRole("button", { name: "校验" });
    validate.focus();

    fireEvent.click(validate);
    expect(dockState()).toBe("docked");
    expect(document.activeElement).toBe(validate);
  });

  it("returns focus to the strip when 收起诊断台 closes a docked drawer", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(document.activeElement).toBe(strip());
  });

  it("returns focus to the strip when ⌘J closes the drawer from inside it, docked or expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expect(drawer().contains(document.activeElement)).toBe(true);

    pressJ();
    expect(document.activeElement).toBe(strip());

    pressJ();
    expandDrawer();
    within(drawer()).getByRole("button", { name: "还原诊断台高度" }).focus();
    pressJ();
    expect(document.activeElement).toBe(strip());
  });

  it("leaves focus where it is when ⌘J closes a drawer that does not hold it", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    const validate = screen.getByRole("button", { name: "校验" });
    validate.focus();

    pressJ();
    expect(dockState()).toBe("collapsed");
    expect(document.activeElement).toBe(validate);
  });

  it("never lets focus fall to the body when the drawer unmounts", () => {
    render(<XFlowEditor value={workflow} />);
    for (const close of [
      () => fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" })),
      () => pressJ(),
      () => {
        expandDrawer();
        fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
      }
    ]) {
      openDrawer();
      close();
      expect(document.activeElement).not.toBe(document.body);
      expect(document.activeElement).toBe(strip());
    }
  });
});

describe("定位节点 (E4b)", () => {
  it("selects the node, pans to it by its id and lands focus on its card in the canvas", () => {
    render(<XFlowEditor value={withIds} />);
    openDrawer();
    expect(inspected()).toContain("start");

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(inspected()).toBe("xflow.function");
    expect(preview.handle.fitNode).toHaveBeenCalledTimes(1);
    // The preview names nodes by id (falling back to name), not by the finding's name.
    expect(preview.handle.fitNode).toHaveBeenCalledWith("node-worker");
    expect(preview.handle.fitView).not.toHaveBeenCalled();
    const card = document.querySelector('.react-flow__node[data-id="node-worker"] .xflow-preview-node__select');
    expect(card).not.toBeNull();
    expect(document.activeElement).toBe(card);
    expect(document.querySelector(".xflow-editor-preview-frame")?.contains(document.activeElement)).toBe(true);
  });

  it("falls back to the node name when the node has no id", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(preview.handle.fitNode).toHaveBeenCalledWith("worker");
  });

  it("hands an expanded drawer back to docked so the canvas it located on is visible", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe("xflow.function");
  });

  it("does not reopen a collapsed inspector", () => {
    render(<XFlowEditor value={broken} />);
    fireEvent.keyDown(window, { key: "b", code: "KeyB", metaKey: true, altKey: true });
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    openDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    expect(preview.handle.fitNode).toHaveBeenCalledWith("worker");
  });

  it("still lands focus on the card when the locate comes from an expanded drawer", () => {
    render(<XFlowEditor value={withIds} />);
    openDrawer();
    expandDrawer();

    // jsdom does not refuse focus on inert elements, so record whether the card
    // sat inside an inert subtree at the moment it was handed focus.
    const inertAtFocus: boolean[] = [];
    const realFocus = HTMLElement.prototype.focus;
    const focusSpy = vi.spyOn(HTMLElement.prototype, "focus").mockImplementation(function (this: HTMLElement, ...args) {
      if (this.classList.contains("xflow-preview-node__select")) inertAtFocus.push(this.closest("[inert]") !== null);
      return realFocus.apply(this, args);
    });
    try {
      fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    } finally {
      focusSpy.mockRestore();
    }
    expect(dockState()).toBe("docked");
    const card = document.querySelector('.react-flow__node[data-id="node-worker"] .xflow-preview-node__select');
    expect(card).not.toBeNull();
    expect(document.activeElement).toBe(card);
    expect(inertAtFocus).toEqual([false]);
  });
});

describe("acting on a finding from an expanded drawer (E4b)", () => {
  const withParams: WorkflowDef = {
    ...workflow,
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "worker", type: "xflow.function", parameters: { a: 1 } },
      { name: "tail", type: "xflow.http" }
    ]
  };
  const workerCard = () =>
    document.querySelector('.xflow-preview-node__select[aria-label^="worker node "]') as HTMLElement;

  it("hands the drawer back to docked for 修正 ... 的参数 JSON", () => {
    render(<XFlowEditor value={withParams} onSave={vi.fn()} />);
    fireEvent.click(workerCard());
    const root = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(root).getByRole("button", { name: "以 JSON 编辑参数" }));
    fireEvent.change(within(root).getByLabelText("参数 JSON"), { target: { value: "{ broken" } });
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    expect(dockState()).toBe("docked");
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "修正 worker 的参数 JSON" }));
    expect(dockState()).toBe("docked");
  });

  it("hands the drawer back to docked for 定位字段", async () => {
    const issue = { node: "worker", path: "/parameters/a", code: "type.number", message: "bad a", severity: "error" as const };
    const onSave = vi.fn(async (next: WorkflowDef) => ({ workflow: next, paramIssues: [issue] }));
    render(<XFlowEditor value={withParams} onSave={onSave} />);
    // A clean draft keeps ⌘S a no-op (baseline clean state): dirty the selected
    // node first, without touching the worker issue path this test follows.
    const inspectorRoot = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspectorRoot).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "禁用" }));
    fireEvent.keyDown(window, { key: "s", metaKey: true });
    await vi.waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    openDrawer();
    const locate = await screen.findByRole("button", { name: "定位字段 worker /parameters/a" });
    expandDrawer();

    fireEvent.click(locate);
    expect(dockState()).toBe("docked");
  });
});

describe("the other auto-open points keep the selection (E4b)", () => {
  it("opens on a save blocked by an unparseable parameters draft, selecting nothing", () => {
    const onSave = vi.fn();
    const withParams: WorkflowDef = {
      ...workflow,
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" },
        { name: "worker", type: "xflow.function", parameters: { a: 1 } },
        { name: "tail", type: "xflow.http" }
      ]
    };
    render(<XFlowEditor value={withParams} onSave={onSave} />);
    fireEvent.click(document.querySelector('.xflow-preview-node__select[aria-label^="worker node "]') as HTMLElement);
    const root = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(root).getByRole("button", { name: "以 JSON 编辑参数" }));
    fireEvent.change(within(root).getByLabelText("参数 JSON"), { target: { value: "{ broken" } });
    const before = inspected();

    expect(dockState()).toBe("collapsed");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    expect(onSave).not.toHaveBeenCalled();
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);
  });

  it("opens after a successful run, selecting nothing, and leaves an expanded drawer expanded", async () => {
    const onRun = vi.fn().mockResolvedValue({ status: "success", nodes: { worker: { status: "success" } } });
    render(<XFlowEditor value={workflow} onRun={onRun} />);
    const before = inspected();

    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(onRun).toHaveBeenCalledTimes(2));
    expect(dockState()).toBe("expanded");
    expect(inspected()).toBe(before);
  });

  it("opens after a partial run, successful or failed, selecting nothing", async () => {
    const onRunNode = vi
      .fn()
      .mockResolvedValueOnce({ status: "success", nodes: { worker: { status: "success" } } })
      .mockRejectedValueOnce(new Error("partial exploded"));
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);
    const before = inspected();
    const node = (name: string) =>
      document.querySelector(`.xflow-preview-node__select[aria-label^="${name} node "]`) as HTMLElement;

    fireEvent.contextMenu(node("tail"), { clientX: 420, clientY: 260 });
    fireEvent.click(screen.getByRole("menuitem", { name: /从此节点运行/ }));
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    // Right-click selects nothing by itself in this editor; the ruling is that the dock does not either.
    const afterSuccess = inspected();
    expect(afterSuccess === before || inspectedName() === "tail").toBe(true);

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    fireEvent.contextMenu(node("tail"), { clientX: 420, clientY: 260 });
    fireEvent.click(screen.getByRole("menuitem", { name: /从此节点运行/ }));
    await vi.waitFor(() => expect(onRunNode).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(afterSuccess);
  });
});

describe("⌘J guards and ⌘K locate (E4b)", () => {
  it("ignores a held chord but still swallows the browser default", () => {
    render(<XFlowEditor value={workflow} />);

    expect(pressJ({ repeat: true })).toBe(false);
    expect(dockState()).toBe("collapsed");
    expect(pressJ()).toBe(false);
    expect(dockState()).toBe("docked");
    expect(pressJ({ repeat: true })).toBe(false);
    expect(dockState()).toBe("docked");
  });

  it("does not toggle the dock behind an open shortcut reference", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "?" });
    expect(screen.getByRole("dialog")).toBeTruthy();

    pressJ();
    expect(dockState()).toBe("collapsed");
  });

  it("steps an expanded drawer down to docked when ⌘K locates a node", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();
    expect(dockState()).toBe("expanded");

    fireEvent.keyDown(window, { key: "k", code: "KeyK", metaKey: true });
    const search = await screen.findByRole("combobox", { name: "定位节点" });
    fireEvent.change(search, { target: { value: "tail" } });
    fireEvent.keyDown(search, { key: "Enter", code: "Enter" });

    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    // C03: an unlabelled node's name is the header's primary line only.
    expect(document.querySelector(".xflow-editor-inspector-head__name")?.textContent).toBe("tail");
  });
});
