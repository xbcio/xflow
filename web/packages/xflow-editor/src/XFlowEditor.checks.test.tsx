import * as React from "react";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";
import type { NodeTypesResponse, WorkflowDef, WorkflowNode } from "@xflow/core";
import { XFlowEditor } from "./index";
import generated from "./node-form/testdata/node-types.generated.json";

// B1-B: the two lightweight pre-checks (WB-07 templates, WB-08 `$nodes`
// references) and the CP-01 visibility they feed -- the card badge and the
// required-field error inside the node form. The checks run beside
// `validateWorkflow`, so their pass rows land in the same "N 项检查通过" line.

beforeEach(() => {
  // Panel collapse is persisted per workflow id; a collapsed inspector from an
  // earlier case must not hide the form here.
  window.localStorage?.clear();
});

const strip = () => screen.getByRole("button", { name: "展开诊断台" });
const openDrawer = () => fireEvent.click(strip());
const drawer = () => screen.getByRole("region", { name: "诊断台" });
const rowOf = (text: string | RegExp): HTMLElement => {
  const row = within(drawer()).getByText(text).closest("li");
  if (!row) throw new Error("issue row not found");
  return row;
};
const expandPasses = (): void => {
  const line = screen.getByText(/^\d+ 项检查通过：/);
  fireEvent.click(within(line.closest("li") as HTMLElement).getByRole("button", { name: "展开通过项" }));
};

const graph = (nodes: WorkflowNode[], templates?: WorkflowDef["node_templates"]): WorkflowDef => ({
  name: "checks",
  ...(templates ? { node_templates: templates } : {}),
  nodes,
  connections: {}
});

describe("template references (WB-07)", () => {
  it("reports a node whose template is not declared, on that node and at the templates area", () => {
    render(<XFlowEditor value={graph([{ name: "api", template: "ghost_tpl" }])} />);
    expect(strip().textContent).toContain("1 错误");

    openDrawer();
    const row = rowOf("节点 api 引用了不存在的节点模板 ghost_tpl");
    expect(row.dataset.tone).toBe("danger");
    expect(row.dataset.area).toBe("templates");
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("api");
    expect(within(row).getByRole("button", { name: "定位节点 api" })).toBeTruthy();
  });

  it("rejects a node that carries both type and template, as the compiler does", () => {
    render(
      <XFlowEditor
        value={graph([{ name: "api", type: "xflow.http", template: "tpl" }], { tpl: { type: "xflow.http" } })}
      />
    );

    openDrawer();
    const row = rowOf(/节点 api 同时声明了 type 与 template/);
    expect(row.dataset.tone).toBe("danger");
    expect(row.dataset.area).toBe("templates");
    // One row per node: the conflict already explains why the template is not
    // resolved, so no second "unknown template" row is added.
    expect(drawer().querySelectorAll('.xflow-editor-issue[data-area="templates"]')).toHaveLength(1);
  });

  it("adds a pass row when every templated node resolves, and one when none are used", () => {
    const { unmount } = render(
      <XFlowEditor value={graph([{ name: "api", template: "tpl" }], { tpl: { type: "xflow.http" } })} />
    );
    openDrawer();
    expect(screen.getByText(/^\d+ 项检查通过：/).textContent).toContain("模板");
    expandPasses();
    expect(screen.getByText("1 个节点的模板引用已解析")).toBeTruthy();
    unmount();

    render(<XFlowEditor value={graph([{ name: "api", type: "xflow.http" }])} />);
    openDrawer();
    expandPasses();
    expect(screen.getByText("没有节点引用模板")).toBeTruthy();
  });
});

describe("$nodes references (WB-08)", () => {
  const refGraph = (parameters: Record<string, unknown>, extraNodes: WorkflowNode[] = []): WorkflowDef => ({
    name: "refs",
    // Declared so the credentials warning never muddies a warning-count assertion.
    credentials: { api_token: { type: "bearer" } },
    nodes: [{ name: "start", type: "xflow.start" }, { name: "api", type: "xflow.http", parameters }, ...extraNodes],
    connections: { start: { main: [{ node: "api" }] } }
  });

  it("reports an unknown quoted reference with the node and the field pointer", () => {
    render(<XFlowEditor value={refGraph({ url: "${{ $nodes['ghost'].url }}" })} />);
    expect(strip().textContent).toContain("1 错误");

    openDrawer();
    const row = rowOf("节点 api 引用了不存在的节点 $nodes['ghost']");
    expect(row.dataset.tone).toBe("danger");
    expect(row.dataset.area).toBe("variables");
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("api");
    expect(within(row).getByRole("button", { name: "定位字段 api /parameters/url" })).toBeTruthy();
  });

  it("reads the dot form the expression evaluator accepts", () => {
    render(<XFlowEditor value={refGraph({ url: "${{ $nodes.ghost.url }}" })} />);
    openDrawer();
    expect(rowOf("节点 api 引用了不存在的节点 $nodes['ghost']").dataset.area).toBe("variables");
  });

  it("adds a pass row only when references exist and all of them resolve", () => {
    const { unmount } = render(<XFlowEditor value={refGraph({ url: "${{ $nodes['start'].out }}" })} />);
    expect(strip().textContent).not.toContain("错误");
    openDrawer();
    expect(screen.getByText(/^\d+ 项检查通过：/).textContent).toContain("变量引用");
    expandPasses();
    expect(screen.getByText("1 处 $nodes 引用均指向已存在的节点")).toBeTruthy();
    unmount();

    // No reference at all: the area has nothing to pass, so it is not listed.
    render(<XFlowEditor value={refGraph({ url: "https://example.com" })} />);
    openDrawer();
    expect(screen.getByText(/^\d+ 项检查通过：/).textContent).not.toContain("变量引用");
  });

  it("warns instead of guessing for a computed subscript, and never claims a pass", () => {
    render(<XFlowEditor value={refGraph({ url: "${{ $nodes[$vars.which].url }}" })} />);
    expect(strip().textContent).toContain("警告");
    expect(strip().textContent).not.toContain("错误");

    openDrawer();
    const row = rowOf("节点 api 的 $nodes 下标不是字面量，无法预检引用是否存在");
    expect(row.dataset.tone).toBe("warning");
    expect(row.dataset.area).toBe("variables");
    expect(screen.getByText(/^\d+ 项检查通过：/).textContent).not.toContain("变量引用");
  });

  it("skips a sub-graph body, whose references resolve against the inner graph", () => {
    const body = {
      type: "xflow.subgraph",
      nodes: [{ name: "inner", type: "xflow.http", parameters: { url: "${{ $nodes['inner_ghost'] }}" } }]
    };
    render(<XFlowEditor value={refGraph({ body })} />);
    expect(strip().textContent).not.toContain("错误");
    expect(strip().textContent).not.toContain("警告");
  });

  it("still scans an xflow.http request body, which is a payload, not a sub-graph", () => {
    render(<XFlowEditor value={refGraph({ body: { url: "${{ $nodes['ghost'] }}" } })} />);
    expect(strip().textContent).toContain("1 错误");
  });
});

describe("required-field errors in the node form (CP-01)", () => {
  const formTypes = generated as unknown as NodeTypesResponse;
  const formWorkflow: WorkflowDef = {
    name: "form",
    nodes: [{ name: "api", type: "xflow.http", parameters: { method: "GET" } }],
    connections: {}
  };
  const selectApi = (): void => {
    const card = document.querySelector<HTMLElement>('.xflow-preview-node__select[aria-label^="api 节点 "]');
    if (!card) throw new Error("api card not found");
    fireEvent.click(card);
  };
  const urlField = (): HTMLElement => {
    const field = document.querySelector<HTMLElement>('[data-composer-path="/parameters/url"]');
    if (!field) throw new Error("url field not found");
    return field;
  };

  it("draws the required marker, a danger error line and aria-invalid/aria-describedby on the control", () => {
    render(<XFlowEditor nodeTypes={formTypes} value={formWorkflow} />);
    selectApi();

    const field = urlField();
    expect(field.querySelector(".xflow-composer-field-required")?.textContent).toBe("*");
    const issue = field.querySelector(".xflow-composer-issue--error");
    expect(issue?.textContent).toContain("必填");
    expect(field.classList.contains("is-error")).toBe(true);

    const control = field.querySelector("input");
    expect(control).toBeTruthy();
    expect(control?.getAttribute("aria-invalid")).toBe("true");
    const described = (control?.getAttribute("aria-describedby") ?? "")
      .split(" ")
      .map((id) => document.getElementById(id)?.textContent ?? "");
    expect(described.some((text) => text.includes("必填"))).toBe(true);
  });

  it("shows the local finding as a count badge on the canvas card", () => {
    render(<XFlowEditor nodeTypes={formTypes} value={formWorkflow} />);
    const card = document.querySelector<HTMLElement>('[data-node-id="api"]');
    const badge = card?.querySelector<HTMLElement>(".xflow-preview-node__issues");
    expect(badge?.getAttribute("aria-label")).toBe("1 个参数错误");
    expect(badge?.textContent).toBe("1");
    expect(badge?.getAttribute("title")).toBe("必填");
    expect(badge?.dataset.tone).toBe("danger");
  });
});
