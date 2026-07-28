import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { XFlowEditor } from "./index";

describe("XFlowEditor", () => {
  const workflow = {
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
    expect(screen.getByRole("region", { name: "属性配置" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "诊断台" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "问题" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "运行日志" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "输入" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "输出" })).toBeTruthy();
    expect(screen.getByRole("region", { name: "Purchase approval preview" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起左侧面板" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起属性面板" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "收起诊断台" })).toBeTruthy();
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
    expect(screen.queryByRole("region", { name: "属性配置" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
    expect(screen.getByRole("region", { name: "属性配置" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "收起诊断台" }));
    expect(screen.queryByText("DSL 摘要")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    expect(screen.getByText("DSL 摘要")).toBeTruthy();
  });

  it("selects outline nodes and edits basic node configuration", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));

    const inspector = screen.getByRole("region", { name: "属性配置" });
    expect(within(inspector).getByText("属性配置")).toBeTruthy();
    expect(within(inspector).getAllByText(/route_by_amount/).length).toBeGreaterThan(0);
    expect(within(inspector).queryByText("running")).toBeNull();
    expect(within(inspector).getByDisplayValue("xflow.switch")).toBeTruthy();
    expect(within(inspector).getByLabelText("节点名称").className).toContain("ant-input-sm");
    expect(within(inspector).getByRole("tab", { name: "配置" }).getAttribute("aria-selected")).toBe("true");
    expect(within(inspector).getByRole("tab", { name: "连接" })).toBeTruthy();
    expect(within(inspector).getByRole("tab", { name: "运行" })).toBeTruthy();
    expect(within(inspector).getByDisplayValue("Route purchase requests by amount")).toBeTruthy();

    fireEvent.change(within(inspector).getByLabelText("节点名称"), {
      target: { value: "amount_router" }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        workflow.nodes[0],
        {
          ...workflow.nodes[1],
          name: "amount_router"
        },
        workflow.nodes[2]
      ],
      connections: {
        start: {
          main: [{ node: "amount_router" }]
        },
        amount_router: {
          level_1: [{ node: "l1_manager" }]
        }
      }
    });
  });

  it("edits workflow metadata from the property panel", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = screen.getByRole("region", { name: "属性配置" });
    fireEvent.change(within(inspector).getByLabelText("工作流名称"), {
      target: { value: "purchase-approval-v2" }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      name: "purchase-approval-v2"
    });
  });

  it("edits workflow-level DSL fields from the property panel", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const inspector = screen.getByRole("region", { name: "属性配置" });
    fireEvent.change(within(inspector).getByLabelText("工作流 Runner 选择 JSON"), {
      target: {
        value: '{ "mode": "required", "matchLabels": { "env": "prod", "region": "cn" } }'
      }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      runnerSelector: {
        mode: "required",
        matchLabels: {
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
      runnerSelector: {
        mode: "required",
        matchLabels: {
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
    const inspector = screen.getByRole("region", { name: "属性配置" });
    fireEvent.change(within(inspector).getByLabelText("节点 Runner 选择 JSON"), {
      target: {
        value: '{ "mode": "default", "matchLabels": { "mode": "local", "env": "prod" } }'
      }
    });

    expect(handleChange).toHaveBeenCalledWith({
      ...workflow,
      nodes: [
        workflow.nodes[0],
        workflow.nodes[1],
        {
          ...workflow.nodes[2],
          runnerSelector: {
            mode: "default",
            matchLabels: {
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
    const inspector = screen.getByRole("region", { name: "属性配置" });
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

  it("adds trigger nodes using DSL-compatible node identifiers", () => {
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
      connections: {
        ...workflow.connections,
        route_by_amount: {
          ...workflow.connections.route_by_amount,
          main: [{ node: "trigger_webhook_1" }]
        }
      }
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

  it("adds a main connection from the selected node in the connections tab", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性配置" });
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

  it("removes a selected connection from the connections tab", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 route_by_amount" }));
    const inspector = screen.getByRole("region", { name: "属性配置" });
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

  it("deletes the selected node and removes dangling connections", () => {
    const handleChange = vi.fn();

    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 l1_manager" }));
    fireEvent.click(within(screen.getByRole("region", { name: "属性配置" })).getByRole("button", { name: "删除节点" }));

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
      id: "wf-draft",
      name: "untitled-workflow",
      version: "1.0.0",
      description: "New workflow draft",
      nodes: [
        {
          name: "start",
          type: "xflow.start",
          kind: "trigger",
          position: { x: 120, y: 160 },
          ui: { label: "开始" }
        }
      ],
      connections: {}
    });
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
