// C4 (Doc C §6.3): the node library is derived from GET /v1/node-types when
// the host passes `nodeTypes`, falls back to the offline list without it, and
// adding a node (click or drop) never writes schema defaults (Doc C §5.2).

import * as React from "react";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, WorkflowDef, WorkflowNode } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";
import { XFlowEditor } from "./index";
import { translateNodeFormText } from "./node-form/i18n";
import { zhCN } from "./node-form/locales/zh-CN";
import type { NodeFormSchema } from "./node-form/schema";
import generated from "./node-form/testdata/node-types.generated.json";

// Pass-through wrapper so a test can reach the canvas's drop callback: React
// Flow never initialises in jsdom, so a real DOM drop cannot resolve a position.
const preview = vi.hoisted(() => ({ props: undefined as undefined | Record<string, unknown> }));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    preview.props = props as unknown as Record<string, unknown>;
    return React.createElement(actual.XFlowPreview, props);
  };
  return { ...actual, XFlowPreview: Wrapped };
});

const nodeTypes = generated as unknown as NodeTypesResponse;
const builtinTypes = [...new Set(nodeTypes.node_types.map((schema) => schema.node_type))];
const supplyTypes = ["xflow.supply.external", "xflow.supply.static"];
/** The library shows display names through the editor's zh-CN catalog. */
const shownLabel = (item: NodeFormSchema) => translateNodeFormText(zhCN, item.display_name ?? item.node_type, item.node_type);

const workflow: WorkflowDef = {
  name: "library",
  spec: "1.0",
  nodes: [{ name: "start", type: "xflow.start", position: { x: 0, y: 0 } }],
  connections: {}
};

const library = () => screen.getByRole("region", { name: "节点" });
const tiles = () => Array.from(library().querySelectorAll<HTMLButtonElement>("button[data-node-type]"));
const tile = (type: string) => {
  const found = library().querySelector<HTMLButtonElement>(`button[data-node-type="${type}"]`);
  if (!found) throw new Error(`no library tile for ${type}`);
  return found;
};
const groupOf = (element: HTMLElement) =>
  element.closest(".xflow-editor-node-group")?.querySelector(".xflow-editor-block-title")?.textContent;
const iconOf = (element: HTMLElement) => element.querySelector(".xflow-editor-node-tile__icon .anticon")?.getAttribute("aria-label");
const lastNode = (spy: ReturnType<typeof vi.fn>) => (spy.mock.calls.at(-1)?.[0] as WorkflowDef).nodes?.at(-1) as WorkflowNode;

function schema(overrides: Partial<NodeFormSchema> & Pick<NodeFormSchema, "node_type">): NodeFormSchema {
  return { spec: "node-form/v1", node_version: 1, kind: "action", fields: [], ...overrides };
}

describe("node library from /v1/node-types (Doc C §6.3)", () => {
  it("renders one tile per registered type, labelled by display_name, plus engine-declared supplies", () => {
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} />);

    expect(tiles().map((element) => element.dataset.nodeType).sort()).toEqual([...builtinTypes, ...supplyTypes].sort());
    for (const item of nodeTypes.node_types) {
      expect(tile(item.node_type).getAttribute("aria-label")).toBe(shownLabel(item));
    }
    expect(tile("xflow.approval").getAttribute("aria-label")).toBe("审批");
    // Existing presentation is kept for known types...
    expect(groupOf(tile("xflow.http"))).toBe("动作与人工");
    expect(iconOf(tile("xflow.http"))).toBe("global");
    expect(groupOf(tile("xflow.trigger.cron"))).toBe("触发器");
    expect(groupOf(tile("xflow.transform.set"))).toBe("数据转换");
    expect(groupOf(tile("xflow.supply.static"))).toBe("供应");
    // ...and every current builtin has an entry, so nothing lands in "其他".
    expect(within(library()).queryByRole("button", { name: /^其他/ })).toBeNull();
    expect(within(library()).getByRole("button", { name: /触发器 5/ })).toBeTruthy();
  });

  it("shows a type the frontend has never heard of with the generic icon, grouped by kind", () => {
    const withNew: NodeTypesResponse = {
      ...nodeTypes,
      node_types: [
        ...nodeTypes.node_types,
        schema({ node_type: "xflow.acme.fax", display_name: "Fax (old)" }),
        schema({ node_type: "xflow.acme.fax", node_version: 2, display_name: "Fax" }),
        schema({ node_type: "xflow.trigger.acme", kind: "trigger", display_name: "Acme Trigger" }),
        schema({ node_type: "xflow.acme.unnamed" })
      ]
    };
    render(<XFlowEditor nodeTypes={withNew} value={workflow} />);

    const fax = tile("xflow.acme.fax");
    expect(tiles().filter((element) => element.dataset.nodeType === "xflow.acme.fax")).toHaveLength(1);
    expect(fax.getAttribute("aria-label")).toBe("Fax"); // latest version wins
    expect(groupOf(fax)).toBe("其他");
    expect(iconOf(fax)).toBe("appstore");
    expect(groupOf(tile("xflow.trigger.acme"))).toBe("触发器");
    expect(iconOf(tile("xflow.trigger.acme"))).toBe("appstore");
    expect(tile("xflow.acme.unnamed").getAttribute("aria-label")).toBe("xflow.acme.unnamed");
    expect(within(library()).getByRole("button", { name: /其他 2/ })).toBeTruthy();
  });

  it("falls back to the offline list when the host provides no nodeTypes", () => {
    render(<XFlowEditor value={workflow} />);

    const types = tiles().map((element) => element.dataset.nodeType);
    expect(types).toContain("xflow.http");
    expect(types).toContain("xflow.supply.static");
    expect(types).not.toContain("xflow.map"); // schema-only builtin
    expect(tile("xflow.http").getAttribute("aria-label")).toBe("HTTP");
    expect(within(library()).getByRole("button", { name: /触发器 4/ })).toBeTruthy();
  });

  it("adds every library type with identity and placement only: no parameters, no defaults", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} onChange={handleChange} />);

    for (const type of [...builtinTypes, ...supplyTypes]) {
      const before = handleChange.mock.calls.length;
      fireEvent.click(tile(type));
      // Exactly one commit: selecting the new node mounts its form, which must
      // not write anything back (no defaults, no empty values).
      expect(handleChange).toHaveBeenCalledTimes(before + 1);
      const node = lastNode(handleChange);
      expect(Object.keys(node).sort()).toEqual(["kind", "name", "position", "type", "ui"]);
      expect(node.type).toBe(type);
      const described = nodeTypes.node_types.find((item) => item.node_type === type);
      expect(node.kind).toBe(described?.kind ?? "supply");
      if (described) expect(node.ui).toEqual({ label: shownLabel(described) });
    }
  }, 60_000);

  it("drags a schema-only type onto the canvas and ignores types outside the library", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor nodeTypes={nodeTypes} value={workflow} onChange={handleChange} />);

    const setData = vi.fn();
    fireEvent.dragStart(tile("xflow.map"), { dataTransfer: { setData, effectAllowed: "" } });
    expect(setData).toHaveBeenCalledWith("application/xflow-node-type", "xflow.map");

    const onDropNode = preview.props?.onDropNode as (type: string, position: { x: number; y: number }) => void;
    React.act(() => onDropNode("xflow.map", { x: 320, y: 180 }));
    expect(lastNode(handleChange)).toEqual({
      name: "map_1",
      type: "xflow.map",
      kind: "action",
      position: { x: 320, y: 180 },
      ui: { label: "遍历" }
    });

    const calls = handleChange.mock.calls.length;
    React.act(() => onDropNode("custom.not_in_library", { x: 0, y: 0 }));
    expect(handleChange).toHaveBeenCalledTimes(calls);
  });

  it("keeps a workflow node of an unregistered type visible and editable as a no-schema node", () => {
    const custom: WorkflowDef = {
      ...workflow,
      nodes: [...(workflow.nodes ?? []), { name: "fax", type: "custom.runner.fax", parameters: { to: "+1" } }]
    };
    render(<XFlowEditor nodeTypes={nodeTypes} value={custom} />);

    fireEvent.click(screen.getByRole("button", { name: "选择节点 fax" }));
    const inspector = screen.getByRole("region", { name: "属性" });
    expect(within(inspector).getByText("该节点类型未在 server 注册，参数按 JSON 编辑")).toBeTruthy();
    expect(within(inspector).getByRole("tab", { name: "JSON" })).toBeTruthy();
    // Diagnosed as a warning, not an error: the type may be registered on a runner.
    const toggle = screen.queryByRole("button", { name: "展开诊断台" });
    if (toggle) fireEvent.click(toggle);
    const line = screen.getByText("节点类型未在 server 注册，参数按 JSON 编辑: fax:custom.runner.fax").closest("p");
    expect(line?.textContent).toMatch(/^warn/);
  });
});
