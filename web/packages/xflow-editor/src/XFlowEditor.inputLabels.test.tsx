import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import { NodeFormCompiler, createNodeFormRegistry } from "./node-form";
import { Inspector } from "./panels/inspector";

const workflow: WorkflowDef = {
  id: "wf-labels",
  name: "Merge labels",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "stock", type: "xflow.function", position: { x: 240, y: 0 } },
    { name: "price", type: "xflow.function", position: { x: 240, y: 160 } },
    { name: "join", type: "xflow.merge", position: { x: 480, y: 80 } },
    { name: "after", type: "xflow.function", position: { x: 720, y: 80 } }
  ],
  connections: {
    start: { main: [{ node: "stock" }, { node: "price" }] },
    stock: { main: [{ node: "join" }] },
    price: { main: [{ node: "join", input: "total" }] },
    join: { main: [{ node: "after" }] }
  }
};

const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
const selectNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  fireEvent.click(node);
  return inspector();
};
const openConnections = (name: string): HTMLElement => {
  selectNode(name);
  fireEvent.click(within(inspector()).getByRole("tab", { name: "连接" }));
  return inspector();
};
const labelField = (source: string): HTMLInputElement =>
  within(inspector()).getByRole("textbox", { name: `输入标签 ${source} main 到 join` }) as HTMLInputElement;
const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;

/** One change event per character, the way a keyboard grows a controlled input. */
function typeInto(element: HTMLElement, text: string): void {
  fireEvent.focus(element);
  let typed = "";
  for (const char of text) {
    typed += char;
    fireEvent.change(element, { target: { value: typed } });
  }
}

describe("merge input labels in the 连接 tab", () => {
  it("offers a label field on every edge into a merge, pre-filled with its current label", () => {
    render(<XFlowEditor value={workflow} />);
    openConnections("join");

    expect(labelField("stock").value).toBe("main");
    const price = within(inspector()).getByRole("textbox", { name: "输入标签 price main 到 join" }) as HTMLInputElement;
    expect(price.value).toBe("total");
  });

  it("gives no label field to an ordinary node", () => {
    render(<XFlowEditor value={workflow} />);
    openConnections("after");

    expect(within(inspector()).queryByRole("textbox", { name: /^输入标签/ })).toBeNull();
    // The incoming edge is still listed, as plain text under its port.
    const incoming = within(inspector()).getByRole("group", { name: "输入端口" });
    expect(within(incoming).getByRole("group", { name: "输入端口 main" }).textContent).toContain("join");
  });

  it("gives no label field to a merge that declares its own ports", () => {
    const declared: WorkflowDef = {
      ...workflow,
      nodes: workflow.nodes?.map((node) =>
        node.name === "join" ? { ...node, inputs: [{ name: "main" }, { name: "total" }] } : node
      )
    };
    render(<XFlowEditor value={declared} />);
    openConnections("join");

    expect(within(inspector()).queryByRole("textbox", { name: /^输入标签/ })).toBeNull();
  });

  it("writes nothing while typing, and commits the finished label on Enter", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("join");

    const field = labelField("stock");
    typeInto(field, "inventory");

    // Half-typed text lives in the field only: no model write, no undo entry.
    expect(handleChange).not.toHaveBeenCalled();
    expect(field.value).toBe("inventory");

    fireEvent.keyDown(field, { key: "Enter" });

    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(lastWorkflow(handleChange).connections?.stock).toEqual({ main: [{ node: "join", input: "inventory" }] });
    expect(lastWorkflow(handleChange).connections?.price).toEqual({ main: [{ node: "join", input: "total" }] });
  });

  it("commits on blur too, as one undoable change", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("join");

    const field = labelField("stock");
    typeInto(field, "inventory");
    fireEvent.blur(field);

    expect(handleChange).toHaveBeenCalledTimes(1);
    fireEvent.click(
      within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" })
    );
    expect(lastWorkflow(handleChange).connections?.stock).toEqual({ main: [{ node: "join" }] });
  });

  it("moves the edge to its new port group and keeps keyboard focus on the field", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    openConnections("join");

    const field = labelField("stock");
    typeInto(field, "inventory");
    fireEvent.keyDown(field, { key: "Enter" });

    const incoming = within(inspector()).getByRole("group", { name: "输入端口" });
    expect(within(incoming).getByRole("group", { name: "输入端口 inventory" })).toBeTruthy();
    expect(within(incoming).queryByRole("group", { name: "输入端口 main" })).toBeNull();
    expect(document.activeElement).toBe(labelField("stock"));
    expect(labelField("stock").value).toBe("inventory");
  });

  it("refuses a malformed label with a message, keeps the draft, and writes nothing", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("join");

    const field = labelField("stock");
    typeInto(field, "1 bad");
    fireEvent.keyDown(field, { key: "Enter" });

    expect(handleChange).not.toHaveBeenCalled();
    const alert = within(inspector()).getByRole("alert");
    expect(alert.textContent).toContain("以字母开头");
    expect(field.getAttribute("aria-invalid")).toBe("true");
    expect(field.getAttribute("aria-describedby")).toBe(alert.id);
    expect(field.value).toBe("1 bad");

    // Typing again drops the stale refusal.
    typeInto(field, "inventory");
    expect(within(inspector()).queryByRole("alert")).toBeNull();
    expect(field.getAttribute("aria-invalid")).toBeNull();
  });

  it("refuses an empty label", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("join");

    const field = labelField("stock");
    fireEvent.change(field, { target: { value: "" } });
    fireEvent.keyDown(field, { key: "Enter" });

    expect(handleChange).not.toHaveBeenCalled();
    expect(within(inspector()).getByRole("alert").textContent).toContain("请输入标签");
  });

  it("Escape restores the committed label without closing anything or writing", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("join");

    const field = labelField("stock");
    typeInto(field, "inven");
    fireEvent.keyDown(field, { key: "Escape" });

    expect(field.value).toBe("main");
    expect(handleChange).not.toHaveBeenCalled();
    // The selection survives: the inspector still shows the merge.
    expect(within(inspector()).getByRole("textbox", { name: "输入标签 price main 到 join" })).toBeTruthy();
  });

  it("does not let the shortcut sheet swallow a ? typed into the field", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    openConnections("join");

    const field = labelField("stock");
    field.focus();
    const notPrevented = fireEvent.keyDown(field, { key: "?" });

    expect(notPrevented).toBe(true);
    expect(screen.queryByRole("dialog", { name: /快捷键/ })).toBeNull();
  });

  it("leaves the field out of a read-only editor", () => {
    const registry = createNodeFormRegistry();
    render(
      <Inspector
        editable={false}
        formHost={{ registry, compiler: new NodeFormCompiler(registry), renameRef: { current: undefined } }}
        parameterTab="form"
        selectedIndex={3}
        selectedNode={workflow.nodes?.[3]}
        workflow={workflow}
      />
    );
    fireEvent.click(within(inspector()).getByRole("tab", { name: "连接" }));

    expect(within(inspector()).queryByRole("textbox", { name: /^输入标签/ })).toBeNull();
    // The labels are still readable as text.
    const incoming = within(inspector()).getByRole("group", { name: "输入端口" });
    expect(within(incoming).getByRole("group", { name: "输入端口 total" })).toBeTruthy();
  });
});
