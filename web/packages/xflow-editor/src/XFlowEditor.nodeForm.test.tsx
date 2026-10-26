// C3 (Doc C §7): the Inspector's node form inside the full XFlowEditor.
// Pins the reducer contract (one composer batch = one commit = one undo
// entry), enum invalidation, rename, validation modes, backend param_issues,
// the no-schema form, the JSON tab debounce and drafts across real unmounts.

import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, ParamIssue, WorkflowDef, WorkflowNode } from "@xflow/core";
import { XFlowEditor, type XFlowEditorSaveResult } from "./index";
import type { NodeFormField } from "./node-form/schema";
import generated from "./node-form/testdata/node-types.generated.json";
import { change, fieldOf, one, pickOption } from "./node-form/testdata/testkit";

/** The canvas node's own select button: the canvas cards stay a selectable list beside the outline tree. */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const nodeTypes = generated as unknown as NodeTypesResponse;
const enforce: NodeTypesResponse = { ...nodeTypes, param_validation_mode: "enforce" };

const serialize = (value: unknown) => JSON.stringify(value);
const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
const selectNode = (name: string) => {
  fireEvent.click(canvasNode(name));
  return inspector();
};
const undoButton = () => within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" });
/** The parameters JSON lives behind the 节点参数 card's </> toggle. */
const jsonSourceButton = (root: HTMLElement = inspector()) =>
  within(root).getByRole("button", { name: "以 JSON 编辑参数" });
const jsonTab = (root: HTMLElement = inspector()) => {
  if (jsonSourceButton(root).getAttribute("aria-pressed") !== "true") fireEvent.click(jsonSourceButton(root));
  return within(root).getByLabelText("参数 JSON") as HTMLTextAreaElement;
};
/** Leaves the JSON source view, where it is open. */
const formTab = (root: HTMLElement = inspector()) => {
  if (jsonSourceButton(root).getAttribute("aria-pressed") !== "true") return;
  fireEvent.click(within(root).getByRole("button", { name: "返回表单" }));
};
/** Lets composer's microtask batch and React effects run. */
const settle = () => act(async () => {
  await Promise.resolve();
  await Promise.resolve();
});
const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;

let consoleError: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  consoleError = vi.spyOn(console, "error");
  // Panel collapse is persisted per workflow id (§6.0); a case that ends with
  // the inspector collapsed must not hand that state to the next case.
  window.localStorage?.clear();
});
afterEach(() => consoleError.mockRestore());

// ------------------------------------------------ (a) mount = zero writes

function fieldType(field: NodeFormField): string {
  return (field.type as string) === "bool" ? "boolean" : field.type;
}

function wrongValue(field: NodeFormField): unknown {
  switch (fieldType(field)) {
    case "string":
      return 42;
    case "number":
      return "not-a-number";
    case "boolean":
      return "yes";
    case "array":
      return { not: "a list" };
    default:
      return [1, 2];
  }
}

function plausibleValue(field: NodeFormField): unknown {
  const option = field.options?.[0]?.value ?? field.options_when?.[0]?.options[0]?.value;
  switch (fieldType(field)) {
    case "string":
      return option ?? (field.widget === "duration" ? "30s" : "text");
    case "number":
      return option ?? 2;
    case "boolean":
      return true;
    case "array":
      return field.item?.fields?.length ? [{}] : ["a"];
    default:
      return field.widget === "key-value" || field.widget === "key-expression" ? { k: "v" } : { any: 1 };
  }
}

type Family = [string, (field: NodeFormField) => unknown, Record<string, unknown>];
const families: Family[] = [
  ["empty", () => undefined, {}],
  ["template-heavy", (field) => `\${{ $vars.${field.name} }}`, { notes: "see {{ x }}", timeout: "${{ $vars.t }}" }],
  ["wrong shape", wrongValue, { retry: [], disabled: "no", runner_selector: "x" }],
  ["unknown keys", plausibleValue, { extra_top: { a: 1 } }]
];

function nodeFor([family, of, common]: Family, schema: (typeof nodeTypes.node_types)[number], index: number): WorkflowNode {
  const parameters: Record<string, unknown> = {};
  for (const field of schema.fields) {
    const value = of(field);
    if (value !== undefined) parameters[field.name] = value;
  }
  if (family === "unknown keys") Object.assign(parameters, { zz_unknown: [1, { deep: null }], __other: "x" });
  return { name: `n${index}`, type: schema.node_type, ...common, parameters } as WorkflowNode;
}

/** Chunk size for the per-family node-type split: small enough that each test
 * stays a few seconds even under v8 coverage, large enough to keep the test
 * count (29 types / chunk) manageable. */
const chunkSize = 5;
function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
  return chunks;
}

function chunkWorkflow(family: Family, schemas: (typeof nodeTypes.node_types)[number][], offset: number): WorkflowDef {
  const [name] = family;
  const nodes = schemas.map((schema, index) => nodeFor(family, schema, offset + index));
  // No id: the baseline only lets a saved definition be saved when something
  // changed, and this case saves the bytes as loaded. An unsaved draft keeps
  // 保存 usable without an edit, so the payload equality stays meaningful.
  return { name, nodes, connections: {} };
}

const chunkedCases = families.flatMap(([name, ...rest]) => {
  const family: Family = [name, ...rest] as Family;
  return chunk(nodeTypes.node_types, chunkSize).map((schemas, chunkIndex) => {
    const types = schemas.map((schema) => schema.node_type).join(", ");
    const label = `${name} #${chunkIndex + 1} (${types})`;
    return [label, family, schemas, chunkIndex * chunkSize] as const;
  });
});

describe("node form: opening every builtin node writes nothing (Doc C §5.1 #1)", () => {
  it.each(chunkedCases)(
    "%s",
    async (_label, family, schemas, offset) => {
      const workflow = chunkWorkflow(family, schemas, offset);
      const before = serialize(workflow);
      const handleChange = vi.fn();
      const handleSave = vi.fn(async (next: WorkflowDef) => next);
      render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onChange={handleChange} onSave={handleSave} />);

      for (const node of workflow.nodes ?? []) {
        const root = selectNode(node.name!);
        expect(within(root).getByRole("button", { name: "以 JSON 编辑参数" }).getAttribute("aria-pressed")).toBe("false");
        expect(root.querySelector(`[data-node-form-schema="${node.type}@1"]`)).not.toBeNull();
        expect(root.querySelectorAll("[data-composer-placeholder]").length, node.type).toBe(0);
        await settle();
      }
      // Close the last node too: collapse the panel (a real unmount).
      fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
      await settle();

      expect(handleChange).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: "保存" }));
      await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
      // The PUT payload is the definition as loaded, byte for byte.
      expect(serialize(handleSave.mock.calls[0]?.[0])).toBe(before);
      expect(consoleError).not.toHaveBeenCalled();
    },
    // Chunks of chunkSize node types each: a few seconds per case even under
    // v8 coverage on a loaded host, versus the old 180s all-29-types case.
    45_000
  );
});

// ---------------------------------------- (b) one batch = one undo entry

describe("node form: the patch reducer (Doc C §6.1)", () => {
  const waitWorkflow: WorkflowDef = {
    id: "wf-batch",
    name: "batch",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "pause", type: "xflow.wait", parameters: {} }
    ],
    connections: { start: { main: [{ node: "pause" }] } }
  };

  it("applies every write of one composer batch in one commit and one undo entry", async () => {
    const handleChange = vi.fn();
    const { container } = render(<XFlowEditor value={waitWorkflow} nodeTypes={nodeTypes} onChange={handleChange} />);
    selectNode("pause");
    await settle();

    // Two writes in the same tick: composer delivers them as ONE onChange
    // batch. A reducer that committed per patch against render state would
    // lose the first write (or record two undo entries).
    await act(async () => {
      change(one(fieldOf(container, "f.parameters.signal_name:control"), "input"), "approved");
      change(one(fieldOf(container, "f.notes:control"), "textarea"), "a note");
    });

    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(lastWorkflow(handleChange).nodes?.[1]).toEqual({
      name: "pause",
      type: "xflow.wait",
      notes: "a note",
      parameters: { signal_name: "approved" }
    });

    fireEvent.click(undoButton());
    expect(lastWorkflow(handleChange)).toEqual(waitWorkflow);
    expect(undoButton()).toHaveProperty("disabled", true);
  });

  it("invalidates a dependent enum in the same batch and the same undo entry (Doc C §4.4)", async () => {
    const workflow: WorkflowDef = {
      id: "wf-enum",
      name: "enum",
      nodes: [{ name: "run", type: "xflow.script", parameters: { language: "js", runtime: "goja", code: "return 1;" } }],
      connections: {}
    };
    const handleChange = vi.fn();
    const { container } = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onChange={handleChange} />);
    selectNode("run");
    await settle();

    pickOption(fieldOf(container, "f.parameters.language:control"), "WebAssembly");
    await settle();

    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(lastWorkflow(handleChange).nodes?.[0]?.parameters).toEqual({ language: "wasm", code: "return 1;" });
    // The runtime select now shows no value (the overlay followed the host).
    expect(fieldOf(container, "f.parameters.runtime:control").querySelector(".ant-select-selection-item")).toBeNull();

    fireEvent.click(undoButton());
    expect(lastWorkflow(handleChange)).toEqual(workflow);
    expect(undoButton()).toHaveProperty("disabled", true);
  });

  it("renames through NodeNameInput: connections follow and the node stays selected (Doc C §4.2)", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={waitWorkflow} nodeTypes={nodeTypes} onChange={handleChange} />);
    const root = selectNode("pause");
    const name = within(root).getByLabelText("节点名称");

    fireEvent.change(name, { target: { value: "hold" } });
    fireEvent.blur(name);
    await settle();

    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(lastWorkflow(handleChange).nodes?.[1]?.name).toBe("hold");
    expect(lastWorkflow(handleChange).connections).toEqual({ start: { main: [{ node: "hold" }] } });
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("hold");
    // The header follows the rename: the display name and the type badge.
    expect(within(inspector()).getByText("hold", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
    expect(within(inspector()).getByText("xflow.wait · v1", { selector: ".xflow-editor-inspector-head__type" })).toBeTruthy();
  });
});

// ------------------------------------------- (f) warn vs enforce blocking

describe("node form: validation mode (Doc C §1 rule 3)", () => {
  const missingUrl: WorkflowDef = {
    // No id: a draft that was never persisted keeps 保存 usable (baseline clean
    // state), so the issue count is the only thing blocking anything here.
    name: "http",
    nodes: [{ name: "api", type: "xflow.http", parameters: { method: "GET" } }],
    connections: {}
  };

  it("warn: shows the issue count on the save button but saves", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={missingUrl} nodeTypes={nodeTypes} onSave={handleSave} />);

    const save = screen.getByRole("button", { name: /保存/ });
    expect(within(save).getByTestId("save-issue-count").textContent).toBe("1");
    expect(save.getAttribute("data-blocked")).toBeNull();
    fireEvent.click(save);
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
  });

  it("enforce: blocks save and run with a message naming the node", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    const handleRun = vi.fn(async () => ({ status: "success" as const, nodes: {} }));
    render(<XFlowEditor value={missingUrl} nodeTypes={enforce} onSave={handleSave} onRun={handleRun} />);

    const save = screen.getByRole("button", { name: /保存/ });
    expect(save.getAttribute("data-blocked")).toBe("true");
    // §6.3: blocked swaps the icon to danger but keeps the quiet default button.
    expect(save.className).not.toContain("ant-btn-color-primary");
    expect(save.className).not.toContain("ant-btn-variant-solid");
    expect(save.className).not.toContain("ant-btn-color-dangerous");
    expect(save.className).not.toContain("ant-btn-dangerous");
    fireEvent.click(save);
    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));
    await settle();

    expect(handleSave).not.toHaveBeenCalled();
    expect(handleRun).not.toHaveBeenCalled();
    expect(screen.getAllByText(/节点 api 的参数未通过校验（1 个错误）/).length).toBeGreaterThan(0);
  });
});

// ------------------------------------------------- (g) backend param_issues

describe("node form: backend param_issues land on their field", () => {
  const workflow: WorkflowDef = {
    // No id: an unsaved draft keeps 保存 usable without an edit.
    name: "issues",
    nodes: [{ name: "api", type: "xflow.http", parameters: { url: "https://example.com" } }],
    connections: {}
  };
  const issue: ParamIssue = { node: "api", path: "/parameters/url", code: "format.url", message: "server rejects this url", severity: "error" };

  it("from a save result", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef): Promise<XFlowEditorSaveResult> => ({ workflow: next, paramIssues: [issue] }));
    const { container } = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    selectNode("api");
    expect(screen.queryByText("server rejects this url")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));

    await waitFor(() => expect(within(fieldOf(container, "f.parameters.url")).getAllByText("server rejects this url").length).toBeGreaterThan(0));
    expect(within(fieldOf(container, "f.parameters.method")).queryByText("server rejects this url")).toBeNull();
  });

  it("from a rejected save carrying paramIssues", async () => {
    const handleSave = vi.fn(async () => {
      throw Object.assign(new Error("workflow_param_invalid"), { paramIssues: [issue] });
    });
    const { container } = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    selectNode("api");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));

    await waitFor(() => expect(within(fieldOf(container, "f.parameters.url")).getAllByText("server rejects this url").length).toBeGreaterThan(0));
  });

  it("rolls a sub-graph body member's issue up onto the parent's body field", async () => {
    const mapWorkflow: WorkflowDef = {
      name: "body-issues",
      nodes: [
        {
          name: "loop",
          type: "xflow.map",
          parameters: {
            items: "{{ $input.items }}",
            body: { type: "xflow.subgraph", parameters: { nodes: [{ name: "inner", type: "xflow.http", parameters: {} }] } }
          }
        }
      ],
      connections: {}
    };
    const bodyIssue: ParamIssue = { node: "loop/inner", path: "/parameters/url", code: "required", message: "url is required", severity: "error" };
    const handleSave = vi.fn(async (next: WorkflowDef): Promise<XFlowEditorSaveResult> => ({ workflow: next, paramIssues: [bodyIssue] }));
    const { container } = render(<XFlowEditor value={mapWorkflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    selectNode("loop");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));

    await waitFor(() => expect(within(fieldOf(container, "f.parameters.body:control")).getAllByText("inner: url is required").length).toBeGreaterThan(0));
  });
});

// ------------------------------------------------------ (h) no schema

describe("node form: nodes without a schema (Doc C §2.1)", () => {
  const workflow: WorkflowDef = {
    id: "wf-custom",
    name: "custom",
    nodes: [{ name: "odd", type: "custom.thing", parameters: { a: 1 } }],
    connections: {}
  };

  it("shows common fields and the JSON-only notice; the JSON tab still edits parameters", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onChange={handleChange} />);
    const root = selectNode("odd");

    expect(within(root).getByText("该节点类型未在 server 注册，参数按 JSON 编辑")).toBeTruthy();
    expect(within(root).getByLabelText("节点名称")).toBeTruthy();
    expect(root.querySelector('[data-node-form-schema="none"]')).not.toBeNull();

    const parameters = jsonTab(root);
    fireEvent.change(parameters, { target: { value: '{ "a": 2 }' } });
    fireEvent.blur(parameters);
    expect(lastWorkflow(handleChange).nodes?.[0]?.parameters).toEqual({ a: 2 });
  });

  it("without any nodeTypes it says the schemas are not loaded instead", () => {
    render(<XFlowEditor value={workflow} />);
    const root = selectNode("odd");
    expect(within(root).getByText(/节点类型 schema 未加载/)).toBeTruthy();
    expect(within(root).queryByText("该节点类型未在 server 注册，参数按 JSON 编辑")).toBeNull();
  });
});

// ------------------------------------------ (i) JSON tab debounce + (j)

describe("node form: the JSON tab (Doc C §6.2)", () => {
  const workflow: WorkflowDef = {
    id: "wf-json",
    name: "json",
    nodes: [
      { name: "api", type: "xflow.http", parameters: { url: "https://a" } },
      { name: "tail", type: "xflow.end" }
    ],
    connections: {}
  };

  it("debounces parseable input into one commit and one undo entry", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onChange={handleChange} />);
    const parameters = jsonTab(selectNode("api"));

    fireEvent.change(parameters, { target: { value: '{ "url": "https://b" }' } });
    fireEvent.change(parameters, { target: { value: '{ "url": "https://bc" }' } });
    fireEvent.change(parameters, { target: { value: '{ "url": "https://bcd" }' } });
    expect(handleChange).not.toHaveBeenCalled();

    await waitFor(() => expect(handleChange).toHaveBeenCalledTimes(1), { timeout: 2000 });
    expect(lastWorkflow(handleChange).nodes?.[0]?.parameters).toEqual({ url: "https://bcd" });
    // The typed text is kept as typed (no reformat, no caret jump).
    expect(parameters.value).toBe('{ "url": "https://bcd" }');
    fireEvent.click(undoButton());
    expect(lastWorkflow(handleChange)).toEqual(workflow);
  });

  it("an unparseable draft makes the form read-only and blocks save with a link to the JSON tab", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    const { container } = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    fireEvent.change(jsonTab(selectNode("api")), { target: { value: "{ broken" } });

    formTab();
    expect(within(inspector()).getByText(/表单暂为只读/)).toBeTruthy();
    const url = one<HTMLInputElement>(fieldOf(container, "f.parameters.url:control"), "input");
    expect(url.readOnly || url.disabled).toBe(true);

    const save = screen.getByRole("button", { name: /保存/ });
    expect(save.getAttribute("data-blocked")).toBe("true");
    fireEvent.click(save);
    expect(handleSave).not.toHaveBeenCalled();

    // Clicking the blocked node name selects it and opens its JSON tab.
    selectNode("tail");
    fireEvent.click(screen.getByRole("button", { name: "修正 api 的参数 JSON" }));
    const root = inspector();
    expect(within(root).getByRole("button", { name: "以 JSON 编辑参数" }).getAttribute("aria-pressed")).toBe("true");
    expect((within(root).getByLabelText("参数 JSON") as HTMLTextAreaElement).value).toBe("{ broken");
    // The header names the node the JSON view belongs to.
    expect(within(root).getByText("api", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
    expect(within(root).getByText("xflow.http · v1", { selector: ".xflow-editor-inspector-head__type" })).toBeTruthy();
  });

  it("keeps drafts across a real unmount: panel collapse and the compact drawer", async () => {
    // The ⋯ menu only exists in the compact layout (user ruling 2026-10-21):
    // measure the editor narrow mid-test, as a container resize would.
    const originalObserver = globalThis.ResizeObserver;
    let resizeEditor: ((width: number) => void) | undefined;
    class MockResizeObserver {
      readonly callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
      }
      observe(target: Element) {
        if (target instanceof HTMLElement && target.classList.contains("xflow-editor")) {
          resizeEditor = (width) => this.callback(
            [{ target, contentRect: { width } } as ResizeObserverEntry],
            this as unknown as ResizeObserver
          );
        }
      }
      unobserve() {}
      disconnect() {}
    }
    globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
    try {
      render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} />);
      fireEvent.change(jsonTab(selectNode("api")), { target: { value: "{ broken" } });

      fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
      expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "展开属性面板" }));
      expect(jsonTab().value).toBe("{ broken");

      // Compact layout: the Inspector lives in a destroyOnHidden drawer, and
      // the ⋯ menu is where that layout keeps its secondary actions.
      act(() => resizeEditor?.(1279));
      expect(screen.getByRole("button", { name: "更多操作" })).toBeTruthy();
      expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
      fireEvent.click(within(screen.getByRole("complementary", { name: "紧凑导航" })).getByRole("button", { name: "属性面板" }));
      await waitFor(() => expect(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).toBeTruthy());
      expect(jsonTab().value).toBe("{ broken");
      expect(within(inspector()).getByText("参数 JSON 格式错误")).toBeTruthy();
    } finally {
      globalThis.ResizeObserver = originalObserver;
    }
  });
});

// ------------------------------------------- (k) error location (P1-c S5)

describe("node form: the diagnostics locate a parameter field", () => {
  const workflow: WorkflowDef = {
    // No id: an unsaved draft keeps 保存 usable without an edit.
    name: "locate",
    nodes: [
      { name: "api", type: "xflow.http", parameters: { url: "https://example.com" } },
      { name: "tail", type: "xflow.end" }
    ],
    connections: {}
  };
  const issue: ParamIssue = { node: "api", path: "/parameters/url", code: "format.url", message: "server rejects this url", severity: "error" };

  it("selects the node, activates the 参数 tab and focuses the field", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef): Promise<XFlowEditorSaveResult> => ({ workflow: next, paramIssues: [issue] }));
    const { container } = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    selectNode("tail");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    const locate = await screen.findByRole("button", { name: "定位字段 api /parameters/url" });
    // Walk away from the target node and its 配置 tab first: locating has to
    // select api, come back to 配置 and focus the field.
    fireEvent.click(within(inspector()).getByRole("tab", { name: "连接" }));
    expect(within(inspector()).getByRole("tab", { name: "连接" }).getAttribute("aria-selected")).toBe("true");
    // §6.0: collapsing the inspector must not swallow a locate request — this
    // entry exists specifically to look at the field.
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    fireEvent.click(locate);
    await settle();

    expect(within(inspector()).getByText("api", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
    expect(within(inspector()).getByRole("tab", { name: "配置" }).getAttribute("aria-selected")).toBe("true");
    await waitFor(() => {
      const field = one<HTMLElement>(container, '[data-composer-path="/parameters/url"]');
      expect(field.contains(document.activeElement)).toBe(true);
    });
  });

  /** Opens the diagnostics board with the server's param_issue in place. */
  const mountWithServerIssue = async () => {
    const handleSave = vi.fn(async (next: WorkflowDef): Promise<XFlowEditorSaveResult> => ({ workflow: next, paramIssues: [issue] }));
    const utils = render(<XFlowEditor value={workflow} nodeTypes={nodeTypes} onSave={handleSave} />);
    selectNode("tail");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    await screen.findByRole("button", { name: "定位字段 api /parameters/url" });
    return utils;
  };
  const clickLocate = async () => {
    fireEvent.click(screen.getByRole("button", { name: "定位字段 api /parameters/url" }));
    await settle();
  };

  it("clears a handled request: a later remount does not re-focus the field", async () => {
    const { container } = await mountWithServerIssue();
    await clickLocate();
    const field = one<HTMLElement>(container, '[data-composer-path="/parameters/url"]');
    await waitFor(() => expect(field.contains(document.activeElement)).toBe(true));
    (document.activeElement as HTMLElement | null)?.blur();

    // A fresh 定位字段 still reaches the field: the cleared state must not
    // let the next request repeat the last nonce and be de-duplicated away
    // by the Composer that is still mounted.
    await clickLocate();
    await waitFor(() =>
      expect(one<HTMLElement>(container, '[data-composer-path="/parameters/url"]').contains(document.activeElement)).toBe(true)
    );
    (document.activeElement as HTMLElement | null)?.blur();

    // The request was consumed and cleared: leaving the 配置 tab and coming
    // back remounts the form, but must not steal the focus again.
    fireEvent.click(within(inspector()).getByRole("tab", { name: "连接" }));
    fireEvent.click(within(inspector()).getByRole("tab", { name: "配置" }));
    await settle();
    expect(one<HTMLElement>(container, '[data-composer-path="/parameters/url"]').contains(document.activeElement)).toBe(false);
  });

  it("clears a handled request: another node has no stale path left to warn about", async () => {
    const { container } = await mountWithServerIssue();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await clickLocate();
      await waitFor(() =>
        expect(one<HTMLElement>(container, '[data-composer-path="/parameters/url"]').contains(document.activeElement)).toBe(true)
      );
      // Selecting a node whose form has no /parameters/url must not replay the
      // consumed request (which would log a focus-target-not-found warning).
      selectNode("tail");
      await settle();
      expect(warn.mock.calls.some((call) => String(call[0]).includes("focus-target-not-found"))).toBe(false);
    } finally {
      warn.mockRestore();
    }
  });
});
