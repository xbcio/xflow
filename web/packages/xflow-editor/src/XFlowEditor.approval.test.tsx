import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef, XFlowEditorCapabilities } from "@xflow/core";
import {
  XFlowEditor,
  type XFlowExecution,
  type XFlowExecutionNode
} from "./index";

// The editor reads its width from a ResizeObserver; jsdom has no layout, so
// seed the wide layout the other editor suites use.
const originalResizeObserver = globalThis.ResizeObserver;
class WidthResizeObserver {
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element): void {
    this.callback(
      [{ target, contentRect: { width: 1280 } } as ResizeObserverEntry],
      this as unknown as ResizeObserver
    );
  }
  unobserve(): void {}
  disconnect(): void {}
}
beforeAll(() => {
  globalThis.ResizeObserver = WidthResizeObserver as unknown as typeof ResizeObserver;
});
afterAll(() => {
  globalThis.ResizeObserver = originalResizeObserver;
});

const allCapabilities: XFlowEditorCapabilities = {
  workflow: true,
  execution: true,
  executionControl: true,
  executionRerun: true
};

const baseParameters: Record<string, unknown> = {
  mode: "any",
  approvers: ["finance-mgr", "alice"],
  timeout: "2h",
  timeout_action: "reject",
  force_approvers: ["ciso"]
};

function makeWorkflow(parameters: Record<string, unknown> = baseParameters): WorkflowDef {
  return {
    id: "wf-approval",
    name: "Approval",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      {
        name: "manager_approval",
        type: "xflow.approval",
        position: { x: 260, y: 120 },
        ui: { label: "经理审批" },
        parameters
      }
    ],
    connections: { start: { main: [{ node: "manager_approval" }] } }
  };
}

/** The same workflow with a non-approval suspended node in front of the gate. */
function makeWorkflowWithWait(): WorkflowDef {
  return {
    id: "wf-approval",
    name: "Approval",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "wait", type: "xflow.http", position: { x: 120, y: 120 } },
      {
        name: "manager_approval",
        type: "xflow.approval",
        position: { x: 380, y: 120 },
        ui: { label: "经理审批" },
        parameters: baseParameters
      }
    ],
    connections: {
      start: { main: [{ node: "wait" }] },
      wait: { main: [{ node: "manager_approval" }] }
    }
  };
}

function makeExecution(
  options: {
    output?: Record<string, unknown>;
    status?: XFlowExecution["status"];
    nodes?: XFlowExecutionNode[];
  } = {}
): XFlowExecution {
  return {
    executionId: "exec-approval",
    status: options.status ?? "running",
    nodes:
      options.nodes ??
      [
        { name: "start", status: "success" },
        { name: "manager_approval", status: "suspended", output: options.output ?? {} }
      ]
  };
}

const suspendedRuntime: RuntimeSnapshot = {
  nodes: {
    wait: { status: "suspended" },
    manager_approval: { status: "suspended" }
  }
};

type EditorProps = Parameters<typeof XFlowEditor>[0];

function editorElement(props: Partial<EditorProps> = {}) {
  return (
    <XFlowEditor
      capabilities={allCapabilities}
      currentUserSubject="finance-mgr"
      execution={makeExecution()}
      onSignal={vi.fn()}
      value={makeWorkflow()}
      {...props}
    />
  );
}

/**
 * Opens §6.12 through the run bar's 等待审批 count, the entry that needs only
 * the execution detail. `dialog` is a re-query so the element stays current
 * across a host poll (rerender) that swaps the execution record.
 */
async function openApprovalDialog(props: Partial<EditorProps> = {}) {
  const utils = render(editorElement(props));
  const bar = document.querySelector<HTMLElement>(".xflow-editor-runbar");
  if (!bar) throw new Error("run bar not found");
  fireEvent.click(within(bar).getByRole("button", { name: /等待审批/ }));
  await screen.findByRole("dialog");
  const dialog = (): HTMLElement => screen.getByRole("dialog");
  return { ...utils, dialog };
}

/** Two suspended approval gates in one execution, for cross-target isolation. */
function makeTwoGateWorkflow(): WorkflowDef {
  return {
    id: "wf-approval",
    name: "Approval",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      {
        name: "approval_one",
        type: "xflow.approval",
        position: { x: 260, y: 120 },
        ui: { label: "门一" },
        parameters: baseParameters
      },
      {
        name: "approval_two",
        type: "xflow.approval",
        position: { x: 520, y: 120 },
        ui: { label: "门二" },
        parameters: baseParameters
      }
    ],
    connections: { start: { main: [{ node: "approval_one" }] } }
  };
}

function makeTwoGateExecution(): XFlowExecution {
  return makeExecution({
    nodes: [
      { name: "start", status: "success" },
      { name: "approval_one", status: "suspended", output: {} },
      { name: "approval_two", status: "suspended", output: {} }
    ]
  });
}

/** Clicks a suspended approval card's 处理 (React Flow hides it from the a11y tree in jsdom). */
function openCard(cardName: string): HTMLElement {
  const select = document.querySelector<HTMLElement>(
    `.xflow-preview-node__select[aria-label^="${cardName} node "]`
  );
  if (!select) throw new Error(`${cardName} card not found`);
  const card = select.closest<HTMLElement>(".react-flow__node") ?? select.parentElement;
  const action = card?.querySelector<HTMLButtonElement>(".xflow-preview-node__note-action");
  if (!action) throw new Error(`${cardName} has no 处理 action`);
  fireEvent.click(action);
  return screen.getByRole("dialog");
}

describe("approval dialog (§6.12)", () => {
  it("opens from the run bar's 等待审批 count with the header, ledger and mode progress", async () => {
    const view = await openApprovalDialog();
    const dialog = view.dialog();

    expect(dialog.textContent).toContain("经理审批");
    expect(dialog.textContent).toContain("manager_approval · exec-approval · 任一审批");
    expect(within(dialog).getByText("任一审批 · 等待任一审批人")).toBeTruthy();

    const ledger = within(dialog).getByLabelText("审批记录");
    expect(ledger.textContent).toContain("finance-mgr");
    expect(ledger.textContent).toContain("alice");
    expect(within(ledger).getAllByText("待处理")).toHaveLength(2);
    expect(within(dialog).getByText(/已处理 0 人 · 待处理 2 人/)).toBeTruthy();
    expect(dialog.textContent).toContain("manager_approval/approval");
    expect(within(dialog).getByText(/超时 2h 后自动驳回（rejected 出口）/)).toBeTruthy();

    // The three port decisions, each naming the outlet it leaves by and
    // carrying the baseline port tone (approved green / rejected red / returned amber).
    expect(within(dialog).getByRole("button", { name: /走 approved 出口/ }).getAttribute("data-tone")).toBe("success");
    expect(within(dialog).getByRole("button", { name: /走 rejected 出口/ }).getAttribute("data-tone")).toBe("danger");
    expect(within(dialog).getByRole("button", { name: /走 returned 出口/ }).getAttribute("data-tone")).toBe("warning");

    // The header is the 28px category block, and the title's own close is the
    // only one (AntD's default corner close would be a second control).
    expect(dialog.querySelector(".xflow-editor-category-icon")).toBeTruthy();
    expect(dialog.querySelector(".ant-modal-close")).toBeNull();
  });

  it("upgrades the approval card's wait note and opens the dialog from 处理", async () => {
    render(
      editorElement({
        execution: makeExecution({
          nodes: [
            { name: "start", status: "success" },
            { name: "wait", status: "suspended" },
            { name: "manager_approval", status: "suspended" }
          ]
        }),
        runtime: suspendedRuntime,
        value: makeWorkflowWithWait()
      })
    );

    const notes = Array.from(
      document.querySelectorAll<HTMLElement>(".xflow-preview-node__note")
    );
    const approvalNote = notes.find((note) =>
      note.textContent?.includes("任一审批 · 等待任一审批人")
    );
    expect(approvalNote).toBeTruthy();
    // The non-approval suspended card keeps the generic note and gains no action.
    const waitNote = notes.find((note) => note.textContent === "等待信号");
    expect(waitNote).toBeTruthy();
    expect(waitNote?.querySelector(".xflow-preview-node__note-action")).toBeNull();

    // React Flow leaves unmeasured nodes visibility:hidden in jsdom, so the
    // card's control is reached by its class, not by an accessible role.
    const action = (approvalNote as HTMLElement).querySelector<HTMLButtonElement>(
      ".xflow-preview-node__note-action"
    );
    expect(action?.textContent).toBe("处理");
    fireEvent.click(action as HTMLButtonElement);
    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain("manager_approval");
    expect(within(dialog).getByText("任一审批 · 等待任一审批人")).toBeTruthy();
  });

  it("handles the first suspended approval when the count names another suspended node", async () => {
    const view = await openApprovalDialog({
      execution: makeExecution({
        nodes: [
          { name: "start", status: "success" },
          { name: "wait", status: "suspended" },
          { name: "manager_approval", status: "suspended" }
        ]
      }),
      value: makeWorkflowWithWait()
    });
    const dialog = view.dialog();

    expect(dialog.textContent).toContain("manager_approval");
    expect(dialog.textContent).not.toContain("wait");
  });

  it("stays read-only for a user outside the approval chain", async () => {
    const view = await openApprovalDialog({ currentUserSubject: "bob" });
    const dialog = view.dialog();

    expect(within(dialog).getByText("你不是审批人，本次审批只读")).toBeTruthy();
    expect(dialog.textContent).toContain("当前登录用户是 bob");
    expect(within(dialog).queryByRole("group", { name: "决定" })).toBeNull();
    expect(within(dialog).queryByRole("button", { name: "提交同意" })).toBeNull();
    expect(within(dialog).queryByRole("button", { name: /转交/ })).toBeNull();
    // The record remains readable to an outsider.
    expect(within(dialog).getByLabelText("审批记录").textContent).toContain("finance-mgr");
  });

  it("lets only the current sequential approver decide, under a per-subject signal name", async () => {
    const onSignal = vi.fn();
    const sequential = makeWorkflow({ ...baseParameters, mode: "sequential" });
    const view = await openApprovalDialog({
      currentUserSubject: "alice",
      onSignal,
      value: sequential
    });

    expect(within(view.dialog()).getByText("未到你，等待 finance-mgr")).toBeTruthy();
    expect(within(view.dialog()).queryByRole("button", { name: "提交同意" })).toBeNull();

    view.rerender(
      editorElement({
        currentUserSubject: "alice",
        execution: makeExecution({
          output: {
            decisions: [
              { approver: "finance-mgr", action: "approve", at: "2026-10-20T10:00:00Z" }
            ]
          }
        }),
        onSignal,
        value: sequential
      })
    );
    const dialog = view.dialog();
    expect(within(dialog).getByText("当前待你处理")).toBeTruthy();

    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));
    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval/alice",
        data: { action: "approve" }
      })
    );
  });

  it("submits an any-mode decision under the shared signal name with the comment", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({ onSignal });
    const dialog = view.dialog();

    expect(within(dialog).queryByRole("button", { name: "强制通过" })).toBeNull();
    fireEvent.change(within(dialog).getByLabelText("审批意见"), {
      target: { value: "预算没问题" }
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval",
        data: { action: "approve", comment: "预算没问题" }
      })
    );
    expect(await within(dialog).findByText("已提交，等待节点处理…")).toBeTruthy();
    // A running execution holds no queued signal, so there is nothing to revoke.
    expect(within(dialog).queryByRole("button", { name: "撤回信号" })).toBeNull();
  });

  it("sends the selected reject decision", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({ onSignal });
    const dialog = view.dialog();

    const rejectChoice = within(dialog).getByRole("button", { name: /走 rejected 出口/ });
    fireEvent.click(rejectChoice);
    expect(rejectChoice.getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(within(dialog).getByRole("button", { name: "提交驳回" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval",
        data: { action: "reject" }
      })
    );
  });

  it("names the signal after the subject in all mode", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({
      onSignal,
      value: makeWorkflow({ ...baseParameters, mode: "all" })
    });
    const dialog = view.dialog();

    expect(within(dialog).getByText("全部审批 0/2 · 等待 finance-mgr")).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval/finance-mgr",
        data: { action: "approve" }
      })
    );
  });

  it("offers 强制通过 only to the force list and sends the force signal", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({ currentUserSubject: "ciso", onSignal });
    const dialog = view.dialog();

    expect(within(dialog).getByText("你不是审批人，只能强制通过")).toBeTruthy();
    // The force-only user sees the name force actually travels under, not the
    // per-subject decision name the node never arms for them.
    expect(dialog.textContent).toContain("manager_approval/approval/force");
    expect(dialog.textContent).not.toContain("manager_approval/approval/ciso");
    fireEvent.click(within(dialog).getByRole("button", { name: "强制通过" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval/force",
        data: { action: "force" }
      })
    );
  });

  it("delegates the open slot to another approver", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({ onSignal });
    const dialog = view.dialog();

    fireEvent.click(within(dialog).getByRole("button", { name: "转交…" }));
    fireEvent.change(within(dialog).getByPlaceholderText("输入审批人标识"), {
      target: { value: "bob" }
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "转交" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval",
        data: { action: "delegate", assignee: "bob" }
      })
    );
  });

  it("adds a signer before the current approver in sequential mode", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({
      onSignal,
      value: makeWorkflow({ ...baseParameters, mode: "sequential" })
    });
    const dialog = view.dialog();

    fireEvent.click(within(dialog).getByRole("button", { name: "加签…" }));
    fireEvent.change(within(dialog).getByPlaceholderText("输入审批人标识"), {
      target: { value: "bob" }
    });
    fireEvent.click(within(dialog).getByRole("radio", { name: "在我之前" }));
    fireEvent.click(within(dialog).getByRole("button", { name: "加签" }));

    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval/finance-mgr",
        data: { action: "add_signer", assignee: "bob", position: "before" }
      })
    );
  });

  it("keeps the actor's slot open after add_signer, as closesSlot does", async () => {
    const view = await openApprovalDialog({
      execution: makeExecution({
        output: {
          decisions: [
            {
              approver: "finance-mgr",
              action: "add_signer",
              assignee: "bob",
              position: "before",
              at: "2026-10-20T10:00:00Z"
            }
          ]
        }
      }),
      value: makeWorkflow({ ...baseParameters, mode: "sequential" })
    });
    const dialog = view.dialog();
    const ledger = within(dialog).getByLabelText("审批记录");

    // The amendment is on the record but is not a closing decision: no ✓, and
    // the actor stays in the open chain.
    const amended = ledger.querySelector<HTMLElement>('.xflow-editor-approval__row[data-state="amended"]');
    expect(amended?.textContent).toContain("finance-mgr");
    expect(amended?.textContent).toContain("加签");
    expect(amended?.textContent).not.toContain("✓");
    const openRows = Array.from(
      ledger.querySelectorAll<HTMLElement>(
        '.xflow-editor-approval__row[data-state="waiting"], .xflow-editor-approval__row[data-state="current"]'
      )
    ).map((row) => row.textContent ?? "");
    expect(openRows.some((text) => text.includes("finance-mgr"))).toBe(true);
    expect(openRows.some((text) => text.includes("bob"))).toBe(true);

    // Sequential restarts at the inserted signer; the actor is not "decided".
    expect(within(dialog).getByText("未到你，等待 bob")).toBeTruthy();
    expect(dialog.textContent).not.toContain("你已在此节点作出决定");
    expect(dialog.textContent).toContain("已处理 0 人 · 待处理 3 人");
  });

  it("does not carry a submission into the next node's dialog", async () => {
    const onSignal = vi.fn();
    render(
      editorElement({
        execution: makeTwoGateExecution(),
        onSignal,
        runtime: { nodes: { approval_one: { status: "suspended" }, approval_two: { status: "suspended" } } },
        value: makeTwoGateWorkflow()
      })
    );

    let dialog = openCard("approval_one");
    fireEvent.change(within(dialog).getByLabelText("审批意见"), {
      target: { value: "A 的意见" }
    });
    fireEvent.click(within(dialog).getByRole("button", { name: /走 rejected 出口/ }));
    fireEvent.click(within(dialog).getByRole("button", { name: "提交驳回" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));
    expect(await within(dialog).findByText("已提交，等待节点处理…")).toBeTruthy();

    fireEvent.click(within(dialog).getByRole("button", { name: "关闭审批对话框" }));
    dialog = openCard("approval_two");

    expect(dialog.textContent).toContain("approval_two");
    // Neither the pending status nor the previous choice/comment may follow.
    expect(within(dialog).queryByText("已提交，等待节点处理…")).toBeNull();
    expect(within(dialog).queryByText("决定已记入审批记录")).toBeNull();
    expect((within(dialog).getByLabelText("审批意见") as HTMLTextAreaElement).value).toBe("");
    expect(
      within(dialog).getByRole("button", { name: /走 approved 出口/ }).getAttribute("aria-pressed")
    ).toBe("true");
  });

  it("delivers force when the mode is an expression the client cannot evaluate", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({
      currentUserSubject: "ciso",
      onSignal,
      value: makeWorkflow({
        mode: "{{ inputs.mode }}",
        approvers: ["finance-mgr"],
        force_approvers: ["ciso"]
      })
    });
    const dialog = view.dialog();

    expect(within(dialog).getByText("审批模式无法在界面判断，请以服务端记录为准")).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", { name: "强制通过" }));

    // force is answered before the mode checks server-side, and its name does
    // not depend on the mode: an expression must not leave a dead button.
    await waitFor(() =>
      expect(onSignal).toHaveBeenCalledWith("exec-approval", {
        name: "manager_approval/approval/force",
        data: { action: "force" }
      })
    );
    expect(dialog.textContent).not.toContain("操作失败");
  });

  it("reads a refusal past the capped ignored trail from ignored_count", async () => {
    const onSignal = vi.fn();
    const trail = Array.from({ length: 20 }, (_, index) => ({
      signal: `manager_approval/approval/other-${index}`,
      reason: "unauthorized-approver"
    }));
    const view = await openApprovalDialog({
      execution: makeExecution({ output: { ignored: trail, ignored_count: 20 } }),
      onSignal
    });
    let dialog = view.dialog();
    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));

    // The trail is still 20 entries (the oldest fell off) but ignored_count grew.
    view.rerender(
      editorElement({
        execution: makeExecution({
          output: {
            ignored: [
              ...trail.slice(1),
              { signal: "manager_approval/approval", reason: "not-current-approver" }
            ],
            ignored_count: 21
          }
        }),
        onSignal
      })
    );
    dialog = view.dialog();
    expect(await within(dialog).findByText("信号未被采纳（未计入）")).toBeTruthy();
    expect(dialog.querySelector(".xflow-editor-approval__status")?.textContent).toContain(
      "尚未轮到该审批人"
    );
  });

  it("does not blame this submission for another signal's refusal", async () => {
    const onSignal = vi.fn();
    const view = await openApprovalDialog({ onSignal });
    let dialog = view.dialog();
    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));
    expect(await within(dialog).findByText("已提交，等待节点处理…")).toBeTruthy();

    view.rerender(
      editorElement({
        execution: makeExecution({
          output: {
            ignored: [{ signal: "manager_approval/approval/bob", reason: "unauthorized-approver" }],
            ignored_count: 1
          }
        }),
        onSignal
      })
    );
    dialog = view.dialog();
    expect(await within(dialog).findByText("有信号未被计入")).toBeTruthy();
    expect(within(dialog).queryByText("信号未被采纳（未计入）")).toBeNull();
    expect(dialog.querySelector(".xflow-editor-approval__status")?.getAttribute("data-tone")).toBe(
      "warning"
    );
  });

  it("reads acceptance back from the polled ledger instead of trusting the POST", async () => {
    const onSignal = vi.fn();
    const sequential = makeWorkflow({ ...baseParameters, mode: "sequential" });
    const view = await openApprovalDialog({ onSignal, value: sequential });

    fireEvent.click(within(view.dialog()).getByRole("button", { name: "提交同意" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));
    expect(await within(view.dialog()).findByText("已提交，等待节点处理…")).toBeTruthy();

    view.rerender(
      editorElement({
        execution: makeExecution({
          output: {
            decisions: [
              { approver: "finance-mgr", action: "approve", at: "2026-10-20T10:00:00Z" }
            ]
          }
        }),
        onSignal,
        value: sequential
      })
    );
    const dialog = view.dialog();
    expect(await within(dialog).findByText("决定已记入审批记录")).toBeTruthy();
    expect(within(dialog).getByText("你已在此节点作出决定")).toBeTruthy();
    expect(
      dialog.querySelector(".xflow-editor-approval__status")?.getAttribute("data-tone")
    ).toBe("success");
  });

  it("shows the 未计入 trail and never reports an ignored signal as a decision", async () => {
    const onSignal = vi.fn();
    const ignoredBefore = {
      signal: "manager_approval/approval/bob",
      reason: "unauthorized-approver"
    };
    const view = await openApprovalDialog({
      execution: makeExecution({
        output: { ignored: [ignoredBefore], ignored_count: 1 }
      }),
      onSignal
    });
    let dialog = view.dialog();

    expect(within(dialog).getByText("未计入的信号（1）")).toBeTruthy();
    expect(within(dialog).getByText("不在审批链")).toBeTruthy();
    expect(within(dialog).queryByText("决定已记入审批记录")).toBeNull();

    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));
    expect(await within(dialog).findByText("已提交，等待节点处理…")).toBeTruthy();

    view.rerender(
      editorElement({
        execution: makeExecution({
          output: {
            ignored: [
              ignoredBefore,
              { signal: "manager_approval/approval", reason: "not-current-approver" }
            ],
            ignored_count: 2
          }
        }),
        onSignal
      })
    );
    dialog = view.dialog();
    expect(await within(dialog).findByText("信号未被采纳（未计入）")).toBeTruthy();
    expect(
      dialog.querySelector(".xflow-editor-approval__status")?.textContent
    ).toContain("尚未轮到该审批人");
    expect(within(dialog).queryByText("决定已记入审批记录")).toBeNull();
  });

  it("revokes a queued signal while the execution is paused", async () => {
    const onSignal = vi.fn();
    const onRevokeSignal = vi.fn();
    const view = await openApprovalDialog({
      execution: makeExecution({ status: "paused" }),
      onRevokeSignal,
      onSignal
    });
    const dialog = view.dialog();

    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));
    await waitFor(() => expect(onSignal).toHaveBeenCalledTimes(1));
    fireEvent.click(await within(dialog).findByRole("button", { name: "撤回信号" }));

    await waitFor(() =>
      expect(onRevokeSignal).toHaveBeenCalledWith("exec-approval", "manager_approval/approval")
    );
    expect(await within(dialog).findByText("已撤回投递的信号")).toBeTruthy();
  });

  it("reports a rejected delivery as a failure, not a pending decision", async () => {
    const onSignal = vi.fn().mockRejectedValue(new Error("signal already consumed or not found"));
    const view = await openApprovalDialog({ onSignal });
    const dialog = view.dialog();

    fireEvent.click(within(dialog).getByRole("button", { name: "提交同意" }));

    expect(await within(dialog).findByText("操作失败")).toBeTruthy();
    expect(within(dialog).getByText(/signal already consumed/)).toBeTruthy();
    // A failed POST must not leave the 已提交 wait line behind.
    expect(within(dialog).queryByText("已提交，等待节点处理…")).toBeNull();
    expect(within(dialog).queryByText("决定已记入审批记录")).toBeNull();
  });

  it("opens the same dialog from the inspector 运行 tab", async () => {
    render(editorElement({ runtime: suspendedRuntime }));
    const node = document.querySelector<HTMLElement>(
      '.xflow-preview-node__select[aria-label^="manager_approval node "]'
    );
    if (!node) throw new Error("approval node not found");
    fireEvent.click(node);

    const panel = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(panel).getByRole("tab", { name: /运行/ }));
    fireEvent.click(within(panel).getByRole("button", { name: "处理审批" }));

    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain("manager_approval");
    expect(within(dialog).getByText("任一审批 · 等待任一审批人")).toBeTruthy();
  });
});
