import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";

// jsdom has no layout: seed a wide editor like the other editor suites.
const originalResizeObserver = globalThis.ResizeObserver;
class WidthResizeObserver {
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element): void {
    this.callback(
      [{ target, contentRect: { width: 1440 } } as ResizeObserverEntry],
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

const plain: WorkflowDef = {
  id: "wf-plain",
  name: "无参数",
  nodes: [{ name: "start", type: "xflow.start", kind: "trigger" }],
  connections: {}
};

const withParams: WorkflowDef = {
  ...plain,
  id: "wf-params",
  name: "采购审批",
  params: {
    amount: { type: "number", required: true, display_name: "金额", validation: { min: 0 } },
    applicant: { type: "string", required: true, display_name: "申请人" },
    department: { type: "string", display_name: "部门", default: "研发" },
    urgent: { type: "boolean", display_name: "加急" },
    extra: { type: "object", display_name: "附加" }
  }
};

const success = { status: "success" as const, nodes: { start: { status: "success" as const } } };

/** One change event per character, the way a keyboard grows a controlled input. */
function typeInto(element: HTMLElement, text: string, seen?: string[]): void {
  fireEvent.focus(element);
  let typed = "";
  for (const char of text) {
    typed += char;
    fireEvent.change(element, { target: { value: typed } });
    seen?.push(typed);
  }
}

// antd pins every aria-labelledby to the literal id "test-id" under test, and
// other selects in the editor carry that id too, so the dialog's accessible name
// does not resolve here. Locate it by its own class instead.
const dialogElement = (): HTMLElement | undefined =>
  screen.queryAllByRole("dialog").find((candidate) => candidate.classList.contains("xflow-editor-run-params"));
const dialog = (): HTMLElement => {
  const found = dialogElement();
  if (!found) throw new Error("run parameters dialog is not open");
  return found;
};
const openedDialog = async (): Promise<HTMLElement> => {
  await waitFor(() => expect(dialogElement()).toBeTruthy());
  return dialog();
};
const field = (label: string): HTMLElement => within(dialog()).getByLabelText(new RegExp(`^${label}`));
const clickRun = () => fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));
const submit = () => fireEvent.click(within(dialog()).getByRole("button", { name: "运行" }));

describe("run parameters dialog", () => {
  it("runs a workflow without declared params at once and calls onRun with the draft alone", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={plain} onRun={onRun} />);

    clickRun();

    await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
    expect(onRun).toHaveBeenCalledWith(plain);
    expect(onRun.mock.calls[0]).toHaveLength(1);
    expect(dialogElement()).toBeUndefined();
  });

  it("opens the dialog for a workflow with params and does not run until confirmed", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);

    clickRun();

    expect(await openedDialog()).toBeTruthy();
    expect(onRun).not.toHaveBeenCalled();
    expect((field("部门") as HTMLInputElement).value).toBe("研发");
    expect(within(dialog()).getByText("采购审批")).toBeTruthy();
  });

  it("blocks submit on required fields, then sends typed values", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();

    submit();
    expect(within(dialog()).getAllByText("必填")).toHaveLength(2);
    expect(onRun).not.toHaveBeenCalled();
    expect(field("金额").getAttribute("aria-invalid")).toBe("true");

    const seen: string[] = [];
    typeInto(field("金额"), "12.5", seen);
    typeInto(field("申请人"), "alice");
    // Half-typed numbers are never sent: nothing reaches onRun while typing.
    expect(seen).toEqual(["1", "12", "12.", "12.5"]);
    expect(onRun).not.toHaveBeenCalled();

    submit();
    await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
    expect(onRun).toHaveBeenCalledWith(withParams, {
      params: { amount: 12.5, applicant: "alice", department: "研发" }
    });
    await waitFor(() => expect(dialogElement()).toBeUndefined());
  });

  it("rejects bad numbers and min violations before any request", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();
    typeInto(field("申请人"), "alice");

    typeInto(field("金额"), "1e");
    submit();
    expect(within(dialog()).getByText("请输入有效数字")).toBeTruthy();

    typeInto(field("金额"), "-3");
    submit();
    expect(within(dialog()).getByText("不能小于 0")).toBeTruthy();
    expect(onRun).not.toHaveBeenCalled();
  });

  it("validates JSON as it is typed and only sends the finished object", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();
    typeInto(field("金额"), "1");
    typeInto(field("申请人"), "bob");

    const json = field("附加");
    typeInto(json, '{"a":[1,');
    fireEvent.blur(json);
    expect(within(dialog()).getByText("JSON 格式错误")).toBeTruthy();
    submit();
    expect(onRun).not.toHaveBeenCalled();

    typeInto(json, '{"a":[1,2]}');
    expect(within(dialog()).queryByText("JSON 格式错误")).toBeNull();
    submit();
    await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
    expect(onRun.mock.calls[0]?.[1]).toEqual({
      params: { amount: 1, applicant: "bob", department: "研发", extra: { a: [1, 2] } }
    });
  });

  it("keeps the dialog and the typed values when the run is refused", async () => {
    const onRun = vi.fn(async () => {
      throw new Error("param \"amount\" must be >= 5");
    });
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();
    typeInto(field("金额"), "2");
    typeInto(field("申请人"), "carol");
    submit();

    expect((await within(dialog()).findByRole("alert")).textContent).toContain("must be >= 5");
    expect((field("金额") as HTMLInputElement).value).toBe("2");
    expect((field("申请人") as HTMLInputElement).value).toBe("carol");

    // Editing a field drops the stale refusal.
    typeInto(field("金额"), "9");
    expect(within(dialog()).queryByRole("alert")).toBeNull();
  });

  it("closes on Escape without running and reopens with the defaults", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();
    typeInto(field("申请人"), "dave");

    fireEvent.keyDown(field("申请人"), { key: "Escape" });
    await waitFor(() => expect(dialogElement()).toBeUndefined());
    expect(onRun).not.toHaveBeenCalled();

    clickRun();
    await openedDialog();
    expect((field("申请人") as HTMLInputElement).value).toBe("");
  });

  it("does not let editor shortcuts swallow a ? typed into a field", async () => {
    render(<XFlowEditor value={withParams} onRun={vi.fn(async () => success)} />);
    clickRun();
    await openedDialog();

    const applicant = field("申请人");
    applicant.focus();
    const notPrevented = fireEvent.keyDown(applicant, { key: "?" });
    expect(notPrevented).toBe(true);
    expect(screen.queryByRole("dialog", { name: /快捷键/ })).toBeNull();
  });

  it("submits with Ctrl+Enter from a field and does not start a second run", async () => {
    const onRun = vi.fn(async () => success);
    render(<XFlowEditor value={withParams} onRun={onRun} />);
    clickRun();
    await openedDialog();
    typeInto(field("金额"), "4");
    typeInto(field("申请人"), "erin");

    await act(async () => {
      fireEvent.keyDown(field("申请人"), { key: "Enter", ctrlKey: true });
    });

    await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
  });

  it("keeps the field focused while a dialog button is pressed, so its error does not appear under the pointer", async () => {
    render(<XFlowEditor value={withParams} onRun={vi.fn(async () => success)} />);
    clickRun();
    await openedDialog();

    // A prevented mousedown is what stops the browser from moving focus (and
    // so from blurring the field and shifting the layout before the click).
    for (const name of ["取消", "运行"]) {
      const notPrevented = fireEvent.mouseDown(within(dialog()).getByRole("button", { name }));
      expect(notPrevented).toBe(false);
    }
  });
});
