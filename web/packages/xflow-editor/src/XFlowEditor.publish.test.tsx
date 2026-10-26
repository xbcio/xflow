import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, WorkflowDef, XFlowEditorCapabilities } from "@xflow/core";
import { XFlowEditor } from "./index";
import generated from "./node-form/testdata/node-types.generated.json";
import { PublishDialog } from "./panels/publishDialog";
import { VersionHistory } from "./panels/versionHistory";
import { workflowChanges } from "./workflowChanges";

// The editor reads its width from a ResizeObserver; jsdom has no layout, so
// seed the wide layout the toolbar tests use. Every test here is §6.10: the
// publish gate and the version history wired to host callbacks.
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

const allCapabilities: XFlowEditorCapabilities = {
  workflow: true,
  execution: true,
  executionControl: true,
  executionRerun: true
};

const workflow: WorkflowDef = {
  id: "wf-publish",
  name: "Orders",
  version: "1.2.0",
  nodes: [
    { name: "start", type: "xflow.start" },
    { name: "api", type: "xflow.http", parameters: { url: "https://a" } }
  ],
  connections: { start: { main: [{ node: "api" }] } }
};

const edited: WorkflowDef = {
  ...workflow,
  nodes: [
    workflow.nodes![0],
    { name: "api", type: "xflow.http", parameters: { url: "https://b" } },
    { name: "tail", type: "xflow.end" }
  ],
  connections: { start: { main: [{ node: "api" }, { node: "tail" }] } }
};

const versions = {
  list: [
    { version: "1.3.0", definitionHash: "abcdef1234567890", current: true, archived: false },
    { version: "1.2.0", definitionHash: "fedcba9876543210", current: false, archived: true },
    { version: "1.1.0", definitionHash: "0011223344556677", current: false, archived: true }
  ],
  total: 3
};

describe("PublishDialog (§6.10)", () => {
  const changes = workflowChanges(workflow, edited);

  it("shows the online version and hash, the patch+1 default and one row per change", () => {
    render(
      <PublishDialog
        changes={changes}
        errorCount={0}
        onlineHash="abcdef1234567890"
        onlineVersion="1.3.0"
        open
        suggestedVersion="1.3.1"
        usedVersions={["1.3.0", "1.2.0"]}
        version="1.3.1"
        warningCount={0}
        workflow={edited}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.textContent).toContain("v1.3.0");
    expect(dialog.textContent).toContain("abcdef12…");
    expect((screen.getByLabelText("新版本号") as HTMLInputElement).value).toBe("1.3.1");
    const rows = dialog.querySelectorAll("[data-diff] [data-change-kind]");
    expect(rows).toHaveLength(changes.length);
    expect(dialog.textContent).toContain("参数 url");
    expect(dialog.textContent).toContain("https://b");
    expect(dialog.querySelector('[data-diff-count="add"]')?.textContent).toContain("新增");
    expect(dialog.textContent).toContain("校验通过");
    // The hint offers both step sizes the baseline names (patch / minor).
    expect(dialog.textContent).toContain("建议 1.3.1（修订） / 1.4.0（功能）");
  });

  it("blocks the gate on validation errors and states the in-flight rule", () => {
    render(
      <PublishDialog
        changes={[]}
        errorCount={2}
        open
        suggestedVersion="1.3.1"
        usedVersions={[]}
        version="1.3.1"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.textContent).toContain("校验有 2 个错误");
    expect(dialog.textContent).toContain("在途执行不受影响");
    expect((screen.getByRole("button", { name: "发布 v1.3.1" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("states a warning count as publishable, with the warning tone and the gate open", () => {
    render(
      <PublishDialog
        changes={[]}
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={[]}
        version="1.3.1"
        warningCount={2}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    const dialog = screen.getByRole("dialog");
    const banner = dialog.querySelector<HTMLElement>(".xflow-editor-banner");

    expect(banner?.getAttribute("data-tone")).toBe("warning");
    expect(banner?.textContent).toContain("可以发布");
    expect(banner?.textContent).toContain("校验有 2 个警告");
    expect(banner?.textContent).toContain("发布后新触发的执行使用新版本，在途执行不受影响");
    expect(dialog.textContent).not.toContain("校验通过");
    // Warnings do not block: the gate stays open.
    expect((screen.getByRole("button", { name: "发布 v1.3.1" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("keeps an error above the warning count and closes the gate", () => {
    render(
      <PublishDialog
        changes={[]}
        errorCount={1}
        open
        suggestedVersion="1.3.1"
        usedVersions={[]}
        version="1.3.1"
        warningCount={2}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    const dialog = screen.getByRole("dialog");
    const banner = dialog.querySelector<HTMLElement>(".xflow-editor-banner");

    expect(banner?.getAttribute("data-tone")).toBe("danger");
    expect(banner?.textContent).toContain("校验未通过");
    expect(banner?.textContent).toContain("校验有 1 个错误");
    // One banner only: the error branch wins over the warning count.
    expect(dialog.querySelectorAll(".xflow-editor-banner")).toHaveLength(1);
    expect(dialog.textContent).not.toContain("可以发布");
    expect((screen.getByRole("button", { name: "发布 v1.3.1" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("refuses an already published label and the online label before any request", () => {
    const { rerender } = render(
      <PublishDialog
        changes={[]}
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={["1.3.0", "1.2.0"]}
        version="1.2.0"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    expect(screen.getByRole("dialog").textContent).toContain("该版本号已发布过");
    expect((screen.getByRole("button", { name: "发布 v1.2.0" }) as HTMLButtonElement).disabled).toBe(true);

    rerender(
      <PublishDialog
        changes={[]}
        errorCount={0}
        onlineVersion="1.3.0"
        open
        suggestedVersion="1.3.1"
        usedVersions={["1.3.0", "1.2.0"]}
        version="1.3.0"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    // The online label is also in usedVersions; the same-label branch must still
    // win, otherwise the real host always shows the generic taken sentence.
    expect(screen.getByRole("dialog").textContent).toContain("与线上版本相同");
    expect((screen.getByRole("button", { name: "发布 v1.3.0" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("renders the mapped failure above the gate", () => {
    render(
      <PublishDialog
        changes={[]}
        error="该版本号已被发布过不同内容，请换一个新版本号再发布。"
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={[]}
        version="1.3.1"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    expect(screen.getByRole("dialog").textContent).toContain("发布失败");
    expect(screen.getByRole("dialog").textContent).toContain("已被发布过不同内容");
  });

  it("keeps a v-prefixed label single-prefixed on the primary button", () => {
    render(
      <PublishDialog
        changes={[]}
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={["1.3.0"]}
        version="v1.3.1"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    expect(screen.getByRole("button", { name: "发布 v1.3.1" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "发布 vv1.3.1" })).toBeNull();
  });

  it("opens the complete diff from the footer", () => {
    render(
      <PublishDialog
        changes={changes}
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={["1.3.0"]}
        version="1.3.1"
        warningCount={0}
        workflow={edited}
        onCancel={() => {}}
        onPublish={() => {}}
        onVersionChange={() => {}}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "查看完整 diff" }));
    const full = document.querySelector("[data-diff-full]");
    expect(full).not.toBeNull();
    expect(full?.querySelectorAll("[data-change-kind]")).toHaveLength(changes.length);
    expect(screen.getByText("完整变更")).toBeTruthy();
  });

  it("says the online state is unconfirmed when the version list failed, and retries", () => {
    const onReloadVersions = vi.fn();
    render(
      <PublishDialog
        changes={[]}
        errorCount={0}
        open
        suggestedVersion="1.3.1"
        usedVersions={[]}
        version="1.3.1"
        versionsError="版本历史加载失败：offline"
        warningCount={0}
        workflow={workflow}
        onCancel={() => {}}
        onPublish={() => {}}
        onReloadVersions={onReloadVersions}
        onVersionChange={() => {}}
      />
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.textContent).toContain("无法确认（版本历史加载失败）");
    expect(dialog.textContent).not.toContain("尚未发布");
    fireEvent.click(within(dialog).getByRole("button", { name: "重试" }));
    expect(onReloadVersions).toHaveBeenCalledTimes(1);
  });
});

describe("VersionHistory (§6.10)", () => {
  it("keeps the server order, marks the online row and shows hash prefixes", () => {
    render(
      <VersionHistory
        versions={versions}
        blocked={{}}
        onReload={() => {}}
        onRollback={() => {}}
        onViewVersion={() => {}}
      />
    );
    const rows = document.querySelectorAll("[data-version]");
    expect(Array.from(rows).map((row) => row.getAttribute("data-version"))).toEqual(["1.3.0", "1.2.0", "1.1.0"]);
    expect(rows[0].hasAttribute("data-online")).toBe(true);
    expect(rows[0].textContent).toContain("线上");
    expect(rows[0].textContent).toContain("abcdef12…");
    // The online row has no rollback control: it is already the installed one.
    expect(rows[0].querySelector('[data-op="rollback"]')).toBeNull();
    expect(rows[1].querySelector('[data-op="rollback"]')).not.toBeNull();
    expect(screen.getByText(/保留最近 50 个归档版本/)).toBeTruthy();
  });

  it("disables a refused row and prints the server reason on it", () => {
    render(
      <VersionHistory
        versions={versions}
        blocked={{ "1.1.0": "当前编译器重算哈希不一致，服务端会拒绝回滚。" }}
        onReload={() => {}}
        onRollback={() => {}}
        onViewVersion={() => {}}
      />
    );
    const blockedRow = document.querySelector('[data-version="1.1.0"]') as HTMLElement;
    const button = blockedRow.querySelector('[data-op="rollback-blocked"]') as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    expect(blockedRow.textContent).toContain("当前编译器重算哈希不一致");
  });

  it("keeps every archived row's 回滚到此 reachable while only the rolling row is busy", () => {
    render(
      <VersionHistory
        versions={versions}
        blocked={{}}
        rollingBackVersion="1.2.0"
        onReload={() => {}}
        onRollback={() => {}}
        onViewVersion={() => {}}
      />
    );
    const row = (version: string) => document.querySelector(`[data-version="${version}"]`) as HTMLElement;

    // The online row carries no rollback at all.
    expect(row("1.3.0").querySelector('[data-op="rollback"]')).toBeNull();
    // Both archived rows offer 回滚到此 and 查看; only the in-flight one is
    // disabled (and shows the button's loading state).
    const rolling = row("1.2.0").querySelector('[data-op="rollback"]') as HTMLButtonElement;
    expect(rolling.disabled).toBe(true);
    expect(rolling.className).toContain("ant-btn-loading");
    const idle = row("1.1.0").querySelector('[data-op="rollback"]') as HTMLButtonElement;
    expect(idle.disabled).toBe(false);
    expect(idle.className).not.toContain("ant-btn-loading");
    for (const version of ["1.2.0", "1.1.0"]) {
      expect(row(version).querySelector('[data-op="view"]')).not.toBeNull();
    }
  });
});

describe("XFlowEditor publish wiring (§6.10)", () => {
  it("keeps 发布 disabled without onPublish, and hides it for a credential without workflow", () => {
    const { unmount } = render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
    const button = screen.getByRole("button", { name: "发布" }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    unmount();

    render(
      <XFlowEditor
        capabilities={{ ...allCapabilities, workflow: false }}
        value={workflow}
        onPublish={async (next) => next}
      />
    );
    expect(screen.queryByRole("button", { name: "发布" })).toBeNull();
  });

  it("opens the dialog on the online version and publishes the typed label", async () => {
    const onLoadVersions = vi.fn(async () => versions);
    const onPublish = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} onLoadVersions={onLoadVersions} onPublish={onPublish} />);

    fireEvent.click(screen.getByRole("button", { name: "发布" }));
    const dialog = await screen.findByRole("dialog");
    await waitFor(() => expect(screen.getByLabelText("新版本号")).toBeTruthy());
    expect(onLoadVersions).toHaveBeenCalled();
    await waitFor(() => expect(dialog.textContent).toContain("abcdef12…"));
    expect(dialog.textContent).toContain("线上 v1.3.0");

    fireEvent.click(screen.getByRole("button", { name: "发布 v1.3.1" }));
    await waitFor(() => expect(onPublish).toHaveBeenCalledTimes(1));
    expect(onPublish.mock.calls[0][0]).toMatchObject({ id: "wf-publish", version: "1.3.1" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(document.querySelector('[aria-live="polite"]')?.textContent).toContain("已发布 v1.3.1");
  });

  it("maps a version-immutable rejection into the dialog instead of closing it", async () => {
    const onPublish = vi.fn(async () => {
      throw Object.assign(new Error("version exists"), { status: 409, code: "workflow_version_immutable" });
    });
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} onPublish={onPublish} />);

    fireEvent.click(screen.getByRole("button", { name: "发布" }));
    fireEvent.click(await screen.findByRole("button", { name: "发布 v1.2.1" }));
    await waitFor(() => expect(onPublish).toHaveBeenCalled());
    const dialog = screen.getByRole("dialog");
    await waitFor(() => expect(dialog.textContent).toContain("该版本号已被发布过不同内容"));
    expect(dialog.textContent).toContain("发布失败");
  });

  it("opens the history from the chip and reports 查看 / 回滚 to the host", async () => {
    const onLoadVersions = vi.fn(async () => versions);
    const onViewVersion = vi.fn();
    const onRollback = vi.fn(async (version: string) => ({ version, unchanged: false }));
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        value={workflow}
        onLoadVersions={onLoadVersions}
        onRollback={onRollback}
        onViewVersion={onViewVersion}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const panel = await screen.findByRole("region", { name: "版本历史" });
    await waitFor(() => expect(panel.querySelectorAll("[data-version]")).toHaveLength(3));
    expect(Array.from(panel.querySelectorAll("[data-version]")).map((row) => row.getAttribute("data-version"))).toEqual([
      "1.3.0",
      "1.2.0",
      "1.1.0"
    ]);

    const row = panel.querySelector('[data-version="1.2.0"]') as HTMLElement;
    fireEvent.click(within(row).getByRole("button", { name: "查看" }));
    expect(onViewVersion).toHaveBeenCalledWith("1.2.0");

    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const reopened = await screen.findByRole("region", { name: "版本历史" });
    const rollbackRow = reopened.querySelector('[data-version="1.2.0"]') as HTMLElement;
    fireEvent.click(within(rollbackRow).getByRole("button", { name: "回滚到此" }));
    fireEvent.click(await screen.findByRole("button", { name: "回滚" }));
    await waitFor(() => expect(onRollback).toHaveBeenCalledWith("1.2.0"));
    await waitFor(() =>
      expect(document.querySelector('[aria-live="polite"]')?.textContent).toContain("已回滚到 v1.2.0")
    );
  });

  it("pins workflow_version_unavailable to the refused row", async () => {
    const onLoadVersions = vi.fn(async () => versions);
    const onRollback = vi.fn(async () => {
      throw Object.assign(new Error("unavailable"), { status: 409, code: "workflow_version_unavailable" });
    });
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        value={workflow}
        onLoadVersions={onLoadVersions}
        onRollback={onRollback}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const panel = await screen.findByRole("region", { name: "版本历史" });
    const row = (await waitFor(() => {
      const found = panel.querySelector('[data-version="1.1.0"]');
      expect(found).not.toBeNull();
      return found;
    })) as HTMLElement;
    fireEvent.click(within(row).getByRole("button", { name: "回滚到此" }));
    fireEvent.click(await screen.findByRole("button", { name: "回滚" }));
    await waitFor(() =>
      expect((row.querySelector('[data-op="rollback-blocked"]') as HTMLButtonElement | null)?.disabled).toBe(true)
    );
    expect(row.textContent).toContain("当前编译器重算哈希不一致");
  });

  it("does not let a failed run block publishing (operation findings are not definition errors)", async () => {
    const onRun = vi.fn(async () => {
      throw new Error("runner offline");
    });
    const onPublish = vi.fn(async (next: WorkflowDef) => next);
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        value={workflow}
        onLoadVersions={async () => versions}
        onPublish={onPublish}
        onRun={onRun}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));
    await waitFor(() => expect(onRun).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("button", { name: "发布" }));
    const dialog = await screen.findByRole("dialog");
    await waitFor(() => expect(dialog.textContent).toContain("线上 v1.3.0"));
    // The run's failure is shown in the diagnostics dock, never as a gate.
    expect(dialog.textContent).not.toMatch(/校验有 \d+ 个错误/);
    expect((screen.getByRole("button", { name: "发布 v1.3.1" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("hides 回滚到此 without the workflow scope but keeps 查看", async () => {
    render(
      <XFlowEditor
        capabilities={{ ...allCapabilities, workflow: false }}
        value={workflow}
        onLoadVersions={async () => versions}
        onRollback={async (version) => ({ version, unchanged: false })}
        onViewVersion={() => {}}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const panel = await screen.findByRole("region", { name: "版本历史" });
    await waitFor(() => expect(panel.querySelectorAll("[data-version]")).toHaveLength(3));
    expect(panel.querySelector('[data-op="rollback"]')).toBeNull();
    expect(panel.querySelector('[data-op="view"]')).not.toBeNull();
  });

  it("still offers 回滚到此 while viewing history, where the editor is read-only", async () => {
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        historicalVersion="1.1.4"
        value={{ ...workflow, version: "1.1.4" }}
        onLoadVersions={async () => versions}
        onRollback={async (version) => ({ version, unchanged: true })}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const panel = await screen.findByRole("region", { name: "版本历史" });
    await waitFor(() => expect(panel.querySelectorAll("[data-version]")).toHaveLength(3));
    expect(panel.querySelector('[data-op="rollback"]')).not.toBeNull();
  });

  it("does not carry a refused version's reason to the next workflow", async () => {
    const onRollback = vi.fn(async () => {
      throw Object.assign(new Error("unavailable"), { status: 409, code: "workflow_version_unavailable" });
    });
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        value={workflow}
        onLoadVersions={async () => versions}
        onRollback={onRollback}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "版本历史" }));
    const first = await screen.findByRole("region", { name: "版本历史" });
    await waitFor(() => expect(first.querySelectorAll("[data-version]")).toHaveLength(3));
    const blockedRow = first.querySelector('[data-version="1.1.0"]') as HTMLElement;
    fireEvent.click(within(blockedRow).getByRole("button", { name: "回滚到此" }));
    fireEvent.click(await screen.findByRole("button", { name: "回滚" }));
    await waitFor(() => expect(first.querySelector('[data-op="rollback-blocked"]')).not.toBeNull());

    // 另存为副本 changes the route id without remounting the editor.
    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        value={{ ...workflow, id: "wf-next" }}
        onLoadVersions={async () => versions}
        onRollback={onRollback}
      />
    );
    await waitFor(() =>
      expect(first.querySelector('[data-version="1.1.0"] [data-op="rollback"]')).not.toBeNull()
    );
    expect(first.querySelector('[data-version="1.1.0"]')?.textContent).not.toContain("当前编译器重算哈希不一致");
  });

  it("reports a version-list failure inside the publish dialog and retries it", async () => {
    const onLoadVersions = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValue(versions);
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        value={workflow}
        onLoadVersions={onLoadVersions}
        onPublish={async (next) => next}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "发布" }));
    const dialog = await screen.findByRole("dialog");
    await waitFor(() => expect(dialog.textContent).toContain("版本历史加载失败，线上 hash 与已用版本号无法确认"));
    // The baseline label is still shown, but the hash is explicitly unknown.
    expect(dialog.textContent).toContain("hash 不可用");
    fireEvent.click(within(dialog).getByRole("button", { name: "重试" }));
    await waitFor(() => expect(dialog.textContent).toContain("线上 v1.3.0"));
    expect(onLoadVersions).toHaveBeenCalledTimes(2);
  });

  it("opens an archived version read-only with a way back", () => {
    const onExitHistory = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        historicalVersion="1.1.4"
        value={{ ...workflow, version: "1.1.4" }}
        onExitHistory={onExitHistory}
        onLoadVersions={async () => versions}
      />
    );

    const chip = screen.getByRole("button", { name: "版本历史" });
    expect(chip.textContent).toContain("v1.1.4 · 历史");
    expect(chip.getAttribute("data-tone")).toBe("neutral");
    expect(screen.queryByRole("button", { name: "保存" })).toBeNull();
    expect(screen.queryByRole("button", { name: "发布" })).toBeNull();
    expect(screen.getByText("历史版本 v1.1.4")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "退出历史" }));
    expect(onExitHistory).toHaveBeenCalledTimes(1);
  });

  it("adds the card-summary row to the publish diff (§6.1.1 / §6.10)", async () => {
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        nodeTypes={generated as unknown as NodeTypesResponse}
        value={workflow}
        onPublish={async (next) => next}
      />
    );
    fireEvent.click(document.querySelector('.xflow-preview-node__select[aria-label^="api 节点 "]') as HTMLElement);
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    const toggle = within(inspector).getByRole("button", { name: "以 JSON 编辑参数" });
    if (toggle.getAttribute("aria-pressed") !== "true") fireEvent.click(toggle);
    fireEvent.change(within(inspector).getByLabelText("参数 JSON"), { target: { value: '{"url":"https://b"}' } });
    // The card redraws from the committed draft: that is the same resolver the
    // publish dialog reads, so waiting here also waits for the diff source.
    await waitFor(() =>
      expect(
        document.querySelector('[data-node-id="api"] .xflow-preview-node__summary')?.textContent
      ).toContain("https://b")
    );
    fireEvent.click(screen.getByRole("button", { name: "发布" }));
    const dialog = await screen.findByRole("dialog");
    const summary = dialog.querySelector<HTMLElement>('[data-change-field="摘要"]');
    expect(summary).not.toBeNull();
    expect(summary?.textContent).toContain("URL https://a");
    expect(summary?.textContent).toContain("URL https://b");
  });
});
