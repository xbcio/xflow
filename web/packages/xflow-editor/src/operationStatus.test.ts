import { describe, expect, it } from "vitest";
import type { NodeStatus, RuntimeSnapshot, WorkflowStatus } from "@xflow/core";
import { operationStatusFromRuntime, workflowStatusFromRuntime } from "./operationStatus";

const nodes = (...statuses: NodeStatus[]): RuntimeSnapshot["nodes"] =>
  Object.fromEntries(statuses.map((status, index) => [`n${index}`, { status }]));

describe("operationStatusFromRuntime", () => {
  const byWorkflowStatus: Array<[WorkflowStatus, string]> = [
    ["pending", "等待开始"],
    ["running", "运行中"],
    ["success", "运行完成"],
    ["failed", "运行失败"],
    ["canceled", "已取消"],
    ["timeout", "运行超时"]
  ];

  for (const [status, label] of byWorkflowStatus) {
    it(`labels a ${status} snapshot "${label}"`, () => {
      expect(operationStatusFromRuntime({ status })).toBe(label);
    });
  }

  it("covers every WorkflowStatus", () => {
    const covered = byWorkflowStatus.map(([status]) => status).sort();
    expect(covered).toEqual(["canceled", "failed", "pending", "running", "success", "timeout"]);
  });

  it("never reports completion for a running snapshot, whatever its nodes say", () => {
    const label = operationStatusFromRuntime({ status: "running", nodes: nodes("success", "success") });
    expect(label).toBe("运行中");
    expect(label).not.toBe("运行完成");
  });

  it("trusts the workflow status over the nodes when both are present", () => {
    expect(operationStatusFromRuntime({ status: "failed", nodes: nodes("success") })).toBe("运行失败");
  });

  describe("without a workflow status", () => {
    it("is running while any node is still in flight", () => {
      for (const active of ["running", "committing", "waiting", "suspended"] as const) {
        expect(operationStatusFromRuntime({ nodes: nodes("success", active) }), active).toBe("运行中");
      }
    });

    it("does not call a run with a failed or continued node complete", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("success", "failed") })).toBe("运行失败");
      expect(operationStatusFromRuntime({ nodes: nodes("success", "continued") })).toBe("运行失败");
    });

    it("lets an in-flight node outrank a failed one", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("failed", "running") })).toBe("运行中");
    });

    it("is complete only when every node is settled and none failed", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("success", "skipped", "pinned", "expired") })).toBe("运行完成");
    });

    it("is pending when nothing has started", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("pending", "pending") })).toBe("等待开始");
    });

    it("is running between nodes: some settled, the rest still pending", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("success", "pending") })).toBe("运行中");
    });

    it("reports a cancelled node as cancelled rather than complete", () => {
      expect(operationStatusFromRuntime({ nodes: nodes("success", "canceled") })).toBe("已取消");
    });

    it("does not guess when the snapshot carries neither status nor nodes", () => {
      expect(operationStatusFromRuntime({})).toBe("状态未知");
      expect(operationStatusFromRuntime({ nodes: {} })).toBe("状态未知");
    });
  });
});

describe("workflowStatusFromRuntime", () => {
  it("returns the snapshot's own status untouched, whatever its nodes say", () => {
    expect(workflowStatusFromRuntime({ status: "running", nodes: nodes("success") })).toBe("running");
    expect(workflowStatusFromRuntime({ status: "timeout" })).toBe("timeout");
  });

  it("derives the status from the nodes when the snapshot has none", () => {
    expect(workflowStatusFromRuntime({ nodes: nodes("success", "failed") })).toBe("failed");
    expect(workflowStatusFromRuntime({ nodes: nodes("success", "skipped") })).toBe("success");
  });

  it("is undefined, not a guess, when there is nothing to read", () => {
    expect(workflowStatusFromRuntime({})).toBeUndefined();
    expect(workflowStatusFromRuntime({ nodes: {} })).toBeUndefined();
  });
});
