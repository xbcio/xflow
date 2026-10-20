import { describe, expect, it } from "vitest";
import {
  edgeVariant,
  executionOpLabel,
  nodeStatusMeta,
  workflowStatusMeta,
  type ExecutionOp,
  type ExecutionStatusKey,
  type NodeStatusKey,
} from "./statusMeta";

// The server's NodeStatus values (types/ NodeStatus*) plus the editor-only `disabled`.
// Exhaustiveness against the TypeScript union is enforced at build time by the
// `Record<NodeStatusKey, …>` annotation; this list pins the runtime count too.
const nodeStatuses: NodeStatusKey[] = [
  "pending",
  "running",
  "committing",
  "success",
  "failed",
  "skipped",
  "pinned",
  "continued",
  "suspended",
  "waiting",
  "expired",
  "canceled",
  "disabled",
];

// types/execution.go and `ExecutionStatus` in api/openapi/xflow-v1.yaml.
const executionStatuses: ExecutionStatusKey[] = [
  "pending",
  "queued",
  "paused",
  "running",
  "success",
  "failed",
  "canceling",
  "canceled",
  "timeout",
];

describe("nodeStatusMeta", () => {
  it("covers the 12 server NodeStatus values plus disabled", () => {
    expect(Object.keys(nodeStatusMeta).sort()).toEqual([...nodeStatuses].sort());
    expect(Object.keys(nodeStatusMeta)).toHaveLength(13);
  });

  it("gives every status a label, tone, icon key and edge column", () => {
    for (const [status, meta] of Object.entries(nodeStatusMeta)) {
      expect(meta.label, status).not.toBe("");
      expect(meta.tone, status).toMatch(/^(neutral|info|success|warning|danger|suspend)$/);
      expect(meta.icon, status).not.toBe("");
      expect(meta.edge, status).not.toBe("");
    }
  });

  it("draws committing as a solid info border without the running pulse", () => {
    expect(nodeStatusMeta.running.card).toContain("脉冲");
    expect(nodeStatusMeta.committing.card).toContain("info");
    expect(nodeStatusMeta.committing.card).toContain("实线");
    expect(nodeStatusMeta.committing.card).not.toContain("同 running");
    expect(nodeStatusMeta.committing.card).not.toBe(nodeStatusMeta.running.card);
  });

  it("draws skipped and disabled by dimming border, fill and icon, never the text", () => {
    for (const status of ["skipped", "disabled"] as const) {
      const { card } = nodeStatusMeta[status];
      expect(card, status).toContain("压暗");
      expect(card, status).toContain("文字不透明");
      // The old wording ("55%") described a whole-card opacity that dragged the text down with it.
      expect(card, status).not.toContain("55%");
    }
    expect(nodeStatusMeta.skipped.card).toContain("虚线");
    expect(nodeStatusMeta.disabled.card).toContain("斜纹");
  });
});

describe("edgeVariant", () => {
  it("resolves a variant for all 13 statuses", () => {
    for (const status of nodeStatuses) {
      expect(edgeVariant(status), status).toBeTruthy();
    }
  });

  it("draws failed as the resting line, never as a break", () => {
    expect(edgeVariant("failed")).toBe("default");
    expect(edgeVariant("pending")).toBe("default");
  });

  it("draws canceled and expired as no edge", () => {
    expect(edgeVariant("canceled")).toBe("none");
    expect(edgeVariant("expired")).toBe("none");
  });

  it("maps the flowing, skipped and success columns", () => {
    expect(edgeVariant("running")).toBe("flow");
    expect(edgeVariant("committing")).toBe("flow");
    expect(edgeVariant("waiting")).toBe("flow-slow");
    expect(edgeVariant("skipped")).toBe("skipped");
    expect(edgeVariant("disabled")).toBe("skipped");
    expect(edgeVariant("success")).toBe("success");
    expect(edgeVariant("pinned")).toBe("success");
  });
});

describe("workflowStatusMeta", () => {
  it("covers exactly the nine server ExecutionStatus values", () => {
    expect(Object.keys(workflowStatusMeta).sort()).toEqual([...executionStatuses].sort());
    expect(Object.keys(workflowStatusMeta)).toHaveLength(9);
  });

  it("has a button label for every operation any status offers", () => {
    const offered = new Set<ExecutionOp>(Object.values(workflowStatusMeta).flatMap((meta) => meta.ops));
    for (const op of offered) {
      expect(executionOpLabel[op], op).toBeTruthy();
    }
    expect(Object.keys(executionOpLabel).sort()).toEqual(["cancel", "pause", "rerun", "resume"]);
  });

  it("offers operations by status as spec §4 lists them", () => {
    const ops = (status: ExecutionStatusKey) => workflowStatusMeta[status].ops;
    expect(ops("pending")).toEqual(["cancel"]);
    expect(ops("queued")).toEqual(["cancel"]);
    expect(ops("running")).toEqual(["pause", "cancel"]);
    expect(ops("paused")).toEqual(["resume", "cancel"]);
    expect(ops("canceling")).toEqual([]);
    for (const terminal of ["success", "failed", "canceled", "timeout"] as const) {
      expect(ops(terminal), terminal).toEqual(["rerun"]);
    }
  });

  it("marks exactly success, failed, canceled and timeout as terminal", () => {
    const terminal = Object.entries(workflowStatusMeta)
      .filter(([, meta]) => meta.terminal)
      .map(([status]) => status)
      .sort();
    expect(terminal).toEqual(["canceled", "failed", "success", "timeout"]);
  });
});
