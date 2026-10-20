import { describe, expect, it } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { countUnsavedChanges, versionChipState, workflowChanges } from "./unsavedChanges";

const baseline: WorkflowDef = {
  id: "wf-1",
  name: "Orders",
  version: "1.2.0",
  nodes: [
    { name: "start", type: "xflow.start" },
    { name: "api", type: "xflow.http", parameters: { url: "https://a" } }
  ],
  connections: {
    start: { main: [{ node: "api" }] }
  },
  settings: { timezone: "UTC" }
};

const withNodes = (nodes: WorkflowDef["nodes"]): WorkflowDef => ({ ...baseline, nodes });

describe("countUnsavedChanges", () => {
  it("counts nothing when the draft is the baseline", () => {
    expect(countUnsavedChanges(baseline, baseline)).toBe(0);
  });

  it("counts an added node once", () => {
    expect(countUnsavedChanges(baseline, withNodes([...baseline.nodes!, { name: "tail", type: "xflow.end" }]))).toBe(1);
  });

  it("counts a removed node once", () => {
    expect(countUnsavedChanges(baseline, withNodes([baseline.nodes![0]]))).toBe(1);
  });

  it("counts a changed node once, however many of its fields changed", () => {
    const changed: WorkflowDef = {
      ...baseline,
      nodes: [
        baseline.nodes![0],
        { name: "api", type: "xflow.http", parameters: { url: "https://b", method: "GET" } }
      ]
    };
    expect(countUnsavedChanges(baseline, changed)).toBe(1);
  });

  it("names changes by nodeKey, not by position", () => {
    const reordered: WorkflowDef = {
      ...baseline,
      nodes: [baseline.nodes![1], baseline.nodes![0]]
    };
    expect(countUnsavedChanges(baseline, reordered)).toBe(0);
  });

  it("adds one for a connections difference, not one per edge", () => {
    const reconnected: WorkflowDef = {
      ...baseline,
      connections: { start: { main: [{ node: "tail" }, { node: "api" }] } }
    };
    expect(countUnsavedChanges(baseline, reconnected)).toBe(1);
  });

  it("adds one for every differing top-level field other than nodes and connections", () => {
    const edited: WorkflowDef = {
      ...baseline,
      name: "Orders v2",
      settings: { timezone: "Asia/Shanghai" }
    };
    expect(countUnsavedChanges(baseline, edited)).toBe(2);
  });

  it("returns to zero after undo restores the baseline", () => {
    const edited: WorkflowDef = { ...baseline, name: "Renamed" };
    expect(countUnsavedChanges(baseline, edited)).toBe(1);
    expect(countUnsavedChanges(baseline, { ...baseline })).toBe(0);
  });
});

describe("workflowChanges", () => {
  it("names an added and a removed node with the node key", () => {
    const added = workflowChanges(baseline, withNodes([...baseline.nodes!, { name: "tail", type: "xflow.end" }]));
    expect(added).toHaveLength(1);
    expect(added[0]).toMatchObject({ kind: "add", target: "tail", fields: [{ field: "节点", after: "xflow.end" }] });

    const removed = workflowChanges(baseline, withNodes([baseline.nodes![0]]));
    expect(removed).toHaveLength(1);
    expect(removed[0]).toMatchObject({ kind: "del", target: "api", fields: [{ field: "节点", before: "xflow.http" }] });
  });

  it("lists every parameter edit inside the one changed-node entry", () => {
    const changed: WorkflowDef = {
      ...baseline,
      nodes: [
        baseline.nodes![0],
        { name: "api", type: "xflow.http", parameters: { url: "https://b", method: "GET" } }
      ]
    };
    const changes = workflowChanges(baseline, changed);
    expect(changes).toHaveLength(1);
    expect(changes[0].kind).toBe("mod");
    expect(changes[0].fields).toEqual([
      { field: "参数 url", before: "https://a", after: "https://b" },
      { field: "参数 method", before: undefined, after: "GET" }
    ]);
  });

  it("adds one 摘要 row when the card summary a changed node would draw changes", () => {
    const changed: WorkflowDef = {
      ...baseline,
      nodes: [baseline.nodes![0], { name: "api", type: "xflow.http", parameters: { url: "https://b" } }]
    };
    const cardSummary = (node: { parameters?: Record<string, unknown> }) =>
      node.parameters?.url ? `URL ${String(node.parameters.url)}` : undefined;

    const withSummary = workflowChanges(baseline, changed, { cardSummary });
    expect(withSummary[0].fields).toEqual([
      { field: "参数 url", before: "https://a", after: "https://b" },
      { field: "摘要", before: "URL https://a", after: "URL https://b" }
    ]);

    // Without the resolver the diff has no schemas to format a summary from, so
    // it reports the parameter edit alone.
    expect(workflowChanges(baseline, changed)[0].fields).toEqual([
      { field: "参数 url", before: "https://a", after: "https://b" }
    ]);
  });

  it("names the display-name edit separately from parameters", () => {
    const changed: WorkflowDef = {
      ...baseline,
      nodes: [baseline.nodes![0], { ...baseline.nodes![1], ui: { label: "获取档案" } }]
    };
    expect(workflowChanges(baseline, changed)[0].fields).toEqual([
      { field: "显示名", before: undefined, after: "获取档案" }
    ]);
  });

  it("reports each added and removed connection edge under the single connection entry", () => {
    const reconnected: WorkflowDef = {
      ...baseline,
      connections: {
        start: { main: [{ node: "tail" }] },
        api: { main: [{ node: "tail" }] }
      },
      nodes: [...baseline.nodes!, { name: "tail", type: "xflow.end" }]
    };
    const changes = workflowChanges(baseline, reconnected);
    const connection = changes.find((change) => change.id === "connections");
    expect(connection).toMatchObject({ kind: "mod", target: "连线" });
    expect(connection?.fields).toEqual(
      expect.arrayContaining([
        { field: "删除", before: "start.main → api.main" },
        { field: "新增", after: "start.main → tail.main" },
        { field: "新增", after: "api.main → tail.main" }
      ])
    );
  });

  it("keeps the count equal to the change array it describes", () => {
    const edited: WorkflowDef = { ...baseline, name: "Orders v2", settings: { timezone: "Asia/Shanghai" } };
    expect(workflowChanges(baseline, edited)).toHaveLength(countUnsavedChanges(baseline, edited));
    expect(workflowChanges(baseline, edited).map((change) => change.target)).toEqual(["name", "settings"]);
  });
});

describe("versionChipState", () => {
  it("reads a clean persisted version as success 线上 vX", () => {
    expect(versionChipState({ persisted: true, version: "1.2.0", changes: 0 })).toEqual({
      tone: "success",
      label: "线上 v1.2.0"
    });
  });

  it("does not double the v when the server label already carries it", () => {
    expect(versionChipState({ persisted: true, version: "v1.2.0", changes: 0 }).label).toBe("线上 v1.2.0");
  });

  it("reads an archived definition as the neutral 历史 chip", () => {
    expect(versionChipState({ persisted: true, version: "1.1.4", changes: 0, historicalVersion: "1.1.4" })).toEqual({
      tone: "neutral",
      label: "v1.1.4 · 历史"
    });
    expect(versionChipState({ persisted: true, changes: 3, historicalVersion: "v1.1.4" }).label).toBe("v1.1.4 · 历史");
  });

  it("reads a clean definition without a version as success 已保存", () => {
    expect(versionChipState({ persisted: true, changes: 0 })).toEqual({ tone: "success", label: "已保存" });
  });

  it("reads an unpersisted, unmodified draft as 未保存 without a count", () => {
    expect(versionChipState({ persisted: false, changes: 0 })).toEqual({ tone: "warning", label: "未保存" });
  });

  it("counts changes against the loaded version", () => {
    expect(versionChipState({ persisted: true, version: "1.2.0", changes: 3 })).toEqual({
      tone: "warning",
      label: "未保存 3 处 · 基于 v1.2.0"
    });
  });

  it("drops the version clause when there is no version yet", () => {
    expect(versionChipState({ persisted: false, changes: 2 })).toEqual({
      tone: "warning",
      label: "未保存 2 处"
    });
  });

  it("goes back to success when the changes cancel out", () => {
    expect(versionChipState({ persisted: true, version: "1.2.0", changes: 0 }).tone).toBe("success");
  });
});
