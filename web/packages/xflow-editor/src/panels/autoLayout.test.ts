import { describe, expect, it } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { defaultNodeBox, layoutGraph, layoutWorkflowPositions, type NodeBox } from "./autoLayout";

const box = (width = 200, height = 60): NodeBox => ({ width, height });

/** True when two boxes overlap in BOTH axes. */
const overlap = (
  a: { x: number; y: number },
  aBox: NodeBox,
  b: { x: number; y: number },
  bBox: NodeBox
): boolean =>
  a.x < b.x + bBox.width && b.x < a.x + aBox.width && a.y < b.y + bBox.height && b.y < a.y + aBox.height;

describe("layered auto layout", () => {
  it("lays a linear chain out left to right, one column per node", () => {
    const nodes = ["a", "b", "c"].map((key) => ({ key, box: box() }));
    const positions = layoutGraph(nodes, [
      { source: "a", target: "b" },
      { source: "b", target: "c" }
    ]);

    const [a, b, c] = [positions.get("a")!, positions.get("b")!, positions.get("c")!];
    expect(b.x).toBeGreaterThan(a.x);
    expect(c.x).toBeGreaterThan(b.x);
    expect(a.y).toBe(b.y);
    expect(b.y).toBe(c.y);
    expect(overlap(a, box(), b, box())).toBe(false);
    expect(overlap(b, box(), c, box())).toBe(false);
  });

  it("stacks the two branches of a diamond in one column and the join after it", () => {
    const nodes = ["a", "left", "right", "d"].map((key) => ({ key, box: box() }));
    const positions = layoutGraph(nodes, [
      { source: "a", target: "left" },
      { source: "a", target: "right" },
      { source: "left", target: "d" },
      { source: "right", target: "d" }
    ]);

    const a = positions.get("a")!;
    const left = positions.get("left")!;
    const right = positions.get("right")!;
    const d = positions.get("d")!;
    expect(left.x).toBe(right.x);
    expect(left.y).not.toBe(right.y);
    expect(overlap(left, box(), right, box())).toBe(false);
    expect(d.x).toBeGreaterThan(left.x);
    expect(a.x).toBeLessThan(left.x);
    // The longest path decides the join's column, so it never lands beside a branch.
    expect(d.x).toBeGreaterThan(right.x);
  });

  it("gives a disconnected node its own column instead of dropping it", () => {
    const nodes = ["a", "b", "island"].map((key) => ({ key, box: box() }));
    const positions = layoutGraph(nodes, [{ source: "a", target: "b" }]);

    expect(positions.size).toBe(3);
    const island = positions.get("island")!;
    expect(Number.isFinite(island.x)).toBe(true);
    expect(Number.isFinite(island.y)).toBe(true);
    expect(overlap(island, box(), positions.get("a")!, box())).toBe(false);
  });

  it("terminates on a cycle and keeps the loop's members apart", () => {
    const nodes = ["a", "b", "c"].map((key) => ({ key, box: box() }));
    const positions = layoutGraph(nodes, [
      { source: "a", target: "b" },
      { source: "b", target: "c" },
      { source: "c", target: "a" }
    ]);

    expect(positions.size).toBe(3);
    for (const key of ["a", "b", "c"]) {
      expect(Number.isFinite(positions.get(key)!.x)).toBe(true);
      expect(Number.isFinite(positions.get(key)!.y)).toBe(true);
    }
    // a -> b -> c holds; the closing edge is the back edge and is skipped.
    expect(positions.get("b")!.x).toBeGreaterThan(positions.get("a")!.x);
    expect(positions.get("c")!.x).toBeGreaterThan(positions.get("b")!.x);
  });

  it("keeps a 50-node graph free of overlaps", () => {
    // A wide diamond-ish mesh: 1 head, 3 layers of 16, 1 tail.
    const nodes: { key: string; box: NodeBox }[] = [{ key: "n0", box: box() }];
    const edges: { source: string; target: string }[] = [];
    let previous = ["n0"];
    let counter = 1;
    for (let layer = 0; layer < 3; layer += 1) {
      const current = Array.from({ length: 16 }, () => `n${counter++}`);
      for (const key of current) {
        nodes.push({ key, box: box() });
        for (const source of previous) edges.push({ source, target: key });
      }
      previous = current;
    }
    nodes.push({ key: "tail", box: box() });
    for (const source of previous) edges.push({ source, target: "tail" });
    expect(nodes).toHaveLength(50);

    const positions = layoutGraph(nodes, edges);
    expect(positions.size).toBe(50);
    for (const a of nodes) {
      for (const b of nodes) {
        if (a.key === b.key) continue;
        const overlapBoth = overlap(positions.get(a.key)!, a.box, positions.get(b.key)!, b.box);
        expect(overlapBoth, `${a.key} overlaps ${b.key}`).toBe(false);
      }
    }
  });

  it("reads keys, edges and boxes off a workflow definition", () => {
    const workflow: WorkflowDef = {
      id: "wf-layout",
      name: "Layout",
      nodes: [
        { name: "start", type: "xflow.start" },
        { name: "api", type: "xflow.http" },
        { name: "end", type: "xflow.end" }
      ],
      connections: { start: { main: [{ node: "api" }] }, api: { main: [{ node: "end" }] } }
    };
    const positions = layoutWorkflowPositions(workflow, (key) => (key === "api" ? box(400, 200) : box()));

    expect([...positions.keys()].sort()).toEqual(["api", "end", "start"]);
    expect(positions.get("api")!.x).toBeGreaterThan(positions.get("start")!.x);
    expect(positions.get("end")!.x).toBeGreaterThan(positions.get("api")!.x + 400);
  });

  it("falls back to the canvas card box when nothing measured a node", () => {
    const workflow: WorkflowDef = {
      id: "wf-default",
      name: "Default",
      nodes: [{ name: "a", type: "xflow.start" }, { name: "b", type: "xflow.end" }],
      connections: { a: { main: [{ node: "b" }] } }
    };
    const positions = layoutWorkflowPositions(workflow);
    expect(positions.get("b")!.x).toBeGreaterThanOrEqual(defaultNodeBox.width);
  });
});
