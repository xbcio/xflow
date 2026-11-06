import { describe, expect, it } from "vitest";
import { foundationTokens } from "@xflow/core";
import { defaultNodeBox } from "./autoLayout";
import { PLACEMENT_GAP, placeNewNode, placementClear, type PlacementRect } from "./placement";

const box = (x: number, y: number, width = defaultNodeBox.width, height = defaultNodeBox.height): PlacementRect => ({
  x,
  y,
  width,
  height
});

/** Smallest distance between two boxes, negative when they overlap. */
const gapBetween = (a: PlacementRect, b: PlacementRect): number => Math.max(
  b.x - (a.x + a.width),
  a.x - (b.x + b.width),
  b.y - (a.y + a.height),
  a.y - (b.y + b.height)
);

describe("placeNewNode", () => {
  it("puts a node one gap downstream of the selection", () => {
    const selected = box(100, 200);
    const placed = placeNewNode({ boxes: [selected], selected });
    expect(placed).toEqual({ x: 100 + defaultNodeBox.width + PLACEMENT_GAP, y: 200 });
  });

  it("keeps its distance from every existing card", () => {
    const boxes = [box(0, 0), box(400, 0), box(400, 64 + PLACEMENT_GAP), box(-400, 200)];
    const selected = box(0, 0);
    const placed = placeNewNode({ boxes, selected });
    const candidate: PlacementRect = { ...placed, ...defaultNodeBox };
    expect(placementClear(candidate, boxes, PLACEMENT_GAP)).toBe(true);
    for (const existing of boxes) expect(gapBetween(candidate, existing)).toBeGreaterThanOrEqual(PLACEMENT_GAP);
  });

  it("walks down the downstream column before trying the next one", () => {
    const selected = box(0, 0);
    // The slot directly downstream is taken; the one below it is free.
    const occupied = box(defaultNodeBox.width + PLACEMENT_GAP, 0);
    const placed = placeNewNode({ boxes: [selected, occupied], selected });
    expect(placed.x).toBe(occupied.x);
    expect(placed.y).toBe(defaultNodeBox.height + PLACEMENT_GAP);
  });

  it("moves to the next column when the whole downstream column is taken", () => {
    const selected = box(0, 0);
    const column = defaultNodeBox.width + PLACEMENT_GAP;
    const step = defaultNodeBox.height + PLACEMENT_GAP;
    const boxes = [selected, ...Array.from({ length: 25 }, (_, index) => box(column, (index - 12) * step))];
    const placed = placeNewNode({ boxes, selected });
    expect(placed.x).toBe(column + defaultNodeBox.width + PLACEMENT_GAP);
  });

  it("falls back to the viewport centre when nothing is selected", () => {
    const placed = placeNewNode({ boxes: [], viewportCenter: { x: 1000, y: 500 } });
    expect(placed).toEqual({
      x: Math.round(1000 - defaultNodeBox.width / 2),
      y: Math.round(500 - defaultNodeBox.height / 2)
    });
  });

  it("spirals away from the centre to the nearest free slot", () => {
    const center = { x: 1000, y: 500 };
    const origin = {
      x: Math.round(center.x - defaultNodeBox.width / 2),
      y: Math.round(center.y - defaultNodeBox.height / 2)
    };
    const occupied = { ...origin, ...defaultNodeBox };
    const placed = placeNewNode({ boxes: [occupied], viewportCenter: center });
    expect(placed.x).toBe(origin.x + defaultNodeBox.width + PLACEMENT_GAP);
    expect(placed.y).toBe(origin.y);
  });

  it("uses the canvas card width, not a guess", () => {
    expect(defaultNodeBox.width).toBe(parseInt(foundationTokens.nodeWidth, 10));
    expect(defaultNodeBox.width).toBe(264);
  });
});
