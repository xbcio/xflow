import { getViewportForBounds } from "@xyflow/react";
import { describe, expect, it } from "vitest";
import { FLOAT_GAP, fitViewportClearOf, type FitRect, type FitViewport } from "./fit";

const options = { minZoom: 0.1, maxZoom: 2, padding: 0.18 };
const area = { width: 1200, height: 700 };
const bounds = { x: 0, y: 0, width: 1400, height: 500 };

/** The fitted content's screen box. */
const screenBox = (viewport: FitViewport, rect: FitRect): FitRect => ({
  x: viewport.x + rect.x * viewport.zoom,
  y: viewport.y + rect.y * viewport.zoom,
  width: rect.width * viewport.zoom,
  height: rect.height * viewport.zoom
});

/** Smallest distance between two boxes, negative when they overlap. */
const gapBetween = (a: FitRect, b: FitRect): number => Math.max(
  b.x - (a.x + a.width),
  a.x - (b.x + b.width),
  b.y - (a.y + a.height),
  a.y - (b.y + b.height)
);

// The canvas chrome as §6.2 lays it out on a 1200x700 pane.
const rulerX = { x: 20, y: 0, width: 1180, height: 20 };
const rulerY = { x: 0, y: 20, width: 20, height: 680 };
const corner = { x: 0, y: 0, width: 20, height: 20 };
const zoomBar = { x: 36, y: 650, width: 380, height: 34 };
const minimap = { x: 984, y: 584, width: 200, height: 100 };

describe("fitViewportClearOf", () => {
  it("is xyflow's own fit when nothing floats over the canvas", () => {
    const viewport = fitViewportClearOf(bounds, area, [], options);
    const reference = getViewportForBounds(bounds, area.width, area.height, options.minZoom, options.maxZoom, options.padding);
    expect(viewport?.zoom).toBeCloseTo(reference.zoom, 10);
    expect(viewport?.x).toBeCloseTo(reference.x, 10);
    expect(viewport?.y).toBeCloseTo(reference.y, 10);
  });

  it("keeps the content FLOAT_GAP clear of every floating layer", () => {
    const layers = [corner, rulerX, rulerY, zoomBar, minimap];
    for (const graph of [bounds, { x: -200, y: 40, width: 600, height: 900 }, { x: 0, y: 0, width: 264, height: 80 }]) {
      const viewport = fitViewportClearOf(graph, area, layers, options)!;
      const content = screenBox(viewport, graph);
      for (const layer of layers) {
        expect(gapBetween(content, layer), JSON.stringify({ graph, layer })).toBeGreaterThanOrEqual(FLOAT_GAP - 1e-9);
      }
      // And still inside the pane.
      expect(content.x).toBeGreaterThanOrEqual(0);
      expect(content.y).toBeGreaterThanOrEqual(0);
      expect(content.x + content.width).toBeLessThanOrEqual(area.width);
      expect(content.y + content.height).toBeLessThanOrEqual(area.height);
    }
  });

  it("binds at exactly FLOAT_GAP when padding no longer keeps the content away", () => {
    // With no padding the gap is the only thing between the content and the layers,
    // so each side it binds on lands on exactly 8px.
    const tight = { ...options, padding: 0 };
    const layers = [corner, rulerX, rulerY, zoomBar, minimap];
    const wide = fitViewportClearOf(bounds, area, layers, tight)!;
    const wideBox = screenBox(wide, bounds);
    expect(wideBox.x).toBeCloseTo(rulerY.width + FLOAT_GAP, 9);
    expect(wideBox.x + wideBox.width).toBeCloseTo(area.width, 9);
    const tall = { x: 0, y: 0, width: 300, height: 1400 };
    const tallBox = screenBox(fitViewportClearOf(tall, area, layers, tight)!, tall);
    expect(tallBox.y).toBeCloseTo(rulerX.height + FLOAT_GAP, 9);
    // Clear of the bottom-left zoom bar by the same 8px, however it got there.
    expect(Math.min(...layers.map((layer) => gapBetween(tallBox, layer)))).toBeCloseTo(FLOAT_GAP, 9);
  });

  it("leaves a layer alone when the fitted content does not reach it", () => {
    // A small graph, centred, never comes near a corner minimap: the fit is the plain one.
    const small = { x: 0, y: 0, width: 264, height: 80 };
    const plain = fitViewportClearOf(small, area, [], options);
    expect(fitViewportClearOf(small, area, [minimap], options)).toEqual(plain);
  });

  it("gives up more zoom only for the layers it has to clear", () => {
    const withRulers = fitViewportClearOf(bounds, area, [corner, rulerX, rulerY], options)!;
    const withMinimap = fitViewportClearOf(bounds, area, [corner, rulerX, rulerY, minimap], options)!;
    const plain = fitViewportClearOf(bounds, area, [], options)!;
    expect(withRulers.zoom).toBeLessThanOrEqual(plain.zoom);
    expect(withMinimap.zoom).toBeLessThanOrEqual(withRulers.zoom);
    expect(withMinimap.zoom).toBeGreaterThan(0.1);
  });

  it("ignores layers that are empty or outside the pane, and reports no fit for empty bounds", () => {
    const plain = fitViewportClearOf(bounds, area, [], options);
    expect(fitViewportClearOf(bounds, area, [{ x: 10, y: 10, width: 0, height: 0 }], options)).toEqual(plain);
    expect(fitViewportClearOf(bounds, area, [{ x: 2000, y: 2000, width: 50, height: 50 }], options)).toEqual(plain);
    expect(fitViewportClearOf({ x: 0, y: 0, width: 0, height: 0 }, area, [], options)).toBeUndefined();
  });
});
