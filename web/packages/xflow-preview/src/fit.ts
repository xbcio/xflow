/**
 * Fitting the graph around the canvas's floating layers (§6.2: the rulers, the zoom
 * bar and the minimap may never cover content, and keep at least FLOAT_GAP px from it).
 * Pure geometry in screen px, so it is tested without a canvas.
 */

/** Screen px kept between fitted content and any floating layer. */
export const FLOAT_GAP = 8;

export interface FitRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface FitViewport {
  x: number;
  y: number;
  zoom: number;
}

export interface FitOptions {
  minZoom: number;
  maxZoom: number;
  /** A numeric padding, read the way xyflow's fitBounds reads one. */
  padding: number;
}

interface Insets {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

const intersects = (a: FitRect, b: FitRect) =>
  a.x < b.x + b.width && b.x < a.x + a.width && a.y < b.y + b.height && b.y < a.y + a.height;

/**
 * Centres `bounds` in a `width` x `height` box. A numeric padding means what it means
 * to xyflow's fitBounds (`floor(v - v / (1 + p))` px per side), so a canvas with no
 * floating layers lands exactly where the plain fit used to put it.
 */
function centredViewport(bounds: FitRect, width: number, height: number, options: FitOptions): FitViewport {
  const padX = Math.floor(width - width / (1 + options.padding));
  const padY = Math.floor(height - height / (1 + options.padding));
  const fitted = Math.min((width - 2 * padX) / bounds.width, (height - 2 * padY) / bounds.height);
  const zoom = Math.min(Math.max(fitted, options.minZoom), options.maxZoom);
  return {
    x: width / 2 - (bounds.x + bounds.width / 2) * zoom,
    y: height / 2 - (bounds.y + bounds.height / 2) * zoom,
    zoom
  };
}

/**
 * The viewport that fits `bounds` (canvas units) into `area`, clear of every layer
 * in `layers` (screen px, relative to the canvas) by FLOAT_GAP. Each layer is either
 * reserved — as a strip along the canvas edge it sits nearest, top/bottom or
 * left/right — or left alone when the fitted content does not reach it. Every
 * combination is tried; the largest zoom wins, and a tie goes to the larger free
 * area, which keeps the graph nearer the canvas centre. Undefined when no
 * combination leaves any room.
 */
export function fitViewportClearOf(
  bounds: FitRect,
  area: { width: number; height: number },
  layers: readonly FitRect[],
  options: FitOptions
): FitViewport | undefined {
  const obstacles = layers
    .filter((layer) => layer.width > 0 && layer.height > 0)
    .map((layer) => ({
      x: layer.x - FLOAT_GAP,
      y: layer.y - FLOAT_GAP,
      width: layer.width + 2 * FLOAT_GAP,
      height: layer.height + 2 * FLOAT_GAP
    }))
    .filter((layer) => intersects(layer, { x: 0, y: 0, ...area }));
  let best: { viewport: FitViewport; free: number } | undefined;

  const evaluate = (insets: Insets, untouched: readonly FitRect[]) => {
    const width = area.width - insets.left - insets.right;
    const height = area.height - insets.top - insets.bottom;
    if (width <= 0 || height <= 0) return;
    const centred = centredViewport(bounds, width, height, options);
    const viewport = { x: centred.x + insets.left, y: centred.y + insets.top, zoom: centred.zoom };
    const content = {
      x: viewport.x + bounds.x * viewport.zoom,
      y: viewport.y + bounds.y * viewport.zoom,
      width: bounds.width * viewport.zoom,
      height: bounds.height * viewport.zoom
    };
    if (untouched.some((layer) => intersects(layer, content))) return;
    const free = width * height;
    if (!best || viewport.zoom > best.viewport.zoom || (viewport.zoom === best.viewport.zoom && free > best.free)) {
      best = { viewport, free };
    }
  };

  const choose = (index: number, insets: Insets, untouched: readonly FitRect[]) => {
    const layer = obstacles[index];
    if (!layer) {
      evaluate(insets, untouched);
      return;
    }
    choose(index + 1, insets, [...untouched, layer]);
    const nearTop = layer.y + layer.height / 2 < area.height / 2;
    choose(index + 1, nearTop
      ? { ...insets, top: Math.max(insets.top, layer.y + layer.height) }
      : { ...insets, bottom: Math.max(insets.bottom, area.height - layer.y) }, untouched);
    const nearLeft = layer.x + layer.width / 2 < area.width / 2;
    choose(index + 1, nearLeft
      ? { ...insets, left: Math.max(insets.left, layer.x + layer.width) }
      : { ...insets, right: Math.max(insets.right, area.width - layer.x) }, untouched);
  };

  if (bounds.width <= 0 || bounds.height <= 0) return undefined;
  choose(0, { top: 0, right: 0, bottom: 0, left: 0 }, []);
  return best?.viewport;
}
