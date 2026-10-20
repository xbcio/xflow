/**
 * Canvas metrics shared by the workbench canvas and the run bar wall (§6.2, §6.3).
 *
 * The rule they encode: the floating row at the top of the canvas must never
 * cover canvas content. A fitView reserves the row's height plus a gap as top
 * padding, which is why `canvasPanY` grows as the zoom shrinks — the padding is
 * in screen pixels, not canvas units.
 */
export const RULER = 20;
export const FLOAT_TOP = 12;
export const FLOAT_LEFT = 16;
export const FLOAT_HEIGHT = 34;
export const CONTENT_TOP_GAP = 16;
/**
 * Canvas units a frame label occupies above its frame's top edge
 * (`.xflow-pv-frame > span`: 22px chip + 4px gap). The fit reserves it so no
 * label is ever clipped by the top of the viewport.
 */
export const FRAME_LABEL_OFFSET = 26;

/**
 * y of a handle's centre inside its card, measured from the card's border box
 * (`.xflow-pv-handle`: `top: 24px` from the padding box, + 1px card border, +
 * half of the 10px handle). Edges attach to the handle, so this — not a
 * hand-picked 24 — is the y a horizontal route runs at.
 */
export const HANDLE_Y = 30;

export const floatRowTop = RULER + FLOAT_TOP;
export const floatRowBottom = floatRowTop + FLOAT_HEIGHT;

/** Screen px the canvas keeps free on the right and bottom for fitted content. */
export const FIT_MARGIN = 16;
/** Smallest zoom the fit will produce before it stops being a view at all. */
export const MIN_SCALE = 0.1;

/**
 * Compact-layout breakpoints (§6.0.1). They are measured on the workbench shell
 * rather than the window, so an embedded workbench collapses with the box it was
 * given; `service/` and the real editor read the same pair from xflow-core.
 */
export const COMPACT_LEFT = 1280;
export const COMPACT_RIGHT = 1024;

/**
 * Vertical pan (screen px) that keeps the topmost canvas content at least
 * `CONTENT_TOP_GAP` below the floating row.
 */
export function canvasPanY(scale: number, minContentTop: number): number {
  return Math.max(0, floatRowBottom + CONTENT_TOP_GAP - RULER - minContentTop * scale);
}

export interface ContentBox {
  /** Right edge of the content in canvas units; the origin is the left edge. */
  maxX: number;
  minY: number;
  maxY: number;
}

/**
 * Zoom that fits `content` into `canvas` with `FIT_MARGIN` on the right and the
 * bottom (fitView, §6.2). The horizontal room is `canvas.width` minus the ruler
 * and the margin — the viewport origin is pinned to the ruler corner, so the
 * content grows rightwards and downwards from there. The vertical room is the
 * same minus the floating row's reserved strip, which `canvasPanY` reproduces
 * in canvas units.
 *
 * `max` keeps a fit from magnifying: the workbench opens at the fitted zoom and
 * never past 1:1.
 */
export function fitScale(canvas: { width: number; height: number }, content: ContentBox, max = 1): number {
  if (canvas.width <= 0 || canvas.height <= 0 || content.maxX <= 0) return max;
  const spanY = Math.max(1, content.maxY - content.minY);
  return Math.max(
    MIN_SCALE,
    Math.min(
      max,
      (canvas.width - RULER - FIT_MARGIN) / content.maxX,
      (canvas.height - RULER - FIT_MARGIN - (floatRowBottom + CONTENT_TOP_GAP - RULER)) / spanY,
    ),
  );
}

/** `mm:ss`, the run bar's only clock format (§6.3). */
export function formatElapsed(seconds: number): string {
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return `${String(minutes).padStart(2, '0')}:${String(rest).padStart(2, '0')}`;
}
