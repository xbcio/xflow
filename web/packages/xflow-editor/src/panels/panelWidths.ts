/**
 * Dock-side panel widths (§6.0「拖动调整宽度」, 2026-11-06).
 *
 * One preference for every workflow: the two docked columns are a property of
 * the user, not of the workflow on screen, so the reader and writer here take no
 * workflow id. The values are the *content* widths — the left column is the rail
 * plus this width — because that is what the resize handles report and what the
 * stylesheet's `--xflow-editor-panel-left-content-width` names.
 */

/** The left rail never resizes; it is part of the docked left column's total. */
export const PANEL_RAIL_WIDTH = 60;

/** §6.0: the canvas keeps this much inline size before a dragged panel stops. */
export const CANVAS_MIN_WIDTH = 480;

/** One arrow-key press on a focused handle (§6.0). */
export const PANEL_RESIZE_KEY_STEP = 16;

export type PanelSide = "left" | "right";

export interface PanelWidthLimits {
  defaultValue: number;
  min: number;
  max: number;
}

/** §6.0: left content 240 (200–400); inspector 320 (320–520). */
export const PANEL_WIDTH_LIMITS: Record<PanelSide, PanelWidthLimits> = {
  left: { defaultValue: 240, min: 200, max: 400 },
  right: { defaultValue: 320, min: 320, max: 520 }
};

/** Shared by every workflow; not namespaced by id (§6.0). */
export const PANEL_WIDTH_STORAGE_KEY = "xflow-editor:panel-widths";

export interface PanelWidths {
  left: number;
  right: number;
}

/** Whole pixels only: pointer moves are fractional on a scaled display. */
export function clampPanelWidth(side: PanelSide, width: number): number {
  const limits = PANEL_WIDTH_LIMITS[side];
  if (!Number.isFinite(width)) return limits.defaultValue;
  return Math.min(limits.max, Math.max(limits.min, Math.round(width)));
}

/**
 * Read the shared preference. A missing, unparsable or partly invalid record
 * falls back per side; a valid but out-of-range number is clamped, not dropped
 * (§6.0). Storage can be denied (private mode), which is the same as absent.
 */
export function readPanelWidths(): PanelWidths {
  const fallback: PanelWidths = {
    left: PANEL_WIDTH_LIMITS.left.defaultValue,
    right: PANEL_WIDTH_LIMITS.right.defaultValue
  };
  if (typeof window === "undefined") return fallback;
  try {
    const raw = window.localStorage.getItem(PANEL_WIDTH_STORAGE_KEY);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw) as Record<string, unknown> | null;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return fallback;
    return {
      left: typeof parsed.left === "number" ? clampPanelWidth("left", parsed.left) : fallback.left,
      right: typeof parsed.right === "number" ? clampPanelWidth("right", parsed.right) : fallback.right
    };
  } catch {
    return fallback;
  }
}

export function writePanelWidths(widths: PanelWidths): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(PANEL_WIDTH_STORAGE_KEY, JSON.stringify(widths));
  } catch {
    // Storage can be full or blocked; the in-memory widths still work.
  }
}

export interface PanelFitInput {
  widths: PanelWidths;
  /** The editor root's content width; 0 while unmeasured. */
  editorWidth: number;
  leftCollapsed: boolean;
  rightCollapsed: boolean;
}

/**
 * The widths the docked grid actually uses. They differ from the stored
 * preference only when the workbench is too narrow for both panels plus the
 * 480px canvas — possible after the window shrinks, since the compact layout
 * starts below 1280. The inspector yields first (it is the narrower range and
 * the left column is the navigation surface), then the left content, each down
 * to its own minimum; a docked editor is always wide enough for both minima.
 *
 * The stored preference is not rewritten: widening the window gives the user
 * back the widths they chose.
 */
export function fitPanelWidths({ widths, editorWidth, leftCollapsed, rightCollapsed }: PanelFitInput): PanelWidths {
  if (editorWidth <= 0) return widths;
  let left = clampPanelWidth("left", widths.left);
  let right = clampPanelWidth("right", widths.right);
  const occupied = () => PANEL_RAIL_WIDTH + (leftCollapsed ? 0 : left) + (rightCollapsed ? 0 : right);
  let overflow = occupied() + CANVAS_MIN_WIDTH - editorWidth;
  if (overflow > 0 && !rightCollapsed) {
    const reduction = Math.min(overflow, Math.max(0, right - PANEL_WIDTH_LIMITS.right.min));
    right -= reduction;
    overflow -= reduction;
  }
  if (overflow > 0 && !leftCollapsed) {
    left -= Math.min(overflow, Math.max(0, left - PANEL_WIDTH_LIMITS.left.min));
  }
  return { left, right };
}

/**
 * The largest width the dragged side may take right now, given the other
 * column's current occupancy (§6.0: 「空间不够时先限制正在拖的这一侧」).
 * Unmeasured (0) keeps the hard maximum, the same first-paint assumption the
 * layout tier makes.
 */
export function panelWidthMax(side: PanelSide, editorWidth: number, otherOccupied: number): number {
  const limits = PANEL_WIDTH_LIMITS[side];
  if (editorWidth <= 0) return limits.max;
  const available = Math.floor(editorWidth - otherOccupied - CANVAS_MIN_WIDTH);
  return Math.max(limits.min, Math.min(limits.max, available));
}
