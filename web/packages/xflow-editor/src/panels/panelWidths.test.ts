import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  CANVAS_MIN_WIDTH,
  PANEL_RAIL_WIDTH,
  PANEL_WIDTH_LIMITS,
  PANEL_WIDTH_STORAGE_KEY,
  clampPanelWidth,
  fitPanelWidths,
  panelWidthMax,
  readPanelWidths,
  writePanelWidths
} from "./panelWidths";

const DEFAULTS = { left: 240, right: 320 };

class MemoryStorage {
  readonly items = new Map<string, string>();
  get length(): number {
    return this.items.size;
  }
  clear(): void {
    this.items.clear();
  }
  getItem(key: string): string | null {
    return this.items.get(key) ?? null;
  }
  key(index: number): string | null {
    return [...this.items.keys()][index] ?? null;
  }
  removeItem(key: string): void {
    this.items.delete(key);
  }
  setItem(key: string, value: string): void {
    this.items.set(key, String(value));
  }
}

const storage = new MemoryStorage();
const installStorage = (value: unknown) => Object.defineProperty(window, "localStorage", { configurable: true, value });
const original = Object.getOwnPropertyDescriptor(window, "localStorage");

beforeEach(() => {
  storage.clear();
  installStorage(storage);
});

afterEach(() => {
  if (original) Object.defineProperty(window, "localStorage", original);
});

describe("clampPanelWidth", () => {
  it("keeps in-range values and rounds fractional pointer positions", () => {
    expect(clampPanelWidth("left", 240)).toBe(240);
    expect(clampPanelWidth("left", 240.6)).toBe(241);
    expect(clampPanelWidth("right", 519.4)).toBe(519);
  });

  it("clamps each side to its own §6.0 range", () => {
    expect(clampPanelWidth("left", 100)).toBe(PANEL_WIDTH_LIMITS.left.min);
    expect(clampPanelWidth("left", 9999)).toBe(PANEL_WIDTH_LIMITS.left.max);
    expect(clampPanelWidth("right", 100)).toBe(PANEL_WIDTH_LIMITS.right.min);
    expect(clampPanelWidth("right", 9999)).toBe(PANEL_WIDTH_LIMITS.right.max);
    // The inspector cannot shrink below the left column's default width.
    expect(clampPanelWidth("right", 240)).toBe(320);
  });

  it("falls back to the default for non-finite input", () => {
    expect(clampPanelWidth("left", Number.NaN)).toBe(240);
    expect(clampPanelWidth("right", Number.POSITIVE_INFINITY)).toBe(320);
  });
});

describe("panel width storage", () => {
  it("starts at the §6.0 defaults when nothing is stored", () => {
    expect(readPanelWidths()).toEqual(DEFAULTS);
  });

  it("round-trips the shared preference", () => {
    writePanelWidths({ left: 384, right: 480 });
    expect(storage.getItem(PANEL_WIDTH_STORAGE_KEY)).toBe('{"left":384,"right":480}');
    expect(readPanelWidths()).toEqual({ left: 384, right: 480 });
  });

  it("clamps out-of-range stored values instead of dropping them", () => {
    storage.setItem(PANEL_WIDTH_STORAGE_KEY, JSON.stringify({ left: 9999, right: 1 }));
    expect(readPanelWidths()).toEqual({ left: 400, right: 320 });
  });

  it("falls back per side for invalid records", () => {
    storage.setItem(PANEL_WIDTH_STORAGE_KEY, JSON.stringify({ left: "wide", right: 410 }));
    expect(readPanelWidths()).toEqual({ left: 240, right: 410 });
    storage.setItem(PANEL_WIDTH_STORAGE_KEY, "not json");
    expect(readPanelWidths()).toEqual(DEFAULTS);
    storage.setItem(PANEL_WIDTH_STORAGE_KEY, "[]");
    expect(readPanelWidths()).toEqual(DEFAULTS);
  });

  it("survives a storage that denies both operations", () => {
    installStorage({
      getItem() {
        throw new Error("denied");
      },
      setItem() {
        throw new Error("denied");
      }
    });
    expect(readPanelWidths()).toEqual(DEFAULTS);
    expect(() => writePanelWidths({ left: 300, right: 400 })).not.toThrow();
  });
});

describe("fitPanelWidths", () => {
  const fit = (editorWidth: number, left: number, right: number, leftCollapsed = false, rightCollapsed = false) =>
    fitPanelWidths({ widths: { left, right }, editorWidth, leftCollapsed, rightCollapsed });

  it("keeps the stored widths when the canvas floor is met", () => {
    expect(fit(1600, 400, 520)).toEqual({ left: 400, right: 520 });
    expect(fit(0, 400, 520)).toEqual({ left: 400, right: 520 });
  });

  it("shrinks the inspector first, then the left content, to keep 480 for the canvas", () => {
    // 1440 - rail 60 - left 400 - 480 leaves 500 for the inspector.
    expect(fit(1440, 400, 520)).toEqual({ left: 400, right: 500 });
    // 1360 leaves 420; the left column then gives 60 more, to its 200 floor? No:
    // 60 + 400 + 420 + 480 = 1360 exactly, so only the inspector moves.
    expect(fit(1360, 400, 520)).toEqual({ left: 400, right: 420 });
    // Below 60 + 400 + 320 + 480 the left content gives the remainder.
    expect(fit(1250, 400, 520)).toEqual({ left: 390, right: 320 });
  });

  it("counts a collapsed column as its rail (left) or nothing (right)", () => {
    expect(fit(1281, 400, 520, false, true)).toEqual({ left: 400, right: 520 });
    expect(fit(1000, 400, 520, true, false)).toEqual({ left: 400, right: 460 });
  });

  it("never shrinks below the hard minimums", () => {
    const fitted = fit(900, 200, 320);
    expect(fitted).toEqual({ left: 200, right: 320 });
  });

  it("keeps the grid arithmetic at the floor", () => {
    const { left, right } = fit(1440, 400, 520);
    expect(PANEL_RAIL_WIDTH + left + right + CANVAS_MIN_WIDTH).toBeLessThanOrEqual(1440);
  });
});

describe("panelWidthMax", () => {
  it("returns the hard maximum before the editor is measured", () => {
    expect(panelWidthMax("left", 0, 0)).toBe(400);
    expect(panelWidthMax("right", 0, 0)).toBe(520);
  });

  it("reserves the other column plus the 480px canvas", () => {
    expect(panelWidthMax("left", 1440, 520)).toBe(400);
    expect(panelWidthMax("right", 1440, 460)).toBe(500);
    expect(panelWidthMax("right", 1600, 460)).toBe(520);
  });

  it("never goes below the side's own minimum", () => {
    expect(panelWidthMax("right", 900, 300)).toBe(320);
  });
});
