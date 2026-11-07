import * as React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LEFT_SPLIT_STORAGE_KEY, LeftZones, OUTLINE_MIN_HEIGHT, SPLIT_KEYBOARD_STEP } from "./leftZones";

/** A Storage the tests own; the jsdom environment may ship none at all. */
function memoryStorage(seed: Record<string, string> = {}): Storage {
  const values = new Map(Object.entries(seed));
  return {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => values.delete(key),
    setItem: (key, value) => values.set(key, String(value))
  };
}

const COLUMN_HEIGHT = 800;
const autoFloor = OUTLINE_MIN_HEIGHT / COLUMN_HEIGHT;
/** What the auto-mode outline measures: its tree, well above the floor. */
const OUTLINE_AUTO_HEIGHT = 300;

/** jsdom has no layout: measure the column and the rule by their own hooks. */
function mockBoxes(outlineRatio?: number) {
  return vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
    if (this.classList.contains("xflow-editor-lib__divider")) {
      const bottom = COLUMN_HEIGHT * (1 - (outlineRatio ?? autoFloor));
      return { bottom, height: 8, left: 0, right: 240, top: bottom - 8, width: 240, x: 0, y: bottom - 8, toJSON: () => ({}) } as DOMRect;
    }
    // The outline zone is the split's last child; the library sits above it.
    if (this.parentElement?.classList.contains("xflow-editor-lib") && this === this.parentElement.lastElementChild) {
      return { bottom: OUTLINE_AUTO_HEIGHT, height: OUTLINE_AUTO_HEIGHT, left: 0, right: 240, top: 0, width: 240, x: 0, y: 0, toJSON: () => ({}) } as DOMRect;
    }
    return { bottom: COLUMN_HEIGHT, height: COLUMN_HEIGHT, left: 0, right: 240, top: 0, width: 240, x: 0, y: 0, toJSON: () => ({}) } as DOMRect;
  });
}

const zones = () => screen.getByRole("complementary", { name: "节点与大纲" });
const divider = () => screen.getByRole("separator", { name: "调整大纲高度" });
/** jsdom's Event ignores clientY, so pointer gestures arrive as MouseEvents. */
const pointer = (type: "pointerDown" | "pointerMove" | "pointerUp", clientY: number) =>
  fireEvent(divider(), new MouseEvent(type.toLowerCase(), { bubbles: true, cancelable: true, clientY }));

function renderZones() {
  return render(
    <LeftZones dual library={<div>library</div>} outline={<div>outline</div>} />
  );
}

beforeEach(() => {
  Object.defineProperty(window, "localStorage", { configurable: true, value: memoryStorage() });
});

afterEach(() => {
  delete (window as unknown as { localStorage?: Storage }).localStorage;
  vi.restoreAllMocks();
});

describe("left column split (§6.0.1)", () => {
  it("starts in auto mode: the outline fits its content, the library takes the rest", () => {
    renderZones();

    expect(zones().getAttribute("data-split")).toBe("auto");
    expect(zones().getAttribute("data-zones")).toBe("dual");
    expect(zones().style.gridTemplateRows).toBe("minmax(0, 1fr) 8px fit-content(50%)");
    expect(divider().getAttribute("aria-orientation")).toBe("horizontal");
    expect(divider().getAttribute("aria-valuenow")).toBeNull();
    expect(divider().tabIndex).toBe(0);
  });

  it("restores a stored ratio, reads it as a percentage and exposes aria-valuenow", () => {
    window.localStorage.setItem(LEFT_SPLIT_STORAGE_KEY, "0.3");
    renderZones();

    expect(zones().getAttribute("data-split")).toBe("manual");
    expect(zones().style.gridTemplateRows).toBe("minmax(0, 1fr) 8px 30.00%");
    expect(divider().getAttribute("aria-valuenow")).toBe("30");
  });

  it("moves the outline by one 28px row per arrow key from its real auto height", () => {
    mockBoxes();
    renderZones();

    // Up from auto: the outline's tree height (300) plus one row.
    fireEvent.keyDown(divider(), { key: "ArrowUp" });
    expect(divider().getAttribute("aria-valuenow")).toBe("41");
    expect(Number(window.localStorage.getItem(LEFT_SPLIT_STORAGE_KEY))).toBeCloseTo(
      (OUTLINE_AUTO_HEIGHT + SPLIT_KEYBOARD_STEP) / COLUMN_HEIGHT
    );

    // Down from auto: the tree height minus one row.
    window.localStorage.clear();
    fireEvent.doubleClick(divider());
    expect(zones().getAttribute("data-split")).toBe("auto");
    fireEvent.keyDown(divider(), { key: "ArrowDown" });
    expect(divider().getAttribute("aria-valuenow")).toBe("34");
    expect(Number(window.localStorage.getItem(LEFT_SPLIT_STORAGE_KEY))).toBeCloseTo(
      (OUTLINE_AUTO_HEIGHT - SPLIT_KEYBOARD_STEP) / COLUMN_HEIGHT
    );
  });

  it("drags the rule: the outline grows when the pointer moves up", () => {
    mockBoxes(0.3);
    window.localStorage.setItem(LEFT_SPLIT_STORAGE_KEY, "0.3");
    renderZones();

    pointer("pointerDown", 500);
    expect(divider().getAttribute("data-dragging")).toBe("true");
    pointer("pointerMove", 480);
    pointer("pointerUp", 480);

    expect(divider().getAttribute("data-dragging")).toBeNull();
    // 20px more outline over an 800px column.
    expect(divider().getAttribute("aria-valuenow")).toBe("33");
    expect(Number(window.localStorage.getItem(LEFT_SPLIT_STORAGE_KEY))).toBeCloseTo(0.325);
  });

  it("returns to auto on a double click and forgets the stored ratio", () => {
    mockBoxes();
    renderZones();
    fireEvent.keyDown(divider(), { key: "ArrowUp" });
    expect(zones().getAttribute("data-split")).toBe("manual");

    fireEvent.doubleClick(divider());
    expect(zones().getAttribute("data-split")).toBe("auto");
    expect(window.localStorage.getItem(LEFT_SPLIT_STORAGE_KEY)).toBeNull();
  });

  it("draws no rule when there is only one zone", () => {
    render(<LeftZones dual={false} outline={<div>outline</div>} />);

    expect(zones().getAttribute("data-zones")).toBe("single");
    expect(screen.queryByRole("separator", { name: "调整大纲高度" })).toBeNull();
  });
});
