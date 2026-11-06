import { beforeEach, describe, expect, it } from "vitest";

import {
  CANVAS_MIN,
  PANEL_WIDTHS,
  RAIL_WIDTH,
  clampPanelWidth,
  fitPanelWidths,
  readStoredWidth,
  storePanelWidth,
} from "./panel-resize";

describe("面板拖动宽度（§6.0，2026-11-06）", () => {
  // 这个 jsdom 环境不提供 localStorage（不透明 origin），装一个内存实现来验证
  // 我们的读写与夹取；不可读的情况在最后一个用例里单独模拟。
  const memory = new Map<string, string>();
  const storage = {
    getItem: (key: string) => memory.get(key) ?? null,
    setItem: (key: string, value: string) => void memory.set(key, String(value)),
    removeItem: (key: string) => void memory.delete(key),
    clear: () => memory.clear(),
  } as unknown as Storage;

  beforeEach(() => {
    memory.clear();
    Object.defineProperty(window, "localStorage", { configurable: true, value: storage });
  });

  it("默认宽度在范围内，存储键带 xflow-pv 前缀", () => {
    for (const side of ["left", "right"] as const) {
      const rule = PANEL_WIDTHS[side];
      expect(rule.fallback).toBeGreaterThanOrEqual(rule.min);
      expect(rule.fallback).toBeLessThanOrEqual(rule.max);
      expect(rule.storageKey.startsWith("xflow-pv-")).toBe(true);
    }
    expect(PANEL_WIDTHS.left.fallback).toBe(240);
    expect(PANEL_WIDTHS.right.fallback).toBe(320);
  });

  it("超出面板范围时夹到 200–400 / 320–520", () => {
    expect(clampPanelWidth("left", 100, 1000)).toBe(200);
    expect(clampPanelWidth("left", 900, 1000)).toBe(400);
    expect(clampPanelWidth("right", 100, 1000)).toBe(320);
    expect(clampPanelWidth("right", 900, 1000)).toBe(520);
  });

  it("画布保留 480：空间不够时先压正在拖的一侧", () => {
    // 1600 − 2 边框 − 60 轨道 − 480 画布 = 1058，左栏仍取自己的上限 400。
    expect(clampPanelWidth("left", 400, 1600 - 2 - RAIL_WIDTH - CANVAS_MIN)).toBe(400);
    // 右侧固定 520 时，左栏只剩 219（1281 − 2 − 60 − 520 − 480）。
    expect(clampPanelWidth("left", 400, 1281 - 2 - RAIL_WIDTH - 520 - CANVAS_MIN)).toBe(219);
  });

  it("读取时夹取越界值，坏值回到默认，写回后可读", () => {
    window.localStorage.setItem(PANEL_WIDTHS.left.storageKey, "9999");
    expect(readStoredWidth("left")).toBe(400);
    window.localStorage.setItem(PANEL_WIDTHS.right.storageKey, "not-a-number");
    expect(readStoredWidth("right")).toBe(320);
    storePanelWidth("right", 480);
    expect(readStoredWidth("right")).toBe(480);
  });

  it("localStorage 不可读时回到默认宽度且不抛错", () => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      get() {
        throw new Error("blocked");
      },
    });
    expect(readStoredWidth("left")).toBe(240);
    expect(readStoredWidth("right")).toBe(320);
    expect(() => storePanelWidth("left", 300)).not.toThrow();
  });
});

describe("窗口变窄的回缩顺序（§6.0，2026-11-07，以真实编辑器为准）", () => {
  const docked = { leftDocked: true, rightDocked: true };
  const fit = (room: number, left: number, right: number) =>
    fitPanelWidths({ room, left, right, ...docked });

  it("放得下时两侧都保持存值", () => {
    // 存值 400 + 520 加画布 480 = 1400，room 1538（1600 宽的外壳）足够。
    expect(fit(1538, 400, 520)).toEqual({ left: 400, right: 520 });
  });

  it("先压属性面板，左栏不动", () => {
    expect(fit(1360, 400, 520)).toEqual({ left: 400, right: 480 });
  });

  it("属性面板压到 320 后才开始压左栏", () => {
    expect(fit(1200, 400, 520)).toEqual({ left: 400, right: 320 });
    expect(fit(1100, 400, 520)).toEqual({ left: 300, right: 320 });
  });

  it("两侧都到下限后不再继续压（画布可以小于 480）", () => {
    expect(fit(900, 400, 520)).toEqual({ left: 200, right: 320 });
  });

  it("只计算停靠的一侧，且不改写存值：变宽后回到原值", () => {
    // 左栏收起时它不占列，room 1100 装得下 520 + 480。
    expect(
      fitPanelWidths({ room: 1100, left: 400, right: 520, leftDocked: false, rightDocked: true }),
    ).toEqual({ left: 400, right: 520 });
    // room 900 只差 100，属性面板让 100 到 420，不必压到下限。
    expect(
      fitPanelWidths({ room: 900, left: 400, right: 520, leftDocked: false, rightDocked: true }),
    ).toEqual({ left: 400, right: 420 });
    const stored = { room: 1100, left: 400, right: 520, ...docked };
    expect(fitPanelWidths(stored)).toEqual({ left: 300, right: 320 });
    expect(stored).toEqual({ room: 1100, left: 400, right: 520, ...docked });
    expect(fitPanelWidths({ ...stored, room: 1538 })).toEqual({ left: 400, right: 520 });
  });

  it("尚未量到宽度时原样返回", () => {
    expect(fit(0, 400, 520)).toEqual({ left: 400, right: 520 });
  });
});
