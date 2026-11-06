import { beforeEach, describe, expect, it } from "vitest";

import {
  CANVAS_MIN,
  PANEL_WIDTHS,
  RAIL_WIDTH,
  clampPanelWidth,
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
