/**
 * §6.0 (2026-11-06) 面板拖动宽度：左栏内容区 240（200–400，左轨 60 不动），
 * 属性面板 320（320–520），画布始终保留至少 480。基准页用自己的一套存储键，
 * 不与真实编辑器共用；读到越界的值就夹回范围内。
 */
export type PanelSide = 'left' | 'right';

export interface PanelWidthRule {
  min: number;
  max: number;
  fallback: number;
  storageKey: string;
  label: string;
}

export const PANEL_WIDTHS: Record<PanelSide, PanelWidthRule> = {
  left: { min: 200, max: 400, fallback: 240, storageKey: 'xflow-pv-left-width', label: '调整节点面板宽度' },
  right: { min: 320, max: 520, fallback: 320, storageKey: 'xflow-pv-right-width', label: '调整属性面板宽度' },
};

/** 左轨宽度与画布最小宽度（§6.0）；夹取时先用完正在拖的一侧。 */
export const RAIL_WIDTH = 60;
export const CANVAS_MIN = 480;
export const RESIZE_STEP = 16;

/**
 * `available` 是画布保留 480 后这一侧能拿到的最大值；夹取同时受面板范围约束。
 * 空间不够时先压正在拖的一侧，另一侧不动。
 */
export function clampPanelWidth(side: PanelSide, desired: number, available: number): number {
  const { min, max } = PANEL_WIDTHS[side];
  return Math.max(min, Math.min(max, available, Math.round(desired)));
}

/** 从 localStorage 读宽度；缺省、越界或不可读时回到默认值与范围。 */
export function readStoredWidth(side: PanelSide): number {
  const { min, max, fallback, storageKey } = PANEL_WIDTHS[side];
  if (typeof window === 'undefined') return fallback;
  try {
    const raw = window.localStorage.getItem(storageKey);
    const parsed = raw === null ? Number.NaN : Number(raw);
    return Number.isFinite(parsed) ? Math.max(min, Math.min(max, Math.round(parsed))) : fallback;
  } catch {
    return fallback;
  }
}

export function storePanelWidth(side: PanelSide, width: number): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(PANEL_WIDTHS[side].storageKey, String(width));
  } catch {
    // 隐私模式下 localStorage 可能直接抛错；宽度只是偏好，丢了不影响使用。
  }
}
