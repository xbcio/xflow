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

export interface PanelFitInput {
  /** 面板可分配的总宽 = 外壳宽 − 2px 边框 − 左轨；≤0 表示尚未测量。 */
  room: number;
  left: number;
  right: number;
  leftDocked: boolean;
  rightDocked: boolean;
}

/**
 * 窗口变窄时的回缩（§6.0，2026-11-07，以真实编辑器为准）：先压属性面板，再压
 * 左栏。这里只算当前该显示多宽 —— 入参是用户的存储偏好，函数不改写它们，窗口
 * 变宽后自然回到偏好值。
 */
export function fitPanelWidths({ room, left, right, leftDocked, rightDocked }: PanelFitInput): { left: number; right: number } {
  let nextLeft = left;
  let nextRight = right;
  if (room <= 0) return { left: nextLeft, right: nextRight };
  const dockedTotal = (leftDocked ? nextLeft : 0) + (rightDocked ? nextRight : 0);
  let overflow = dockedTotal + CANVAS_MIN - room;
  if (overflow <= 0) return { left: nextLeft, right: nextRight };
  if (rightDocked) {
    const give = Math.min(overflow, Math.max(0, nextRight - PANEL_WIDTHS.right.min));
    nextRight -= give;
    overflow -= give;
  }
  if (leftDocked && overflow > 0) {
    const give = Math.min(overflow, Math.max(0, nextLeft - PANEL_WIDTHS.left.min));
    nextLeft -= give;
    overflow -= give;
  }
  return { left: nextLeft, right: nextRight };
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
