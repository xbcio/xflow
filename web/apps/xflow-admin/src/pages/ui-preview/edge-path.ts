import { HANDLE_Y } from './metrics';

/**
 * Where an edge meets its two cards, in canvas units.
 *
 * Geometry, not data: an edge is a *relation* between two cards, and the cards
 * decide where it starts and ends (their measured rects), never a `d` string
 * written by hand. The hand-written version drifted the moment a card changed
 * height — the replay's `e6` started 19 units below 主管审批 and its `e3`/`e4`
 * began underneath a card — and nothing in the page could notice, because the
 * numbers and the cards were two independent copies of the same layout.
 *
 * Every route below puts both endpoints **on** a card border, which is what S2
 * measures (≤2 units): a route that ends inside a card, or short of one, is a
 * routing bug rather than a drawing style.
 */
export interface EdgeRect {
  /** 卡片 id：`edgePathOf` 靠它把一条边解析到两端的卡片。 */
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

/** A port chip's centre as a fraction (0..1) of its card's width. */
export interface PortAnchor {
  node: string;
  name: string;
  frac: number;
}

export type EdgeRoute =
  /** 同一行的两列：右缘出句柄 → 左缘入句柄，一条水平线（两端 y 相同）。 */
  | { kind: 'h' }
  /** 同一列的两行：源卡下缘 → 目标卡上缘，沿卡片中线竖直向下。 */
  | { kind: 'v' }
  /**
   * 跨列下行：从源卡下缘落到 `lane` 高度、横穿、再落到目标卡上缘。
   * `lane` 是走线走廊的 y（§6.2 里两个分区框之间那条 16 单位宽的缝），
   * 由布局决定；`fromPort` 是这条线离开的出口 chip 名（实测其中心），
   * `toFrac` 是落到目标卡上缘的位置（0.5 = 中线）。
   */
  | { kind: 'elbow'; lane: number; fromPort: string; toFrac: number };

const round = (value: number) => Math.round(value);

/**
 * The path for one edge. `fromPortX` is the measured x of the port chip the edge
 * leaves, when the caller has one; without it the route falls back to the card's
 * own bottom edge at the same fraction the port row would put it (the caller
 * passes nothing when the node draws no ports).
 */
export function edgePath(from: EdgeRect, to: EdgeRect, route: EdgeRoute, fromPortX?: number): string {
  const outX = from.x + from.w;
  const outY = from.y + HANDLE_Y;
  switch (route.kind) {
    case 'h':
      return `M${round(outX)} ${round(outY)} L ${round(to.x)} ${round(outY)}`;
    case 'v': {
      // 同列路由：两卡中线相同，这条线才是竖直的（不同列要用 elbow）。
      const x = round(from.x + from.w / 2);
      return `M${x} ${round(from.y + from.h)} L ${x} ${round(to.y)}`;
    }
    case 'elbow': {
      const sx = round(fromPortX ?? from.x + from.w * 0.5);
      const tx = round(to.x + to.w * route.toFrac);
      const sy = round(from.y + from.h);
      const ty = round(to.y);
      const r = 8;
      const dir = tx < sx ? -1 : 1;
      return (
        `M${sx} ${sy} V ${round(route.lane - r)} Q ${sx} ${round(route.lane)} ${sx + dir * r} ${round(route.lane)}` +
        ` H ${tx - dir * r} Q ${tx} ${round(route.lane)} ${tx} ${round(route.lane + r)} V ${ty}`
      );
    }
  }
}

/**
 * Resolve one edge against what the canvas measured. Returns `null` before the
 * first measurement (or for an edge whose route is not declared), and the caller
 * then draws nothing for that frame — the same "not measurable yet" state the
 * minimap and the fit are in.
 *
 * Structural typing on purpose: `data.ts` imports {@link EdgeRoute}, so taking
 * its `PreviewEdge` here would close an import cycle.
 */
export function edgePathOf(
  edge: { from?: string; to?: string; route?: EdgeRoute },
  rects: EdgeRect[],
  ports: PortAnchor[],
): string | null {
  const route = edge.route;
  if (!route || !edge.from || !edge.to) return null;
  const from = rects.find((rect) => rect.id === edge.from);
  const to = rects.find((rect) => rect.id === edge.to);
  if (!from || !to) return null;
  const chip =
    route.kind === 'elbow'
      ? ports.find((port) => port.node === edge.from && port.name === route.fromPort)
      : undefined;
  const chipX = chip ? from.x + from.w * chip.frac : undefined;
  return edgePath(from, to, route, chipX);
}
