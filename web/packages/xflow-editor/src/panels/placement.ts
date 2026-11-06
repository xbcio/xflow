// Where a node the user adds from the library lands (§6.2). Placement is pure
// geometry: the editor measures the existing cards and passes their boxes in,
// so the same function serves the real canvas and the tests.
//
// With a selected node the search starts one gap downstream of it, in the
// canvas's flow direction (auto layout columns run left to right), and walks
// that column downwards, then upwards, before trying the next column. Without
// a selection it starts at the viewport centre and spirals out. A candidate is
// accepted only when it keeps at least `gap` px between itself and every
// existing card.

import { defaultNodeBox, type NodeBox } from "./autoLayout";

/** Clear space kept between the new card and every existing one. */
export const PLACEMENT_GAP = 48;

/** How far the search walks before giving up (it always terminates, this only bounds it). */
const SEARCH_REACH = 24;

export interface PlacementRect extends NodeBox {
  x: number;
  y: number;
}

export interface PlacementOptions {
  /** Boxes the new node must keep clear of, in canvas units. */
  boxes: readonly PlacementRect[];
  /** The selected node's box; its downstream side anchors the search. */
  selected?: PlacementRect;
  /** Flow coordinates of the viewport centre, used when nothing is selected. */
  viewportCenter?: { x: number; y: number };
  /** The new card's size; defaults to the canvas card's own measured default. */
  size?: NodeBox;
  gap?: number;
}

/** True when `candidate` is at least `gap` px away from every box. */
export function placementClear(
  candidate: PlacementRect,
  boxes: readonly PlacementRect[],
  gap = PLACEMENT_GAP
): boolean {
  return boxes.every((box) =>
    candidate.x + candidate.width + gap <= box.x
    || box.x + box.width + gap <= candidate.x
    || candidate.y + candidate.height + gap <= box.y
    || box.y + box.height + gap <= candidate.y
  );
}

/**
 * The top-left point a newly added node should take. Deterministic for a given
 * workflow and viewport; the returned point is always clear of `boxes`.
 */
export function placeNewNode(options: PlacementOptions): { x: number; y: number } {
  const size = options.size ?? defaultNodeBox;
  const gap = options.gap ?? PLACEMENT_GAP;
  const boxes = options.boxes;

  if (options.selected) {
    const selected = options.selected;
    const startX = selected.x + selected.width + gap;
    const originY = selected.y;
    const stepY = size.height + gap;
    for (let column = 0; column < SEARCH_REACH; column += 1) {
      const x = startX + column * (size.width + gap);
      for (let row = 0; row < SEARCH_REACH; row += 1) {
        // 0, +1, -1, +2, -2 … so the first free slot stays on the selected
        // node's own line.
        const offset = Math.ceil(row / 2) * stepY * (row % 2 === 1 ? 1 : -1);
        const candidate = { x, y: originY + offset, ...size };
        if (placementClear(candidate, boxes, gap)) return { x: candidate.x, y: candidate.y };
      }
    }
  }

  const center = options.viewportCenter ?? { x: 0, y: 0 };
  const originX = Math.round(center.x - size.width / 2);
  const originY = Math.round(center.y - size.height / 2);
  const stepX = size.width + gap;
  const stepY = size.height + gap;
  // 0, +1, −1, +2, −2 …: the downstream/下 side of a ring is tried before the
  // opposite one, so an equally near slot to the right wins over one to the left.
  const ring = (radius: number) => {
    const values: number[] = [];
    for (let value = 0; value <= radius; value += 1) {
      values.push(value);
      if (value !== 0) values.push(-value);
    }
    return values;
  };
  for (let radius = 0; radius <= SEARCH_REACH; radius += 1) {
    for (const dy of ring(radius)) {
      for (const dx of ring(radius)) {
        if (Math.max(Math.abs(dx), Math.abs(dy)) !== radius) continue;
        const candidate = { x: originX + dx * stepX, y: originY + dy * stepY, ...size };
        if (placementClear(candidate, boxes, gap)) return { x: candidate.x, y: candidate.y };
      }
    }
  }
  return { x: originX, y: originY };
}
