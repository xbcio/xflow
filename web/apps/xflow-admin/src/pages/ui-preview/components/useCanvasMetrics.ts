import { useCallback, useLayoutEffect, useRef, useState, type RefObject } from 'react';

import { FRAME_LABEL_OFFSET, type ContentBox } from '../metrics';
import type { PortAnchor } from '../edge-path';

export interface NodeRect {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  tone: string;
}

export interface CanvasMetrics {
  rects: NodeRect[];
  /** Where each port chip sits across its card (0..1), for edges leaving a port. */
  ports: PortAnchor[];
  width: number;
  height: number;
  /** Union of every card and group frame, or null before the first measurement. */
  content: ContentBox | null;
}

function sameRects(a: NodeRect[], b: NodeRect[]): boolean {
  return (
    a.length === b.length &&
    a.every((rect, index) => {
      const other = b[index];
      return (
        rect.id === other.id &&
        rect.x === other.x &&
        rect.y === other.y &&
        rect.w === other.w &&
        rect.h === other.h &&
        rect.tone === other.tone
      );
    })
  );
}

function samePorts(a: PortAnchor[], b: PortAnchor[]): boolean {
  return (
    a.length === b.length &&
    a.every((port, index) => {
      const other = b[index];
      return port.node === other.node && port.name === other.name && port.frac === other.frac;
    })
  );
}

function sameContent(a: ContentBox | null, b: ContentBox | null): boolean {
  if (a === null || b === null) return a === b;
  return a.maxX === b.maxX && a.minY === b.minY && a.maxY === b.maxY;
}

/**
 * Measures what the canvas actually rendered: card rects (their height depends
 * on the card summary), where each port chip sits across its card (edges leaving
 * a port start there), the group frames, and the canvas box. Used by the
 * minimap, by the ruler highlight, by the edge routes and by the fitView, which
 * must all follow the real card extent rather than a fixed height.
 *
 * The canvas box is observed, not just read on render: the compact layout
 * collapses a column through a `grid-template-columns` transition, and the
 * canvas only reaches its final width 160ms later. Read on render alone the fit
 * kept the width from the first frame of that transition — a 1200px host fit
 * its graph to the 500px canvas it had while both columns were still docked —
 * and nothing re-rendered afterwards to correct it.
 */
export function useCanvasMetrics(canvasRef: RefObject<HTMLDivElement | null>): CanvasMetrics {
  const [metrics, setMetrics] = useState<CanvasMetrics>({
    rects: [],
    ports: [],
    width: 0,
    height: 0,
    content: null,
  });
  const latest = useRef(metrics);

  const measure = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rects = Array.from(canvas.querySelectorAll<HTMLElement>('.xflow-pv-node')).map(
      (node) => ({
        id: node.dataset.nodeId ?? '',
        x: node.offsetLeft,
        y: node.offsetTop,
        w: node.offsetWidth,
        h: node.offsetHeight,
        tone: node.dataset.tone ?? 'neutral',
      }),
    );
    /*
     * Port chips are measured as a **fraction** of their card's width, not in
     * canvas units: the chip sits two offset parents deep (`.xflow-pv-node-ports`
     * is `position: relative`), so its own `offsetLeft` is relative to the ports
     * row, and the canvas may be scaled by the zoom. A ratio of two screen rects
     * is both transform- and border-safe — it is the same number at any zoom.
     */
    const ports: PortAnchor[] = Array.from(
      canvas.querySelectorAll<HTMLElement>('.xflow-pv-port'),
    ).flatMap((chip) => {
      const card = chip.closest<HTMLElement>('.xflow-pv-node');
      const id = card?.dataset.nodeId;
      const name = chip.textContent?.trim();
      if (!card || !id || !name) return [];
      const chipRect = chip.getBoundingClientRect();
      const cardRect = card.getBoundingClientRect();
      if (cardRect.width <= 0) return [];
      return [
        { node: id, name, frac: (chipRect.left + chipRect.width / 2 - cardRect.left) / cardRect.width },
      ];
    });
    const frames = Array.from(canvas.querySelectorAll<HTMLElement>('.xflow-pv-frame'));
    const boxes = [...Array.from(canvas.querySelectorAll<HTMLElement>('.xflow-pv-node')), ...frames];
    const content: ContentBox | null =
      boxes.length === 0
        ? null
        : {
            maxX: Math.max(...boxes.map((box) => box.offsetLeft + box.offsetWidth)),
            // A frame's label pokes above the frame box and is content too.
            minY: Math.min(
              ...boxes.map(
                (box) =>
                  box.offsetTop - (box.classList.contains('xflow-pv-frame') ? FRAME_LABEL_OFFSET : 0),
              ),
            ),
            maxY: Math.max(...boxes.map((box) => box.offsetTop + box.offsetHeight)),
          };
    const next: CanvasMetrics = {
      rects,
      ports,
      width: canvas.clientWidth,
      height: canvas.clientHeight,
      content,
    };
    const previous = latest.current;
    if (
      previous.width === next.width &&
      previous.height === next.height &&
      sameRects(previous.rects, next.rects) &&
      samePorts(previous.ports, next.ports) &&
      sameContent(previous.content, next.content)
    ) {
      return;
    }
    latest.current = next;
    setMetrics(next);
  }, [canvasRef]);

  useLayoutEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    // Card summaries decide the content box, so re-measure whenever the tree
    // renders as well as whenever the box resizes.
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(canvas);
    return () => observer.disconnect();
  });

  return metrics;
}
