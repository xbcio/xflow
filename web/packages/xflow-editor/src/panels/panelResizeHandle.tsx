import * as React from "react";
import { PANEL_RESIZE_KEY_STEP, clampPanelWidth, type PanelSide } from "./panelWidths";

/**
 * §6.0「拖动调整宽度」: the 8px hit strip that straddles one docked panel's inner
 * edge. The strip is invisible until hover, focus or drag, when a 2px primary
 * line marks the edge; the element itself is the hit area (no `::after`
 * extension), so no ancestor's `overflow` can clip it.
 *
 * Pointer capture is taken only after the pointer has travelled
 * {@link DRAG_THRESHOLD_PX}: a capture set on pointerdown retargets the
 * subsequent clicks and swallows the handle's double-click reset (the same trap
 * documented for pointer capture on drag handles elsewhere in the web tree).
 * Until then the press is an ordinary click, so a double-click still reaches
 * `onDoubleClick`.
 */

/** Travel before the press becomes a drag, in px. */
const DRAG_THRESHOLD_PX = 3;

export interface PanelResizeHandleProps {
  side: PanelSide;
  /** Accessible name: 「调整节点面板宽度」/「调整属性面板宽度」. */
  label: string;
  value: number;
  min: number;
  max: number;
  defaultValue: number;
  onChange: (next: number) => void;
  onResizeStart?: () => void;
  onResizeEnd?: () => void;
}

interface DragSession {
  pointerId: number;
  startX: number;
  startValue: number;
  captured: boolean;
}

export function PanelResizeHandle({
  defaultValue,
  label,
  max,
  min,
  onChange,
  onResizeEnd,
  onResizeStart,
  side,
  value
}: PanelResizeHandleProps) {
  const handleRef = React.useRef<HTMLDivElement | null>(null);
  const dragRef = React.useRef<DragSession | null>(null);
  const [dragging, setDragging] = React.useState(false);

  const commit = (next: number) => onChange(clampPanelWidth(side, Math.min(max, Math.max(min, next))));

  const onPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !event.isPrimary || dragRef.current) return;
    const session: DragSession = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startValue: value,
      captured: false
    };
    dragRef.current = session;
    let frame = 0;
    let pending: number | undefined;

    const flush = () => {
      frame = 0;
      if (pending === undefined) return;
      const next = pending;
      pending = undefined;
      commit(next);
    };
    const end = (pointerId: number) => {
      if (dragRef.current !== session || pointerId !== session.pointerId) return;
      dragRef.current = null;
      if (frame) window.cancelAnimationFrame(frame);
      flush();
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      window.removeEventListener("pointercancel", cancel);
      handleRef.current?.removeEventListener("lostpointercapture", lostCapture);
      if (session.captured) {
        try {
          handleRef.current?.releasePointerCapture(session.pointerId);
        } catch {
          // The pointer is already gone; nothing to release.
        }
        onResizeEnd?.();
      }
      setDragging(false);
    };
    const move = (pointer: PointerEvent) => {
      if (dragRef.current !== session || pointer.pointerId !== session.pointerId) return;
      const delta = pointer.clientX - session.startX;
      if (!session.captured) {
        if (Math.abs(delta) < DRAG_THRESHOLD_PX) return;
        session.captured = true;
        // Capture now, not on pointerdown: the press is a drag from here on.
        try {
          handleRef.current?.setPointerCapture(session.pointerId);
        } catch {
          // A pointer that already left cannot be captured; window listeners
          // still keep the drag following.
        }
        handleRef.current?.addEventListener("lostpointercapture", lostCapture);
        // The few pixels before the threshold may have started a selection.
        window.getSelection()?.removeAllRanges();
        setDragging(true);
        onResizeStart?.();
      }
      pointer.preventDefault();
      const next = session.startValue + (side === "left" ? delta : -delta);
      pending = next;
      if (!frame) frame = window.requestAnimationFrame(flush);
    };
    const up = (pointer: PointerEvent) => end(pointer.pointerId);
    const cancel = (pointer: PointerEvent) => end(pointer.pointerId);
    const lostCapture = () => end(session.pointerId);

    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
    window.addEventListener("pointercancel", cancel);
  };

  const onKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    let next: number | undefined;
    if (event.key === "ArrowLeft") next = value + (side === "right" ? PANEL_RESIZE_KEY_STEP : -PANEL_RESIZE_KEY_STEP);
    else if (event.key === "ArrowRight") next = value + (side === "left" ? PANEL_RESIZE_KEY_STEP : -PANEL_RESIZE_KEY_STEP);
    else if (event.key === "Home") next = min;
    else if (event.key === "End") next = max;
    else if (event.key === "Enter") next = defaultValue;
    if (next === undefined) return;
    event.preventDefault();
    onChange(clampPanelWidth(side, Math.min(max, Math.max(min, next))));
  };

  return (
    <div
      aria-label={label}
      aria-orientation="vertical"
      aria-valuemax={max}
      aria-valuemin={min}
      aria-valuenow={value}
      className={`xflow-editor-panel-resize xflow-editor-panel-resize--${side}`}
      data-dragging={dragging ? "true" : undefined}
      onDoubleClick={() => onChange(clampPanelWidth(side, defaultValue))}
      onKeyDown={onKeyDown}
      onPointerDown={onPointerDown}
      ref={handleRef}
      role="separator"
      tabIndex={0}
    />
  );
}
