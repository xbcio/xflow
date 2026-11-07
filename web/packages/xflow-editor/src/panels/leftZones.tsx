import * as React from "react";

/**
 * The left column's two zones (§6.0.1, 2026-11-07). Auto mode lets the
 * outline size to its tree — at least three rows, at most half the column —
 * and gives the library the rest; dragging the 8px rule stores a ratio, and a
 * double click returns to auto. Both zones scroll inside their own body.
 */

export const LEFT_SPLIT_STORAGE_KEY = "xflow-editor.left-split";
/** One outline row is 28px (§6.0.1); three of them plus the list's gutters. */
export const OUTLINE_MIN_HEIGHT = 132;
/** The rule's hit area; the visible line is drawn inside it. */
export const SPLIT_HANDLE_HEIGHT = 8;
/** One ArrowUp / ArrowDown step on the separator. */
export const SPLIT_KEYBOARD_STEP = 28;

function storageOrUndefined(): Storage | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

function readStoredRatio(storage: Storage | undefined = storageOrUndefined()): number | undefined {
  try {
    const raw = storage?.getItem(LEFT_SPLIT_STORAGE_KEY);
    if (!raw) return undefined;
    const value = Number(raw);
    return Number.isFinite(value) && value > 0 && value < 1 ? value : undefined;
  } catch {
    return undefined;
  }
}

function storeRatio(ratio: number | undefined, storage: Storage | undefined = storageOrUndefined()): void {
  try {
    if (ratio === undefined) storage?.removeItem(LEFT_SPLIT_STORAGE_KEY);
    else storage?.setItem(LEFT_SPLIT_STORAGE_KEY, String(ratio));
  } catch {
    // Storage is an enhancement; a blocked one keeps the session's own ratio.
  }
}

/** Outline height in ratio units, clamped to its three-row floor and half the column. */
function clampRatio(ratio: number, containerHeight: number): number {
  if (containerHeight <= 0) return ratio;
  const floor = Math.min(OUTLINE_MIN_HEIGHT / containerHeight, 0.5);
  return Math.min(0.5, Math.max(floor, ratio));
}

export function LeftZones({
  library,
  outline,
  dual
}: {
  /** The 节点 zone; absent in preview, where the outline owns the column. */
  library?: React.ReactNode;
  outline: React.ReactNode;
  /** Editing shows both zones and the draggable rule. */
  dual: boolean;
}): React.ReactElement {
  const containerRef = React.useRef<HTMLElement>(null);
  const [ratio, setRatio] = React.useState<number | undefined>(() => readStoredRatio());
  const [dragging, setDragging] = React.useState(false);
  const dragStart = React.useRef<{ y: number; outlineHeight: number; containerHeight: number } | undefined>(undefined);

  const updateRatio = React.useCallback((next: number) => {
    const clamped = clampRatio(next, containerRef.current?.getBoundingClientRect().height ?? 0);
    setRatio(clamped);
    storeRatio(clamped);
  }, []);

  const handlePointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    const container = containerRef.current;
    if (!container) return;
    const containerRect = container.getBoundingClientRect();
    const handleRect = event.currentTarget.getBoundingClientRect();
    const outlineHeight = Math.max(0, containerRect.bottom - handleRect.bottom);
    dragStart.current = { y: event.clientY, outlineHeight, containerHeight: containerRect.height };
    setDragging(true);
    event.currentTarget.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  };

  const handlePointerMove = (event: React.PointerEvent<HTMLDivElement>) => {
    const start = dragStart.current;
    if (!start) return;
    const nextOutline = start.outlineHeight + (start.y - event.clientY);
    const nextRatio = start.containerHeight > 0 ? nextOutline / start.containerHeight : 0;
    updateRatio(nextRatio);
  };

  const endDrag = (event: React.PointerEvent<HTMLDivElement>) => {
    dragStart.current = undefined;
    setDragging(false);
    event.currentTarget.releasePointerCapture?.(event.pointerId);
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "ArrowUp" && event.key !== "ArrowDown") return;
    event.preventDefault();
    const height = containerRef.current?.getBoundingClientRect().height ?? 0;
    const current = ratio ?? (height > 0 ? OUTLINE_MIN_HEIGHT / height : 0.5);
    const step = height > 0 ? SPLIT_KEYBOARD_STEP / height : 0;
    updateRatio(current + (event.key === "ArrowUp" ? step : -step));
  };

  const rows = !dual
    ? "minmax(0, 1fr)"
    : ratio === undefined
      ? "minmax(0, 1fr) 8px fit-content(50%)"
      : `minmax(0, 1fr) 8px ${(ratio * 100).toFixed(2)}%`;

  return (
    <aside
      aria-label="节点与大纲"
      className="xflow-editor-lib"
      data-split={ratio === undefined ? "auto" : "manual"}
      data-zones={dual ? "dual" : "single"}
      ref={containerRef}
      style={{ gridTemplateRows: rows }}
    >
      {dual ? library : null}
      {dual ? (
        <div
          aria-label="调整大纲高度"
          aria-orientation="horizontal"
          aria-valuemax={50}
          aria-valuemin={0}
          aria-valuenow={ratio === undefined ? undefined : Math.round(ratio * 100)}
          className="xflow-editor-lib__divider"
          data-dragging={dragging ? "true" : undefined}
          role="separator"
          tabIndex={0}
          onDoubleClick={() => {
            setRatio(undefined);
            storeRatio(undefined);
          }}
          onKeyDown={handleKeyDown}
          onPointerDown={handlePointerDown}
          onPointerMove={handlePointerMove}
          onPointerUp={endDrag}
          onPointerCancel={endDrag}
        />
      ) : null}
      {outline}
    </aside>
  );
}
