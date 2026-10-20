import * as React from "react";
import {
  ApartmentOutlined,
  ColumnWidthOutlined,
  ExpandOutlined,
  MinusOutlined,
  PictureOutlined,
  PlusOutlined
} from "@ant-design/icons";
import { type ReactFlowState, useReactFlow, useStoreApi } from "@xyflow/react";
import { foundationTokens } from "@xflow/core";
import { Button, Divider, Slider, Tooltip } from "antd";
import type { FitRect } from "./fit";

/** Ruler thickness in px; the stylesheet reads the same value as `--xflow-ruler`. */
export const RULER_PX = Number.parseFloat(foundationTokens.ruler);

/** The zoom domain the slider offers (§6.2), as React Flow's min/max zoom. */
export const MIN_ZOOM = 0.1;
export const MAX_ZOOM = 2;

/**
 * §6.2 minimap size. Handed to MiniMap as its style, not only set in CSS: xyflow
 * sizes the svg and its viewBox from `style.width/height` (200x150 when absent),
 * so a box shrunk by CSS alone would crop the map's lower third.
 */
export const MINIMAP_SIZE = { width: 200, height: 100 } as const;

/**
 * Marks an element as a floating layer the fit has to keep content clear of. The
 * minimap and the attribution are xyflow's own and are found by class instead.
 */
export const FLOAT_ATTRIBUTE = "data-xflow-float";
const floatingSelector = `[${FLOAT_ATTRIBUTE}], .react-flow__minimap, .react-flow__attribution`;

export interface FitSurface {
  area: { width: number; height: number };
  /** Floating layers, in screen px relative to the canvas's top-left corner. */
  layers: FitRect[];
}

/**
 * Measures the flow pane inside `canvas` and the floating layers drawn over it;
 * zero-size layers are dropped. The pane, not the canvas, is the box measured: its
 * top-left corner is the origin the viewport transform is relative to.
 */
export function measureSurface(canvas: HTMLElement | null): FitSurface | undefined {
  if (!canvas) return undefined;
  const pane = canvas.querySelector<HTMLElement>(".react-flow") ?? canvas;
  const box = pane.getBoundingClientRect();
  if (box.width <= 0 || box.height <= 0) return undefined;
  const layers = [...pane.querySelectorAll(floatingSelector)]
    .map((element) => element.getBoundingClientRect())
    .filter((rect) => rect.width > 0 && rect.height > 0)
    .map((rect) => ({ x: rect.left - box.left, y: rect.top - box.top, width: rect.width, height: rect.height }));
  return { area: { width: box.width, height: box.height }, layers };
}

/**
 * Subscribes to one value of React Flow's store, re-rendering only when `equal`
 * says it changed. A pan or zoom therefore re-renders the chrome, never the cards.
 */
function useFlowState<T>(select: (state: ReactFlowState) => T, equal: (a: T, b: T) => boolean): T {
  const store = useStoreApi();
  const [value, setValue] = React.useState(() => select(store.getState()));
  React.useEffect(() => {
    const update = (state: ReactFlowState) => {
      const next = select(state);
      setValue((current) => (equal(current, next) ? current : next));
    };
    update(store.getState());
    return store.subscribe(update);
  }, [equal, select, store]);
  return value;
}

const sameTuple = (a: readonly number[], b: readonly number[]) =>
  a.length === b.length && a.every((value, index) => Object.is(value, b[index]));

/** Canvas units between two labelled ticks; 100 unless that would crowd the labels. */
const RULER_STEPS = [100, 200, 500, 1000, 2000, 5000, 10000];
/**
 * Narrowest gap, in px, that still fits the widest label short of five digits
 * ("-1000": five 10px-mono glyphs plus the 3px lead) with a little air. 100 units
 * keep their labels down to 36% zoom; the step only widens below that.
 */
const RULER_MIN_MAJOR_PX = 36;

export interface RulerTick {
  /** Offset along the ruler, in px. */
  position: number;
  /** Canvas coordinate the tick marks. */
  label: number;
}

/**
 * Ticks for a ruler `length` px long whose canvas origin sits `origin` px along it.
 * §6.2 fixes 10-unit minor and 100-unit labelled ticks; below the zoom where 100
 * units are narrower than a label, the step widens (200, 500, 1000 …) and the minor
 * tick stays a tenth of it, so the labels never overlap.
 */
export function rulerTicks(length: number, zoom: number, origin: number): { major: number; ticks: RulerTick[] } {
  const step = RULER_STEPS.find((candidate) => candidate * zoom >= RULER_MIN_MAJOR_PX) ?? RULER_STEPS.at(-1)!;
  const major = step * zoom;
  const ticks: RulerTick[] = [];
  if (!(major > 0) || !(length > 0)) return { major, ticks };
  for (let k = Math.ceil(-origin / major); origin + k * major <= length; k += 1) {
    ticks.push({ position: origin + k * major, label: k * step });
  }
  return { major, ticks };
}

function RulerAxis({ axis, length, zoom, origin, highlight }: {
  axis: "x" | "y";
  length: number;
  zoom: number;
  origin: number;
  highlight?: { start: number; size: number };
}): React.ReactElement {
  const { major, ticks } = rulerTicks(length, zoom, origin);
  const along = axis === "x" ? "left" : "top";
  const extent = axis === "x" ? "width" : "height";
  const style = {
    "--xflow-preview-ruler-major": `${major}px`,
    "--xflow-preview-ruler-minor": `${major / 10}px`,
    "--xflow-preview-ruler-origin": `${origin}px`
  } as React.CSSProperties;
  return (
    <div
      className={`xflow-preview-ruler xflow-preview-ruler--${axis}`}
      style={style}
      aria-hidden="true"
      data-axis={axis}
      {...{ [FLOAT_ATTRIBUTE]: "" }}
    >
      {highlight ? (
        <span className="xflow-preview-ruler__highlight" style={{ [along]: highlight.start, [extent]: highlight.size }} />
      ) : null}
      {ticks.map((tick) => (
        <span key={tick.label} className="xflow-preview-ruler__tick" style={{ [along]: tick.position }}>
          {tick.label}
        </span>
      ))}
    </div>
  );
}

/** What the rulers read from React Flow, as one flat tuple so equality is cheap. */
type RulerState = readonly [x: number, y: number, zoom: number, width: number, height: number,
  nodeX: number, nodeY: number, nodeWidth: number, nodeHeight: number];

/**
 * §6.2 rulers along the canvas's top and left edges: they follow pan and zoom, and
 * the selected node's extent is highlighted on both. Rendered inside React Flow,
 * over the pane, and never under the pointer.
 */
export function CanvasRulers({ selectedNodeId }: { selectedNodeId?: string }): React.ReactElement {
  const select = React.useCallback((state: ReactFlowState): RulerState => {
    const [x, y, zoom] = state.transform;
    const node = selectedNodeId ? state.nodeLookup?.get(selectedNodeId) : undefined;
    const position = node?.internals.positionAbsolute;
    const nodeWidth = node?.measured.width ?? 0;
    const nodeHeight = node?.measured.height ?? 0;
    return [x, y, zoom, state.width ?? 0, state.height ?? 0,
      position?.x ?? 0, position?.y ?? 0, position ? nodeWidth : 0, position ? nodeHeight : 0];
  }, [selectedNodeId]);
  const [x, y, zoom, width, height, nodeX, nodeY, nodeWidth, nodeHeight] = useFlowState(select, sameTuple);
  const hasNode = nodeWidth > 0 && nodeHeight > 0;
  // Both rulers start after the corner square, so a canvas coordinate's offset
  // along a ruler is its screen offset minus the ruler thickness.
  return (
    <>
      <span className="xflow-preview-ruler__corner" aria-hidden="true" {...{ [FLOAT_ATTRIBUTE]: "" }} />
      <RulerAxis
        axis="x"
        length={width - RULER_PX}
        zoom={zoom}
        origin={x - RULER_PX}
        highlight={hasNode ? { start: x + nodeX * zoom - RULER_PX, size: nodeWidth * zoom } : undefined}
      />
      <RulerAxis
        axis="y"
        length={height - RULER_PX}
        zoom={zoom}
        origin={y - RULER_PX}
        highlight={hasNode ? { start: y + nodeY * zoom - RULER_PX, size: nodeHeight * zoom } : undefined}
      />
    </>
  );
}

function ChromeButton({ label, active, ...rest }: React.ComponentProps<typeof Button> & {
  label: string;
  active?: boolean;
}): React.ReactElement {
  return (
    <Tooltip title={label}>
      <Button
        type="text"
        size="small"
        aria-label={label}
        aria-pressed={active}
        data-active={active === undefined ? undefined : String(active)}
        className="xflow-preview-zoombar__button"
        {...rest}
      />
    </Tooltip>
  );
}

const selectZoom = (state: ReactFlowState) => state.transform[2];

/** The zoom a ±10% step lands on: whole percents, clamped to the slider's domain. */
export function steppedZoom(zoom: number, direction: 1 | -1): number {
  const percent = Math.round(zoom * 100) + direction * 10;
  return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, percent / 100));
}

/**
 * §6.2 bottom-left tool bar: zoom − / slider (10%–200%) / value / +, then fit,
 * auto layout (only when the owner can lay the graph out), and the ruler and
 * minimap toggles with their ⇧R / ⇧M shortcuts.
 */
export function ZoomBar({ onFit, onAutoLayout, ruler, onRulerChange, minimap, onMinimapChange }: {
  onFit: () => void;
  onAutoLayout?: () => void;
  ruler: boolean;
  onRulerChange: (visible: boolean) => void;
  minimap: boolean;
  onMinimapChange: (visible: boolean) => void;
}): React.ReactElement {
  const flow = useReactFlow();
  const zoom = useFlowState(selectZoom, Object.is);
  const percent = Math.round(zoom * 100);
  const zoomTo = (next: number) => {
    void flow.zoomTo(next);
  };
  return (
    <div className="xflow-preview-zoombar" role="toolbar" aria-label="画布缩放" {...{ [FLOAT_ATTRIBUTE]: "" }}>
      <ChromeButton label="缩小" icon={<MinusOutlined />} onClick={() => zoomTo(steppedZoom(zoom, -1))} />
      <Slider
        className="xflow-preview-zoombar__slider"
        min={MIN_ZOOM * 100}
        max={MAX_ZOOM * 100}
        value={percent}
        ariaLabelForHandle="缩放比例"
        onChange={(value) => zoomTo(value / 100)}
        tooltip={{ formatter: (value) => `${value}%` }}
      />
      <span className="xflow-preview-zoombar__value">{percent}%</span>
      <ChromeButton label="放大" icon={<PlusOutlined />} onClick={() => zoomTo(steppedZoom(zoom, 1))} />
      <Divider orientation="vertical" className="xflow-preview-zoombar__separator" />
      <ChromeButton label="适应画布" icon={<ExpandOutlined />} onClick={onFit} />
      {onAutoLayout ? <ChromeButton label="自动布局" icon={<ApartmentOutlined />} onClick={onAutoLayout} /> : null}
      <ChromeButton
        label="标尺 (⇧R)"
        icon={<ColumnWidthOutlined />}
        active={ruler}
        onClick={() => onRulerChange(!ruler)}
      />
      <ChromeButton
        label="缩略图 (⇧M)"
        icon={<PictureOutlined />}
        active={minimap}
        onClick={() => onMinimapChange(!minimap)}
      />
    </div>
  );
}
