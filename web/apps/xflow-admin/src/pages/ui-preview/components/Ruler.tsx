import type { CSSProperties } from 'react';

/**
 * Canvas ruler (§6.2): 20px rulers on top and left, a tick every 10 canvas
 * units, a numbered tick every 100, both following pan and zoom. The selected
 * node's extent is highlighted on both axes. Positions are screen pixels from
 * the ruler's own origin, so `pan + i * 100 * scale` lines up with the viewport
 * transform `translate(RULER + pan) scale(scale)`.
 */
export function Ruler({
  axis,
  length,
  scale,
  pan,
  highlight,
}: {
  axis: 'x' | 'y';
  length: number;
  scale: number;
  pan: number;
  highlight?: { start: number; size: number };
}) {
  const major = Math.max(1, 100 * scale);
  const ticks: number[] = [];
  for (let i = 0; pan + i * major <= length; i += 1) ticks.push(i);

  const style = {
    '--xflow-pv-major': `${major}px`,
    '--xflow-pv-minor': `${major / 10}px`,
    '--xflow-pv-origin': `${pan}px`,
  } as CSSProperties;

  return (
    <div
      className={`xflow-pv-ruler xflow-pv-ruler--${axis}`}
      style={style}
      aria-hidden="true"
      data-axis={axis}
    >
      {highlight ? (
        <span
          className="xflow-pv-ruler-hl"
          style={
            axis === 'x'
              ? { left: highlight.start, width: highlight.size }
              : { top: highlight.start, height: highlight.size }
          }
        />
      ) : null}
      {ticks.map((tick) => (
        <span
          key={tick}
          style={axis === 'x' ? { left: pan + tick * major } : { top: pan + tick * major }}
        >
          {tick * 100}
        </span>
      ))}
    </div>
  );
}
