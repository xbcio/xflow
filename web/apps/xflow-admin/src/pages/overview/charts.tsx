import type { ExecutionStatsBucket, ExecutionStatus, OverviewWindow } from '@xflow/api';
import { workflowStatusMeta } from '@xflow/core';
import { Tooltip } from 'antd';
import { useState } from 'react';

import { fullTimestamp } from '../workflows/row-parts';
import { failedCount, histogramTotal, OVERVIEW_WINDOW_LABEL, statusCount } from './model';

/**
 * A-A overview charts. Both are hand-drawn SVG so the console gains a
 * dashboard without a charting dependency; the palette is the shared
 * `workflowStatusMeta` tone table, so a status can only look one way.
 */

/** Stack/legend order: terminal outcomes first, live states after. */
export const OVERVIEW_STATUS_ORDER: ExecutionStatus[] = [
  'success',
  'failed',
  'timeout',
  'canceled',
  'canceling',
  'running',
  'paused',
  'queued',
  'pending',
];

export type SparkTone = 'primary' | 'success' | 'danger';

const SPARK_WIDTH = 96;
const SPARK_HEIGHT = 28;
const SPARK_PAD = 2;

/**
 * A 24-point inline trend. `null` means "no value in this bucket" (the success
 * sparkline has no terminal run in some hours), so the line breaks there rather
 * than drawing a zero the API never asserted. The chart is decorative: the KPI
 * value beside it carries the accessible number.
 */
export function Sparkline({
  values,
  tone,
  label,
}: {
  values: Array<number | null>;
  tone: SparkTone;
  label: string;
}) {
  const numeric = values.filter((value): value is number => value !== null && Number.isFinite(value));
  if (numeric.length === 0) return null;

  const innerWidth = SPARK_WIDTH - SPARK_PAD * 2;
  const innerHeight = SPARK_HEIGHT - SPARK_PAD * 2;
  const max = Math.max(...numeric);
  const min = Math.min(...numeric);
  const span = max - min;
  const point = (value: number, index: number): readonly [number, number] => {
    const x = values.length > 1 ? SPARK_PAD + (index / (values.length - 1)) * innerWidth : SPARK_WIDTH / 2;
    const y = span === 0 ? SPARK_PAD + innerHeight / 2 : SPARK_PAD + (1 - (value - min) / span) * innerHeight;
    return [x, y];
  };

  const segments: Array<Array<readonly [number, number]>> = [];
  let current: Array<readonly [number, number]> = [];
  values.forEach((value, index) => {
    if (value === null || !Number.isFinite(value)) {
      if (current.length > 0) segments.push(current);
      current = [];
      return;
    }
    current.push(point(value, index));
  });
  if (current.length > 0) segments.push(current);

  return (
    <svg
      className="xflow-ov-spark"
      data-tone={tone}
      viewBox={`0 0 ${SPARK_WIDTH} ${SPARK_HEIGHT}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={label}
    >
      {segments.map((points, index) =>
        points.length === 1 ? (
          <circle
            key={index}
            className="xflow-ov-spark-dot"
            cx={points[0][0]}
            cy={points[0][1]}
            r={1.5}
          />
        ) : (
          <polyline
            key={index}
            className="xflow-ov-spark-line"
            points={points.map(([x, y]) => `${x},${y}`).join(' ')}
            vectorEffect="non-scaling-stroke"
          />
        ),
      )}
    </svg>
  );
}

/** One stacked segment with only its top corners rounded. */
function topRoundedPath(x: number, y: number, width: number, height: number, radius: number): string {
  const r = Math.max(0, Math.min(radius, width / 2, height));
  return [
    `M${x},${y + height}`,
    `V${y + r}`,
    `Q${x},${y} ${x + r},${y}`,
    `H${x + width - r}`,
    `Q${x + width},${y} ${x + width},${y + r}`,
    `V${y + height}`,
    'Z',
  ].join(' ');
}

/** `HH:00` for 24h, `MM-DD HH:00` for 7d; an unparsable stamp stays verbatim. */
export function bucketTimeLabel(start: string, range: OverviewWindow): string {
  const date = new Date(start);
  if (Number.isNaN(date.getTime())) return start;
  const pad = (part: number) => String(part).padStart(2, '0');
  const hour = `${pad(date.getHours())}:00`;
  return range === '24h' ? hour : `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${hour}`;
}

const HISTOGRAM_WIDTH = 960;
const HISTOGRAM_HEIGHT = 200;
const HISTOGRAM_GAP = 3;
const HISTOGRAM_PLOT = HISTOGRAM_HEIGHT - 6;

/**
 * The window's executions as one stacked column per bucket. Each column is a
 * focusable group with an aria-label and a hover/focus tooltip; the same data
 * is repeated in a visually hidden table for screen readers, so the chart is
 * never the only copy of the numbers.
 */
export function StatusHistogram({
  histogram,
  range,
}: {
  histogram: ExecutionStatsBucket[];
  range: OverviewWindow;
}) {
  const totalsByStatus = OVERVIEW_STATUS_ORDER.map((status) => ({
    status,
    total: histogram.reduce((sum, bucket) => sum + bucket.counts[status], 0),
  }));
  const visible = totalsByStatus.filter((entry) => entry.total > 0);
  const grandTotal = histogramTotal(histogram);
  // One tooltip at a time: AntD keeps each Tooltip's overlay alive through its
  // leave animation, so tabbing quickly across bars used to leave the previous
  // bar's tooltip on screen under the next one. The open state lives here
  // instead of inside each Tooltip, and focusing or hovering a bar replaces it.
  const [activeBucket, setActiveBucket] = useState<string | null>(null);

  if (histogram.length === 0 || grandTotal === 0) {
    return <p className="xflow-ov-empty">窗口内暂无执行。</p>;
  }

  const bucketTotals = histogram.map((bucket) => statusCount(bucket.counts));
  const maxTotal = Math.max(1, ...bucketTotals);
  const barWidth = (HISTOGRAM_WIDTH - HISTOGRAM_GAP * (histogram.length - 1)) / histogram.length;
  const bucketHours = range === '7d' ? 7 : 1;
  const axisIndexes = [0, Math.floor(histogram.length / 2), histogram.length - 1];

  return (
    <>
      <svg
        className="xflow-ov-histogram"
        viewBox={`0 0 ${HISTOGRAM_WIDTH} ${HISTOGRAM_HEIGHT}`}
        preserveAspectRatio="none"
        role="group"
        aria-label={`${OVERVIEW_WINDOW_LABEL[range]}执行状态分布，共 ${grandTotal} 次`}
      >
        <g aria-hidden="true">
          {[0.25, 0.5, 0.75].map((fraction) => (
            <line
              key={fraction}
              className="xflow-ov-grid"
              x1={0}
              x2={HISTOGRAM_WIDTH}
              y1={HISTOGRAM_HEIGHT - HISTOGRAM_PLOT * fraction}
              y2={HISTOGRAM_HEIGHT - HISTOGRAM_PLOT * fraction}
            />
          ))}
          <line
            className="xflow-ov-grid xflow-ov-baseline"
            x1={0}
            x2={HISTOGRAM_WIDTH}
            y1={HISTOGRAM_HEIGHT - 0.5}
            y2={HISTOGRAM_HEIGHT - 0.5}
          />
        </g>
        {histogram.map((bucket, bucketIndex) => {
          const x = bucketIndex * (barWidth + HISTOGRAM_GAP);
          const total = bucketTotals[bucketIndex];
          const segments = visible
            .map(({ status }) => ({ status, count: bucket.counts[status] }))
            .filter((segment) => segment.count > 0);
          let y = HISTOGRAM_HEIGHT;
          const shapes = segments.map((segment, segmentIndex) => {
            const height = (segment.count / maxTotal) * HISTOGRAM_PLOT;
            const top = y - height;
            y = top;
            const tone = workflowStatusMeta[segment.status].tone;
            return segmentIndex === segments.length - 1 ? (
              <path
                key={segment.status}
                className="xflow-ov-seg"
                data-tone={tone}
                data-status={segment.status}
                d={topRoundedPath(x, top, barWidth, height, 3)}
              />
            ) : (
              <rect
                key={segment.status}
                className="xflow-ov-seg"
                data-tone={tone}
                data-status={segment.status}
                x={x}
                y={top}
                width={barWidth}
                height={height}
              />
            );
          });
          const label =
            total === 0
              ? `${bucketTimeLabel(bucket.start, range)} · 共 0 次`
              : `${bucketTimeLabel(bucket.start, range)} · 共 ${total} 次${visible
                  .map(({ status }) => ` · ${workflowStatusMeta[status].label} ${bucket.counts[status]}`)
                  .join('')}`;
          return (
            <Tooltip
              key={bucket.start}
              title={label}
              open={activeBucket === bucket.start}
              onOpenChange={(open) =>
                setActiveBucket((current) => (open ? bucket.start : current === bucket.start ? null : current))
              }
              trigger={['hover', 'focus']}
              mouseEnterDelay={0}
              destroyOnHidden
            >
              <g
                className="xflow-ov-bucket"
                data-bucket={bucket.start}
                data-empty={total === 0 ? 'true' : undefined}
                tabIndex={0}
                role="img"
                aria-label={label}
              >
                <rect
                  className="xflow-ov-bucket-hit"
                  x={x}
                  y={0}
                  width={barWidth}
                  height={HISTOGRAM_HEIGHT}
                />
                {/* An empty bucket is a tick at the baseline, not a zero-height
                    column: it keeps the hour readable without pretending the
                    window had bars there. */}
                {total === 0 ? (
                  <rect
                    className="xflow-ov-tick"
                    x={x}
                    y={HISTOGRAM_HEIGHT - 2}
                    width={barWidth}
                    height={2}
                    rx={1}
                  />
                ) : (
                  shapes
                )}
              </g>
            </Tooltip>
          );
        })}
      </svg>
      <div className="xflow-ov-axis" aria-hidden="true">
        {axisIndexes.map((index, position) => (
          <span key={`${index}-${position}`}>{bucketTimeLabel(histogram[index].start, range)}</span>
        ))}
      </div>
      <div className="xflow-ov-legend" aria-label="状态图例">
        {visible.map(({ status, total }) => (
          <span className="xflow-ov-legend-item" key={status}>
            <span
              className="xflow-ov-legend-swatch"
              data-tone={workflowStatusMeta[status].tone}
              data-status={status}
              aria-hidden="true"
            />
            {workflowStatusMeta[status].label}
            <span className="xflow-ov-legend-count">{total}</span>
          </span>
        ))}
      </div>
      <table className="xflow-admin-visually-hidden">
        <caption>
          {OVERVIEW_WINDOW_LABEL[range]}执行状态分布，每 {bucketHours} 小时一桶
        </caption>
        <thead>
          <tr>
            <th scope="col">时间</th>
            {visible.map(({ status }) => (
              <th scope="col" key={status}>
                {workflowStatusMeta[status].label}
              </th>
            ))}
            <th scope="col">合计</th>
          </tr>
        </thead>
        <tbody>
          {histogram.map((bucket) => (
            <tr key={bucket.start}>
              <th scope="row">{fullTimestamp(bucket.start)}</th>
              {visible.map(({ status }) => (
                <td key={status}>{bucket.counts[status]}</td>
              ))}
              <td>{statusCount(bucket.counts)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

/** The failure KPI's sparkline: failed + timeout per bucket. */
export function failedSeries(histogram: ExecutionStatsBucket[]): number[] {
  return histogram.map((bucket) => failedCount(bucket.counts));
}
