import type {
  WorkflowExecutionStats,
  WorkflowRecentExecution,
  WorkflowStatsWindow,
  WorkflowSummary,
} from '@xflow/api';
import { workflowStatusMeta } from '@xflow/core';
import { useEffect, useState } from 'react';

/**
 * A/B proposal: the workflow row is one readable health snapshot. These are the
 * pure projections the row draws — time, duration, rate and the last-ten-run
 * strip — kept out of `WorkflowCenter.tsx` so the page stays about data flow.
 *
 * Every field is optional on the wire: a caller without `execution.read` (or an
 * older server) gets a row WITHOUT stats/recent, and the row must then say
 * "—", never invent a run, rate or duration.
 */

/**
 * Trigger kind → the console's label. The workflow list passes node kinds
 * (OpenAPI `triggers`); the execution list also passes the server-derived
 * `trigger_kind` of a run, hence `api` / `rerun` / `subworkflow`.
 */
export const TRIGGER_LABELS: Record<string, string> = {
  api: '手动',
  rerun: '重跑',
  subworkflow: '子流程',
  timer: '定时',
  cron: 'Cron',
  webhook: 'Webhook',
  kafka: 'Kafka',
  redis: 'Redis',
};

/** The list shows the baseline's `v`-prefixed version, matching the editor chip. */
export function versionLabel(version: string): string {
  return version.startsWith('v') ? version : `v${version}`;
}

/** Local `YYYY-MM-DD HH:mm:ss`, matching the executions list; unparsable values stay verbatim. */
export function fullTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const pad = (part: number) => String(part).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(
    date.getHours(),
  )}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

/**
 * Relative age for the 更新 column, with the full timestamp kept in `title`.
 * A future timestamp is drawn verbatim instead of as "刚刚": clock skew must
 * not turn into a claim the row cannot support.
 */
export function relativeTime(value: string, now = Date.now()): string {
  const date = new Date(value);
  const stamp = date.getTime();
  if (Number.isNaN(stamp)) return value;
  const diff = now - stamp;
  if (diff < 0) return fullTimestamp(value);
  if (diff < 60_000) return '刚刚';
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)} 分钟前`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)} 小时前`;
  if (diff < 30 * 86_400_000) return `${Math.floor(diff / 86_400_000)} 天前`;
  const pad = (part: number) => String(part).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** The longest failure summary the rail may display, in characters. */
const FAILURE_SUMMARY_MAX = 120;

/**
 * R2: the display-safe summary of a failed run. Only the first line survives,
 * internal URL paths and Go type names are dropped, and the whole line is
 * capped at 120 characters. A leading `code:` becomes the category prefix
 * (`http.address_not_allowed · …`). The raw message stays in the run record
 * the row opens — it can name internal protocol routes and Go types.
 */
export function failureSummary(error: string | undefined): string {
  const firstLine = (error ?? '').split(/\r?\n/, 1)[0]?.replace(/\s+/g, ' ').trim() ?? '';
  if (!firstLine) return '';
  const match = /^([a-z][a-z0-9_.-]{1,63}):\s+/.exec(firstLine);
  const code = match?.[1];
  const rest = (match ? firstLine.slice(match[0].length) : firstLine)
    .replace(/https?:\/\/\S+/g, '')
    // Internal request paths, e.g. `/v1/runners/result`.
    .replace(/\s\/[A-Za-z0-9_{}.-]+(?:\/[A-Za-z0-9_{}.-]+)+/g, '')
    // Go type names, e.g. `*protocol.ReportResultRequest`.
    .replace(/\*?(?:[a-z][\w]*\.)+[A-Z]\w*/g, '')
    // The marshalling wrapper that only names the removed type.
    .replace(/\bjson: error calling MarshalJSON for type\s*:?/g, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+([:;,.])/g, '$1')
    .trim();
  const line = code ? (rest ? `${code} · ${rest}` : code) : rest;
  return line.length > FAILURE_SUMMARY_MAX
    ? `${line.slice(0, FAILURE_SUMMARY_MAX - 1)}…`
    : line;
}

/** Compact duration: `850ms`, `2.4s`, `1m 12s`, `2h 5m`. */export function formatDuration(ms: number): string {
  const total = Math.max(0, Math.round(ms));
  if (total < 1000) return `${total}ms`;
  if (total < 60_000) return `${(total / 1000).toFixed(1)}s`;
  const seconds = Math.floor(total / 1000);
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    const rest = seconds % 60;
    return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`;
  }
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`;
}

/** `success_rate` is a 0–1 fraction; the console prints one decimal percent. */
export function formatPercent(rate: number): string {
  return `${(rate * 100).toFixed(1)}%`;
}

/** The toolbar's window, in the words the column tooltips use. */
export function windowLabel(window: WorkflowStatsWindow): string {
  if (window === '24h') return '近 24 小时';
  if (window === '7d') return '近 7 天';
  return '近 30 天';
}

/** Where the 更新 cell's timestamp came from, in priority order. */
export type UpdatedSource = 'workflow' | 'execution-updated' | 'execution-created';

export interface UpdatedStamp {
  /** The RFC3339 value both the `<time>` and the title describe. */
  value: string;
  source: UpdatedSource;
  /** The full local time, named by the source that supplied it. */
  title: string;
}

/**
 * The 更新 column is the workflow's REGISTRY stamp when the server has one
 * (registered/replaced, OpenAPI `WorkflowListItem.updated_at`). Rows written
 * before that column existed only carry execution data, so the freshest
 * execution timestamp is the fallback — and its title says exactly that.
 * Absence of both is UNKNOWN: the caller draws "—", never a guess.
 */
export function updatedStamp(row: WorkflowSummary): UpdatedStamp | undefined {
  const workflowUpdated = row.updatedAt?.trim();
  if (workflowUpdated) {
    return {
      value: workflowUpdated,
      source: 'workflow',
      title: `工作流更新于 ${fullTimestamp(workflowUpdated)}`,
    };
  }
  const executionUpdated = row.latestExecution?.updatedAt;
  if (executionUpdated) {
    return {
      value: executionUpdated,
      source: 'execution-updated',
      title: `最近一次执行更新于 ${fullTimestamp(executionUpdated)}`,
    };
  }
  const executionCreated = row.recentExecutions?.[0]?.createdAt;
  if (executionCreated) {
    return {
      value: executionCreated,
      source: 'execution-created',
      title: `最近一次执行创建于 ${fullTimestamp(executionCreated)}`,
    };
  }
  return undefined;
}

/**
 * CSS media queries are the only truthful source for "does this viewport show
 * the secondary columns": a JS width read would disagree with the stylesheet
 * mid-resize. The first render already answers, so a 1024 host never mounts
 * the hidden columns for a frame.
 */
export function useMinWidth(query: string): boolean {
  const [matches, setMatches] = useState<boolean>(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return false;
    return window.matchMedia(query).matches;
  });
  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia(query);
    setMatches(media.matches);
    const onChange = (event: MediaQueryListEvent) => setMatches(event.matches);
    media.addEventListener?.('change', onChange);
    return () => media.removeEventListener?.('change', onChange);
  }, [query]);
  return matches;
}

/** The row star; the filled shape is the only visual difference. */
export function StarIcon({ filled = false }: { filled?: boolean }) {
  return (
    <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true" focusable="false">
      <path
        d="M12 3.8l2.5 5 5.5.8-4 3.9.9 5.5-4.9-2.6-4.9 2.6.9-5.5-4-3.9 5.5-.8z"
        fill={filled ? 'currentColor' : 'none'}
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/**
 * Trigger kinds as chips plus the raw cron expression the server projected.
 * The expression is the readable schedule the design asks for; the chips keep
 * the kind taxonomy, and the full summary stays in `title` when the cell clips.
 */
export function TriggerSummary({
  triggers,
  cronExpressions,
}: {
  triggers: string[];
  cronExpressions?: string[];
}) {
  // Deduplicate again so a stale mock cannot draw the same chip twice; the
  // server already deduplicates and sorts.
  const kinds = [...new Set(triggers)];
  const expressions = [...new Set((cronExpressions ?? []).filter(Boolean))];
  if (kinds.length === 0) {
    // No `xflow.trigger.*` node: the workflow can only be started by hand.
    return (
      <span className="xflow-admin-faint" data-trigger="manual">
        手动
      </span>
    );
  }
  const summary = [
    ...kinds.map((kind) => TRIGGER_LABELS[kind] ?? kind),
    ...expressions,
  ].join(' · ');
  return (
    <span className="xflow-row-triggers" title={summary}>
      {kinds.map((kind) => (
        <span className="xflow-admin-chip" key={kind} data-trigger={kind}>
          {TRIGGER_LABELS[kind] ?? kind}
        </span>
      ))}
      {expressions.map((expression) => (
        <span
          className="xflow-row-cron"
          key={expression}
          data-cron={expression}
          title={`Cron 表达式：${expression}`}
        >
          {expression}
        </span>
      ))}
    </span>
  );
}

/**
 * The last ≤10 runs as 8×16 chips, oldest on the left. Colour is a shortcut,
 * never the message: every chip carries the status label and full time in
 * `title`/`aria-label`, and each one opens that run record.
 */
export function RunStrip({
  executions,
  onOpen,
}: {
  executions?: WorkflowRecentExecution[];
  onOpen: (executionId: string) => void;
}) {
  if (!executions || executions.length === 0) {
    return (
      <span className="xflow-admin-faint" data-recent="missing">
        —
      </span>
    );
  }
  // Wire order is newest first; the strip reads chronologically.
  const ordered = [...executions].reverse().slice(-10);
  return (
    <span className="xflow-row-runs" data-recent={ordered.length} role="group" aria-label="最近 10 次运行">
      {ordered.map((run) => {
        const meta = workflowStatusMeta[run.status];
        const time = fullTimestamp(run.createdAt);
        const duration =
          run.durationMs === undefined || run.durationMs === null
            ? undefined
            : formatDuration(run.durationMs);
        const title = [meta?.label ?? run.status, time, duration].filter(Boolean).join(' · ');
        return (
          <button
            type="button"
            className="xflow-row-run"
            key={run.executionId}
            data-tone={meta?.tone ?? 'neutral'}
            data-status={run.status}
            data-exec={run.executionId}
            title={title}
            aria-label={`${title}，打开运行记录 ${run.executionId}`}
            onClick={(event) => {
              event.stopPropagation();
              onOpen(run.executionId);
            }}
          />
        );
      })}
    </span>
  );
}

/** Success rate over the requested window; absent stats stay an em dash. */
export function SuccessRateCell({ stats }: { stats?: WorkflowExecutionStats }) {
  if (!stats) {
    return (
      <span className="xflow-admin-faint" data-stats="missing">
        —
      </span>
    );
  }
  if (stats.successRate === null) {
    return (
      <span
        className="xflow-admin-faint"
        data-stats="no-terminal"
        title={`${windowLabel(stats.window)}内没有终态运行`}
      >
        —
      </span>
    );
  }
  // R2: neutral text + a small ratio bar; only the bar carries the threshold
  // colour, and the accessible name always states the percentage in words.
  const percent = formatPercent(stats.successRate);
  const tone =
    stats.successRate >= 0.95 ? 'success' : stats.successRate >= 0.8 ? 'warning' : 'danger';
  return (
    <span
      className="xflow-row-rate"
      data-tone={tone}
      data-runs={stats.runs}
      title={`${windowLabel(stats.window)} · 运行 ${stats.runs} 次 · 成功 ${stats.succeeded} · 失败 ${stats.failed} · 成功率 ${percent}`}
      aria-label={`${windowLabel(stats.window)}成功率 ${percent}`}
    >
      <span className="xflow-row-rate-value">{percent}</span>
      <span className="xflow-row-rate-bar" aria-hidden="true">
        <span
          className="xflow-row-rate-fill"
          style={{ width: `${Math.min(100, Math.max(0, stats.successRate * 100))}%` }}
        />
      </span>
    </span>
  );
}

/** P50 duration of terminal runs; unknown (no sample) stays an em dash. */
export function DurationCell({ stats }: { stats?: WorkflowExecutionStats }) {
  if (!stats || stats.p50Ms === undefined || stats.p50Ms === null) {
    return (
      <span
        className="xflow-admin-faint"
        data-duration="missing"
        title={stats ? `${windowLabel(stats.window)}内没有可计算耗时的终态运行` : undefined}
      >
        —
      </span>
    );
  }
  return (
    <span
      className="xflow-row-duration"
      data-ms={stats.p50Ms}
      title={`${windowLabel(stats.window)} · 样本 ${stats.sampleSize} 次终态运行`}
    >
      {formatDuration(stats.p50Ms)}
    </span>
  );
}
