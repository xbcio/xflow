import {
  XFlowApiError,
  type ExecutionStatsBucket,
  type ExecutionStatusCounts,
  type OverviewWindow,
} from '@xflow/api';

/**
 * A-A overview: the pure projections behind the dashboard. Kept out of the
 * page component so the wording of a degraded read and the KPI arithmetic can
 * be exercised without rendering SVG.
 */

/** The window switch's copy, used by the header and the chart caption. */
export const OVERVIEW_WINDOW_LABEL: Record<OverviewWindow, string> = {
  '24h': '近 24 小时',
  '7d': '近 7 天',
};

/** `failed` and `timeout` are one outcome to an operator reading the dashboard. */
export function failedCount(counts: ExecutionStatusCounts): number {
  return counts.failed + counts.timeout;
}

/** All nine status counts of one bucket. */
export function statusCount(counts: ExecutionStatusCounts): number {
  return (
    counts.pending +
    counts.queued +
    counts.paused +
    counts.running +
    counts.success +
    counts.failed +
    counts.canceling +
    counts.canceled +
    counts.timeout
  );
}

/** The window's total, summed from the histogram the KPIs were derived from. */
export function histogramTotal(histogram: ExecutionStatsBucket[]): number {
  return histogram.reduce((sum, bucket) => sum + statusCount(bucket.counts), 0);
}

export type DeltaDirection = 'up' | 'down' | 'flat';
/** How a movement should read: volume changes are neutral, worsening is bad. */
export type DeltaSemantics = 'good' | 'bad' | 'neutral';

export interface OverviewDelta {
  direction: DeltaDirection;
  /** Short badge text, e.g. `+12.5%` or `持平`. */
  label: string;
  /** Tooltip carrying the absolute previous-window number. */
  title: string;
  semantics: DeltaSemantics;
}

/**
 * Run volume against the immediately preceding window. The API publishes only
 * `runs_previous`, so this is the one KPI with a real second-window comparison.
 * More runs are not inherently good or bad, hence `neutral`; the same shape
 * with `semantics: 'bad'` is what a failure-count delta would use.
 */
export function runsDelta(runs: number, previous: number): OverviewDelta {
  if (previous <= 0) {
    if (runs === 0) {
      return { direction: 'flat', label: '持平', title: '上一窗口没有执行', semantics: 'neutral' };
    }
    return {
      direction: 'up',
      label: `新增 ${runs} 次`,
      title: '上一窗口没有执行，无法计算百分比',
      semantics: 'neutral',
    };
  }
  const diff = runs - previous;
  if (diff === 0) {
    return { direction: 'flat', label: '持平', title: `上一窗口 ${previous} 次`, semantics: 'neutral' };
  }
  const percent = (Math.abs(diff) / previous) * 100;
  return {
    direction: diff > 0 ? 'up' : 'down',
    label: `${diff > 0 ? '+' : '−'}${percent.toFixed(1)}%`,
    title: `上一窗口 ${previous} 次`,
    semantics: 'neutral',
  };
}

/**
 * Failures (failed + timeout) against the preceding window. Unlike run volume,
 * a rising failure count is bad and a falling one is good, so the badge carries
 * that meaning in its colour. The comparison is only drawn when the API
 * published `failed_previous`; the caller passes it here only when present.
 */
export function failedDelta(failed: number, previous: number): OverviewDelta {
  if (previous <= 0) {
    if (failed === 0) {
      return { direction: 'flat', label: '持平', title: '上一窗口没有失败', semantics: 'neutral' };
    }
    return {
      direction: 'up',
      label: `新增 ${failed} 次`,
      title: '上一窗口没有失败，无法计算百分比',
      semantics: 'bad',
    };
  }
  const diff = failed - previous;
  if (diff === 0) {
    return {
      direction: 'flat',
      label: '持平',
      title: `上一窗口 ${previous} 次`,
      semantics: 'neutral',
    };
  }
  const percent = (Math.abs(diff) / previous) * 100;
  return {
    direction: diff > 0 ? 'up' : 'down',
    label: `${diff > 0 ? '+' : '−'}${percent.toFixed(1)}%`,
    title: `上一窗口 ${previous} 次`,
    semantics: diff > 0 ? 'bad' : 'good',
  };
}

export interface OverviewFailure {
  /** Chinese headline; a 501 gets the deployment-specific wording. */
  message: string;
  detail: string;
  status?: number;
  code?: string;
  /** The deployment has no execution statistics (HTTP 501). */
  unsupported: boolean;
}

/**
 * Maps a rejected `getOverview` onto the page's error state. The three states
 * the server distinguishes get their own copy: a deployment that cannot
 * aggregate (501), a caller that may not read it (403), and everything else.
 * The HTTP status is always kept so the banner can print it.
 */
export function describeOverviewError(reason: unknown): OverviewFailure {
  if (reason instanceof XFlowApiError) {
    if (reason.status === 501 || reason.code === 'overview_unsupported') {
      return {
        message: '当前部署未启用执行统计',
        detail: '该部署的执行存储不支持概览聚合。',
        status: 501,
        code: reason.code,
        unsupported: true,
      };
    }
    if (reason.status === 403) {
      return {
        message: '没有查看概览的权限',
        detail: '当前凭据缺少 execution 权限。',
        status: 403,
        code: reason.code,
        unsupported: false,
      };
    }
    if (reason.status === 401) {
      return {
        message: '登录状态已失效',
        detail: '请重新登录后再试。',
        status: 401,
        code: reason.code,
        unsupported: false,
      };
    }
    return {
      message: '概览加载失败',
      detail: reason.message || '服务端返回了错误。',
      status: reason.status,
      code: reason.code,
      unsupported: false,
    };
  }
  return {
    message: '概览加载失败',
    detail:
      reason instanceof Error && reason.message
        ? reason.message
        : '网络请求失败，请检查连接后重试。',
    unsupported: false,
  };
}
