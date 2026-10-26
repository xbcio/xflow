import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  getOverview: vi.fn(),
}));

vi.mock('@umijs/max', () => ({
  history: { push: vi.fn(), replace: vi.fn(), location: { pathname: '/overview' } },
}));

vi.mock('@xflow/api', () => {
  class MockXFlowApiError extends Error {
    override readonly name = 'XFlowApiError';

    constructor(
      readonly status: number,
      message: string,
      readonly code?: string,
    ) {
      super(message);
    }
  }
  return {
    XFlowApiError: MockXFlowApiError,
    createXFlowApiClient: () => ({ getOverview: host.getOverview }),
  };
});

import { XFlowApiError, type OverviewResponse, type ExecutionStatusCounts } from '@xflow/api';
import { history } from '@umijs/max';

import { routes } from '../../../config/routes';
import OverviewPage from './index';
import { describeOverviewError, failedDelta, runsDelta } from './model';

const zeroCounts = (): ExecutionStatusCounts => ({
  pending: 0,
  queued: 0,
  paused: 0,
  running: 0,
  success: 0,
  failed: 0,
  canceling: 0,
  canceled: 0,
  timeout: 0,
});

/** 24 buckets; hour 0 succeeds, hour 1 fails twice and times out once. */
function overview(overrides: Partial<OverviewResponse> = {}): OverviewResponse {
  const histogram = Array.from({ length: 24 }, (_value, index) => {
    const counts = zeroCounts();
    if (index === 0) counts.success = 5;
    if (index === 1) {
      counts.failed = 2;
      counts.timeout = 1;
    }
    return { start: new Date(Date.UTC(2026, 9, 23, index)).toISOString(), counts };
  });
  return {
    window: '24h',
    generatedAt: '2026-10-24T01:00:00Z',
    kpis: {
      runs: 8,
      runsPrevious: 4,
      // 3 failures now vs 5 in the preceding window: falling is good.
      failedPrevious: 5,
      successRate: 0.875,
      p50Ms: 800,
      p95Ms: 42000,
      activeWorkflows: 2,
      sparkline: {
        runs: histogram.map((bucket) => bucket.counts.success + bucket.counts.failed),
        successRate: histogram.map((bucket) =>
          bucket.counts.success + bucket.counts.failed === 0
            ? null
            : bucket.counts.success / (bucket.counts.success + bucket.counts.failed),
        ),
      },
    },
    histogram,
    topFailing: [
      {
        workflowId: 'wf-1',
        workflowName: '客户同步',
        failed: 3,
        lastError: 'upstream 503',
        lastFailedAt: '2026-10-23T02:00:00Z',
      },
    ],
    ...overrides,
  };
}

function renderPage() {
  return render(
    <App>
      <OverviewPage />
    </App>,
  );
}

const kpi = (id: string) => document.querySelector(`[data-kpi="${id}"]`) as HTMLElement;

beforeEach(() => {
  vi.clearAllMocks();
  host.getOverview.mockResolvedValue(overview());
});

describe('overview dashboard (A-A)', () => {
  it('registers the route before the workflow list, behind execution read', () => {
    const paths = (routes as unknown as Array<{ path?: string; access?: string }>).map(
      (route) => route.path,
    );
    const route = (routes as unknown as Array<{ path?: string; access?: string }>).find(
      (entry) => entry.path === '/overview',
    );

    expect(route?.access).toBe('canViewExecutions');
    expect(paths.indexOf('/overview')).toBeLessThan(paths.indexOf('/workflows'));
  });

  it('draws the fixed skeleton first, then real KPIs, the 24-bucket chart and top-5', async () => {
    let resolve: (value: OverviewResponse) => void = () => {};
    host.getOverview.mockReturnValue(new Promise<OverviewResponse>((done) => (resolve = done)));
    renderPage();

    expect(screen.getByRole('status', { name: '正在加载概览' })).toBeTruthy();
    resolve(overview());

    await waitFor(() => expect(document.querySelectorAll('.xflow-ov-bucket')).toHaveLength(24));
    expect(within(kpi('runs')).getByText('8')).toBeTruthy();
    // 8 now vs 4 in the preceding window.
    expect(within(kpi('runs')).getByText('+100.0%')).toBeTruthy();
    expect(within(kpi('success-rate')).getByText('87.5%')).toBeTruthy();
    expect(within(kpi('failed')).getByText('3')).toBeTruthy();
    // The failure KPI carries a second-window comparison; falling is good.
    expect(within(kpi('failed')).getByText('−40.0%')).toBeTruthy();
    expect(kpi('failed').querySelector('.xflow-ov-delta')?.getAttribute('data-semantics')).toBe('good');
    expect(within(kpi('p50')).getByText('800ms')).toBeTruthy();
    expect(within(kpi('active-workflows')).getByText('2')).toBeTruthy();
    // The visually hidden table repeats every bucket for screen readers.
    expect(document.querySelectorAll('.xflow-admin-visually-hidden tbody tr')).toHaveLength(24);

    const firstBucket = document.querySelector('.xflow-ov-bucket');
    expect(firstBucket?.getAttribute('tabindex')).toBe('0');
    expect(firstBucket?.getAttribute('aria-label')).toContain('成功 5');

    // Only hours with executions draw bars; the other 22 hours are baseline
    // ticks, so the chart no longer reads as 24 sparse empty columns.
    expect(document.querySelectorAll('.xflow-ov-bucket[data-empty="true"]')).toHaveLength(22);
    expect(document.querySelectorAll('.xflow-ov-tick')).toHaveLength(22);
    expect(document.querySelectorAll('.xflow-ov-seg')).not.toHaveLength(0);

    fireEvent.click(screen.getByRole('button', { name: /客户同步/ }));
    expect(history.push).toHaveBeenCalledWith('/workflows/wf-1');
  });

  it('shows the empty-window copy instead of 24 empty columns', async () => {
    host.getOverview.mockResolvedValue(
      overview({
        histogram: Array.from({ length: 24 }, (_value, index) => ({
          start: new Date(Date.UTC(2026, 9, 23, index)).toISOString(),
          counts: zeroCounts(),
        })),
      }),
    );
    renderPage();

    expect(await screen.findByText('窗口内暂无执行。')).toBeTruthy();
    expect(document.querySelectorAll('.xflow-ov-bucket')).toHaveLength(0);
    expect(document.querySelectorAll('.xflow-ov-tick')).toHaveLength(0);
  });

  it('replaces the histogram tooltip when focus jumps between bars', async () => {
    renderPage();
    await waitFor(() => expect(document.querySelectorAll('.xflow-ov-bucket')).toHaveLength(24));
    const buckets = Array.from(document.querySelectorAll<SVGGElement>('.xflow-ov-bucket'));
    const described = () => buckets.filter((bucket) => bucket.getAttribute('aria-describedby'));
    const visibleTooltips = () => document.querySelectorAll('.ant-tooltip:not(.ant-tooltip-hidden)');

    fireEvent.focus(buckets[0]);
    await waitFor(() => expect(described()).toEqual([buckets[0]]));
    expect(visibleTooltips()).toHaveLength(1);

    // Focus moves straight to the next bar without an intervening blur. The
    // open tooltip must be replaced rather than stacked: a second overlay
    // visible at the same time is the bug this pins.
    fireEvent.focus(buckets[1]);
    await waitFor(() => expect(described()).toEqual([buckets[1]]));
    expect(visibleTooltips()).toHaveLength(1);
    expect(visibleTooltips()[0]?.textContent).toContain('失败 2');

    fireEvent.blur(buckets[1]);
    await waitFor(() => expect(described()).toHaveLength(0));
  });

  it('degrades the cluster card to a permission note instead of failing', async () => {
    renderPage();
    await screen.findByText('运行数');

    const cluster = document.querySelector('[data-cluster="unavailable"]');
    expect(cluster).not.toBeNull();
    expect(cluster?.textContent).toContain('需要 management.runner.list 权限');
    expect(screen.queryByText('当前部署未启用执行统计')).toBeNull();
  });

  it('shows runner summary, capacity and leader state when the segment is present', async () => {
    host.getOverview.mockResolvedValue(
      overview({ cluster: { isLeader: true, runners: { online: 2, total: 3, capacity: 8, inFlight: 3 } } }),
    );
    renderPage();
    await screen.findByText('在线 Runner');

    const cluster = document.querySelector('[data-cluster="available"]') as HTMLElement;
    expect(within(cluster).getByText('2 / 3')).toBeTruthy();
    expect(within(cluster).getByText('本节点')).toBeTruthy();
    expect(cluster.querySelector('[role="progressbar"]')?.getAttribute('aria-valuenow')).toBe('3');
  });

  it('gives a 501 deployment its own copy, the HTTP status and a working retry', async () => {
    host.getOverview.mockRejectedValueOnce(
      new XFlowApiError(501, 'the configured execution store does not support the overview', 'overview_unsupported'),
    );
    host.getOverview.mockResolvedValueOnce(overview());
    renderPage();

    expect(await screen.findByText('当前部署未启用执行统计')).toBeTruthy();
    // The status code is diagnostic detail, not Chinese copy.
    expect(screen.queryByText(/HTTP 501/)).toBeNull();
    expect(document.querySelector('[data-error-detail]')?.getAttribute('data-error-detail')).toContain(
      'HTTP 501',
    );

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    await screen.findByText('运行数');
    expect(host.getOverview).toHaveBeenCalledTimes(2);
  });

  it('keeps the server text and status out of the failure copy', async () => {
    host.getOverview.mockRejectedValueOnce(new XFlowApiError(500, 'internal error'));
    renderPage();

    expect(await screen.findByText('概览加载失败')).toBeTruthy();
    const banner = document.querySelector('[data-error-detail]') as HTMLElement;
    expect(banner.textContent).not.toContain('internal error');
    expect(banner.textContent).not.toContain('HTTP 500');
    expect(banner.getAttribute('data-error-detail')).toBe('HTTP 500 · internal error');
    // Headline and hint are separated, so a reader does not hear them fused.
    expect(banner.textContent).toMatch(/概览加载失败\s+请稍后重试。/);
  });

  it('switches the window and requests that window from the server', async () => {
    renderPage();
    await screen.findByText('运行数');

    fireEvent.click(screen.getByText('7 天'));
    await waitFor(() => expect(host.getOverview).toHaveBeenLastCalledWith({ window: '7d' }));
    await screen.findByText('运行数');
  });
});

describe('overview helpers', () => {
  it('treats a missing previous window as no percentage, not a divide-by-zero', () => {
    expect(runsDelta(5, 0)).toMatchObject({ direction: 'up', label: '新增 5 次', semantics: 'neutral' });
    expect(runsDelta(0, 0)).toMatchObject({ direction: 'flat', label: '持平' });
    expect(runsDelta(10, 8)).toMatchObject({ direction: 'up', label: '+25.0%' });
    expect(runsDelta(6, 8)).toMatchObject({ direction: 'down', label: '−25.0%' });
  });

  it('reads a rising failure count as bad and a falling one as good', () => {
    expect(failedDelta(8, 6)).toMatchObject({ direction: 'up', label: '+33.3%', semantics: 'bad' });
    expect(failedDelta(4, 6)).toMatchObject({ direction: 'down', label: '−33.3%', semantics: 'good' });
    expect(failedDelta(6, 6)).toMatchObject({ direction: 'flat', semantics: 'neutral' });
    // No failures on either side is a positive flat, not a fabricated rise.
    expect(failedDelta(0, 0)).toMatchObject({ direction: 'flat', semantics: 'neutral' });
    expect(failedDelta(3, 0)).toMatchObject({ direction: 'up', semantics: 'bad' });
  });

  it('maps a 501 to unsupported and keeps the HTTP status on every branch', () => {
    expect(
      describeOverviewError(new XFlowApiError(501, 'nope', 'overview_unsupported')),
    ).toMatchObject({ message: '当前部署未启用执行统计', status: 501, unsupported: true });
    expect(describeOverviewError(new XFlowApiError(403, 'forbidden'))).toMatchObject({
      message: '没有查看概览的权限',
      status: 403,
    });
    expect(describeOverviewError(new XFlowApiError(500, 'boom'))).toMatchObject({
      message: '概览加载失败',
      detail: 'boom',
      status: 500,
    });
  });
});
