import { act, createEvent, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  search: '',
  push: vi.fn(),
  replace: vi.fn(),
  listWorkflows: vi.fn(),
  listNodeTypes: vi.fn(),
  createWorkflow: vi.fn(),
  runWorkflow: vi.fn(),
  getWorkflow: vi.fn(),
  fetchRunners: vi.fn(),
  fetchRunnerPools: vi.fn(),
  listExecutions: vi.fn(),
  clientOptions: undefined as { fetcher?: unknown } | undefined,
  navCounts: [] as Array<number | undefined>,
  currentUser: { subject: 'tester', namespace: 'team-a', scopes: [] } as
    | { subject: string; namespace: string; scopes: string[] }
    | undefined,
}));

vi.mock('@umijs/max', () => ({
  history: { push: host.push, replace: host.replace },
  useLocation: () => ({ pathname: '/workflows', search: host.search }),
  useAccess: () => ({
    canViewExecutions: host.currentUser?.scopes.includes('execution') ?? false,
  }),
  useModel: () => ({ initialState: { currentUser: host.currentUser } }),
}));

vi.mock('@xflow/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xflow/api')>();
  return {
    ...actual,
    createXFlowApiClient: (options: { fetcher?: unknown }) => {
      host.clientOptions = options;
      return {
        listWorkflows: host.listWorkflows,
        listNodeTypes: host.listNodeTypes,
        createWorkflow: (workflow: unknown) => host.createWorkflow(workflow),
        runWorkflow: (id: string) => host.runWorkflow(id),
        getWorkflow: (id: string) => host.getWorkflow(id),
        listExecutions: host.listExecutions,
      };
    },
  };
});

vi.mock('../../services/management', () => ({
  fetchRunners: () => host.fetchRunners(),
  fetchRunnerPools: () => host.fetchRunnerPools(),
}));

vi.mock('../../layouts/AdminShell', () => ({
  useAdminWorkflowCount: (count: number | undefined) => host.navCounts.push(count),
}));

import { authorizedFetch } from '../../services/http';
import { XFlowApiError, type WorkflowSummary } from '@xflow/api';
import { WorkflowCenter } from './WorkflowCenter';
import { failureSummary } from './row-parts';

const wf = (
  id: string,
  name: string,
  version: string,
  triggers: string[] = [],
  latestExecution?: WorkflowSummary['latestExecution'],
  extra: Partial<WorkflowSummary> = {},
): WorkflowSummary => ({
  id,
  name,
  version,
  definitionHash: `sha256:${id}`,
  registryRevision: 1,
  triggers,
  ...(latestExecution ? { latestExecution } : {}),
  ...extra,
});

const page = (list: ReturnType<typeof wf>[], total = list.length) => ({ list, total });

/** The list call the page always makes, before test-specific overrides. */
const listArgs = (overrides: Record<string, unknown> = {}) => ({
  page: 1,
  pageSize: 20,
  q: undefined,
  include: ['stats', 'recent'],
  window: '24h',
  ...overrides,
});

/**
 * The suite's global matchMedia stub answers `matches: false`, which is the
 * 1024 viewport (secondary columns dropped). Tests for P50/更新 need the 1280+
 * answer; they install this and restore it in `finally`.
 */
const setWideViewport = (matches: boolean) => {
  const original = window.matchMedia;
  window.matchMedia = ((query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia;
  return () => {
    window.matchMedia = original;
  };
};

const failure = (
  id: string,
  workflowName: string,
  status: 'failed' | 'timeout' = 'failed',
  workflowId = 'wf-1',
  error = 'upstream 503',
) => ({
  executionId: id,
  namespace: 'team-a',
  workflowId,
  workflowName,
  status,
  error,
  createdAt: '2026-10-19T14:02:23Z',
  updatedAt: '2026-10-19T14:02:25Z',
});

/**
 * Node 22's experimental `localStorage` global shadows jsdom's implementation,
 * leaving `window.localStorage` undefined. Favourites are a real localStorage
 * contract (P6), so the suite installs a plain Storage per test.
 */
class MemoryStorage implements Storage {
  private map = new Map<string, string>();
  get length(): number {
    return this.map.size;
  }
  clear(): void {
    this.map.clear();
  }
  getItem(key: string): string | null {
    return this.map.get(key) ?? null;
  }
  key(index: number): string | null {
    return [...this.map.keys()][index] ?? null;
  }
  removeItem(key: string): void {
    this.map.delete(key);
  }
  setItem(key: string, value: string): void {
    this.map.set(key, String(value));
  }
}

beforeEach(() => {
  vi.clearAllMocks();
  Object.defineProperty(window, 'localStorage', {
    value: new MemoryStorage(),
    configurable: true,
  });
  host.currentUser = {
    subject: 'tester',
    namespace: 'team-a',
    // The KPI strip reads the runner roster and pools only with these scopes;
    // the no-scope test below asserts the requests are skipped entirely.
    scopes: ['workflow', 'execution', 'management.runner.list', 'management.runner_pool.read'],
  };
  host.search = '';
  host.navCounts = [];
  host.listWorkflows.mockResolvedValue(page([wf('wf-1', '客户同步', 'v3'), wf('wf-2', '夜间对账', 'v1.4.0')]));
  host.listNodeTypes.mockResolvedValue({
    param_validation_mode: 'warn',
    node_types: Array.from({ length: 28 }, (_, index) => ({ node_type: `t${index}` })),
  });
  host.fetchRunners.mockResolvedValue([
    { runner_id: 'r1', enrolled: true, state: 'online' },
    { runner_id: 'r2', enrolled: true, state: 'offline' },
    { runner_id: 'r3', enrolled: false, state: 'never_connected' },
  ]);
  host.fetchRunnerPools.mockResolvedValue([{ id: 'p1' }, { id: 'p2' }, { id: 'p3' }]);
  host.listExecutions.mockResolvedValue({
    list: [
      failure('exec-9', '告警归因', 'timeout', 'wf-1', 'context deadline exceeded'),
      failure('exec-7', '夜间对账', 'failed', 'wf-2', 'upstream 503'),
    ],
    total: 2,
  });
  host.createWorkflow.mockResolvedValue({ workflowId: 'wf-imported' });
  host.runWorkflow.mockResolvedValue({ executionId: 'exec-new' });
  host.getWorkflow.mockResolvedValue({ workflow: { name: '客户同步', nodes: [] } });
});

const rows = () => [...document.querySelectorAll('tr[data-workflow]')].map((row) => row.getAttribute('data-workflow'));
const card = (id: string) => document.querySelector(`[data-env="${id}"]`);
const pageHeader = () => document.querySelector('[data-page-header]') as HTMLElement;
const headerButton = (name: string) => within(pageHeader()).getByRole('button', { name });

describe('workflow table', () => {
  it('renders a server page with only the columns the backend has (ruling 20)', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    expect(host.listWorkflows).toHaveBeenCalledWith(listArgs());
    expect(rows()).toEqual(['wf-1', 'wf-2']);
    const nameCell = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-name');
    // The second line now carries 描述 · 版本 · ID; without a description the
    // row draws only the two facts it has (A/B row rework).
    expect(nameCell?.textContent).toBe('客户同步v3wf-1');
    // Long names truncate in the fixed layout; the full value stays in title.
    expect(nameCell?.querySelector('.xflow-row-name__title')?.getAttribute('title')).toBe('客户同步');
    expect(document.querySelector('tr[data-workflow="wf-1"] [data-version]')?.textContent).toBe('v3');

    const headers = [...document.querySelectorAll('thead th')].map((th) => th.textContent ?? '');
    expect(headers.some((text) => text.includes('名称'))).toBe(true);
    expect(headers.some((text) => text.includes('触发'))).toBe(true);
    expect(headers.some((text) => text.includes('最近运行'))).toBe(true);
    expect(headers.some((text) => text.includes('成功率'))).toBe(true);
    // Ruling 20: the list endpoint has no node count, so the column is gone.
    expect(headers).not.toContain('节点');
    expect(document.body.textContent).not.toContain('需新字段');
  });

  it('shows only the first eight characters of the id and copies the full value (AL-11)', async () => {
    const longId = 'wf-1234567890abcdef';
    host.listWorkflows.mockResolvedValue(page([wf(longId, '客户同步', 'v3')]));
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain(longId));

    const cell = document.querySelector(`tr[data-workflow="${longId}"] .xflow-admin-col-name`) as HTMLElement;
    const shortId = cell.querySelector('[data-copy-id]') as HTMLButtonElement;
    expect(shortId.textContent).toBe('wf-12345');
    expect(shortId.getAttribute('title')).toBe(longId);

    fireEvent.click(shortId);
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(longId));
    // The copy control must not fall through to the row's open action.
    expect(host.push).not.toHaveBeenCalled();
  });

  it('renders trigger kinds as Chinese chips, deduplicated, with unknown kinds verbatim', async () => {
    host.listWorkflows.mockResolvedValue(
      page([
        wf('wf-1', '客户同步', 'v3', ['webhook', 'timer', 'webhook', 'smtp']),
        wf('wf-2', '夜间对账', 'v1', ['cron', 'kafka']),
      ]),
    );
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const entry = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-entry');
    expect(entry?.textContent).toBe('Webhook定时smtp');
    expect(
      [...(entry?.querySelectorAll('.xflow-admin-chip') ?? [])].map((chip) =>
        chip.getAttribute('data-trigger'),
      ),
    ).toEqual(['webhook', 'timer', 'smtp']);
    expect(document.querySelector('tr[data-workflow="wf-2"] .xflow-admin-col-entry')?.textContent).toBe(
      'CronKafka',
    );
  });

  it('shows 手动 when a workflow has no trigger node', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const entry = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-entry');
    expect(entry?.textContent).toBe('手动');
    expect(entry?.querySelector('.xflow-admin-chip')).toBeNull();
  });

  it('renders the newest runs as a chronological status strip and opens one run record', async () => {
    host.listWorkflows.mockResolvedValue(
      page([
        wf(
          'wf-1',
          '客户同步',
          'v3',
          ['webhook'],
          {
            executionId: 'exec-9',
            status: 'success',
            // No zone suffix: parsed as local time, so the assertion is TZ-independent.
            createdAt: '2026-10-19T14:02:23',
            updatedAt: '2026-10-19T14:02:29',
          },
          {
            recentExecutions: [
              // Wire order is newest first; the strip must read oldest → newest.
              { executionId: 'exec-3', status: 'failed', createdAt: '2026-10-19T14:03:23', durationMs: 1234 },
              { executionId: 'exec-2', status: 'success', createdAt: '2026-10-19T14:02:23', durationMs: 980 },
              { executionId: 'exec-1', status: 'running', createdAt: '2026-10-19T14:01:23' },
            ],
          },
        ),
        wf('wf-2', '夜间对账', 'v1'),
      ]),
    );
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const cell = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-recent');
    const chips = [...(cell?.querySelectorAll('.xflow-row-run') ?? [])] as HTMLElement[];
    expect(chips.map((chip) => chip.getAttribute('data-exec'))).toEqual(['exec-1', 'exec-2', 'exec-3']);
    expect(chips.map((chip) => chip.getAttribute('data-tone'))).toEqual(['info', 'success', 'danger']);
    // Colour is never the only signal: the label and full time live in title/aria.
    expect(chips[2].getAttribute('title')).toContain('失败');
    expect(chips[2].getAttribute('title')).toContain('2026-10-19 14:03:23');
    expect(chips[2].getAttribute('title')).toContain('1.2s');
    expect(chips[0].getAttribute('aria-label')).toContain('运行中');
    fireEvent.click(chips[2]);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-3');

    // A workflow whose recent-execution lookup the server could not answer
    // shows no invented run: absence is unknown, drawn as an em dash.
    const empty = document.querySelector('tr[data-workflow="wf-2"] .xflow-admin-col-recent');
    expect(empty?.textContent).toBe('—');
    expect(empty?.querySelector('.xflow-row-run')).toBeNull();
  });

  it('keeps the run strip to one tab stop and moves it with the arrow keys', async () => {
    host.listWorkflows.mockResolvedValue(
      page([
        wf('wf-1', '客户同步', 'v3', [], undefined, {
          recentExecutions: [
            { executionId: 'exec-3', status: 'failed', createdAt: '2026-10-19T14:03:23' },
            { executionId: 'exec-2', status: 'success', createdAt: '2026-10-19T14:02:23' },
            { executionId: 'exec-1', status: 'success', createdAt: '2026-10-19T14:01:23' },
          ],
        }),
      ]),
    );
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const group = screen.getByRole('group', { name: '最近运行' });
    const chips = () => [...group.querySelectorAll('.xflow-row-run')] as HTMLButtonElement[];
    const stops = () => chips().filter((chip) => chip.tabIndex === 0).map((chip) => chip.getAttribute('data-exec'));
    expect(chips()).toHaveLength(3);
    expect(stops()).toEqual(['exec-1']);

    fireEvent.keyDown(chips()[0], { key: 'ArrowRight' });
    expect(document.activeElement).toBe(chips()[1]);
    expect(stops()).toEqual(['exec-2']);

    fireEvent.keyDown(chips()[1], { key: 'End' });
    expect(document.activeElement).toBe(chips()[2]);
    // The strip does not wrap past either end, and never leaves the group.
    fireEvent.keyDown(chips()[2], { key: 'ArrowRight' });
    expect(document.activeElement).toBe(chips()[2]);
    expect(stops()).toEqual(['exec-3']);

    fireEvent.keyDown(chips()[2], { key: 'Home' });
    expect(document.activeElement).toBe(chips()[0]);
    fireEvent.keyDown(chips()[0], { key: 'ArrowLeft' });
    expect(document.activeElement).toBe(chips()[0]);
    expect(stops()).toEqual(['exec-1']);

    // Clicking still opens that run, and a click also parks the tab stop there.
    fireEvent.click(chips()[1]);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-2');
    expect(stops()).toEqual(['exec-2']);
  });

  it('renders the window success rate and P50, and degrades omitted fields to —', async () => {
    const restoreViewport = setWideViewport(true);
    try {
      host.listWorkflows.mockResolvedValue(
        page([
          wf('wf-1', '客户同步', 'v3', [], undefined, {
            stats: {
              window: '24h',
              runs: 12,
              succeeded: 10,
              failed: 2,
              successRate: 0.8333,
              p50Ms: 72000,
              p95Ms: 90000,
              sampleSize: 12,
            },
            latestExecution: {
              executionId: 'exec-9',
              status: 'success',
              createdAt: '2026-10-19T14:02:23',
              updatedAt: '2026-10-19T14:02:29',
            },
          }),
          wf('wf-2', '夜间对账', 'v1'),
        ]),
      );
      render(<WorkflowCenter />);
      await waitFor(() => expect(rows()).toContain('wf-1'));

      const rate = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-rate');
      expect(rate?.textContent).toBe('83.3%');
      // 80–95%: the bar is warning-coloured, the text stays neutral.
      expect(rate?.querySelector('.xflow-row-rate')?.getAttribute('data-tone')).toBe('warning');
      expect(rate?.querySelector('.xflow-row-rate')?.getAttribute('aria-label')).toBe(
        '近 24 小时成功率 83.3%',
      );
      expect(
        Number.parseFloat(
          (rate?.querySelector('.xflow-row-rate-fill') as HTMLElement).style.width,
        ),
      ).toBeCloseTo(83.33, 1);
      expect(rate?.querySelector('.xflow-row-rate')?.getAttribute('title')).toContain('运行 12 次');
      const p50 = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-p50');
      expect(p50?.textContent).toBe('1m 12s');
      expect(p50?.querySelector('.xflow-row-duration')?.getAttribute('data-ms')).toBe('72000');

      // Absent stats is UNKNOWN, never a fabricated zero or duration.
      const bare = document.querySelector('tr[data-workflow="wf-2"]');
      expect(bare?.querySelector('.xflow-admin-col-rate')?.textContent).toBe('—');
      expect(bare?.querySelector('.xflow-admin-col-p50')?.textContent).toBe('—');
    } finally {
      restoreViewport();
    }
  });

  it('colours only the ratio bar by threshold, never the rate text (R2)', async () => {
    const restoreViewport = setWideViewport(true);
    try {
      const rated = (id: string, rate: number, succeeded: number) =>
        wf(id, id, 'v1', [], undefined, {
          stats: {
            window: '24h',
            runs: 100,
            succeeded,
            failed: 100 - succeeded,
            successRate: rate,
            p50Ms: 900,
            p95Ms: 1200,
            sampleSize: 100,
          },
        });
      host.listWorkflows.mockResolvedValue(
        page([rated('wf-a', 0.96, 96), rated('wf-b', 0.8, 80), rated('wf-c', 0.79, 79)]),
      );
      render(<WorkflowCenter />);
      await waitFor(() => expect(rows()).toContain('wf-a'));

      const tone = (id: string) =>
        document.querySelector(`tr[data-workflow="${id}"] .xflow-row-rate`)?.getAttribute('data-tone');
      // ≥95% success, 80–95% warning, <80% danger.
      expect(tone('wf-a')).toBe('success');
      expect(tone('wf-b')).toBe('warning');
      expect(tone('wf-c')).toBe('danger');
      // The percentage itself is drawn as text in every band.
      expect(
        document.querySelector('tr[data-workflow="wf-c"] .xflow-row-rate-value')?.textContent,
      ).toBe('79.0%');
    } finally {
      restoreViewport();
    }
  });

  it('shows the relative update time with the full timestamp in its title', async () => {
    const restoreViewport = setWideViewport(true);
    try {
      host.listWorkflows.mockResolvedValue(
        page([
          wf('wf-1', '客户同步', 'v3', [], {
            executionId: 'exec-9',
            status: 'success',
            createdAt: '2020-01-02T03:04:05',
            updatedAt: '2020-01-02T03:04:05',
          }),
        ]),
      );
      render(<WorkflowCenter />);
      await waitFor(() => expect(rows()).toContain('wf-1'));

      const time = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-updated time');
      expect(time?.getAttribute('datetime')).toBe('2020-01-02T03:04:05');
      expect(time?.getAttribute('title')).toBe('最近一次执行更新于 2020-01-02 03:04:05');
      // Older than 30 days: the cell falls back to the date, deterministically.
      expect(time?.textContent).toBe('2020-01-02');
    } finally {
      restoreViewport();
    }
  });

  it('prefers the registry updated_at over the latest execution time', async () => {
    const restoreViewport = setWideViewport(true);
    try {
      host.listWorkflows.mockResolvedValue(
        page([
          wf('wf-1', '客户同步', 'v3', [], {
            executionId: 'exec-9',
            status: 'success',
            createdAt: '2020-01-02T03:04:05',
            updatedAt: '2026-10-19T14:02:29',
          }, {
            // No zone suffix: parsed as local time, so the assertion is
            // TZ-independent (the existing execution-stamp tests do the same).
            updatedAt: '2020-01-02T03:04:05',
          }),
          // Old record without the registry stamp: the execution time is the
          // only recency signal and the title says so.
          wf('wf-2', '夜间对账', 'v1', [], {
            executionId: 'exec-8',
            status: 'success',
            createdAt: '2020-01-02T03:04:05',
            updatedAt: '2020-01-02T03:04:05',
          }),
        ]),
      );
      render(<WorkflowCenter />);
      await waitFor(() => expect(rows()).toContain('wf-1'));

      const registry = document.querySelector(
        'tr[data-workflow="wf-1"] .xflow-admin-col-updated time',
      );
      expect(registry?.getAttribute('datetime')).toBe('2020-01-02T03:04:05');
      expect(registry?.getAttribute('data-updated-source')).toBe('workflow');
      expect(registry?.getAttribute('title')).toBe('工作流更新于 2020-01-02 03:04:05');
      expect(registry?.textContent).toBe('2020-01-02');

      const fallback = document.querySelector(
        'tr[data-workflow="wf-2"] .xflow-admin-col-updated time',
      );
      expect(fallback?.getAttribute('data-updated-source')).toBe('execution-updated');
      expect(fallback?.getAttribute('title')).toBe('最近一次执行更新于 2020-01-02 03:04:05');
    } finally {
      restoreViewport();
    }
  });

  it('switches the stats window from the toolbar and returns to page 1', async () => {
    host.listWorkflows.mockResolvedValue(page([wf('wf-1', '第一页', 'v1')], 41));
    render(<WorkflowCenter />);
    await screen.findByText('第一页');

    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1] as HTMLElement);
    await waitFor(() =>
      expect(host.listWorkflows).toHaveBeenLastCalledWith(listArgs({ page: 2 })),
    );

    fireEvent.click(screen.getByText('7d'));
    await waitFor(() =>
      expect(host.listWorkflows).toHaveBeenLastCalledWith(listArgs({ window: '7d' })),
    );
    expect(screen.getByRole('toolbar', { name: '工作流列表工具栏' })).toBeTruthy();
  });

  it('stars a workflow into the per-identity local key and filters the page by favourites', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const star = () =>
      document.querySelector('tr[data-workflow="wf-1"] .xflow-row-favorite') as HTMLElement;
    expect(star().getAttribute('aria-pressed')).toBe('false');
    fireEvent.click(star());
    expect(star().getAttribute('aria-pressed')).toBe('true');
    // P6: no favourites table this iteration; the key is scoped to subject and
    // namespace so two identities sharing a browser stay separate.
    expect(JSON.parse(window.localStorage.getItem('xflow.fav.tester.team-a') ?? '[]')).toEqual([
      'wf-1',
    ]);

    fireEvent.click(screen.getByRole('button', { name: /只看收藏/ }));
    expect(rows()).toEqual(['wf-1']);
    expect(document.querySelector('[data-filtered-count]')?.textContent).toBe('收藏 1');

    // Un-starring the only favourite leaves the honest empty state for the filter.
    fireEvent.click(star());
    expect(await screen.findByText('当前页没有收藏的工作流')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: '显示全部' }));
    expect(rows()).toEqual(['wf-1', 'wf-2']);
  });

  it('falls back to the anonymous favourites key without a fetched identity', async () => {
    host.currentUser = undefined;
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    fireEvent.click(document.querySelector('tr[data-workflow="wf-1"] .xflow-row-favorite') as HTMLElement);
    expect(JSON.parse(window.localStorage.getItem('xflow.fav.anonymous.default') ?? '[]')).toEqual([
      'wf-1',
    ]);
  });

  it('paginates on the server and keeps the page footer derived from total', async () => {
    host.listWorkflows.mockImplementation(({ page: requested }) =>
      Promise.resolve(
        requested === 1
          ? page([wf('wf-1', '第一页', 'v1')], 41)
          : page([wf('wf-21', '第二页', 'v2')], 41),
      ),
    );
    render(<WorkflowCenter />);
    await screen.findByText('第一页');

    const footer = () => document.querySelector('.xflow-admin-page');
    expect(footer()?.textContent).toContain('第 1 / 3 页');
    // AD-18: more than one page also states the server total.
    expect(footer()?.textContent).toContain('共 41 条');
    const [previous, next] = [...document.querySelectorAll('.xflow-admin-page button')] as HTMLButtonElement[];
    expect(previous.disabled).toBe(true);
    expect(next.disabled).toBe(false);

    fireEvent.click(next);
    await screen.findByText('第二页');
    expect(host.listWorkflows).toHaveBeenLastCalledWith(listArgs({ page: 2 }));
    expect(rows()).toEqual(['wf-21']);
    expect(footer()?.textContent).toContain('第 2 / 3 页');
  });

  it('opens the row menu, then opens, copies, runs and exports from it', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    const createObjectURL = vi.fn(() => 'blob:mock');
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, 'createObjectURL', { value: createObjectURL, configurable: true });
    Object.defineProperty(URL, 'revokeObjectURL', { value: revokeObjectURL, configurable: true });
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});

    try {
      render(<WorkflowCenter />);
      await waitFor(() => expect(rows()).toContain('wf-1'));

      const menuButton = screen.getByRole('button', { name: '更多操作：客户同步' });
      fireEvent.click(menuButton);
      fireEvent.click(await screen.findByRole('menuitem', { name: '打开' }));
      expect(host.push).toHaveBeenCalledWith('/workflows/wf-1');

      host.push.mockClear();
      fireEvent.click(menuButton);
      fireEvent.click(await screen.findByRole('menuitem', { name: '复制 ID' }));
      await waitFor(() => expect(writeText).toHaveBeenCalledWith('wf-1'));
      expect(host.push).not.toHaveBeenCalled();

      fireEvent.click(menuButton);
      fireEvent.click(await screen.findByRole('menuitem', { name: '运行' }));
      await waitFor(() => expect(host.runWorkflow).toHaveBeenCalledWith('wf-1'));
      await waitFor(() =>
        expect(host.push).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-new'),
      );

      fireEvent.click(menuButton);
      fireEvent.click(await screen.findByRole('menuitem', { name: '导出 DSL' }));
      await waitFor(() => expect(host.getWorkflow).toHaveBeenCalledWith('wf-1'));
      await waitFor(() => expect(createObjectURL).toHaveBeenCalledTimes(1));
      expect(anchorClick).toHaveBeenCalled();
      expect(revokeObjectURL).toHaveBeenCalledWith('blob:mock');
    } finally {
      anchorClick.mockRestore();
    }
  });

  it('opens a row on click or Enter without stealing an in-row control', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const row = document.querySelector('tr[data-workflow="wf-1"]') as HTMLElement;
    expect(row.getAttribute('tabindex')).toBe('0');
    expect(row.className).toContain('xflow-admin-row-clickable');

    fireEvent.click(row);
    expect(host.push).toHaveBeenLastCalledWith('/workflows/wf-1');

    fireEvent.keyDown(row, { key: 'Enter' });
    expect(host.push).toHaveBeenLastCalledWith('/workflows/wf-1');

    // The star is the row's own control: toggling it must not navigate.
    host.push.mockClear();
    fireEvent.click(document.querySelector('tr[data-workflow="wf-1"] .xflow-row-favorite') as HTMLElement);
    expect(host.push).not.toHaveBeenCalled();
    expect(
      document.querySelector('tr[data-workflow="wf-1"] .xflow-row-favorite')?.getAttribute('data-fav'),
    ).toBe('true');
  });

  it('draws the name as a real link to the workflow that a plain click routes in-app', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const link = document.querySelector(
      'tr[data-workflow="wf-1"] .xflow-admin-col-name a[href]',
    ) as HTMLAnchorElement;
    expect(link.getAttribute('href')).toBe('/workflows/wf-1');
    expect(link.textContent).toBe('客户同步');
    expect(screen.getByRole('link', { name: '客户同步' })).toBe(link);

    // The row handler also listens, so a click must navigate exactly once and
    // must not fall through to the browser's own navigation.
    const click = createEvent.click(link);
    fireEvent(link, click);
    expect(click.defaultPrevented).toBe(true);
    expect(host.push).toHaveBeenCalledTimes(1);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1');
  });

  it('leaves modified and non-primary link clicks to the browser', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const link = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-name a[href]') as HTMLElement;
    // Read the flag after React has had its turn, then stop jsdom from really
    // navigating (it has no navigation and logs an error for every anchor).
    const seen: boolean[] = [];
    const record = (event: Event) => {
      seen.push(event.defaultPrevented);
      event.preventDefault();
    };
    document.addEventListener('click', record);
    try {
      for (const init of [{ metaKey: true }, { ctrlKey: true }, { shiftKey: true }, { altKey: true }, { button: 1 }]) {
        fireEvent(link, createEvent.click(link, init));
      }
    } finally {
      document.removeEventListener('click', record);
    }
    expect(seen).toEqual([false, false, false, false, false]);
    expect(host.push).not.toHaveBeenCalled();
  });

  it('draws a fixed skeleton on the first load instead of a spinner over an empty header', async () => {
    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listWorkflows.mockReturnValue(new Promise((done) => { resolve = done; }));
    render(<WorkflowCenter />);

    await waitFor(() => expect(document.querySelector('.xflow-admin-table-skeleton')).not.toBeNull());
    expect(document.querySelectorAll('.xflow-admin-skeleton-row')).toHaveLength(5);
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();

    resolve(page([wf('wf-1', '客户同步', 'v3')]));
    await waitFor(() => expect(rows()).toContain('wf-1'));
    expect(document.querySelector('.xflow-admin-table-skeleton')).toBeNull();
  });

  it('keeps the loaded page on screen while the next page loads and after it fails', async () => {
    host.listWorkflows.mockResolvedValueOnce(page([wf('wf-1', '第一页', 'v1')], 41));
    let failSecond!: (reason: Error) => void;
    host.listWorkflows.mockImplementationOnce(
      () =>
        new Promise((_resolve, reject) => {
          failSecond = reject;
        }),
    );
    render(<WorkflowCenter />);
    await screen.findByText('第一页');

    const next = document.querySelectorAll('.xflow-admin-page button')[1] as HTMLButtonElement;
    fireEvent.click(next);
    // AL-28: the rows stay and the 2px bar replaces the Spin overlay.
    const bar = await screen.findByRole('progressbar', { name: '正在刷新工作流' });
    expect(bar.getAttribute('aria-busy')).toBe('true');
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();
    expect(rows()).toEqual(['wf-1']);

    await act(async () => {
      failSecond(new Error('503 page 2'));
    });
    expect(await screen.findByText('工作流列表加载失败')).toBeTruthy();
    expect(rows()).toEqual(['wf-1']);
    expect(screen.queryByRole('progressbar')).toBeNull();
  });

  it('marks the card busy and blocks 运行 and the pager while the next page loads', async () => {
    host.listWorkflows.mockResolvedValueOnce(page([wf('wf-1', '第一页', 'v1')], 41));
    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listWorkflows.mockImplementationOnce(() => new Promise((done) => { resolve = done; }));
    render(<WorkflowCenter />);
    await screen.findByText('第一页');
    const card = () => document.querySelector('[data-table="workflows"]') as HTMLElement;
    expect(card().getAttribute('aria-busy')).toBe('false');

    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);
    await waitFor(() => expect(card().getAttribute('aria-busy')).toBe('true'));
    const pager = [...document.querySelectorAll('.xflow-admin-page button')] as HTMLButtonElement[];
    expect(pager.every((button) => button.disabled)).toBe(true);

    // Running starts a real execution, so the old row cannot start one.
    fireEvent.click(screen.getByRole('button', { name: '更多操作：第一页' }));
    const run = await screen.findByRole('menuitem', { name: '运行' });
    expect(run.getAttribute('aria-disabled')).toBe('true');

    resolve(page([wf('wf-31', '第二页', 'v1')], 41));
    await waitFor(() => expect(card().getAttribute('aria-busy')).toBe('false'));
  });

  it('wires both @xflow/api reads through the authenticated fetcher', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    expect(host.clientOptions?.fetcher).toBe(authorizedFetch);
  });

  it('turns an empty first page into a first-run state with both creation entries', async () => {
    host.listWorkflows.mockResolvedValue(page([], 0));
    render(<WorkflowCenter />);

    const empty = (await screen.findByText('还没有工作流')).closest('.ant-empty') as HTMLElement;
    expect(empty).not.toBeNull();
    expect(screen.queryByText('没有匹配的记录')).toBeNull();
    // AD-18: a single page draws no footer at all.
    expect(document.querySelector('.xflow-admin-page')).toBeNull();

    fireEvent.click(within(empty).getByRole('button', { name: '新建工作流' }));
    expect(host.push).toHaveBeenCalledWith('/workflows/new');

    // The page owns the dialog now, so the empty-state CTA opens it directly.
    fireEvent.click(within(empty).getByRole('button', { name: '导入 DSL' }));
    expect(await screen.findByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。')).toBeTruthy();
  });

  it('shows an empty version as an em dash', async () => {
    host.listWorkflows.mockResolvedValue(page([wf('wf-1', '客户同步', '')]));
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    // AD-08: a missing version must not render as a lone "v"; the row's
    // metadata line now carries the version.
    expect(
      document.querySelector('tr[data-workflow="wf-1"] .xflow-row-name__version')?.textContent,
    ).toBe('—');
  });

  it('reports a failed fetch and refetches the same page on 重试', async () => {
    host.listWorkflows.mockRejectedValueOnce(new XFlowApiError(500, 'internal error'));
    host.listWorkflows.mockResolvedValueOnce(page([wf('wf-1', '客户同步', 'v3')]));
    render(<WorkflowCenter />);

    expect(await screen.findByText('工作流列表加载失败')).toBeTruthy();
    // The server's text and status stay out of the Chinese copy; they live in
    // the detail attribute, and the headline and hint read as separate words.
    const banner = document.querySelector('.xflow-admin-banner') as HTMLElement;
    expect(banner.textContent).not.toContain('internal error');
    expect(banner.textContent).not.toContain('HTTP 500');
    expect(banner.querySelector('[data-error-detail]')?.getAttribute('data-error-detail')).toBe(
      'HTTP 500 · internal error',
    );
    expect(banner.textContent).toMatch(/工作流列表加载失败\s+请稍后重试。/);
    // AD-04: the error banner owns the screen; the first-run empty state must
    // not appear beside it.
    expect(screen.queryByText('还没有工作流')).toBeNull();
    expect(document.querySelector('.ant-table-tbody .ant-empty')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    await waitFor(() => expect(rows()).toContain('wf-1'));
    expect(host.listWorkflows).toHaveBeenCalledTimes(2);
    expect(screen.queryByText('工作流列表加载失败')).toBeNull();
  });

  it('publishes the server total for the nav badge, and hides it while loading or after a failure', async () => {
    render(<WorkflowCenter />);
    // First render is loading: no badge yet.
    expect(host.navCounts.at(0)).toBeUndefined();
    await waitFor(() => expect(host.navCounts).toContain(2));
  });

  it('forwards ?q= to the server instead of filtering one page client-side', async () => {
    host.search = '?q= 客户 ';
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    // The shell keeps the trimmed query in the URL; the page passes it to the
    // list endpoint, which filters before pagination.
    expect(host.listWorkflows).toHaveBeenCalledWith(listArgs({ q: '客户' }));
    expect(screen.queryByTestId('search-note')).toBeNull();
    expect(document.body.textContent).not.toContain('暂不支持');
  });

  it('puts search in the table toolbar and focuses it with "/"', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const search = screen.getByLabelText('搜索工作流');
    // The page owns its search now; the shell's top bar only has ⌘K.
    expect(search.closest('[role="toolbar"]')).not.toBeNull();
    fireEvent.keyDown(window, { key: '/' });
    expect(document.activeElement).toBe(search);
  });

  it('leaves "/" alone while the user is typing elsewhere', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const typing = document.createElement('input');
    document.body.appendChild(typing);
    typing.focus();
    const event = createEvent.keyDown(typing, { key: '/' });
    const preventDefault = vi.spyOn(event, 'preventDefault');
    fireEvent(typing, event);
    expect(preventDefault).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(typing);
    typing.remove();
  });

  it('returns to page 1 when the query changes', async () => {
    host.listWorkflows.mockResolvedValue(page([wf('wf-1', '第一页', 'v1')], 41));
    const view = render(<WorkflowCenter />);
    await screen.findByText('第一页');

    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1] as HTMLElement);
    await waitFor(() =>
      expect(host.listWorkflows).toHaveBeenLastCalledWith(listArgs({ page: 2 })),
    );

    host.search = '?q=客户';
    host.listWorkflows.mockResolvedValue(page([wf('wf-1', '第一页', 'v1')], 1));
    view.rerender(<WorkflowCenter />);

    await waitFor(() =>
      expect(host.listWorkflows).toHaveBeenLastCalledWith(listArgs({ q: '客户' })),
    );
    // The new query never rides the old page number.
    expect(host.listWorkflows.mock.calls).not.toContainEqual([listArgs({ page: 2, q: '客户' })]);
  });

  it('shows the no-match empty state when a search returns nothing and offers to clear it', async () => {
    host.search = '?q=不存在';
    host.listWorkflows.mockResolvedValue(page([], 0));
    render(<WorkflowCenter />);

    expect(await screen.findByText('没有匹配的工作流')).toBeTruthy();
    expect(screen.queryByText('还没有工作流')).toBeNull();
    expect(document.querySelector('.xflow-admin-page')).toBeNull();

    // AD-05: the filter empty state must not pretend there are no workflows.
    fireEvent.click(screen.getByRole('button', { name: '清除搜索' }));
    expect(host.replace).toHaveBeenCalledWith('/workflows');
  });
});

describe('overview KPI strip', () => {
  it('counts server-reported liveness, pools and node types', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('runners')).not.toBeNull());

    expect(card('runners')?.textContent).toContain('1 / 3');
    expect(card('pools')?.textContent).toContain('3');
    expect(card('types')?.textContent).toContain('28');
    expect(card('runners')?.textContent).toContain('Runner 在线');
    expect(document.querySelector('[data-panel="env-kpis"]')).not.toBeNull();
  });

  it('draws a placeholder for a metric that is still loading', () => {
    host.fetchRunners.mockReturnValue(new Promise(() => {}));
    host.fetchRunnerPools.mockReturnValue(new Promise(() => {}));
    host.listNodeTypes.mockReturnValue(new Promise(() => {}));
    render(<WorkflowCenter />);

    // The strip exists from the first render, so data never pushes it down.
    expect(document.querySelector('[data-panel="env-kpis"]')).not.toBeNull();
    for (const id of ['runners', 'pools', 'types']) {
      expect(card(id)?.textContent).toContain('…');
    }
  });

  it('skips the management reads entirely without their scopes', async () => {
    host.currentUser = { subject: 'tester', namespace: 'team-a', scopes: ['workflow'] };
    render(<WorkflowCenter />);

    await waitFor(() => expect(card('types')).not.toBeNull());
    expect(host.fetchRunners).not.toHaveBeenCalled();
    expect(host.fetchRunnerPools).not.toHaveBeenCalled();
    // The scope-less reads behave like a 403: their cards are not drawn.
    expect(card('runners')).toBeNull();
    expect(card('pools')).toBeNull();
    expect(card('types')?.textContent).toContain('28');
  });

  it('hides a forbidden metric instead of drawing a dash (AL-08)', async () => {
    host.fetchRunners.mockRejectedValue(new Error('403 management scope missing'));
    host.fetchRunnerPools.mockRejectedValue(new Error('404 management module disabled'));
    render(<WorkflowCenter />);

    await waitFor(() => expect(card('types')).not.toBeNull());
    expect(card('runners')).toBeNull();
    expect(card('pools')).toBeNull();
    expect(card('types')?.textContent).toContain('28');
    // No dead cells, no "无权限" footnotes in the strip.
    const strip = document.querySelector('[data-panel="env-kpis"]') as HTMLElement;
    expect(strip.textContent).not.toContain('—');
    expect(strip.textContent).not.toContain('无权限');
    // The workflow list does not depend on the management module.
    await waitFor(() => expect(rows()).toContain('wf-1'));
  });

  it('hides the whole strip when every read is unavailable', async () => {
    host.fetchRunners.mockRejectedValue(new Error('404 management module disabled'));
    host.fetchRunnerPools.mockRejectedValue(new Error('404 management module disabled'));
    host.listNodeTypes.mockRejectedValue(new Error('502 node-types'));
    render(<WorkflowCenter />);

    await waitFor(() => expect(host.listNodeTypes).toHaveBeenCalled());
    // Let Promise.allSettled settle and the state flush.
    await act(async () => {});
    expect(document.querySelector('[data-panel="env-kpis"]')).toBeNull();
    expect(document.body.textContent).not.toContain('无权限');
  });

  it('keeps the readable metrics when one read fails', async () => {
    host.listNodeTypes.mockRejectedValue(new Error('502'));
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('runners')).not.toBeNull());
    await waitFor(() => expect(card('types')).toBeNull());
    expect(card('pools')?.textContent).toContain('3');
    expect(card('runners')?.textContent).toContain('1 / 3');
  });
});

describe('layout breakpoints', () => {
  it('sinks the right rail below the table below 1440px (AL-01)', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const layout = document.querySelector('[data-testid="workflow-center-layout"]') as HTMLElement;
    expect(layout.className).toContain('grid-cols-1');
    // The rail appears only at ≥1440: at 1280 a 320px rail left the table
    // narrower than its own minimum and clipped the 打开 column.
    expect(layout.className).toContain('min-[1440px]:grid-cols-[minmax(0,1fr)_320px]');
    expect(layout.className).not.toContain('xl:grid-cols');
  });
});

describe('recent failures', () => {
  const failureRows = () => [
    ...document.querySelectorAll<HTMLElement>('[data-panel="recent-failures"] .xflow-admin-run'),
  ];

  it('asks for the last-24h failed/timeout runs and draws name, time and error', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(failureRows()).toHaveLength(2));

    const options = host.listExecutions.mock.calls[0][0] as {
      pageSize: number;
      statuses: string[];
      createdAfter: string;
    };
    expect(options.pageSize).toBe(5);
    expect(options.statuses).toEqual(['failed', 'timeout']);
    // A fixed 24h window: the bound is "now minus one day" (allow clock drift).
    expect(Math.abs(Date.now() - 24 * 60 * 60 * 1000 - new Date(options.createdAfter).getTime())).toBeLessThan(10_000);

    expect(document.querySelector('[data-panel="recent-failures"]')?.textContent).toContain('最近失败');
    expect(failureRows().map((row) => row.getAttribute('data-workflow'))).toEqual([
      '告警归因',
      '夜间对账',
    ]);
    expect(failureRows().map((row) => row.getAttribute('data-status'))).toEqual(['timeout', 'failed']);
    // One line per failure: name, relative time, error摘要; the full error and
    // status stay in title/aria-label so colour is never the only signal.
    expect(failureRows()[0].textContent).toContain('告警归因');
    expect(failureRows()[0].textContent).toContain('context deadline exceeded');
    expect(failureRows()[0].getAttribute('aria-label')).toContain('超时');
    expect(failureRows()[0].getAttribute('title')).toContain('context deadline exceeded');
    expect(failureRows()[1].getAttribute('aria-label')).toContain('失败');
    // AD-09: failure rows are focusable and open their execution record.
    expect(failureRows()[0].getAttribute('tabindex')).toBe('0');
    expect(failureRows()[0].className).toContain('xflow-admin-row-clickable');
    // The execution id is not the row label any more: it would truncate and
    // repeat the table's run strip instead of naming the failure.
    expect(failureRows()[0].textContent).not.toContain('exec-9');
  });

  it('opens the failed run on click or Enter', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(failureRows()).toHaveLength(2));

    fireEvent.click(failureRows()[0]);
    expect(host.push).toHaveBeenLastCalledWith('/workflows/wf-1?exec=exec-9');

    fireEvent.keyDown(failureRows()[1], { key: 'Enter' });
    expect(host.push).toHaveBeenLastCalledWith('/workflows/wf-2?exec=exec-7');
  });

  it('falls back to the execution list for a row without a workflow id', async () => {
    host.listExecutions.mockResolvedValue({
      list: [failure('exec-5', '旧流程', 'failed', '')],
      total: 1,
    });
    render(<WorkflowCenter />);
    await waitFor(() => expect(failureRows()).toHaveLength(1));

    fireEvent.click(failureRows()[0]);
    expect(host.push).toHaveBeenLastCalledWith('/executions');
  });

  it('shows a positive empty state when the window has no failures', async () => {
    host.listExecutions.mockResolvedValue({ list: [], total: 0 });
    render(<WorkflowCenter />);

    const empty = await screen.findByText('最近 24h 无失败');
    expect(empty.getAttribute('data-state')).toBe('empty');
    expect(empty.getAttribute('data-tone')).toBe('good');
    expect(failureRows()).toHaveLength(0);
  });

  it('does not read or promise failures without execution read (P2-4)', async () => {
    host.currentUser = { subject: 'tester', namespace: 'team-a', scopes: ['workflow'] };
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));
    await act(async () => {});

    const panel = document.querySelector('[data-panel="recent-failures"]') as HTMLElement;
    expect(panel.querySelector('[data-state="forbidden"]')?.textContent).toContain(
      '需要 execution 权限',
    );
    expect(panel.querySelector('[data-state="loading"]')).toBeNull();
    expect(panel.querySelector('[data-state="error"]')).toBeNull();
    expect(host.listExecutions).not.toHaveBeenCalled();
  });

  it('shows only a sanitized summary, never the raw protocol error (P3)', async () => {
    const raw =
      'xflow: permanent error\ntask result cannot be reported: runner protocol /v1/runners/result: ' +
      'json: error calling MarshalJSON for type *protocol.ReportResultRequest: json: unsupported value: +Inf';
    host.listExecutions.mockResolvedValue({
      list: [failure('exec-raw', '告警归因', 'failed', 'wf-1', raw)],
      total: 1,
    });
    render(<WorkflowCenter />);
    await waitFor(() => expect(failureRows()).toHaveLength(1));

    const row = failureRows()[0];
    const shown = row.querySelector('.xflow-admin-run-error') as HTMLElement;
    for (const text of [
      shown.textContent ?? '',
      shown.getAttribute('title') ?? '',
      row.getAttribute('title') ?? '',
      row.getAttribute('aria-label') ?? '',
    ]) {
      expect(text).not.toContain('/v1/runners/result');
      expect(text).not.toContain('ReportResultRequest');
      expect(text).not.toContain('MarshalJSON');
      expect(text).not.toContain('\n');
    }
    // First line + leading error code only.
    expect(shown.textContent).toBe('xflow · permanent error');
    expect(shown.getAttribute('title')).toBe('xflow · permanent error');
  });

  it('strips internal paths and Go types and caps the summary at 120 chars (P3)', () => {
    const inline =
      'runner protocol /v1/runners/result: json: error calling MarshalJSON for type ' +
      '*protocol.ReportResultRequest: json: unsupported value: +Inf';
    const cleaned = failureSummary(inline);
    expect(cleaned).not.toContain('/v1/runners/result');
    expect(cleaned).not.toContain('ReportResultRequest');
    expect(cleaned).not.toContain('MarshalJSON');
    expect(cleaned).toContain('unsupported value: +Inf');

    const long = failureSummary(`map.expr_eval: ${'x'.repeat(400)}`);
    expect(long.length).toBe(120);
    expect(long.endsWith('…')).toBe(true);
    expect(failureSummary('   ')).toBe('');
  });

  it('retries a failed failure-list fetch without touching the table', async () => {
    // The rejection shape the HTTP client produces: its English message must
    // never reach the card, but the status stays available for diagnosis.
    host.listExecutions.mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 500'), {
        response: { status: 500 },
      }),
    );
    host.listExecutions.mockResolvedValueOnce({
      list: [failure('exec-1', '客户同步', 'failed')],
      total: 1,
    });
    render(<WorkflowCenter />);

    const cardError = await screen.findByText('加载失败');
    expect(screen.queryByText(/Request failed/)).toBeNull();
    const box = cardError.closest('[data-state="error"]') as HTMLElement;
    expect(box.getAttribute('data-error-detail')).toBe(
      'HTTP 500 · Request failed with status code 500',
    );
    expect(box.getAttribute('title')).toBe('HTTP 500 · Request failed with status code 500');

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    expect(await screen.findByText('客户同步')).toBeTruthy();
    expect(host.listExecutions).toHaveBeenLastCalledWith(
      expect.objectContaining({ pageSize: 5, statuses: ['failed', 'timeout'] }),
    );
  });
});

describe('page header and create entries (提案 C)', () => {
  it('puts the page title and both creation actions in the page header', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading.textContent).toBe('工作流');
    expect(heading.className).toBe('xflow-admin-page-title');

    fireEvent.click(headerButton('新建工作流'));
    expect(host.push).toHaveBeenCalledWith('/workflows/new');

    fireEvent.click(headerButton('导入 DSL'));
    expect(await screen.findByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。')).toBeTruthy();
    // The action lives on the page now: it opens the dialog without a URL hop.
    expect(host.push).not.toHaveBeenCalledWith('/workflows?import=1');
  });

  it('no longer repeats creation in the right rail (AL-07)', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));
    expect(document.querySelector('[data-create]')).toBeNull();
  });

  it('opens the same dialog for an ?import=1 deep link and consumes the flag on close', async () => {
    host.search = '?import=1&q=orders';
    render(<WorkflowCenter />);

    expect(
      await screen.findByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。'),
    ).toBeTruthy();
    // The handoff rides alongside existing query state without disturbing it.
    await waitFor(() =>
      expect(host.listWorkflows).toHaveBeenCalledWith(listArgs({ q: 'orders' })),
    );

    fireEvent.click(screen.getByRole('button', { name: /取\s*消/ }));
    await waitFor(() =>
      expect(screen.queryByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。')).toBeNull(),
    );
    // Consumed, so the same URL can request the dialog again.
    expect(host.replace).toHaveBeenCalledWith('/workflows?q=orders');
  });
});

/** The import success path uses App.useApp's message/notification hosts. */
function renderImport() {
  return render(
    <App>
      <WorkflowCenter />
    </App>,
  );
}

async function openImportDialog() {
  const view = renderImport();
  await waitFor(() => expect(rows()).toContain('wf-1'));
  fireEvent.click(headerButton('导入 DSL'));
  await screen.findByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。');
  return view;
}

async function pasteJson(text: string) {
  fireEvent.click(screen.getByText('粘贴内容'));
  const textarea = await screen.findByLabelText('粘贴 DSL 定义');
  fireEvent.change(textarea, { target: { value: text } });
}

function submitImport() {
  // The page header also has an 导入 DSL button; the dialog footer is the
  // control that submits.
  const dialog = screen.getByRole('dialog');
  fireEvent.click(within(dialog).getByRole('button', { name: /导\s*入/ }));
}

/** The Segmented mode also carries this label, so pin the query to the file input. */
function importFileInput(): HTMLInputElement {
  return screen.getByLabelText('选择 DSL 文件', {
    selector: 'input[type="file"]',
  }) as HTMLInputElement;
}

describe('import DSL', () => {
  it('opens the dialog and discards the selected source and error on close', async () => {
    await openImportDialog();
    await pasteJson('{');
    submitImport();
    expect(await screen.findByText('JSON 解析失败')).toBeTruthy();

    fireEvent.click(screen.getByRole('button', { name: /取\s*消/ }));
    await waitFor(() =>
      expect(screen.queryByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。')).toBeNull(),
    );

    // Reopening starts clean: no stale error and no pasted definition.
    fireEvent.click(headerButton('导入 DSL'));
    expect(await screen.findByText('支持 JSON（.json）与 YAML（.yaml / .yml）定义。')).toBeTruthy();
    expect(screen.queryByText('JSON 解析失败')).toBeNull();
    expect(screen.queryByLabelText('粘贴 DSL 定义')).toBeNull();
  });

  it('rejects invalid JSON with a line/column and never registers', async () => {
    await openImportDialog();
    // Missing comma: V8 reports position 18 = line 3, column 3.
    await pasteJson('{\n  "name": "x"\n  "nodes": []\n}');
    submitImport();

    expect(await screen.findByText('JSON 解析失败')).toBeTruthy();
    expect(await screen.findByText(/第 3 行第 3 列/)).toBeTruthy();
    expect(host.createWorkflow).not.toHaveBeenCalled();
  });

  it('rejects a non-object root, a non-array nodes field and an empty name locally', async () => {
    await openImportDialog();

    await pasteJson('[]');
    submitImport();
    expect(await screen.findByText('顶层必须是对象')).toBeTruthy();

    await pasteJson('{"name":"x","nodes":{}}');
    submitImport();
    expect(await screen.findByText('nodes 必须是数组')).toBeTruthy();

    await pasteJson('{"nodes":[]}');
    submitImport();
    expect(await screen.findByText('name 必须是非空字符串')).toBeTruthy();

    expect(host.createWorkflow).not.toHaveBeenCalled();
  });

  it('strips the client id and namespace but forwards every other field untouched', async () => {
    await openImportDialog();
    host.createWorkflow.mockResolvedValue({ workflowId: 'wf-9' });
    const definition = {
      id: 'client-chosen-id',
      namespace: 'another-team',
      name: '导入流',
      nodes: [],
      description: '保留字段',
      settings: { timeout: '10s' },
      editor_metadata: { viewport: { x: 1, y: 2, zoom: 1 } },
    };
    await pasteJson(JSON.stringify(definition));
    submitImport();

    await waitFor(() => expect(host.createWorkflow).toHaveBeenCalledTimes(1));
    const payload = host.createWorkflow.mock.calls[0][0];
    expect(payload).not.toHaveProperty('id');
    expect(payload).not.toHaveProperty('namespace');
    expect(payload).toEqual({
      name: '导入流',
      nodes: [],
      description: '保留字段',
      settings: { timeout: '10s' },
      editor_metadata: { viewport: { x: 1, y: 2, zoom: 1 } },
    });
    expect(await screen.findByText('已导入工作流 wf-9')).toBeTruthy();
    await waitFor(() => expect(host.push).toHaveBeenCalledWith('/workflows/wf-9'));
  });

  it('shows server warnings and param issues, then opens the created workflow', async () => {
    await openImportDialog();
    host.createWorkflow.mockResolvedValue({
      workflowId: 'wf-9',
      warnings: ['node "pause" has no stable id'],
      paramIssues: [
        {
          node: 'pause',
          path: '/parameters/duration',
          code: 'required',
          message: 'duration is required',
          severity: 'warning',
        },
      ],
    });
    await pasteJson('{"name":"导入流","nodes":[]}');
    submitImport();

    await waitFor(() => expect(host.push).toHaveBeenCalledWith('/workflows/wf-9'));
    expect(await screen.findByText(/已导入工作流 wf-9/)).toBeTruthy();
    expect(screen.getByText(/has no stable id/)).toBeTruthy();
    expect(screen.getByText(/duration is required/)).toBeTruthy();
  });

  it('maps a server error code to Chinese help and keeps the message expandable', async () => {
    await openImportDialog();
    host.createWorkflow.mockRejectedValue(
      new XFlowApiError(
        409,
        'another live workflow already holds this namespace and name',
        'workflow_name_conflict',
      ),
    );
    await pasteJson('{"name":"导入流","nodes":[]}');
    submitImport();

    expect(await screen.findByText('名称冲突：已有同名工作流。')).toBeTruthy();
    expect(screen.getByText('服务端详情')).toBeTruthy();
    expect(
      screen.getByText('another live workflow already holds this namespace and name'),
    ).toBeTruthy();
    expect(host.push).not.toHaveBeenCalled();
  });

  it('shows enforce-mode param issues and maps a compile failure', async () => {
    await openImportDialog();
    host.createWorkflow.mockRejectedValueOnce(
      new XFlowApiError(
        400,
        'workflow "orders": 1 invalid parameter(s)',
        'workflow_param_invalid',
        undefined,
        undefined,
        [
          {
            node: 'pause',
            path: '/parameters/duration',
            code: 'required',
            message: 'duration is required',
            severity: 'error',
          },
        ],
      ),
    );
    await pasteJson('{"name":"导入流","nodes":[]}');
    submitImport();
    expect(
      await screen.findByText('参数校验失败：服务端以严格模式拒绝了节点参数。'),
    ).toBeTruthy();
    expect(screen.getByText(/pause\/parameters\/duration：duration is required/)).toBeTruthy();

    host.createWorkflow.mockRejectedValueOnce(
      new XFlowApiError(400, 'compile: unknown node type', 'workflow_compile_failed'),
    );
    submitImport();
    expect(await screen.findByText('编译失败：服务端无法编译该工作流定义。')).toBeTruthy();
    expect(host.push).not.toHaveBeenCalled();
  });

  it('rejects a selected file above 1 MiB without reading or registering it', async () => {
    await openImportDialog();
    const big = new File(['{}'], 'big.json', { type: 'application/json' });
    Object.defineProperty(big, 'size', { value: 1024 * 1024 + 1 });
    fireEvent.change(importFileInput(), { target: { files: [big] } });

    expect(await screen.findByText('文件超过 1 MiB 上限')).toBeTruthy();
    expect(host.createWorkflow).not.toHaveBeenCalled();
  });

  it('imports a selected .yaml file now that YAML is wired in', async () => {
    await openImportDialog();
    fireEvent.change(importFileInput(), {
      target: {
        files: [new File(['name: 导入流\nnodes: []\n'], 'flow.yaml', { type: 'application/yaml' })],
      },
    });
    expect(await screen.findByText(/flow\.yaml/)).toBeTruthy();
    submitImport();

    await waitFor(() => expect(host.push).toHaveBeenCalledWith('/workflows/wf-imported'));
    expect(host.createWorkflow).toHaveBeenCalledTimes(1);
    expect(host.createWorkflow.mock.calls[0][0]).toEqual({ name: '导入流', nodes: [] });
  });

  it('reads a selected .json file and registers its definition', async () => {
    await openImportDialog();
    host.createWorkflow.mockResolvedValue({ workflowId: 'wf-file' });
    fireEvent.change(importFileInput(), {
      target: {
        files: [new File(['{"name":"文件流","nodes":[]}'], 'flow.json', { type: 'application/json' })],
      },
    });
    expect(await screen.findByText(/flow\.json/)).toBeTruthy();
    submitImport();

    await waitFor(() => expect(host.push).toHaveBeenCalledWith('/workflows/wf-file'));
    expect(host.createWorkflow.mock.calls[0][0]).toEqual({ name: '文件流', nodes: [] });
  });

  it('disables the footer and accepts only one request while submitting', async () => {
    await openImportDialog();
    let resolve!: (value: { workflowId: string }) => void;
    host.createWorkflow.mockReturnValue(
      new Promise((done) => {
        resolve = done;
      }),
    );
    await pasteJson('{"name":"导入流","nodes":[]}');
    submitImport();

    const cancel = screen.getByRole('button', { name: /取\s*消/ }) as HTMLButtonElement;
    await waitFor(() => expect(cancel.disabled).toBe(true));
    submitImport();
    expect(host.createWorkflow).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolve({ workflowId: 'wf-9' });
    });
    await waitFor(() => expect(host.push).toHaveBeenCalledWith('/workflows/wf-9'));
  });
});
