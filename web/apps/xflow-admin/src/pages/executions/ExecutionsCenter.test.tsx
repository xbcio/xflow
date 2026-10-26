import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App } from 'antd';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  search: '',
  push: vi.fn(),
  replace: vi.fn(),
  listExecutions: vi.fn(),
  listWorkflows: vi.fn(),
  getExecution: vi.fn(),
  rerunExecution: vi.fn(),
  pauseExecution: vi.fn(),
  resumeExecution: vi.fn(),
  cancelExecution: vi.fn(),
  // URLs built by the REAL @xflow/api client for listExecutions calls: the
  // page's data path is mocked, the query string is not.
  listUrls: [] as string[],
  clientOptions: undefined as { fetcher?: unknown } | undefined,
}));

vi.mock('@umijs/max', () => ({
  history: {
    push: host.push,
    replace: host.replace,
    location: { pathname: '/executions' },
  },
  useLocation: () => ({ pathname: '/executions', search: host.search }),
}));

vi.mock('@xflow/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xflow/api')>();
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
    createXFlowApiClient: (options: { baseUrl: string; fetcher?: unknown }) => {
      host.clientOptions = options;
      const recordingClient = actual.createXFlowApiClient({
        baseUrl: options.baseUrl,
        fetcher: (async (url: string) => {
          host.listUrls.push(String(url));
          return {
            ok: true,
            status: 200,
            headers: { get: () => null },
            text: async () =>
              JSON.stringify({ success: true, code: 'ok', data: { list: [], total: 0 } }),
          } as unknown as Response;
        }) as typeof fetch,
      });
      return {
        listExecutions: (listOptions: unknown) => {
          // Record the authentic URL through the real client; the page still
          // renders from the mocked response below.
          void recordingClient.listExecutions(listOptions as never).catch(() => {});
          return host.listExecutions(listOptions);
        },
        listWorkflows: host.listWorkflows,
        getExecution: host.getExecution,
        rerunExecution: host.rerunExecution,
        pauseExecution: host.pauseExecution,
        resumeExecution: host.resumeExecution,
        cancelExecution: host.cancelExecution,
      };
    },
  };
});

import { XFlowApiError } from '@xflow/api';

import { authorizedFetch } from '../../services/http';
import { ExecutionsCenter } from './ExecutionsCenter';

const row = (id: string, status: string, overrides: Record<string, unknown> = {}) => ({
  executionId: id,
  namespace: 'default',
  workflowName: '客户同步',
  status,
  error: '',
  createdAt: '2026-10-19T14:02:23Z',
  updatedAt: '2026-10-19T14:02:29Z',
  ...overrides,
});

const page = (list: unknown[], total = list.length) => ({ list, total });
const rows = () => [...document.querySelectorAll('tbody tr[data-exec]')];
const execRow = (id: string) => document.querySelector(`tr[data-exec="${id}"]`);
const ops = (id: string) =>
  [...(execRow(id)?.querySelectorAll('[data-op]') ?? [])].map((button) =>
    button.getAttribute('data-op'),
  );

function renderPage() {
  return render(
    <App>
      <ExecutionsCenter />
    </App>,
  );
}

/** Open the multi-status filter and toggle one option. */
async function selectStatus(label: string) {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: '状态' }));
  fireEvent.click(await screen.findByRole('option', { name: label }));
}

const failedDetail = {
  executionId: 'exec-7',
  status: 'failed',
  nodes: [
    { name: 'webhook', status: 'success' },
    { name: 'fetch_profile', status: 'failed', error: '503' },
    { name: 'notify_sales', status: 'pending' },
  ],
};

const originalMatchMedia = window.matchMedia;

afterEach(() => {
  window.matchMedia = originalMatchMedia;
});

beforeEach(() => {
  vi.clearAllMocks();
  host.search = '';
  host.clientOptions = undefined;
  host.listUrls.length = 0;
  // AD-02 marks 更新 as responsive ['xl']; jsdom has no matchMedia, which would
  // hide it from every breakpoint assertion below.
  window.matchMedia = ((query: string) => ({
    matches: true,
    media: query,
    onchange: null,
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia;
  host.listExecutions.mockResolvedValue(
    page(
      [
        row('exec-9', 'running'),
        row('exec-8', 'success'),
        row('exec-7', 'failed', {
          error: 'score service returned 503\nstack line two',
          updatedAt: '2026-10-19T14:02:33Z',
        }),
      ],
      41,
    ),
  );
  host.listWorkflows.mockResolvedValue({
    list: [
      { id: 'wf-1', name: '客户同步', version: 'v1', definitionHash: 'h1', registryRevision: 1 },
      { id: 'wf-2', name: '采购审批', version: 'v1', definitionHash: 'h2', registryRevision: 1 },
    ],
    total: 2,
  });
  host.getExecution.mockResolvedValue(failedDetail);
  host.rerunExecution.mockResolvedValue({ executionId: 'exec-10', status: 'running' });
  host.pauseExecution.mockResolvedValue({ executionId: 'exec-9', status: 'paused' });
  host.resumeExecution.mockResolvedValue({ executionId: 'exec-6', status: 'running' });
  host.cancelExecution.mockResolvedValue({ accepted: true });
});

describe('execution table', () => {
  it('renders a server page with the status table, copyable id, relative times and first error line', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(host.listExecutions).toHaveBeenCalledWith({ page: 1, pageSize: 20 });
    const success = execRow('exec-8');
    expect(success?.querySelector('.xflow-admin-col-status')?.textContent).toBe('成功');
    expect(success?.querySelector('.xflow-admin-col-exec .xflow-admin-mono')?.textContent).toBe(
      '8',
    );
    // Real ids are 41 chars; the cell has to stay one line (ellipsis) and the
    // whole id is the copy button's accessible name.
    expect(success?.querySelector('.xflow-admin-col-exec .xflow-admin-ellipsis')).not.toBeNull();
    expect(success?.querySelector('[data-copy-exec="exec-8"]')).not.toBeNull();
    // Start time is relative, with the full local timestamp in the tooltip.
    const start = success?.querySelector('.xflow-admin-col-start time');
    expect(start?.getAttribute('title')).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    expect(start?.getAttribute('datetime')).toBe('2026-10-19T14:02:23Z');
    // P3: a row without finished_at draws an em dash, never a fabricated end.
    expect(success?.querySelector('.xflow-admin-col-finish')?.textContent).toBe('—');
    // Only the first line of a failure is drawn; the full text is the tooltip.
    expect(execRow('exec-7')?.querySelector('.xflow-admin-col-error')?.textContent).toBe(
      'score service returned 503',
    );
    expect(execRow('exec-9')?.querySelector('.xflow-admin-col-error')?.textContent).toBe('—');
  });

  it('renders the P3 fields and degrades rows written before the columns to —', async () => {
    host.listExecutions.mockResolvedValue(
      page([
        row('exec-new', 'success', {
          durationMs: 22000,
          finishedAt: '2026-10-19T14:02:45Z',
          triggerKind: 'api',
          retries: 2,
          nodeProgress: { done: 8, total: 8 },
        }),
        row('exec-old', 'failed'),
      ]),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(2));

    const fresh = execRow('exec-new');
    expect(fresh?.querySelector('.xflow-admin-col-trigger')?.textContent).toBe('手动');
    expect(fresh?.querySelector('[data-trigger="api"]')).not.toBeNull();
    expect(fresh?.querySelector('.xflow-admin-col-progress')?.textContent).toBe('8/8');
    expect(
      fresh?.querySelector('.xflow-exec-progress__fill')?.getAttribute('data-percent'),
    ).toBe('100');
    expect(fresh?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('22.0s');
    expect(fresh?.querySelector('[data-retries="2"]')?.textContent).toBe('2');
    const finish = fresh?.querySelector('.xflow-admin-col-finish time');
    expect(finish?.getAttribute('datetime')).toBe('2026-10-19T14:02:45Z');
    expect(finish?.getAttribute('title')).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

    // Old rows carry none of the new columns: absence is UNKNOWN, not zero.
    const old = execRow('exec-old');
    expect(old?.querySelector('.xflow-admin-col-trigger')?.textContent).toBe('—');
    expect(old?.querySelector('.xflow-admin-col-progress')?.textContent).toBe('—');
    expect(old?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('—');
    expect(old?.querySelector('.xflow-admin-col-retries')?.textContent).toBe('—');
    expect(old?.querySelector('.xflow-admin-col-finish')?.textContent).toBe('—');
  });

  it('groups the status filter by 需关注 and 终态 and sends every selected status', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.mouseDown(screen.getByRole('combobox', { name: '状态' }));
    expect(await screen.findByText('需关注')).toBeTruthy();
    expect(screen.getByText('终态')).toBeTruthy();
    // Every terminal outcome is reachable from the two groups.
    for (const label of ['失败', '超时', '成功', '已取消']) {
      expect(screen.getByRole('option', { name: label })).toBeTruthy();
    }

    fireEvent.click(screen.getByRole('option', { name: '失败' }));
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        statuses: ['failed'],
      }),
    );
    expect(host.replace).toHaveBeenLastCalledWith('/executions?status=failed');

    fireEvent.click(screen.getByRole('option', { name: '超时' }));
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        statuses: ['failed', 'timeout'],
      }),
    );
    expect(host.replace).toHaveBeenLastCalledWith('/executions?status=failed%2Ctimeout');
    // The real client serializes the selection as repeated status parameters.
    expect(host.listUrls.at(-1)).toContain('status=failed');
    expect(host.listUrls.at(-1)).toContain('status=timeout');
  });

  it('reads q and the status filter from the URL on load', async () => {
    host.search = '?status=failed,timeout&q=exec-a99';
    renderPage();

    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenCalledWith({
        page: 1,
        pageSize: 20,
        q: 'exec-a99',
        statuses: ['failed', 'timeout'],
      }),
    );
    expect(screen.getByDisplayValue('exec-a99')).toBeTruthy();
  });

  it('debounces the id-prefix search and sends only the last query', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const search = screen.getByRole('textbox', { name: '搜索执行' });
    fireEvent.change(search, { target: { value: 'exec-a9' } });
    fireEvent.change(search, { target: { value: 'exec-a990' } });
    // The first keystroke did not issue a request on its own.
    expect(host.listExecutions).toHaveBeenCalledTimes(1);

    await waitFor(
      () =>
        expect(host.listExecutions).toHaveBeenLastCalledWith({
          page: 1,
          pageSize: 20,
          q: 'exec-a990',
        }),
      { timeout: 1500 },
    );
    const qCalls = host.listExecutions.mock.calls.filter(
      ([options]) => (options as { q?: string }).q !== undefined,
    );
    expect(qCalls).toHaveLength(1);
    expect(host.replace).toHaveBeenLastCalledWith('/executions?q=exec-a990');
  });

  it('focuses the search box with the / shortcut', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const search = screen.getByRole('textbox', { name: '搜索执行' });
    fireEvent.keyDown(window, { key: '/' });
    expect(document.activeElement).toBe(search);
  });

  it('drops 结束时间 and 重试 below 1280 so the compact table fits', async () => {
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia;
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const headers = [...document.querySelectorAll('thead th')].map((th) => th.textContent ?? '');
    expect(headers).toContain('开始时间');
    expect(headers).not.toContain('结束时间');
    expect(headers).not.toContain('重试');
    expect(document.querySelector('.xflow-admin-col-finish')).toBeNull();
    expect(document.querySelector('.xflow-admin-col-retries')).toBeNull();
  });

  it('copies the full execution id without opening the row', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const copy = execRow('exec-9')?.querySelector('[data-copy-exec="exec-9"]') as HTMLElement;
    fireEvent.click(copy);
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('exec-9'));
    expect(host.push).not.toHaveBeenCalled();
    await waitFor(() =>
      expect(document.querySelector('.ant-message')?.textContent).toContain('已复制执行 ID'),
    );
  });

  it('paginates on the server and derives the footer from total', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(document.querySelector('.xflow-admin-page')?.textContent).toContain('第 1 / 3 页');
    // AD-18: more than one page also states the server total.
    expect(document.querySelector('.xflow-admin-page')?.textContent).toContain('共 41 条');
    const [previous, next] = [
      ...document.querySelectorAll('.xflow-admin-page button'),
    ] as HTMLButtonElement[];
    expect(previous.disabled).toBe(true);
    fireEvent.click(next);

    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 2, pageSize: 20 }),
    );
  });

  it('fixes column widths so the error and ops columns are never squeezed out', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const table = document.querySelector('[data-table="executions"] table') as HTMLTableElement;
    // Auto layout lets the 41-character no-wrap id set a ~990px min width; the
    // card then clips the right-aligned ops column at narrower viewports.
    expect(table.style.tableLayout).toBe('fixed');
    const cols = [
      ...document.querySelectorAll('[data-table="executions"] colgroup col'),
    ] as HTMLElement[];
    // At 1280+ the wide set ends with the ops column; the error column keeps
    // 112px as the first diagnostic field.
    expect(cols[cols.length - 1]?.style.width).toBe('100px');
    const errorCol = document.querySelector(
      '[data-table="executions"] colgroup col:nth-child(10)',
    ) as HTMLElement;
    expect(errorCol.style.width).toBe('112px');
  });

  it('derives row operations from workflowStatusMeta: no controls on success/canceled', async () => {
    host.listExecutions.mockResolvedValue(
      page([
        row('exec-9', 'running'),
        row('exec-6', 'paused'),
        row('exec-8', 'success'),
        row('exec-7', 'failed'),
        row('exec-4', 'canceled'),
        row('exec-3', 'timeout'),
      ]),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(6));

    expect(ops('exec-9')).toEqual(['pause', 'cancel']);
    expect(ops('exec-6')).toEqual(['resume', 'cancel']);
    expect(ops('exec-7')).toEqual(['rerun']);
    expect(ops('exec-3')).toEqual(['rerun']);
    // Ruling 2026-10-21: a successful or canceled source offers no row action.
    expect(ops('exec-8')).toEqual([]);
    expect(ops('exec-4')).toEqual([]);
    expect(execRow('exec-9')?.querySelector('[data-ops]')?.getAttribute('data-ops')).toBe(
      'pause,cancel',
    );

    const pause = execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLButtonElement;
    expect(pause.disabled).toBe(false);
    const resume = execRow('exec-6')?.querySelector('[data-op="resume"]') as HTMLButtonElement;
    expect(resume.textContent).toBe('继续');
    expect(resume.disabled).toBe(false);
    const rerun = execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLButtonElement;
    expect(rerun.disabled).toBe(false);
    expect(execRow('exec-8')?.querySelector('[data-op="rerun"]')).toBeNull();
    expect(execRow('exec-4')?.querySelector('[data-op="rerun"]')).toBeNull();
  });

  it('pauses a running row, toasts the transition and refreshes the current page', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);

    await waitFor(() => expect(host.pauseExecution).toHaveBeenCalledWith('exec-9'));
    // LE-03: the same page and filters are refetched, not reset.
    await waitFor(() => expect(host.listExecutions).toHaveBeenCalledTimes(2));
    expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 1, pageSize: 20 });
    await waitFor(() =>
      expect(document.querySelector('.ant-notification')?.textContent).toContain('已暂停 exec-9'),
    );
  });

  it('shows 继续 for a paused row and resumes it', async () => {
    host.listExecutions.mockResolvedValue(page([row('exec-6', 'paused')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    const resume = execRow('exec-6')?.querySelector('[data-op="resume"]') as HTMLButtonElement;
    expect(resume.textContent).toBe('继续');
    fireEvent.click(resume);

    await waitFor(() => expect(host.resumeExecution).toHaveBeenCalledWith('exec-6'));
    await waitFor(() => expect(host.listExecutions).toHaveBeenCalledTimes(2));
    await waitFor(() =>
      expect(document.querySelector('.ant-notification')?.textContent).toContain('已继续 exec-6'),
    );
  });

  it('requires a confirmation before cancelling and cancels on confirm', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="cancel"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByText('exec-9')).toBeTruthy();
    // Nothing is sent until the operator confirms the second step.
    expect(host.cancelExecution).not.toHaveBeenCalled();

    fireEvent.click(within(dialog).getByRole('button', { name: /返\s*回/ }));
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(host.cancelExecution).not.toHaveBeenCalled();

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="cancel"]') as HTMLElement);
    const confirm = await screen.findByRole('dialog');
    fireEvent.click(within(confirm).getByRole('button', { name: '确认取消' }));

    await waitFor(() => expect(host.cancelExecution).toHaveBeenCalledWith('exec-9'));
    await waitFor(() => expect(host.listExecutions).toHaveBeenCalledTimes(2));
    await waitFor(() =>
      expect(document.querySelector('.ant-notification')?.textContent).toContain('已请求取消 exec-9'),
    );
  });

  it('names the missing execution.control scope on a 403 control failure', async () => {
    host.pauseExecution.mockRejectedValueOnce(new XFlowApiError(403, 'forbidden', 'forbidden'));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);

    expect(await screen.findByText(/execution\.control/)).toBeTruthy();
    // A failed control does not refetch the page.
    expect(host.listExecutions).toHaveBeenCalledTimes(1);
  });

  it('sends created_after for the chosen time range, from page 1, stacked with status', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    // Move off page 1 first: a filter change must come back to page 1.
    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 2, pageSize: 20 }),
    );
    // Let the page-2 response settle before opening a portal dropdown:
    // an option node replaced by the re-render would swallow the click.
    await waitFor(() => expect(document.querySelector('.ant-spin-spinning')).toBeNull());

    fireEvent.mouseDown(screen.getByRole('combobox', { name: '时间范围' }));
    fireEvent.click(await screen.findByRole('option', { name: '最近 1 小时' }));

    await waitFor(() => {
      const last = host.listExecutions.mock.calls.at(-1)?.[0] as {
        page?: number;
        createdAfter?: string;
      };
      expect(last.page).toBe(1);
      expect(last.createdAfter).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/);
    });
    const lastCall = host.listExecutions.mock.calls.at(-1)?.[0] as { createdAfter: string };
    // Date.now() is the baseline: the bound is one hour before "now".
    const ageMinutes = (Date.now() - Date.parse(lastCall.createdAfter)) / 60_000;
    expect(ageMinutes).toBeGreaterThan(59);
    expect(ageMinutes).toBeLessThan(61);
    // The real client serializes the bound as the exclusive query parameter.
    expect(host.listUrls.at(-1)).toContain('created_after=');

    // Status and time range stack on one request.
    await selectStatus('失败');
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith(
        expect.objectContaining({
          page: 1,
          statuses: ['failed'],
          createdAfter: expect.any(String),
        }),
      ),
    );
  });

  it('filters by workflow id from the searchable dropdown, from page 1', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 2, pageSize: 20 }),
    );
    await waitFor(() => expect(document.querySelector('.ant-spin-spinning')).toBeNull());

    const workflowSelect = screen.getByRole('combobox', { name: '工作流' });
    fireEvent.mouseDown(workflowSelect);
    fireEvent.change(workflowSelect, { target: { value: '采购' } });
    fireEvent.click(await screen.findByRole('option', { name: '采购审批' }));

    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        workflowId: 'wf-2',
      }),
    );
    expect(host.listUrls.at(-1)).toContain('workflow_id=wf-2');

    // Workflow and status narrow the same server request.
    await selectStatus('失败');
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        workflowId: 'wf-2',
        statuses: ['failed'],
      }),
    );
  });

  it('keeps the workflow filter usable when the options fail to load', async () => {
    host.listWorkflows.mockRejectedValue(new Error('403 forbidden'));
    host.listExecutions.mockResolvedValue(
      page([row('exec-1', 'running', { workflowId: 'wf-9', workflowName: '客户同步' })], 1),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    // The list itself still loaded, and the select falls back to 全部工作流.
    expect(screen.getByText('全部工作流')).toBeTruthy();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '工作流' }));
    // The row's workflow_id + workflow_name backfill the missing option list.
    expect(await screen.findByRole('option', { name: '客户同步' })).toBeTruthy();
    expect(screen.getByRole('option', { name: '全部工作流' })).toBeTruthy();
    expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 1, pageSize: 20 });
  });

  it('keeps the loaded page when a refresh fails, speaks Chinese and hides the empty grid signals', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    host.listExecutions.mockRejectedValueOnce(new Error('503 service unavailable'));
    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);
    expect(await screen.findByText('运行记录加载失败')).toBeTruthy();
    // AD-04: the English transport text is diagnostic detail, not copy.
    expect(screen.queryByText('503 service unavailable')).toBeNull();
    expect(screen.getByText('请稍后重试。')).toBeTruthy();
    expect(document.querySelector('[data-error-detail]')?.getAttribute('data-error-detail')).toContain(
      '503 service unavailable',
    );
    // The count and the pagination footer contradict an error state.
    expect(document.querySelector('.xflow-admin-panel-count')).toBeNull();
    expect(document.querySelector('.xflow-admin-page')).toBeNull();
    expect(rows()).toHaveLength(3);

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    await waitFor(() => expect(host.listExecutions).toHaveBeenCalledTimes(3));
    expect(document.querySelector('.xflow-admin-panel-count')?.textContent).toBe('41');
  });

  it('shows a fixed skeleton on the first load instead of a spinner over an empty table', async () => {
    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listExecutions.mockReturnValue(new Promise((done) => { resolve = done; }));
    renderPage();

    await waitFor(() => expect(document.querySelector('.xflow-admin-table-skeleton')).not.toBeNull());
    expect(document.querySelectorAll('.xflow-admin-skeleton-row')).toHaveLength(5);
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();
    // No empty-state copy while the first page is still unknown.
    expect(screen.queryByText('没有匹配的记录')).toBeNull();

    resolve(page([row('exec-9', 'running')], 1));
    await waitFor(() => expect(rows()).toHaveLength(1));
    expect(document.querySelector('.xflow-admin-table-skeleton')).toBeNull();
  });

  it('shows the baseline empty text and one page for an empty result', async () => {
    host.listExecutions.mockResolvedValue(page([], 0));
    renderPage();

    expect(await screen.findByText('没有匹配的记录')).toBeTruthy();
    // AD-18: a single page draws no footer.
    expect(document.querySelector('.xflow-admin-page')).toBeNull();
  });

  it('never draws "没有匹配的记录" underneath an initial failure', async () => {
    host.listExecutions.mockRejectedValue(new Error('internal server error'));
    renderPage();

    expect(await screen.findByText('运行记录加载失败')).toBeTruthy();
    expect(screen.queryByText('没有匹配的记录')).toBeNull();
    expect(document.querySelector('.xflow-admin-page')).toBeNull();
    expect(screen.getByText('请稍后重试。')).toBeTruthy();
  });

  it('wires the client through the authenticated fetcher', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(host.clientOptions?.fetcher).toBe(authorizedFetch);
  });
});

describe('rerun dialog', () => {
  it('labels the range and explains the idempotency promise', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');

    expect(within(dialog).getByText('范围')).toBeTruthy();
    expect(within(dialog).getByRole('radio', { name: '从头运行' })).toBeTruthy();
    expect(await within(dialog).findByRole('radio', { name: /从失败节点继续/ })).toBeTruthy();
    // LE-05: repeats return the first execution; a fresh rerun creates one.
    expect(within(dialog).getByText('重复提交不会重复创建执行')).toBeTruthy();
    expect(within(dialog).getByText(/返回首次创建的执行 id/)).toBeTruthy();
    expect(within(dialog).getByText(/重新发起重跑才会创建新的执行/)).toBeTruthy();
  });

  it('defaults to the first failed node and posts from_node with a generated idempotency key', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    expect(host.getExecution).toHaveBeenCalledWith('exec-7');
    const failedRadio = (await within(dialog).findByRole('radio', {
      name: /从失败节点继续/,
    })) as HTMLInputElement;
    expect(failedRadio.checked).toBe(true);
    expect(within(dialog).getByRole('radio', { name: '从头运行' })).toBeTruthy();

    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));
    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(1));
    const [id, options] = host.rerunExecution.mock.calls[0];
    expect(id).toBe('exec-7');
    expect(options.fromNode).toBe('fetch_profile');
    expect(options.includeSuccess).toBe(false);
    expect(options.idempotencyKey).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
  });

  it('reruns from the head when 从头 is chosen', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');

    fireEvent.click(await within(dialog).findByRole('radio', { name: '从头运行' }));
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(1));
    expect(host.rerunExecution.mock.calls[0][1].fromNode).toBe('webhook');
  });

  it('treats duplicate as success, stays on the list, highlights the new row and toasts its id', async () => {
    host.rerunExecution.mockResolvedValue({
      executionId: 'exec-10',
      status: 'queued',
      queuePosition: 1,
      duplicate: true,
    });
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    host.listExecutions.mockResolvedValue(
      page([row('exec-10', 'queued'), row('exec-9', 'running')], 42),
    );

    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    await waitFor(() => expect(execRow('exec-10')).not.toBeNull());
    expect(execRow('exec-10')?.className).toContain('xflow-admin-row-highlight');
    expect(rows()[0].getAttribute('data-exec')).toBe('exec-10');
    expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 1, pageSize: 20 });
    await waitFor(() =>
      expect(document.querySelector('.ant-notification')?.textContent).toContain('exec-10'),
    );
  });

  it('clears the status filter after a rerun so the new execution is on page 1', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    await selectStatus('失败');
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        statuses: ['failed'],
      }),
    );

    // Mimic the server: with statuses=[failed] the queued rerun is not returned.
    host.listExecutions.mockImplementation((options: { statuses?: string[] }) =>
      Promise.resolve(
        options.statuses?.length
          ? page([row('exec-7', 'failed')], 1)
          : page([row('exec-10', 'queued'), row('exec-9', 'running')], 42),
      ),
    );
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    await waitFor(() => expect(execRow('exec-10')).not.toBeNull());
    expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 1, pageSize: 20 });
    expect(execRow('exec-10')?.className).toContain('xflow-admin-row-highlight');
    expect(rows()[0].getAttribute('data-exec')).toBe('exec-10');
  });

  it('issues a fresh Idempotency-Key for each dialog open', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const openAndSubmit = async (expected: number) => {
      fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
      const dialog = await screen.findByRole('dialog');
      await within(dialog).findByRole('radio', { name: /从失败节点/ });
      fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));
      await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(expected));
      return host.rerunExecution.mock.calls[expected - 1][1].idempotencyKey as string;
    };

    const first = await openAndSubmit(1);
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    const second = await openAndSubmit(2);

    expect(first).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(second).not.toBe(first);
  });

  it('reuses one Idempotency-Key across retries after 429 and says to retry later', async () => {
    host.rerunExecution.mockRejectedValueOnce(
      new XFlowApiError(429, 'workflow concurrency limit reached', 'workflow_concurrency_limit'),
    );
    host.rerunExecution.mockResolvedValueOnce({ executionId: 'exec-10', status: 'running' });
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    const ok = within(dialog).getByRole('button', {
      name: '从这里重跑',
    }) as HTMLButtonElement;
    fireEvent.click(ok);
    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(1));
    expect(await screen.findByText('并发上限已满，请稍后重试。')).toBeTruthy();

    await waitFor(() => expect(ok.disabled).toBe(false));
    fireEvent.click(ok);
    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(2));
    expect(host.rerunExecution.mock.calls[0][1].idempotencyKey).toBe(
      host.rerunExecution.mock.calls[1][1].idempotencyKey,
    );
  });

  it('does not report a namespace rate limit as a full concurrency queue', async () => {
    host.rerunExecution.mockRejectedValueOnce(
      new XFlowApiError(429, 'rate limit exceeded', 'rate_limited'),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    expect(await screen.findByText('请求过于频繁，请稍后重试。')).toBeTruthy();
    expect(screen.queryByText('并发上限已满，请稍后重试。')).toBeNull();
  });

  it('maps execution_not_rerunnable to explicit copy', async () => {
    host.rerunExecution.mockRejectedValueOnce(
      new XFlowApiError(409, 'execution is not in a rerunnable status', 'execution_not_rerunnable'),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    expect(await screen.findByText(/只有失败、已取消、超时或成功的执行可以重跑/)).toBeTruthy();
  });
});

describe('AL-28 refresh bar', () => {
  it('keeps the old rows and shows a labelled indeterminate bar while a later page loads', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    // Nothing is loading: no bar, and no space reserved for one.
    expect(document.querySelector('.xflow-admin-table-progress')).toBeNull();

    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listExecutions.mockReturnValueOnce(new Promise((done) => { resolve = done; }));
    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);

    const bar = await waitFor(() => {
      const found = document.querySelector('.xflow-admin-table-progress');
      expect(found).not.toBeNull();
      return found as HTMLElement;
    });
    expect(bar.getAttribute('role')).toBe('progressbar');
    expect(bar.getAttribute('aria-busy')).toBe('true');
    expect(bar.getAttribute('aria-label')).toBe('正在刷新运行记录');
    // The old page stays readable; there is no Spin overlay over it.
    expect(rows()).toHaveLength(3);
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();
    expect(document.querySelector('.xflow-admin-table-skeleton')).toBeNull();

    resolve(page([row('exec-30', 'success')], 41));
    await waitFor(() => expect(execRow('exec-30')).not.toBeNull());
    expect(document.querySelector('.xflow-admin-table-progress')).toBeNull();
  });

  it('keeps the old rows under the bar while a status filter refetches', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    host.listExecutions.mockReturnValueOnce(new Promise(() => {}));
    await selectStatus('失败');

    await waitFor(() => expect(document.querySelector('.xflow-admin-table-progress')).not.toBeNull());
    expect(rows()).toHaveLength(3);
  });

  it('does not draw the bar on the first load, where the skeleton owns the state', async () => {
    host.listExecutions.mockReturnValue(new Promise(() => {}));
    renderPage();
    await waitFor(() => expect(document.querySelector('.xflow-admin-table-skeleton')).not.toBeNull());
    expect(document.querySelector('.xflow-admin-table-progress')).toBeNull();
  });
});

describe('AL-28 loading guards', () => {
  it('marks the card busy and disables row operations and the pager while a refetch is in flight', async () => {
    host.listExecutions.mockResolvedValue(
      page([row('exec-9', 'running'), row('exec-7', 'failed')], 41),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(2));
    const card = () => document.querySelector('[data-table="executions"]') as HTMLElement;
    expect(card().getAttribute('aria-busy')).toBe('false');
    const rerun = () => execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLButtonElement;
    expect(rerun().disabled).toBe(false);

    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listExecutions.mockReturnValueOnce(new Promise((done) => { resolve = done; }));
    await selectStatus('失败');

    await waitFor(() => expect(card().getAttribute('aria-busy')).toBe('true'));
    // The old rows are still on screen but cannot start an operation.
    expect(rows()).toHaveLength(2);
    expect(rerun().disabled).toBe(true);
    fireEvent.click(rerun());
    expect(screen.queryByRole('dialog')).toBeNull();
    const pager = [...document.querySelectorAll('.xflow-admin-page button')] as HTMLButtonElement[];
    expect(pager.every((button) => button.disabled)).toBe(true);

    resolve(page([row('exec-7', 'failed')], 1));
    await waitFor(() => expect(card().getAttribute('aria-busy')).toBe('false'));
    expect(rerun().disabled).toBe(false);
  });
});

describe('AL-29 result toasts', () => {
  const popup = () => document.querySelector('.ant-notification') as HTMLElement | null;

  it('opens a bottom-right notification with 打开执行 that follows the execution', async () => {
    // The shared fixture rows carry no workflowId, so there is nowhere to open;
    // this row names its workflow, as every real list item does.
    host.listExecutions.mockResolvedValue(
      page([row('exec-9', 'running', { workflowId: 'wf-1' })], 1),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);
    await waitFor(() => expect(popup()?.textContent).toContain('已暂停 exec-9'));

    expect(popup()?.className).toContain('ant-notification-bottomRight');
    // The page's own message holder is not used for results any more.
    expect(document.querySelector('.ant-message-notice')).toBeNull();
    const open = screen.getByRole('button', { name: '打开执行' });
    fireEvent.click(open);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1?exec=exec-9');
  });

  it('offers no 打开执行 when the row names no workflow to open', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);
    await waitFor(() => expect(popup()?.textContent).toContain('已暂停 exec-9'));
    expect(screen.queryByRole('button', { name: '打开执行' })).toBeNull();
  });

  it('pauses the close timer on hover and does not auto-close before 4s', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      renderPage();
      await vi.waitFor(() => expect(rows()).toHaveLength(3));

      fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);
      await vi.waitFor(() => expect(popup()?.textContent).toContain('已暂停 exec-9'));

      await vi.advanceTimersByTimeAsync(3000);
      expect(popup()?.textContent).toContain('已暂停 exec-9');

      // Hovering holds the toast well past the 4s mark.
      const notice = document.querySelector('.ant-notification-notice') as HTMLElement;
      fireEvent.mouseEnter(notice);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(popup()?.textContent).toContain('已暂停 exec-9');

      fireEvent.mouseLeave(notice);
      await vi.advanceTimersByTimeAsync(5000);
      await vi.waitFor(() => expect(popup()?.textContent ?? '').not.toContain('已暂停 exec-9'));
    } finally {
      vi.useRealTimers();
    }
  });

  it('reports a failure readably without internal paths or type names', async () => {
    host.pauseExecution.mockRejectedValueOnce(
      new Error('POST /v1/executions/exec-9/pause: *protocol.PauseRequest rejected'),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);
    await waitFor(() => expect(popup()?.textContent).toContain('暂停失败'));

    expect(popup()?.className).toContain('ant-notification-bottomRight');
    expect(popup()?.textContent).not.toContain('/v1/');
    expect(popup()?.textContent).not.toContain('protocol.');
    expect(screen.queryByRole('button', { name: '打开执行' })).toBeNull();
  });

  it('never shows the server text of a failure, only a fixed Chinese phrase', async () => {
    host.pauseExecution.mockRejectedValueOnce(
      new Error('dial tcp 10.0.0.7:6379: connect: refused; Authorization: Bearer eyJhbGciOi.secret'),
    );
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.click(execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLElement);
    await waitFor(() => expect(popup()?.textContent).toContain('暂停失败'));

    expect(popup()?.textContent).toContain('操作失败，请稍后重试。');
    expect(document.body.textContent).not.toContain('10.0.0.7');
    expect(document.body.textContent).not.toContain('6379');
    expect(document.body.textContent).not.toContain('Bearer');
    expect(document.body.textContent).not.toContain('eyJhbGciOi');
  });

  it('keeps the rerun failure phrase fixed when the server text carries an address', async () => {
    host.rerunExecution.mockRejectedValueOnce(new Error('POST 10.0.0.8:8080 /v1/x failed, Bearer abc.def'));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);
    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('radio', { name: /从失败节点/ });
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    await waitFor(() => expect(popup()?.textContent).toContain('重跑提交失败'));
    expect(popup()?.textContent).toContain('重跑提交失败，请稍后重试。');
    expect(document.body.textContent).not.toContain('10.0.0.8');
    expect(document.body.textContent).not.toContain('Bearer');
  });

  it('announces a success as a status and reuses one toast for a repeated operation', async () => {
    host.listExecutions.mockResolvedValue(
      page([row('exec-9', 'running', { workflowId: 'wf-1' })], 1),
    );
    host.pauseExecution.mockResolvedValue({ executionId: 'exec-9', status: 'paused' });
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));
    const pause = () => execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLButtonElement;

    fireEvent.click(pause());
    await waitFor(() => expect(popup()?.textContent).toContain('已暂停 exec-9'));
    await waitFor(() => expect(pause().disabled).toBe(false));
    fireEvent.click(pause());
    await waitFor(() => expect(host.pauseExecution).toHaveBeenCalledTimes(2));

    // Same operation on the same target: one toast, not a stack.
    expect(document.querySelectorAll('.ant-notification-notice')).toHaveLength(1);
    expect(document.querySelector('.ant-notification-notice')?.getAttribute('role')).toBe('status');
  });
});

describe('execution id cell', () => {
  const longId = 'exec-7c3f9a2e-1b4d-4e8a-9f6c-2d5b8a1e0c47';
  const shortOf = (id: string) =>
    execRow(id)?.querySelector('[data-copy-exec] .xflow-admin-mono')?.textContent;

  it('drops the exec- prefix and draws the first eight characters after it', async () => {
    host.listExecutions.mockResolvedValue(page([row(longId, 'success')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    const copy = execRow(longId)?.querySelector('[data-copy-exec]') as HTMLElement;
    expect(copy.getAttribute('data-copy-exec')).toBe(longId);
    expect(copy.getAttribute('title')).toBe(longId);
    expect(copy.getAttribute('aria-label')).toBe(`复制执行 ID ${longId}`);
    expect(shortOf(longId)).toBe('7c3f9a2e');
    expect(copy.textContent).not.toContain('exec-');
  });

  it('shows 65d1abcd for exec-65d1abcd-xxxx and keeps the whole id as the name', async () => {
    const id = 'exec-65d1abcd-xxxx';
    host.listExecutions.mockResolvedValue(page([row(id, 'success')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    expect(shortOf(id)).toBe('65d1abcd');
    const copy = execRow(id)?.querySelector('[data-copy-exec]') as HTMLElement;
    expect(copy.getAttribute('title')).toBe(id);
    expect(copy.getAttribute('aria-label')).toContain(id);
  });

  it('takes the first eight characters of an id without the exec- prefix', async () => {
    const id = 'abcdefghijkl';
    host.listExecutions.mockResolvedValue(page([row(id, 'success')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    expect(shortOf(id)).toBe('abcdefgh');
    expect(execRow(id)?.querySelector('[data-copy-exec]')?.getAttribute('title')).toBe(id);
  });

  it('copies the whole id, not the short form', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    host.listExecutions.mockResolvedValue(page([row(longId, 'success')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    fireEvent.click(execRow(longId)?.querySelector('[data-copy-exec]') as HTMLElement);
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(longId));
  });

  // Fixed layout: the colgroup width is the column. Eight mono glyphs are
  // ~58px (0.6em of the 12px cell font); the cell adds 28px of padding and, from
  // 1280, a 16px copy icon with its gap. Fewer than that ellipsizes the id.
  const columnWidths = () =>
    [...document.querySelectorAll('[data-table="executions"] colgroup col')].map((col) =>
      parseFloat((col as HTMLElement).style.width),
    );
  const idColWidth = () => columnWidths()[0] ?? 0;

  it('gives the id column room for eight glyphs plus the copy icon from 1280', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(idColWidth()).toBeGreaterThanOrEqual(8 * 7.22 + 28 + 16);
  });

  it('gives the compact id column room for eight glyphs', async () => {
    window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia;
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(idColWidth()).toBeGreaterThanOrEqual(8 * 7.22 + 28);
    // The extra width comes out of other columns, not out of the card.
    expect(columnWidths().reduce((sum, width) => sum + width, 0)).toBe(750);
  });

  it('keeps the wide table width: the columns still add up to the scroll width', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(columnWidths().reduce((sum, width) => sum + width, 0)).toBe(1008);
  });
});

describe('row navigation link', () => {
  const linkRow = () =>
    page([row('exec a/1', 'success', { workflowId: 'wf-1', workflowName: '客户同步' })], 1);
  const rowLink = () =>
    execRow('exec a/1')?.querySelector('.xflow-admin-col-workflow a[href]') as HTMLAnchorElement;

  it('puts a real link in the workflow name cell that targets the run record', async () => {
    host.listExecutions.mockResolvedValue(linkRow());
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    expect(rowLink().getAttribute('href')).toBe('/workflows/wf-1?exec=exec%20a%2F1');
    expect(rowLink().textContent).toBe('客户同步');
  });

  it('navigates once on a plain click and leaves modified clicks to the browser', async () => {
    host.listExecutions.mockResolvedValue(linkRow());
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    fireEvent.click(rowLink());
    expect(host.push).toHaveBeenCalledTimes(1);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1?exec=exec%20a%2F1');

    host.push.mockClear();
    for (const init of [{ metaKey: true }, { ctrlKey: true }, { shiftKey: true }, { altKey: true }, { button: 1 }]) {
      const notPrevented = fireEvent.click(rowLink(), init);
      expect(notPrevented).toBe(true);
    }
    expect(host.push).not.toHaveBeenCalled();
  });

  it('keeps the whole row clickable and Enter working', async () => {
    host.listExecutions.mockResolvedValue(linkRow());
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    fireEvent.click(execRow('exec a/1') as HTMLElement);
    expect(host.push).toHaveBeenCalledTimes(1);
    fireEvent.keyDown(execRow('exec a/1') as HTMLElement, { key: 'Enter' });
    expect(host.push).toHaveBeenCalledTimes(2);
  });

  it('draws a plain name when the row names no workflow to open', async () => {
    host.listExecutions.mockResolvedValue(page([row('exec-9', 'running')], 1));
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(1));

    expect(execRow('exec-9')?.querySelector('.xflow-admin-col-workflow a')).toBeNull();
  });
});

describe('list failure banner', () => {
  it('separates title and description and never shows the server text', async () => {
    host.listExecutions.mockRejectedValue(new Error('internal error'));
    renderPage();

    const title = await screen.findByText('运行记录加载失败');
    const description = document.querySelector('[data-error-detail]') as HTMLElement;
    expect(description.textContent).toBe('请稍后重试。');
    expect(description.getAttribute('data-error-detail')).toContain('internal error');
    expect(screen.queryByText('internal error')).toBeNull();
    expect(document.querySelector('.xflow-admin-banner')?.textContent).not.toMatch(/加载失败.*加载失败/);
    // innerText is not implemented by jsdom: assert the DOM carries a real
    // separator or a block boundary between the two pieces.
    const container = title.parentElement as HTMLElement;
    const joined = container.textContent ?? '';
    expect(joined).toMatch(/运行记录加载失败\s+请稍后重试。/);
  });
});
