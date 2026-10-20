import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  listExecutions: vi.fn(),
  getExecution: vi.fn(),
  rerunExecution: vi.fn(),
  clientOptions: undefined as { fetcher?: unknown } | undefined,
}));

vi.mock('@umijs/max', () => ({
  history: { push: vi.fn(), replace: vi.fn(), location: { pathname: '/executions' } },
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
    createXFlowApiClient: (options: { fetcher?: unknown }) => {
      host.clientOptions = options;
      return {
        listExecutions: host.listExecutions,
        getExecution: host.getExecution,
        rerunExecution: host.rerunExecution,
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

const failedDetail = {
  executionId: 'exec-7',
  status: 'failed',
  nodes: [
    { name: 'webhook', status: 'success' },
    { name: 'fetch_profile', status: 'failed', error: '503' },
    { name: 'notify_sales', status: 'pending' },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  host.clientOptions = undefined;
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
  host.getExecution.mockResolvedValue(failedDetail);
  host.rerunExecution.mockResolvedValue({ executionId: 'exec-10', status: 'running' });
});

describe('execution table', () => {
  it('renders a server page with the status table, mono id, created time, terminal duration and first error line', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(host.listExecutions).toHaveBeenCalledWith({ page: 1, pageSize: 20 });
    const success = execRow('exec-8');
    expect(success?.querySelector('.xflow-admin-col-status')?.textContent).toBe('成功');
    expect(success?.querySelector('.xflow-admin-col-exec .xflow-admin-mono')?.textContent).toBe(
      'exec-8',
    );
    // Real ids are 41 chars; the cell has to stay one line (ellipsis + tooltip).
    expect(success?.querySelector('.xflow-admin-col-exec .xflow-admin-ellipsis')).not.toBeNull();
    expect(success?.querySelector('.xflow-admin-col-created')?.textContent).toMatch(
      /\d{2}:\d{2}:\d{2}/,
    );
    // 14:02:23 → 14:02:29 is terminal, so the duration is drawn.
    expect(success?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('6.0 s');
    // A live execution has no settled duration.
    expect(execRow('exec-9')?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('—');
    // Only the first line of a failure is drawn; the full text is the tooltip.
    expect(execRow('exec-7')?.querySelector('.xflow-admin-col-error')?.textContent).toBe(
      'score service returned 503',
    );
    expect(execRow('exec-9')?.querySelector('.xflow-admin-col-error')?.textContent).toBe('—');
  });

  it('rounds the displayed duration once, so it never reads 60 seconds', async () => {
    host.listExecutions.mockResolvedValue(
      page([
        row('exec-6', 'success', {
          createdAt: '2026-10-19T14:00:00Z',
          updatedAt: '2026-10-19T14:00:59.960Z',
        }),
        row('exec-5', 'success', {
          createdAt: '2026-10-19T14:00:00Z',
          updatedAt: '2026-10-19T14:01:59.600Z',
        }),
      ]),
    );
    renderPage();
    await waitFor(() => expect(execRow('exec-6')).not.toBeNull());

    // 59.96 s rounds to one whole minute; the old code printed "60.0 s".
    expect(execRow('exec-6')?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('1m 0s');
    // 119.6 s rounds to two whole minutes; the old code printed "1m 60s".
    expect(execRow('exec-5')?.querySelector('.xflow-admin-col-duration')?.textContent).toBe('2m 0s');
  });

  it('filters on the server, by one status, from page 1', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    const filters = document.querySelector('[data-panel="exec-filters"]') as HTMLElement;
    fireEvent.click(within(filters).getByText('失败'));

    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        status: 'failed',
      }),
    );
  });

  it('paginates on the server and derives the footer from total', async () => {
    renderPage();
    await screen.findByText('exec-9');

    expect(document.querySelector('.xflow-admin-page')?.textContent).toContain('第 1 / 3 页');
    const [previous, next] = [
      ...document.querySelectorAll('.xflow-admin-page button'),
    ] as HTMLButtonElement[];
    expect(previous.disabled).toBe(true);
    fireEvent.click(next);

    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({ page: 2, pageSize: 20 }),
    );
  });

  it('derives row operations from workflowStatusMeta and marks pause/cancel as 未接入', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(ops('exec-9')).toEqual(['pause', 'cancel']);
    expect(ops('exec-8')).toEqual(['rerun']);
    expect(ops('exec-7')).toEqual(['rerun']);
    expect(execRow('exec-9')?.querySelector('[data-ops]')?.getAttribute('data-ops')).toBe(
      'pause,cancel',
    );

    const pause = execRow('exec-9')?.querySelector('[data-op="pause"]') as HTMLButtonElement;
    expect(pause.disabled).toBe(true);
    expect(pause.getAttribute('aria-description')).toBe('未接入');
    // Ruling 24 asks for a VISIBLE tooltip, not just the aria description.
    fireEvent.mouseEnter(pause.parentElement as HTMLElement);
    expect(await screen.findByText('未接入')).toBeTruthy();
    const rerun = execRow('exec-8')?.querySelector('[data-op="rerun"]') as HTMLButtonElement;
    expect(rerun.disabled).toBe(false);
  });

  it('keeps the loaded page when a refresh fails and retries the same page', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    host.listExecutions.mockRejectedValueOnce(new Error('503 service unavailable'));
    fireEvent.click(document.querySelectorAll('.xflow-admin-page button')[1]);
    expect(await screen.findByText('运行记录加载失败')).toBeTruthy();
    expect(screen.getByText('503 service unavailable')).toBeTruthy();
    expect(rows()).toHaveLength(3);

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    await waitFor(() => expect(host.listExecutions).toHaveBeenCalledTimes(3));
  });

  it('shows the baseline empty text and one page for an empty result', async () => {
    host.listExecutions.mockResolvedValue(page([], 0));
    renderPage();

    expect(await screen.findByText('没有匹配的记录')).toBeTruthy();
    expect(document.querySelector('.xflow-admin-page')?.textContent).toContain('第 1 / 1 页');
  });

  it('wires the client through the authenticated fetcher', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(host.clientOptions?.fetcher).toBe(authorizedFetch);
  });
});

describe('rerun dialog', () => {
  it('defaults to the first failed node and posts from_node with a generated idempotency key', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-7')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    expect(host.getExecution).toHaveBeenCalledWith('exec-7');
    const failedRadio = (await within(dialog).findByRole('radio', {
      name: /从失败节点/,
    })) as HTMLInputElement;
    expect(failedRadio.checked).toBe(true);
    expect(within(dialog).getByRole('radio', { name: '从头' })).toBeTruthy();

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

    fireEvent.click(await within(dialog).findByRole('radio', { name: '从头' }));
    fireEvent.click(within(dialog).getByRole('button', { name: '从这里重跑' }));

    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(1));
    expect(host.rerunExecution.mock.calls[0][1].fromNode).toBe('webhook');
  });

  it('removes the failed-node origin for a success source and requires the confirmation checkbox', async () => {
    host.getExecution.mockResolvedValueOnce({
      executionId: 'exec-8',
      status: 'success',
      nodes: [{ name: 'webhook', status: 'success' }],
    });
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    fireEvent.click(execRow('exec-8')?.querySelector('[data-op="rerun"]') as HTMLElement);

    const dialog = await screen.findByRole('dialog');
    await within(dialog).findByRole('checkbox', { name: '确认重跑已成功的执行' });
    expect(within(dialog).queryByRole('radio', { name: /从失败节点/ })).toBeNull();
    const ok = within(dialog).getByRole('button', {
      name: '从这里重跑',
    }) as HTMLButtonElement;
    expect(ok.disabled).toBe(true);

    fireEvent.click(within(dialog).getByRole('checkbox', { name: '确认重跑已成功的执行' }));
    await waitFor(() => expect(ok.disabled).toBe(false));
    fireEvent.click(ok);

    await waitFor(() => expect(host.rerunExecution).toHaveBeenCalledTimes(1));
    expect(host.rerunExecution.mock.calls[0][1]).toMatchObject({
      fromNode: 'webhook',
      includeSuccess: true,
    });
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
      expect(document.querySelector('.ant-message')?.textContent).toContain('exec-10'),
    );
  });

  it('clears the status filter after a rerun so the new execution is on page 1', async () => {
    renderPage();
    await waitFor(() => expect(rows()).toHaveLength(3));
    const filters = document.querySelector('[data-panel="exec-filters"]') as HTMLElement;
    fireEvent.click(within(filters).getByText('失败'));
    await waitFor(() =>
      expect(host.listExecutions).toHaveBeenLastCalledWith({
        page: 1,
        pageSize: 20,
        status: 'failed',
      }),
    );

    // Mimic the server: with status=failed the queued rerun is not returned.
    host.listExecutions.mockImplementation((options: { status?: string }) =>
      Promise.resolve(
        options.status
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
