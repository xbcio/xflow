import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  search: '',
  push: vi.fn(),
  listWorkflows: vi.fn(),
  listNodeTypes: vi.fn(),
  fetchRunners: vi.fn(),
  fetchRunnerPools: vi.fn(),
  fetchRecentExecutions: vi.fn(),
  clientOptions: undefined as { fetcher?: unknown } | undefined,
}));

vi.mock('@umijs/max', () => ({
  history: { push: host.push },
  useLocation: () => ({ pathname: '/workflows', search: host.search }),
}));

vi.mock('@xflow/api', () => ({
  createXFlowApiClient: (options: { fetcher?: unknown }) => {
    host.clientOptions = options;
    return {
      listWorkflows: host.listWorkflows,
      listNodeTypes: host.listNodeTypes,
    };
  },
}));

vi.mock('../../services/management', () => ({
  fetchRunners: () => host.fetchRunners(),
  fetchRunnerPools: () => host.fetchRunnerPools(),
}));

vi.mock('../../services/executions', () => ({
  fetchRecentExecutions: (size: number) => host.fetchRecentExecutions(size),
}));

import { authorizedFetch } from '../../services/http';
import { WorkflowCenter } from './WorkflowCenter';

const wf = (id: string, name: string, version: string) => ({
  id,
  name,
  version,
  definitionHash: `sha256:${id}`,
  registryRevision: 1,
});

const page = (list: ReturnType<typeof wf>[], total = list.length) => ({ list, total });

const exec = (id: string, workflowName: string, status: string) => ({
  executionId: id,
  workflowName,
  status,
  createdAt: '2026-10-19T14:02:23Z',
});

beforeEach(() => {
  vi.clearAllMocks();
  host.search = '';
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
  host.fetchRecentExecutions.mockResolvedValue([
    exec('exec-9', '告警归因', 'running'),
    exec('exec-8', '客户同步', 'success'),
    exec('exec-7', '夜间对账', 'failed'),
  ]);
});

const rows = () => [...document.querySelectorAll('tr[data-workflow]')].map((row) => row.getAttribute('data-workflow'));
const card = (id: string) => document.querySelector(`[data-env="${id}"]`);

describe('workflow table', () => {
  it('renders a server page with only the columns the backend has (ruling 20)', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    expect(host.listWorkflows).toHaveBeenCalledWith({ page: 1, pageSize: 20 });
    expect(rows()).toEqual(['wf-1', 'wf-2']);
    const nameCell = document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-name');
    expect(nameCell?.textContent).toBe('客户同步wf-1');
    expect(document.querySelector('tr[data-workflow="wf-1"] .xflow-admin-col-version')?.textContent).toBe('v3');

    const headers = [...document.querySelectorAll('thead th')].map((th) => th.textContent ?? '');
    expect(headers.some((text) => text.includes('名称'))).toBe(true);
    expect(headers.some((text) => text.includes('版本'))).toBe(true);
    // Ruling 20: the list endpoint has no node count, so the column is gone.
    expect(headers).not.toContain('节点');
  });

  it('marks 触发 and 最近运行 as 需新字段 instead of inventing values', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    expect(document.querySelectorAll('.xflow-admin-todo')).toHaveLength(2);
    for (const cls of ['.xflow-admin-col-entry', '.xflow-admin-col-lastrun']) {
      expect(document.querySelector(`tr[data-workflow="wf-1"] ${cls}`)?.textContent).toBe('—');
    }
    expect(document.body.textContent).toContain('需新字段');
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
    const [previous, next] = [...document.querySelectorAll('.xflow-admin-page button')] as HTMLButtonElement[];
    expect(previous.disabled).toBe(true);
    expect(next.disabled).toBe(false);

    fireEvent.click(next);
    await screen.findByText('第二页');
    expect(host.listWorkflows).toHaveBeenLastCalledWith({ page: 2, pageSize: 20 });
    expect(rows()).toEqual(['wf-21']);
    expect(footer()?.textContent).toContain('第 2 / 3 页');
  });

  it('opens the editor route for a row', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));
    fireEvent.click(document.querySelector('tr[data-workflow="wf-1"] [data-op="open"]') as HTMLElement);
    expect(host.push).toHaveBeenCalledWith('/workflows/wf-1');
  });

  it('spins while loading and keeps the rows already on screen', async () => {
    let resolve!: (value: ReturnType<typeof page>) => void;
    host.listWorkflows.mockReturnValue(new Promise((done) => { resolve = done; }));
    render(<WorkflowCenter />);

    await waitFor(() => expect(document.querySelector('.ant-spin-spinning')).not.toBeNull());
    resolve(page([wf('wf-1', '客户同步', 'v3')]));
    await waitFor(() => expect(rows()).toContain('wf-1'));
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();
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
    await waitFor(() => expect(document.querySelector('.ant-spin-spinning')).not.toBeNull());
    expect(rows()).toEqual(['wf-1']);

    await act(async () => {
      failSecond(new Error('503 page 2'));
    });
    expect(await screen.findByText('工作流列表加载失败')).toBeTruthy();
    expect(rows()).toEqual(['wf-1']);
  });

  it('wires both @xflow/api reads through the authenticated fetcher', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    expect(host.clientOptions?.fetcher).toBe(authorizedFetch);
  });

  it('shows the baseline empty text when the page has no rows', async () => {
    host.listWorkflows.mockResolvedValue(page([], 0));
    render(<WorkflowCenter />);

    expect(await screen.findByText('没有匹配的记录')).toBeTruthy();
    expect(document.querySelector('.xflow-admin-page')?.textContent).toContain('第 1 / 1 页');
    const next = document.querySelectorAll('.xflow-admin-page button')[1] as HTMLButtonElement;
    expect(next.disabled).toBe(true);
  });

  it('reports a failed fetch and refetches the same page on 重试', async () => {
    host.listWorkflows.mockRejectedValueOnce(new Error('503 service unavailable'));
    host.listWorkflows.mockResolvedValueOnce(page([wf('wf-1', '客户同步', 'v3')]));
    render(<WorkflowCenter />);

    expect(await screen.findByText('工作流列表加载失败')).toBeTruthy();
    expect(screen.getByText('503 service unavailable')).toBeTruthy();

    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    await waitFor(() => expect(rows()).toContain('wf-1'));
    expect(host.listWorkflows).toHaveBeenCalledTimes(2);
    expect(screen.queryByText('工作流列表加载失败')).toBeNull();
  });

  it('never applies a fake client-side filter for the search box', async () => {
    host.search = '?q=客户';
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    // Only the server page is rendered; `q` is not forwarded because the list
    // endpoint has no search parameter, and the page says so instead of
    // pretending the current page is a filtered result set.
    expect(host.listWorkflows).toHaveBeenCalledWith({ page: 1, pageSize: 20 });
    expect(rows()).toEqual(['wf-1', 'wf-2']);
    expect(screen.getByTestId('search-note').textContent).toContain('暂不支持按名称 / ID 搜索');
  });
});

describe('status cards', () => {
  it('counts server-reported liveness, pools and node types', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('runners')).not.toBeNull());

    expect(card('runners')?.textContent).toContain('1 / 3');
    expect(card('pools')?.textContent).toContain('3');
    expect(card('types')?.textContent).toContain('28');
    expect(card('runners')?.textContent).toContain('Runner 在线');
  });

  it('hides all three cards when the management module is unavailable', async () => {
    host.fetchRunners.mockRejectedValue(new Error('404 management module disabled'));
    host.fetchRunnerPools.mockRejectedValue(new Error('404 management module disabled'));
    render(<WorkflowCenter />);

    await waitFor(() => expect(host.fetchRunnerPools).toHaveBeenCalled());
    expect(host.listNodeTypes).toHaveBeenCalled();
    // Let Promise.allSettled settle and the (hidden) state flush.
    await act(async () => {});
    expect(card('runners')).toBeNull();
    expect(card('pools')).toBeNull();
    expect(card('types')).toBeNull();
    // The workflow list does not depend on the management module.
    await waitFor(() => expect(rows()).toContain('wf-1'));
  });

  it('degrades a single card instead of the whole group', async () => {
    host.listNodeTypes.mockRejectedValue(new Error('502'));
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('runners')).not.toBeNull());

    expect(card('pools')?.textContent).toContain('3');
    expect(card('types')?.textContent).toContain('—');
  });

  it('keeps the other cards when only the runner roster fails', async () => {
    host.fetchRunners.mockRejectedValue(new Error('502 runners'));
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('runners')).not.toBeNull());

    expect(card('runners')?.textContent).toContain('—');
    expect(card('pools')?.textContent).toContain('3');
    expect(card('types')?.textContent).toContain('28');
  });

  it('keeps the other cards when only the pool list fails', async () => {
    host.fetchRunnerPools.mockRejectedValue(new Error('502 pools'));
    render(<WorkflowCenter />);
    await waitFor(() => expect(card('pools')).not.toBeNull());

    expect(card('pools')?.textContent).toContain('—');
    expect(card('runners')?.textContent).toContain('1 / 3');
  });
});

describe('recent runs', () => {
  it('asks for three cross-workflow rows and renders their server fields', async () => {
    render(<WorkflowCenter />);
    await screen.findByText('exec-9');

    expect(host.fetchRecentExecutions).toHaveBeenCalledWith(3);
    const runRows = [...document.querySelectorAll('[data-panel="recent-runs"] .xflow-admin-run')];
    expect(runRows).toHaveLength(3);
    expect(runRows.map((row) => row.getAttribute('data-workflow'))).toEqual(['告警归因', '客户同步', '夜间对账']);
    expect(runRows.map((row) => row.getAttribute('data-status'))).toEqual(['running', 'success', 'failed']);
    expect(runRows[1].textContent).toContain('成功');
    expect(runRows[2].textContent).toContain('失败');
  });

  it('retries a failed recent-run fetch without touching the table', async () => {
    host.fetchRecentExecutions.mockRejectedValueOnce(new Error('502 recent'));
    host.fetchRecentExecutions.mockResolvedValueOnce([exec('exec-1', '客户同步', 'success')]);
    render(<WorkflowCenter />);

    expect(await screen.findByText('502 recent')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));
    expect(await screen.findByText('exec-1')).toBeTruthy();
    expect(host.fetchRecentExecutions).toHaveBeenLastCalledWith(3);
  });
});

describe('create entries', () => {
  it('starts a blank workflow and describes import DSL as 未接入', async () => {
    render(<WorkflowCenter />);
    await waitFor(() => expect(rows()).toContain('wf-1'));

    fireEvent.click(screen.getByRole('button', { name: /新\s*建/ }));
    expect(host.push).toHaveBeenCalledWith('/workflows/new');

    const importButton = screen.getByRole('button', { name: '选择文件' }) as HTMLButtonElement;
    expect(importButton.disabled).toBe(true);
    expect(importButton.getAttribute('aria-description')).toBe('未接入');
  });
});
