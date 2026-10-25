import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  listNodeTypes: vi.fn(),
  clientOptions: undefined as { baseUrl?: string; fetcher?: unknown } | undefined,
}));

vi.mock('@xflow/api', () => ({
  createXFlowApiClient: (options: { baseUrl?: string; fetcher?: unknown }) => {
    host.clientOptions = options;
    return { listNodeTypes: host.listNodeTypes };
  },
}));

vi.mock('../../services/http', () => ({ authorizedFetch: vi.fn() }));

import { authorizedFetch } from '../../services/http';
import NodeTypesPage from './index';

const field = (name: string) => ({ name, path: `/parameters/${name}`, type: 'string' });

const schema = (
  type: string,
  category: string,
  displayName: string,
  params: number,
  description?: string,
  examples?: unknown[],
) => ({
  spec: 'node-form/v1',
  node_type: type,
  node_version: 1,
  kind: 'action',
  display_name: displayName,
  category,
  ...(description ? { description } : {}),
  ...(examples ? { examples } : {}),
  fields: Array.from({ length: params }, (_value, index) => field(`p${index}`)),
});

const httpExamples = [
  {
    name: '简单请求',
    description: 'GET 一个地址',
    params: { url: 'https://example.com', method: 'GET', retries: 2 },
  },
];

const payload = {
  param_validation_mode: 'warn' as const,
  node_types: [
    schema('xflow.http', 'action', 'HTTP', 4, '调用外部 HTTP 服务', httpExamples),
    schema('xflow.function', 'code', 'Function', 2),
    schema('acme.custom', '', 'Custom step', 1),
  ],
};

const rows = () =>
  [...document.querySelectorAll('tr[data-node-type]')].map((row) =>
    row.getAttribute('data-node-type'),
  );

const row = (id: string) => document.querySelector(`tr[data-node-type="${id}"]`) as HTMLElement | null;

beforeEach(() => {
  vi.resetAllMocks();
  host.clientOptions = undefined;
  host.listNodeTypes.mockResolvedValue(payload);
});

describe('node library page', () => {
  it('reads the node types with the authenticated /v1 client and renders them read-only', async () => {
    render(<NodeTypesPage />);
    await waitFor(() => expect(rows()).toHaveLength(3));

    expect(host.clientOptions).toMatchObject({ baseUrl: '/v1', fetcher: authorizedFetch });
    expect(host.listNodeTypes).toHaveBeenCalledTimes(1);

    const http = row('xflow.http@1');
    expect(http?.textContent).toContain('HTTP');
    expect(http?.textContent).toContain('xflow.http · v1');
    expect(http?.textContent).toContain('调用外部 HTTP 服务');
    expect(http?.querySelector('[data-category]')?.getAttribute('data-category')).toBe('action');
    expect(http?.querySelector('[data-category]')?.textContent).toContain('动作');
    expect(http?.querySelector('[data-param-count]')?.textContent).toBe('4');
    // AD-19: counts align on the digit, not on the glyph.
    expect(http?.querySelector('[data-param-count]')?.className).toContain('xflow-admin-num');
    // P1: the one-line description cell keeps the full text in its title.
    const description = http?.querySelector('.xflow-admin-node-desc');
    expect(description?.textContent).toBe('调用外部 HTTP 服务');
    expect(description?.getAttribute('title')).toBe('调用外部 HTTP 服务');
    // A type without a description degrades to the dash, not an error.
    expect(row('xflow.function@1')?.querySelector('.xflow-admin-node-desc')).toBeNull();
    expect(row('xflow.function@1')?.textContent).toContain('—');

    // The response's validation mode is surfaced, not guessed client-side.
    expect(screen.getByText(/参数校验模式：warn/)).toBeTruthy();
    // Read-only page: no editing entry points.
    expect(screen.queryByRole('button', { name: /编辑|新建|保存/ })).toBeNull();
  });

  it('draws skeleton rows on the first load instead of a spinner over an empty table', async () => {
    let resolve!: (value: typeof payload) => void;
    host.listNodeTypes.mockReturnValue(new Promise((done) => { resolve = done; }));
    render(<NodeTypesPage />);

    await waitFor(() => expect(document.querySelector('.xflow-admin-table-skeleton')).not.toBeNull());
    expect(document.querySelectorAll('.xflow-admin-skeleton-row')).toHaveLength(5);
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();

    resolve(payload);
    await waitFor(() => expect(rows()).toHaveLength(3));
    expect(document.querySelector('.xflow-admin-table-skeleton')).toBeNull();
  });

  it('gives the page a single h1', async () => {
    render(<NodeTypesPage />);
    await waitFor(() => expect(rows()).toHaveLength(3));
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('节点库');
    expect(screen.queryAllByRole('heading', { level: 2 })).toHaveLength(0);
  });

  it('filters by category on the client', async () => {
    render(<NodeTypesPage />);
    await waitFor(() => expect(rows()).toHaveLength(3));

    fireEvent.mouseDown(screen.getByRole('combobox', { name: '按分类筛选' }));
    fireEvent.click(await screen.findByRole('option', { name: '代码' }));

    await waitFor(() => expect(rows()).toEqual(['xflow.function@1']));
  });

  it('searches by display name, node type, or description and says when nothing matches', async () => {
    render(<NodeTypesPage />);
    await waitFor(() => expect(rows()).toHaveLength(3));

    const search = screen.getByLabelText('搜索节点类型');
    fireEvent.change(search, { target: { value: 'http' } });
    await waitFor(() => expect(rows()).toEqual(['xflow.http@1']));

    // Search matches the display name too, not just the type.
    fireEvent.change(search, { target: { value: 'custom step' } });
    await waitFor(() => expect(rows()).toEqual(['acme.custom@1']));

    // P1: a word only present in the description still finds the type.
    fireEvent.change(search, { target: { value: '服务' } });
    await waitFor(() => expect(rows()).toEqual(['xflow.http@1']));

    fireEvent.change(search, { target: { value: 'no-such-node' } });
    expect(await screen.findByText('没有匹配的节点类型。')).toBeTruthy();
  });

  it('expands a row into examples with monospace params and copies them as JSON', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    const originalClipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard');
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
    try {
      render(<NodeTypesPage />);
      await waitFor(() => expect(rows()).toHaveLength(3));

      const http = row('xflow.http@1');
      expect(http?.querySelector('.xflow-node-types-examples')).toBeNull();
      fireEvent.click(http!.querySelector('button.ant-table-row-expand-icon') as HTMLElement);

      expect(await screen.findByText('简单请求')).toBeTruthy();
      expect(screen.getByText('GET 一个地址')).toBeTruthy();
      const params = document.querySelector('.xflow-node-types-params') as HTMLElement;
      expect(params.className).toContain('xflow-node-types-params');
      expect(params.textContent).toContain('url');
      expect(params.textContent).toContain('https://example.com');
      expect(params.textContent).toContain('retries');
      expect(params.textContent).toContain('2');

      fireEvent.click(screen.getByRole('button', { name: '复制示例 简单请求 的 JSON' }));
      await waitFor(() =>
        expect(writeText).toHaveBeenCalledWith(JSON.stringify(httpExamples[0].params, null, 2)),
      );
      expect(await screen.findByText('已复制')).toBeTruthy();
    } finally {
      if (originalClipboard) Object.defineProperty(navigator, 'clipboard', originalClipboard);
      else Reflect.deleteProperty(navigator, 'clipboard');
    }
  });

  it('only offers expansion for types that declare examples', async () => {
    render(<NodeTypesPage />);
    await waitFor(() => expect(rows()).toHaveLength(3));

    // The expandable row's icon is live; a type without examples gets the
    // inert spacer AntD renders for a non-expandable row.
    const httpIcon = row('xflow.http@1')?.querySelector('.ant-table-row-expand-icon');
    expect(httpIcon?.className).not.toContain('ant-table-row-expand-icon-spaced');
    const functionIcon = row('xflow.function@1')?.querySelector('.ant-table-row-expand-icon');
    expect(functionIcon?.className).toContain('ant-table-row-expand-icon-spaced');
  });

  it('names the missing scope on a 403 and retries from the alert', async () => {
    const forbidden = Object.assign(new Error('forbidden'), { status: 403 });
    host.listNodeTypes.mockRejectedValueOnce(forbidden).mockResolvedValueOnce(payload);

    render(<NodeTypesPage />);
    expect(await screen.findByText('节点库加载失败')).toBeTruthy();
    expect(screen.getByText(/workflow\.read scope/)).toBeTruthy();

    // AntD's default autoInsertSpace splits a two-character label ("重 试")
    // when the page renders outside the shell's ConfigProvider.
    fireEvent.click(screen.getByRole('button', { name: /重\s*试/ }));

    await waitFor(() => expect(rows()).toHaveLength(3));
    expect(host.listNodeTypes).toHaveBeenCalledTimes(2);
  });

  it('shows the server-empty state instead of an error', async () => {
    host.listNodeTypes.mockResolvedValue({ param_validation_mode: 'off', node_types: [] });

    render(<NodeTypesPage />);

    expect(await screen.findByText('暂无已注册的节点类型。')).toBeTruthy();
    expect(rows()).toHaveLength(0);
  });
});
