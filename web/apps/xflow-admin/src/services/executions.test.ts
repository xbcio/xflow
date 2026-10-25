import { beforeEach, describe, expect, it, vi } from 'vitest';

import { fetchRecentExecutions } from './executions';

const request = vi.hoisted(() => vi.fn());

vi.mock('@umijs/max', () => ({ request }));

const row = (overrides: Record<string, unknown> = {}) => ({
  execution_id: 'exec-9',
  namespace: 'ns-a',
  workflow_id: 'wf-1',
  workflow_name: '客户同步',
  status: 'success',
  error: '',
  created_at: '2026-10-19T14:02:23Z',
  updated_at: '2026-10-19T14:02:29Z',
  ...overrides,
});

beforeEach(() => {
  request.mockReset();
});

describe('fetchRecentExecutions', () => {
  it('asks for exactly the requested number of rows and maps the wire fields', async () => {
    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: {
        list: [
          row(),
          row({ execution_id: 'exec-8', workflow_name: '告警归因', status: 'paused' }),
        ],
        total: 2,
      },
    });

    await expect(fetchRecentExecutions(3)).resolves.toEqual([
      {
        executionId: 'exec-9',
        workflowId: 'wf-1',
        workflowName: '客户同步',
        status: 'success',
        createdAt: '2026-10-19T14:02:23Z',
      },
      {
        executionId: 'exec-8',
        workflowId: 'wf-1',
        workflowName: '告警归因',
        status: 'paused',
        createdAt: '2026-10-19T14:02:23Z',
      },
    ]);
    expect(request).toHaveBeenCalledWith('/v1/executions?page_size=3', { method: 'GET' });
  });

  it('maps a missing workflow_id to an empty unattributed id', async () => {
    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { list: [row({ workflow_id: undefined })], total: 1 },
    });

    await expect(fetchRecentExecutions(1)).resolves.toEqual([
      expect.objectContaining({ executionId: 'exec-9', workflowId: '' }),
    ]);
  });

  it('rejects a failure envelope and a payload without a list', async () => {
    request.mockResolvedValue({
      success: false,
      code: 'forbidden',
      message: 'execution scope missing',
      data: { list: [], total: 0 },
    });
    await expect(fetchRecentExecutions(3)).rejects.toThrow('execution scope missing');

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: {} });
    await expect(fetchRecentExecutions(3)).rejects.toThrow(/缺少列表数据/);
  });

  it('rejects a row with a missing field or an unknown status', async () => {
    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { list: [row({ status: 'not-a-status' })], total: 1 },
    });
    await expect(fetchRecentExecutions(3)).rejects.toThrow(/无法识别的行/);

    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { list: [row({ execution_id: undefined })], total: 1 },
    });
    await expect(fetchRecentExecutions(3)).rejects.toThrow(/无法识别的行/);
  });
});
