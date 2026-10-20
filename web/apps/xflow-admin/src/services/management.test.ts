import { beforeEach, describe, expect, it, vi } from 'vitest';

import { fetchLeader, fetchRunnerPools, fetchRunners } from './management';

const request = vi.hoisted(() => vi.fn());

vi.mock('@umijs/max', () => ({ request }));

beforeEach(() => {
  request.mockReset();
});

describe('fetchRunners', () => {
  it('reads the roster from the enveloped management endpoint', async () => {
    const roster = [{ runner_id: 'r1', enrolled: true, state: 'online' }];
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: roster });

    await expect(fetchRunners()).resolves.toEqual(roster);
    expect(request).toHaveBeenCalledWith('/v1/management/runners', { method: 'GET' });
  });

  it('rejects an envelope without a roster', async () => {
    request.mockResolvedValue({ success: false, code: 'forbidden', message: 'no management', data: [] });
    await expect(fetchRunners()).rejects.toThrow('no management');

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: {} });
    await expect(fetchRunners()).rejects.toThrow(/no roster/);
  });
});

describe('fetchLeader', () => {
  it('reads this replica leadership from the enveloped endpoint', async () => {
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: { is_leader: true } });
    await expect(fetchLeader()).resolves.toBe(true);
    expect(request).toHaveBeenCalledWith('/v1/management/leader', { method: 'GET' });

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: { is_leader: false } });
    await expect(fetchLeader()).resolves.toBe(false);
  });

  it('rejects an envelope that carries no boolean leadership', async () => {
    request.mockResolvedValue({
      success: false,
      code: 'forbidden',
      message: 'leader scope missing',
      data: { is_leader: true },
    });
    await expect(fetchLeader()).rejects.toThrow('leader scope missing');

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: {} });
    await expect(fetchLeader()).rejects.toThrow(/no leader status/);
  });
});

describe('fetchRunnerPools', () => {
  it('reads visible pools from the enveloped management endpoint', async () => {
    const pools = [{ id: 'pool-a', name: 'A' }];
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: pools });

    await expect(fetchRunnerPools()).resolves.toEqual(pools);
    expect(request).toHaveBeenCalledWith('/v1/management/runner-pools', { method: 'GET' });
  });

  it('rejects an envelope without a pool list', async () => {
    request.mockResolvedValue({
      success: false,
      code: 'forbidden',
      message: 'management.runner_pool.read missing',
      data: [],
    });
    await expect(fetchRunnerPools()).rejects.toThrow('management.runner_pool.read missing');

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: {} });
    await expect(fetchRunnerPools()).rejects.toThrow(/no pools/);
  });
});
