import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  drainRunner,
  fetchLeader,
  fetchRunnerPools,
  fetchRunners,
  resumeRunner,
  revokeRunnerIdentity,
} from './management';

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

describe('drainRunner / resumeRunner', () => {
  it('encodes the runner id, sends the idempotency header and only the reason body', async () => {
    const snapshot = { desired_state: 'draining', generation: 4, reason: '维护' };
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: snapshot });

    await expect(
      drainRunner('runner/with space', { reason: '维护', requestId: 'req-1' }),
    ).resolves.toEqual(snapshot);
    expect(request).toHaveBeenCalledWith('/v1/management/runners/runner%2Fwith%20space/drain', {
      method: 'POST',
      headers: { 'X-Request-Id': 'req-1' },
      data: { reason: '维护' },
    });
  });

  it('posts resume to its own path and returns the active snapshot', async () => {
    const snapshot = { desired_state: 'active', generation: 5 };
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: snapshot });

    await expect(resumeRunner('r1', { reason: '恢复', requestId: 'req-2' })).resolves.toEqual(
      snapshot,
    );
    expect(request).toHaveBeenCalledWith('/v1/management/runners/r1/resume', {
      method: 'POST',
      headers: { 'X-Request-Id': 'req-2' },
      data: { reason: '恢复' },
    });
  });

  it('rejects a snapshot without a valid desired state or generation', async () => {
    request.mockResolvedValue({
      success: false,
      code: 'forbidden',
      message: 'control scope missing',
      data: { desired_state: 'draining', generation: 1 },
    });
    await expect(drainRunner('r1', { reason: '维护', requestId: 'req-1' })).rejects.toThrow(
      'control scope missing',
    );

    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { desired_state: 'paused', generation: 1 },
    });
    await expect(drainRunner('r1', { reason: '维护', requestId: 'req-1' })).rejects.toThrow(
      /no control snapshot/,
    );

    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { desired_state: 'draining', generation: -1 },
    });
    await expect(drainRunner('r1', { reason: '维护', requestId: 'req-1' })).rejects.toThrow(
      /no control snapshot/,
    );

    request.mockResolvedValue({
      success: true,
      code: 'ok',
      message: '',
      data: { desired_state: 'draining' },
    });
    await expect(resumeRunner('r1', { reason: '恢复', requestId: 'req-1' })).rejects.toThrow(
      /no control snapshot/,
    );
  });

  it('propagates the server rejection unchanged so the caller can read its code', async () => {
    const error = Object.assign(new Error('X-Request-Id was reused with a different request'), {
      response: { status: 409, data: { code: 'idempotency_key_reused' } },
    });
    request.mockRejectedValue(error);

    await expect(drainRunner('r1', { reason: '维护', requestId: 'req-1' })).rejects.toBe(error);
  });
});

describe('revokeRunnerIdentity', () => {
  it('encodes the runner id and validates the revocation receipt', async () => {
    const receipt = { runner_id: 'runner/1', status: 'revoked' };
    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: receipt });

    await expect(revokeRunnerIdentity('runner/1')).resolves.toEqual(receipt);
    expect(request).toHaveBeenCalledWith('/v1/management/runners/runner%2F1/revoke-identity', {
      method: 'POST',
    });
  });

  it('rejects a receipt without a runner id or status', async () => {
    request.mockResolvedValue({
      success: false,
      code: 'not_implemented',
      message: 'issued identity store is not configured',
      data: { runner_id: 'r1', status: 'revoked' },
    });
    await expect(revokeRunnerIdentity('r1')).rejects.toThrow('issued identity store is not configured');

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: { runner_id: 'r1' } });
    await expect(revokeRunnerIdentity('r1')).rejects.toThrow(/no receipt/);

    request.mockResolvedValue({ success: true, code: 'ok', message: '', data: { status: 'revoked' } });
    await expect(revokeRunnerIdentity('r1')).rejects.toThrow(/no receipt/);
  });

  it('propagates a 403 rejection unchanged', async () => {
    const error = Object.assign(new Error('forbidden'), {
      response: { status: 403, data: { code: 'forbidden' } },
    });
    request.mockRejectedValue(error);

    await expect(revokeRunnerIdentity('r1')).rejects.toBe(error);
  });
});
