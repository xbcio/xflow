import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const replace = vi.hoisted(() => vi.fn());

vi.mock('@umijs/max', () => ({
  history: { location: { pathname: '/workflows' }, replace },
}));

import { authorizedFetch } from './http';
import { clearToken, getToken, setToken } from './token';

beforeEach(() => {
  clearToken();
  replace.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('authorizedFetch', () => {
  it('adds the session Bearer token to every xflow-api request', async () => {
    setToken('tok-1');
    const fetchMock = vi.fn().mockResolvedValue({ status: 200 } as Response);
    vi.stubGlobal('fetch', fetchMock);

    await authorizedFetch('/v1/workflows?page=1', { method: 'GET' });

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect((init.headers as Headers).get('Authorization')).toBe('Bearer tok-1');
  });

  it('clears the token and returns to login on 401', async () => {
    setToken('tok-2');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ status: 401 } as Response));

    const response = await authorizedFetch('/v1/node-types');

    expect(response.status).toBe(401);
    expect(getToken()).toBeNull();
    expect(replace).toHaveBeenCalledWith('/login');
  });
});
