import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// `authorizedFetch` reads `history.location.pathname` before deciding whether
// to send a 401 to /login; the getter keeps that read live in the real module.
const replace = vi.hoisted(() => vi.fn());

vi.mock('@umijs/max', () => ({
  history: { location: { pathname: '/workflows' }, replace },
}));

import { clearToken, setToken } from './token';
import { fetchCurrentUser } from './user';

function success(data: unknown): Response {
  return new Response(
    JSON.stringify({ success: true, code: '200', message: '', data }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  );
}

beforeEach(() => {
  clearToken();
  replace.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('fetchCurrentUser', () => {
  it('delegates to GET /v1/current-user through the authorized fetcher', async () => {
    setToken('tok-user');
    const fetchMock = vi.fn().mockResolvedValue(
      success({ subject: 'dev-operator', namespace: 'default', scopes: ['workflow', 'execution.control'] }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(fetchCurrentUser()).resolves.toEqual({
      subject: 'dev-operator',
      namespace: 'default',
      scopes: ['workflow', 'execution.control'],
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toBe('/v1/current-user');
    expect((init.headers as Headers).get('Authorization')).toBe('Bearer tok-user');
  });

  it('rejects a failure envelope instead of inventing an identity', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({ success: false, code: 'unauthenticated', message: 'unauthorized' }),
        { status: 401, headers: { 'content-type': 'application/json' } },
      ),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(fetchCurrentUser()).rejects.toMatchObject({
      name: 'XFlowApiError',
      status: 401,
      code: 'unauthenticated',
    });
  });
});
