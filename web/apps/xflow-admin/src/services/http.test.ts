import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { UNAUTHORIZED_PATH } from '../auth';

// vi.mock 的工厂会被提升到 import 之前，可变引用必须放进 vi.hoisted。
const mocks = vi.hoisted(() => ({
  replace: vi.fn(),
  pathname: '/workflows',
}));

vi.mock('@umijs/max', () => ({
  history: {
    location: {
      // getter 让每个用例可以改当前路径，而不用重建 mock 对象。
      get pathname() {
        return mocks.pathname;
      },
    },
    replace: mocks.replace,
  },
}));

/**
 * 每个用例重新加载 http/auth/token：src/auth.ts 在模块加载时读取构建期
 * XFLOW_AUTH_LOGIN_URL，静态 import 会把它锁死在文件加载那一刻的配置上。
 */
async function loadModules(): Promise<{
  http: typeof import('./http');
  token: typeof import('./token');
}> {
  vi.resetModules();
  const http = await import('./http');
  const token = await import('./token');
  return { http, token };
}

beforeEach(() => {
  mocks.replace.mockReset();
  mocks.pathname = '/workflows';
  delete process.env.XFLOW_AUTH_LOGIN_URL;
  sessionStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  delete process.env.XFLOW_AUTH_LOGIN_URL;
});

describe('authorizedFetch', () => {
  it('adds the session Bearer token to every xflow-api request', async () => {
    const { http, token } = await loadModules();
    token.setToken('tok-1');
    const fetchMock = vi.fn().mockResolvedValue({ status: 200 } as Response);
    vi.stubGlobal('fetch', fetchMock);

    await http.authorizedFetch('/v1/workflows?page=1', { method: 'GET' });

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect((init.headers as Headers).get('Authorization')).toBe('Bearer tok-1');
  });

  it('clears the token, lands on the in-app unauthorized page and still returns the response on 401', async () => {
    const { http, token } = await loadModules();
    token.setToken('tok-2');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ status: 401 } as Response));

    const response = await http.authorizedFetch('/v1/node-types');

    expect(response.status).toBe(401);
    expect(token.getToken()).toBeNull();
    expect(mocks.replace).toHaveBeenCalledTimes(1);
    expect(mocks.replace).toHaveBeenCalledWith(UNAUTHORIZED_PATH);
  });

  it('does not redirect again when the current page is already the unauthorized entry', async () => {
    mocks.pathname = UNAUTHORIZED_PATH;
    const { http, token } = await loadModules();
    token.setToken('tok-3');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ status: 401 } as Response));

    const response = await http.authorizedFetch('/v1/node-types');

    expect(response.status).toBe(401);
    expect(token.getToken()).toBeNull();
    expect(mocks.replace).not.toHaveBeenCalled();
  });

  it('navigates to the configured gateway entry with location.assign on 401', async () => {
    process.env.XFLOW_AUTH_LOGIN_URL = 'https://sso.example.com/login';
    const assign = vi.fn();
    const { http, token } = await loadModules();
    token.setToken('tok-4');
    vi.stubGlobal('window', { location: { assign } });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ status: 401 } as Response));

    const response = await http.authorizedFetch('/v1/node-types');

    expect(response.status).toBe(401);
    expect(token.getToken()).toBeNull();
    expect(assign).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledWith('https://sso.example.com/login');
    expect(mocks.replace).not.toHaveBeenCalled();
  });
});
