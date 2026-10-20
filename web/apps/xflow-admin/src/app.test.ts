import { beforeEach, describe, expect, it, vi } from 'vitest';

import { UNAUTHORIZED_PATH } from './auth';

type Identity = { subject: string; namespace: string; scopes: string[] };

const identity: Identity = { subject: 'alice', namespace: 'default', scopes: ['workflow'] };

// vi.mock 的工厂会被提升到 import 之前，可变引用必须放进 vi.hoisted。
const mocks = vi.hoisted(() => ({
  history: {
    location: { pathname: '/' },
    replace: vi.fn(),
  },
  fetchCurrentUser: vi.fn<() => Promise<Identity | undefined>>(),
  getToken: vi.fn<() => string | null>(() => null),
  clearToken: vi.fn(),
}));

vi.mock('@umijs/max', () => ({ history: mocks.history }));
vi.mock('./services/user', () => ({ fetchCurrentUser: mocks.fetchCurrentUser }));
vi.mock('./services/token', () => ({
  getToken: mocks.getToken,
  clearToken: mocks.clearToken,
}));

/** 每个用例重新加载 app.ts，让 src/auth.ts 按当前 process.env 重新读取构建期配置。 */
async function loadApp() {
  return import('./app');
}

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
  delete process.env.XFLOW_AUTH_LOGIN_URL;
  mocks.history.location.pathname = '/';
  mocks.getToken.mockReturnValue('bearer-token');
  mocks.fetchCurrentUser.mockResolvedValue(identity);
});

describe('getInitialState', () => {
  it('未授权页不请求 /v1/current-user', async () => {
    mocks.history.location.pathname = UNAUTHORIZED_PATH;
    const { getInitialState } = await loadApp();

    const state = await getInitialState();

    expect(mocks.fetchCurrentUser).not.toHaveBeenCalled();
    expect(state.currentUser).toBeUndefined();
    expect(state.fetchUserInfo).toBeTypeOf('function');
  });

  it('配置的登录入口页同样不请求 /v1/current-user', async () => {
    process.env.XFLOW_AUTH_LOGIN_URL = '/gateway/login';
    mocks.history.location.pathname = '/gateway/login';
    const { getInitialState } = await loadApp();

    const state = await getInitialState();

    expect(mocks.fetchCurrentUser).not.toHaveBeenCalled();
    expect(state.currentUser).toBeUndefined();
  });

  it('普通页面持有令牌时照常拉取 current-user', async () => {
    mocks.history.location.pathname = '/workflows';
    const { getInitialState } = await loadApp();

    const state = await getInitialState();

    expect(mocks.fetchCurrentUser).toHaveBeenCalledTimes(1);
    expect(state.currentUser).toEqual(identity);
  });
});

describe('handleRequestError', () => {
  it('未配置入口时 401 清空令牌并落到未授权页', async () => {
    mocks.history.location.pathname = '/workflows';
    const { handleRequestError } = await loadApp();
    const error = { response: { status: 401 } };

    let caught: unknown;
    try {
      handleRequestError(error);
    } catch (thrown) {
      caught = thrown;
    }

    expect(caught).toBe(error);
    expect(mocks.clearToken).toHaveBeenCalledTimes(1);
    expect(mocks.history.replace).toHaveBeenCalledTimes(1);
    expect(mocks.history.replace).toHaveBeenCalledWith(UNAUTHORIZED_PATH);
  });

  it('已在未授权页时 401 清空令牌但不再重复跳转', async () => {
    mocks.history.location.pathname = UNAUTHORIZED_PATH;
    const { handleRequestError } = await loadApp();

    expect(() => handleRequestError({ response: { status: 401 } })).toThrow();

    expect(mocks.clearToken).toHaveBeenCalledTimes(1);
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });

  it('配置了绝对入口时 401 交给 location.assign，且目标不携带令牌', async () => {
    process.env.XFLOW_AUTH_LOGIN_URL = 'https://sso.example.com/login';
    const assign = vi.fn();
    vi.stubGlobal('window', { location: { assign } });
    const { handleRequestError } = await loadApp();

    expect(() => handleRequestError({ response: { status: 401 } })).toThrow();

    expect(assign).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledWith('https://sso.example.com/login');
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });

  it('非 401 既不清理令牌也不跳转，原样抛出', async () => {
    mocks.history.location.pathname = '/workflows';
    const { handleRequestError } = await loadApp();
    const error = { response: { status: 500 } };

    expect(() => handleRequestError(error)).toThrow();

    expect(mocks.clearToken).not.toHaveBeenCalled();
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });
});

describe('request 插件', () => {
  it('errorConfig 指向统一的 401 处理函数，而不是另一份内联实现', async () => {
    const { handleRequestError, request } = await loadApp();

    expect(request.errorConfig?.errorHandler).toBe(handleRequestError);
  });
});
