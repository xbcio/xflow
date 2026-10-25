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

/** requestError.ts 依赖 src/auth.ts 的构建期配置，按同样方式重新加载。 */
async function loadRequestError() {
  return import('./requestError');
}

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
  delete process.env.XFLOW_AUTH_LOGIN_URL;
  delete process.env.XFLOW_DEV_PROXY_AUTH;
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

  it('开发代理承诺认证且无令牌时仍拉取 current-user', async () => {
    // config/config.ts 的 define 在 dev 产物里把该标志替换成字面量 true；
    // 测试没有经过 define，读到的是环境变量字符串。
    process.env.XFLOW_DEV_PROXY_AUTH = 'true';
    mocks.getToken.mockReturnValue(null);
    mocks.history.location.pathname = '/workflows';
    const { getInitialState } = await loadApp();

    const state = await getInitialState();

    expect(mocks.fetchCurrentUser).toHaveBeenCalledTimes(1);
    expect(state.currentUser).toEqual(identity);
  });

  it('无令牌且开发代理不认证时不请求 current-user', async () => {
    mocks.getToken.mockReturnValue(null);
    mocks.history.location.pathname = '/workflows';
    const { getInitialState } = await loadApp();

    const state = await getInitialState();

    expect(mocks.fetchCurrentUser).not.toHaveBeenCalled();
    expect(state.currentUser).toBeUndefined();
  });
});

describe('handleRequestError', () => {
  it('未配置入口时 401 清空令牌并落到未授权页', async () => {
    mocks.history.location.pathname = '/workflows';
    const { handleRequestError } = await loadRequestError();
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
    const { handleRequestError } = await loadRequestError();

    expect(() => handleRequestError({ response: { status: 401 } })).toThrow();

    expect(mocks.clearToken).toHaveBeenCalledTimes(1);
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });

  it('配置了绝对入口时 401 交给 location.assign，且目标不携带令牌', async () => {
    process.env.XFLOW_AUTH_LOGIN_URL = 'https://sso.example.com/login';
    const assign = vi.fn();
    vi.stubGlobal('window', { location: { assign } });
    const { handleRequestError } = await loadRequestError();

    expect(() => handleRequestError({ response: { status: 401 } })).toThrow();

    expect(assign).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledWith('https://sso.example.com/login');
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });

  it('非 401 既不清理令牌也不跳转，原样抛出', async () => {
    mocks.history.location.pathname = '/workflows';
    const { handleRequestError } = await loadRequestError();
    const error = { response: { status: 500 } };

    expect(() => handleRequestError(error)).toThrow();

    expect(mocks.clearToken).not.toHaveBeenCalled();
    expect(mocks.history.replace).not.toHaveBeenCalled();
  });
});

describe('request 插件', () => {
  it('errorConfig 指向统一的 401 处理函数，而不是另一份内联实现', async () => {
    const { request } = await loadApp();
    const { handleRequestError } = await loadRequestError();

    expect(request.errorConfig?.errorHandler).toBe(handleRequestError);
  });
});

describe('app 插件导出', () => {
  // Umi 把 app.tsx 的每个导出当作运行时插件 key；不在 validKeys 里的 key 会让浏览器
  // 启动时抛 `register failed, invalid key ... from plugin .../src/app.tsx` 并白屏
  // （25c67294 把 handleRequestError 导出在这里，就踩了这个坑）。
  // 集合来自本应用一次真实构建生成的 src/.umi/core/plugin.ts getValidKeys()，新增
  // Umi 插件导致 key 扩充时同步更新这里。
  const UMI_RUNTIME_KEYS = new Set([
    'accessProvider',
    'dataflowProvider',
    'getInitialState',
    'i18nProvider',
    'innerProvider',
    'locale',
    'modifyClientRenderOpts',
    'modifyContextOpts',
    'modifyServerLoaderRequest',
    'onRouteChange',
    'outerProvider',
    'patchClientRoutes',
    'patchRoutes',
    'qiankun',
    'render',
    'request',
    'rootContainer',
  ]);

  it('只包含 Umi 允许的运行时插件 key', async () => {
    const app = await loadApp();

    expect(Object.keys(app)).not.toContain('handleRequestError');
    expect(Object.keys(app).filter((key) => !UMI_RUNTIME_KEYS.has(key))).toEqual([]);
  });
});
