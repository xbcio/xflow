import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  UNAUTHORIZED_PATH,
  applyUnauthorizedRedirect,
  isAuthEntryPath,
  resolveUnauthorizedRedirect,
  shouldFetchCurrentUser,
} from './auth';

describe('401 去向：未配置 XFLOW_AUTH_LOGIN_URL', () => {
  it('业务页 401 落到应用内未授权页，而不是不存在的 /login', () => {
    const decision = resolveUnauthorizedRedirect('/workflows', '');
    expect(decision).toEqual({ target: 'history', to: UNAUTHORIZED_PATH });
  });

  it('执行决策只调用 history.replace，不触碰 location.assign', () => {
    const sink = { replace: vi.fn(), assign: vi.fn() };
    const decision = applyUnauthorizedRedirect('/executions', sink, '');
    expect(decision).toEqual({ target: 'history', to: UNAUTHORIZED_PATH });
    expect(sink.replace).toHaveBeenCalledTimes(1);
    expect(sink.replace).toHaveBeenCalledWith(UNAUTHORIZED_PATH);
    expect(sink.assign).not.toHaveBeenCalled();
  });

  it('已在未授权页时不再重复跳转', () => {
    expect(resolveUnauthorizedRedirect(UNAUTHORIZED_PATH, '')).toEqual({ target: 'none' });
  });
});

describe('401 去向：配置了 XFLOW_AUTH_LOGIN_URL', () => {
  const absolute = 'https://sso.example.com/login?app=xflow';
  const relative = '/gateway/login';

  it('绝对 URL 交给 location.assign，且原样使用构建期配置', () => {
    const sink = { replace: vi.fn(), assign: vi.fn() };
    const decision = applyUnauthorizedRedirect('/workflows', sink, absolute);
    expect(decision).toEqual({ target: 'location', to: absolute });
    expect(sink.assign).toHaveBeenCalledTimes(1);
    expect(sink.assign).toHaveBeenCalledWith(absolute);
    expect(sink.replace).not.toHaveBeenCalled();
  });

  it('同源相对路径也走 location.assign，避免只在 SPA 内换路由', () => {
    const sink = { replace: vi.fn(), assign: vi.fn() };
    const decision = applyUnauthorizedRedirect('/workflows', sink, relative);
    expect(decision).toEqual({ target: 'location', to: relative });
    expect(sink.assign).toHaveBeenCalledTimes(1);
    expect(sink.assign).toHaveBeenCalledWith(relative);
    expect(sink.replace).not.toHaveBeenCalled();
  });

  it('已在入口页（含末尾斜杠与 query）时不重复跳转，避免循环', () => {
    expect(resolveUnauthorizedRedirect('/gateway/login', relative)).toEqual({ target: 'none' });
    expect(resolveUnauthorizedRedirect('/gateway/login/', relative)).toEqual({ target: 'none' });
    expect(
      resolveUnauthorizedRedirect('/gateway/login?next=%2Fworkflows', relative),
    ).toEqual({ target: 'none' });
    expect(resolveUnauthorizedRedirect('/gateway/login', `  ${relative}  `)).toEqual({
      target: 'none',
    });
  });

  it('未授权页同样不会再跳到入口，避免循环', () => {
    expect(resolveUnauthorizedRedirect(UNAUTHORIZED_PATH, absolute)).toEqual({ target: 'none' });
    expect(resolveUnauthorizedRedirect(UNAUTHORIZED_PATH, relative)).toEqual({ target: 'none' });
  });

  it('同源绝对入口也按入口页判定，不重复跳转', () => {
    const sameOriginAbsolute = new URL('/gateway/login', window.location.origin).href;
    expect(resolveUnauthorizedRedirect('/gateway/login/', sameOriginAbsolute)).toEqual({
      target: 'none',
    });
    expect(resolveUnauthorizedRedirect('/workflows', sameOriginAbsolute)).toEqual({
      target: 'location',
      to: sameOriginAbsolute,
    });
  });
});

describe('回归：未配置入口时的判定不受当前文档 URL 影响', () => {
  beforeEach(() => {
    // jsdom 默认文档 URL 是根路径，会掩盖 `new URL('', ...)` 解析成当前文档
    // 自身的 bug；把文档 URL 放到业务页上，空配置仍必须判定为「不是入口页」。
    window.history.replaceState({}, '', '/workflows');
  });

  afterEach(() => {
    window.history.replaceState({}, '', '/');
  });

  it('空配置不会把当前页面误判成登录入口', () => {
    expect(isAuthEntryPath('/workflows', '')).toBe(false);
    expect(shouldFetchCurrentUser('/workflows', '')).toBe(true);
    expect(resolveUnauthorizedRedirect('/workflows', '')).toEqual({
      target: 'history',
      to: UNAUTHORIZED_PATH,
    });
  });
});

describe('XFLOW_AUTH_LOGIN_URL 形态校验', () => {
  it.each([
    'javascript:alert(1)',
    'data:text/html,<script>alert(1)</script>',
    '//sso.example.com/login',
    '/\\sso.example.com/login',
    'sso.example.com/login',
    'http://',
    'not a url',
  ])('非法配置 %s 按未配置处理', (value) => {
    expect(isAuthEntryPath('/workflows', value)).toBe(false);
    expect(resolveUnauthorizedRedirect('/workflows', value)).toEqual({
      target: 'history',
      to: UNAUTHORIZED_PATH,
    });
  });

  it('接受 http(s) 绝对 URL 与单个前导 / 的同源路径', () => {
    expect(resolveUnauthorizedRedirect('/workflows', 'https://sso.example.com/login')).toEqual({
      target: 'location',
      to: 'https://sso.example.com/login',
    });
    expect(resolveUnauthorizedRedirect('/workflows', 'http://sso.example.com/login')).toEqual({
      target: 'location',
      to: 'http://sso.example.com/login',
    });
    expect(resolveUnauthorizedRedirect('/workflows', '/gateway/login')).toEqual({
      target: 'location',
      to: '/gateway/login',
    });
  });

  it('循环判定大小写不敏感', () => {
    expect(resolveUnauthorizedRedirect('/GATEWAY/Login', '/gateway/login')).toEqual({
      target: 'none',
    });
    expect(resolveUnauthorizedRedirect('/UNAUTHORIZED', '')).toEqual({ target: 'none' });
  });
});

describe('非法构建期配置的加载期告警', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('模块加载时告警一次并回退到未配置行为', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.resetModules();
    vi.stubEnv('XFLOW_AUTH_LOGIN_URL', 'javascript:alert(1)');

    const reloaded = await import('./auth');

    expect(reloaded.AUTH_LOGIN_URL).toBe('');
    expect(warn).toHaveBeenCalledTimes(1);
    expect(reloaded.resolveUnauthorizedRedirect('/workflows')).toEqual({
      target: 'history',
      to: UNAUTHORIZED_PATH,
    });
  });
});

describe('入口页不拉取 current-user', () => {
  it('未授权页与配置的登录入口都跳过拉取', () => {
    expect(isAuthEntryPath(UNAUTHORIZED_PATH, '')).toBe(true);
    expect(shouldFetchCurrentUser(UNAUTHORIZED_PATH, '')).toBe(false);
    expect(isAuthEntryPath('/gateway/login', '/gateway/login')).toBe(true);
    expect(shouldFetchCurrentUser('/gateway/login/', '/gateway/login')).toBe(false);
  });

  it('普通业务页仍然拉取', () => {
    expect(isAuthEntryPath('/workflows', '')).toBe(false);
    expect(shouldFetchCurrentUser('/workflows', '')).toBe(true);
    expect(shouldFetchCurrentUser('/workflows', '/gateway/login')).toBe(true);
  });
});
