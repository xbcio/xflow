import type { RequestConfig, RequestOptions } from '@umijs/max';
import { history } from '@umijs/max';

import { applyUnauthorizedRedirect, isAuthEntryPath } from './auth';
import { clearToken, getToken } from './services/token';
import type { CurrentIdentity } from './services/user';
import { fetchCurrentUser } from './services/user';

export interface InitialState {
  currentUser?: CurrentIdentity;
  /** 重新拉取当前用户，登录成功后调用以刷新权限 */
  fetchUserInfo: () => Promise<CurrentIdentity | undefined>;
}

/**
 * initialState 插件契约：返回值即全局态，access.ts 与 useModel('@@initialState') 消费它。
 *
 * 入口页（未授权说明页 / 网关登录入口）没有可恢复的身份，直接跳过拉取——
 * 判定见 src/auth.ts 的 isAuthEntryPath。
 */
export async function getInitialState(): Promise<InitialState> {
  const fetchUserInfo = async (): Promise<CurrentIdentity | undefined> => {
    if (!getToken()) {
      return undefined;
    }
    try {
      return await fetchCurrentUser();
    } catch {
      // 拉取失败按未登录处理；具体跳转由 errorConfig 的 401 分支负责
      return undefined;
    }
  };

  if (isAuthEntryPath(history.location.pathname)) {
    return { fetchUserInfo };
  }
  return { currentUser: await fetchUserInfo(), fetchUserInfo };
}

/**
 * 401 统一处理：清空令牌后按构建期 XFLOW_AUTH_LOGIN_URL 决定去向。
 * 未配置时落到应用内的 /unauthorized，而不是跳不存在的 /login（见 src/auth.ts）。
 * 非 401 原样抛出，交由调用方处理。
 */
export function handleRequestError(error: unknown): void {
  const status = (error as { response?: { status?: number } })?.response?.status;
  if (status === 401) {
    clearToken();
    applyUnauthorizedRedirect(history.location.pathname, {
      replace: (path) => history.replace(path),
      assign: (url) => window.location.assign(url),
    });
  }
  throw error;
}

/**
 * request 插件契约。
 *
 * 统一注入 Bearer token（OpenAPI 契约声明的 bearerAuth），401 语义见 handleRequestError。
 */
export const request: RequestConfig = {
  requestInterceptors: [
    (config: RequestOptions) => {
      const token = getToken();
      if (token) {
        config.headers = { ...config.headers, Authorization: `Bearer ${token}` };
      }
      return config;
    },
  ],
  errorConfig: {
    errorHandler: handleRequestError,
  },
};
