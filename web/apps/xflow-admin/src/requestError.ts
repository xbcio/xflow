import { history } from '@umijs/max';

import { applyUnauthorizedRedirect } from './auth';
import { clearToken } from './services/token';

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
