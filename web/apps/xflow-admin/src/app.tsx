import { StyleProvider } from '@ant-design/cssinjs';
import type { RequestConfig, RequestOptions } from '@umijs/max';
import { history } from '@umijs/max';
import type { ReactNode } from 'react';

import { clearToken, getToken } from '@/services/token';
import type { CurrentIdentity } from '@/services/user';
import { fetchCurrentUser } from '@/services/user';

const LOGIN_PATH = '/login';

export interface InitialState {
  currentUser?: CurrentIdentity;
  /** 重新拉取当前用户，登录成功后调用以刷新权限 */
  fetchUserInfo: () => Promise<CurrentIdentity | undefined>;
}

/**
 * initialState 插件契约：返回值即全局态，access.ts 与 useModel('@@initialState') 消费它。
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

  if (history.location.pathname === LOGIN_PATH) {
    return { fetchUserInfo };
  }
  return { currentUser: await fetchUserInfo(), fetchUserInfo };
}

/**
 * request 插件契约。
 *
 * 统一注入 Bearer token（OpenAPI 契约声明的 bearerAuth），并在 401 时清空令牌跳登录页。
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
    errorHandler: (error: unknown) => {
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 401) {
        clearToken();
        if (history.location.pathname !== LOGIN_PATH) {
          history.replace(LOGIN_PATH);
        }
      }
      throw error;
    },
  },
};

/**
 * 应用根容器。AntD 的 CSS-in-JS 样式整体进入 `@layer antd`（层的顺序在 tailwind.css 里
 * 声明，设计文档 §7.1）：xflow 语义类排在它后面，无需提权即可覆盖；Tailwind 工具类排在
 * 最后，布局永远生效。只在这里包一次，各路由不得再自行包 StyleProvider（重复包会生成两份样式）。
 */
export function rootContainer(container: ReactNode) {
  return <StyleProvider layer>{container}</StyleProvider>;
}
