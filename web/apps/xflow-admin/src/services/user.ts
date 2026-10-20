import { createXFlowApiClient, type CurrentIdentity } from '@xflow/api';

import { authorizedFetch } from './http';

/**
 * 调用者自身的已核验身份，与 GET /v1/current-user 的响应体逐字对应。
 *
 * 用服务端词汇而不是在前端另起一套 id/name/avatar/permissions：服务端并没有
 * 一个显示名或头像可下发，自造只会得到恒等于 subject 的 name 和永远为空的
 * avatar；而 scopes 本来就是鉴权器真正检查的那组值。
 *
 * 注意粒度：服务端当前按操作而不是按读/写划分 scope（workflow / execution /
 * execution.control / execution.rerun）；只读拆分（workflow.read）落地前，读与
 * 写同属 workflow。capabilities 投影见 `@xflow/core` 的 `capabilitiesFromScopes`。
 *
 * 全部字段由服务端从已认证凭证解析得出，请求里没有任何参数能指定主体，因此
 * 这个响应只可能描述调用者自己。
 */
export type { CurrentIdentity };

// One module-level client: `authorizedFetch` carries the admin session and is
// the same fetcher the pages hand their own clients, so there is no second
// envelope parser and no unauthenticated fallback to the global `fetch`.
const api = createXFlowApiClient({ baseUrl: '/v1', fetcher: authorizedFetch });

export function fetchCurrentUser(): Promise<CurrentIdentity> {
  return api.getCurrentUser();
}
