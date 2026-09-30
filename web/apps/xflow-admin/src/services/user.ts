import { request } from '@umijs/max';

/**
 * 调用者自身的已核验身份，与 GET /v1/current-user 的响应体逐字对应。
 *
 * 用服务端词汇而不是在前端另起一套 id/name/avatar/permissions：服务端并没有
 * 一个显示名或头像可下发，自造只会得到恒等于 subject 的 name 和永远为空的
 * avatar；而 scopes 本来就是鉴权器真正检查的那组值。
 *
 * 注意粒度：scope 按操作划分（workflow / execution / management.read …），
 * 一个 scope 同时覆盖该操作的读与写，没有 workflow:read 这种写法。
 *
 * 全部字段由服务端从已认证凭证解析得出，请求里没有任何参数能指定主体，因此
 * 这个响应只可能描述调用者自己。
 */
export interface CurrentIdentity {
  subject: string;
  namespace: string;
  scopes: string[];
}

export async function fetchCurrentUser(): Promise<CurrentIdentity> {
  return request<CurrentIdentity>('/v1/current-user', { method: 'GET' });
}
