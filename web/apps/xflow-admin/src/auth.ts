/**
 * 401 之后的去向决策。
 *
 * 本应用没有登录页：开发期由 config/proxy.ts 在服务端注入 Bearer 令牌，生产由
 * 部署层网关负责鉴权。因此 401 只有两种去处：
 *   - 配置了构建期 XFLOW_AUTH_LOGIN_URL：整页导航到网关的登录/鉴权入口（相对路径
 *     同样走 window.location.assign，绝不能只在 SPA 内换路由）；
 *   - 未配置：落到应用内的 /unauthorized 说明页，而不是跳一个不存在的 /login。
 *
 * 跳转目标只来自构建期配置，不接受运行时输入；令牌不进 URL，也不落到日志里。
 */

/** 未配置网关入口时 401 的落地页。它是一条真实路由，不再是 `*` 兜底的 404。 */
export const UNAUTHORIZED_PATH = '/unauthorized';

/** 只接受 http(s) 绝对 URL。`javascript:` 等伪协议在这里就被挡掉。 */
function isAbsoluteHttpUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * 同源绝对路径（单个前导 /）。以 // 开头的协议相对 URL、浏览器会折叠成 //
 * 的 `/\` 写法都不是站内路径。
 */
function isSameOriginPath(url: string): boolean {
  return url.startsWith('/') && !url.startsWith('//') && !url.startsWith('/\\');
}

/**
 * 构建期配置的形态校验：只接受 http(s) 绝对 URL 或同源路径，其余一律按
 * 未配置处理。畸形值若原样使用，会让 401 跳到 `javascript:`、协议相对地址
 * 之类的非预期目标，或者因 URL 解析失败卡死跳转。
 */
function validAuthLoginUrl(raw: string | undefined): string {
  const value = (raw ?? '').trim();
  if (!value) return '';
  return isAbsoluteHttpUrl(value) || isSameOriginPath(value) ? value : '';
}

/**
 * 构建期由 config/config.ts 的 define 注入；未配置或非法时为空串。
 * 非法配置在这里（模块加载/构建产物初始化时）告警一次，而不是每次 401 都喊。
 */
export const AUTH_LOGIN_URL = (() => {
  const raw = process.env.XFLOW_AUTH_LOGIN_URL;
  const value = typeof raw === 'string' ? raw.trim() : '';
  if (!value) return '';
  const valid = validAuthLoginUrl(value);
  if (!valid) {
    console.warn(
      `[xflow-admin] XFLOW_AUTH_LOGIN_URL 不是 http(s) 绝对 URL 或单个前导 / 的同源路径，已按未配置处理；401 将落到 ${UNAUTHORIZED_PATH}。`,
    );
  }
  return valid;
})();

export type UnauthorizedRedirect =
  | { target: 'none' }
  | { target: 'history'; to: string }
  | { target: 'location'; to: string };

export interface UnauthorizedRedirectSink {
  /** react-router history.replace；用于应用内的未授权页，避免给 401 增加历史记录。 */
  replace: (path: string) => void;
  /** window.location.assign；用于网关入口（含同源相对路径），会发起整页导航。 */
  assign: (url: string) => void;
}

/** 去掉 query/hash 与末尾斜杠并折叠大小写，仅用于比较同源路径。 */
function comparablePath(input: string): string {
  const cut = input.search(/[?#]/);
  const path = cut === -1 ? input : input.slice(0, cut);
  const trimmed = path.length > 1 && path.endsWith('/') ? path.slice(0, -1) : path;
  // 循环判定不区分大小写：入口配置写成 /Gateway/Login、地址栏是 /gateway/login
  // 时仍然是同一个入口页，否则会来回跳转。
  return trimmed.toLowerCase();
}

/**
 * 登录入口的 pathname，用于判定「已经在入口页」。
 * 相对路径直接取；绝对 URL 只有与当前页面同源时才有意义——跨源入口一旦
 * 导航过去，本应用就不会再执行，无需也无法做循环判定。空串/非法配置返回
 * undefined：`new URL('')` 会解析成当前文档自身，那会把任何页面都误判成入口页。
 */
function entryPathname(loginUrl: string): string | undefined {
  const value = validAuthLoginUrl(loginUrl);
  if (!value) {
    return undefined;
  }
  if (isSameOriginPath(value)) {
    return comparablePath(value);
  }
  try {
    const entry = new URL(value, window.location.href);
    return entry.origin === window.location.origin ? comparablePath(entry.pathname) : undefined;
  } catch {
    // 非法配置按未配置入口处理（调用方会落到 /unauthorized），不让坏值卡死跳转。
    return undefined;
  }
}

/**
 * 入口页判定：未授权说明页，或当前已位于配置的同源登录入口。
 * 两者都不应再次触发 401 跳转，否则会形成循环。
 */
export function isAuthEntryPath(pathname: string, loginUrl: string = AUTH_LOGIN_URL): boolean {
  const current = comparablePath(pathname);
  if (current === UNAUTHORIZED_PATH) {
    return true;
  }
  const entry = entryPathname(loginUrl);
  return entry !== undefined && current === entry;
}

/** 入口页不拉取 /v1/current-user：那里没有可恢复的身份，请求只会再吃一次 401。 */
export function shouldFetchCurrentUser(
  pathname: string,
  loginUrl: string = AUTH_LOGIN_URL,
): boolean {
  return !isAuthEntryPath(pathname, loginUrl);
}

/** 纯决策：给定当前路径与构建期配置，401 应落到哪里。 */
export function resolveUnauthorizedRedirect(
  pathname: string,
  loginUrl: string = AUTH_LOGIN_URL,
): UnauthorizedRedirect {
  const entry = validAuthLoginUrl(loginUrl);
  if (isAuthEntryPath(pathname, entry)) {
    return { target: 'none' };
  }
  if (!entry) {
    return { target: 'history', to: UNAUTHORIZED_PATH };
  }
  // 配置了入口就整页导航，相对路径也不例外：history.replace 只在 SPA 内换路由，
  // 不会向服务器发请求，由网关托管的登录页永远到不了——那正是原先 `/login` 死链的坑。
  return { target: 'location', to: entry };
}

/** 执行决策并返回它，便于调用方与测试观察同一份结果。 */
export function applyUnauthorizedRedirect(
  pathname: string,
  sink: UnauthorizedRedirectSink,
  loginUrl: string = AUTH_LOGIN_URL,
): UnauthorizedRedirect {
  const decision = resolveUnauthorizedRedirect(pathname, loginUrl);
  if (decision.target === 'history') {
    sink.replace(decision.to);
  } else if (decision.target === 'location') {
    sink.assign(decision.to);
  }
  return decision;
}
