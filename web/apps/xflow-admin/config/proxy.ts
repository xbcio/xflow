import { readFileSync } from 'node:fs';

/**
 * 开发代理。仅开发期生效，不进产物——生产环境由部署层反代。
 *
 * target 默认指向本地 apiserver；开发时可用 XFLOW_API_TARGET 覆盖端口。
 * 契约见 api/openapi/xflow-v1.yaml，全部端点在 /v1 前缀下。
 */

/**
 * 后端以 --auth-tokens-file 启动后 /v1 需要 Authorization: Bearer。前端没有
 * 登录入口（config/routes.ts 里没有 /login），所以开发期由代理在服务端注入：
 * 令牌不进前端产物、不进 sessionStorage，也不出现在进程命令行里。
 *
 * XFLOW_DEV_TOKEN 直接给令牌；XFLOW_DEV_TOKEN_FILE 指向 --auth-tokens-file 用的
 * 那份 JSON，取第一条映射的 token。两者都未设置则不注入，代理行为与之前一致。
 */
function resolveDevToken(): string | undefined {
  const inline = process.env.XFLOW_DEV_TOKEN;
  if (inline) {
    return inline;
  }
  const file = process.env.XFLOW_DEV_TOKEN_FILE;
  if (!file) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(readFileSync(file, 'utf8'));
    const first = Array.isArray(parsed) ? (parsed[0] as { token?: unknown } | undefined) : undefined;
    if (typeof first?.token === 'string') {
      return first.token;
    }
    console.warn(`xflow-admin: ${file} has no token in its first mapping; /v1 requests will be unauthenticated`);
  } catch (err) {
    console.warn(`xflow-admin: cannot read XFLOW_DEV_TOKEN_FILE ${file}: ${(err as Error).message}`);
  }
  return undefined;
}

const devToken = resolveDevToken();

export const proxy = {
  '/v1': {
    target: process.env.XFLOW_API_TARGET ?? 'http://127.0.0.1:8080',
    changeOrigin: true,
    ...(devToken ? { headers: { Authorization: `Bearer ${devToken}` } } : {}),
  },
};
