import type { InitialState } from './app';

/**
 * access 插件契约：返回的对象即权限位集合，供 useAccess() / <Access> /
 * config/routes.ts 的 access 字段消费。
 *
 * 权限位由后端下发的 scopes 派生——前端只做展示层裁剪，服务端仍须独立鉴权。
 */
export default function access(initialState: InitialState | undefined) {
  const scopes = initialState?.currentUser?.scopes ?? [];
  const can = (scope: string) => scopes.includes(scope);

  return {
    // scope 的粒度是按操作划分的：一个 workflow 同时覆盖读与写，服务端没有
    // workflow:read / workflow:write 这样的区分。两个位都保留，是为了路由仍能按
    // 语义声明自己要什么；它们目前必然同真同假，要让只读令牌真的只读，得先在
    // 服务端把 scope 拆细。
    canViewWorkflows: can('workflow'),
    canEditWorkflows: can('workflow'),
    canViewExecutions: can('execution'),
  };
}
