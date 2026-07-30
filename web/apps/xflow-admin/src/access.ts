import type { InitialState } from './app';

/**
 * access 插件契约：返回的对象即权限位集合，供 useAccess() / <Access> /
 * config/routes.ts 的 access 字段消费。
 *
 * 权限位由后端下发的 permissions 派生——前端只做展示层裁剪，服务端仍须独立鉴权。
 */
export default function access(initialState: InitialState | undefined) {
  const permissions = initialState?.currentUser?.permissions ?? [];
  const can = (permission: string) => permissions.includes(permission);

  return {
    canViewWorkflows: can('workflow:read'),
    canEditWorkflows: can('workflow:write'),
    canViewExecutions: can('execution:read'),
  };
}
