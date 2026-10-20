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

  // 读与写分开声明：B5 落地前服务端只有 `workflow`（读等同写），B5 之后只读
  // 凭据持 `workflow.read`。查看位同时接受两者，这样拆分前后路由都可进入；
  // 编辑位只认 `workflow`，拆分后只读凭据自然失去编辑入口。
  const canReadWorkflow = can('workflow') || can('workflow.read');

  return {
    canViewWorkflows: canReadWorkflow,
    canEditWorkflows: can('workflow'),
    canViewExecutions: can('execution'),
  };
}
