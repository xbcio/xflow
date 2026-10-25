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

  // /v1/management/* 的读取分散在三个独立 scope 上：fleet 列表要
  // management.runner.list、池列表要 management.runner_pool.read、Leader 要
  // management.leader.read。页面把它们当三块独立降级，所以路由位取并集：
  // 任一读 scope 都允许进入，缺的那一块在页内说明；一个都没有时导航与路由都
  // 裁剪掉。management.runner.read 只够按 id 查单个 Runner，不足以枚举，故不计入。
  const canReadRunners =
    can('management.runner.list') ||
    can('management.runner_pool.read') ||
    can('management.leader.read');

  return {
    canViewWorkflows: canReadWorkflow,
    canEditWorkflows: can('workflow'),
    canViewExecutions: can('execution'),
    canViewRunners: canReadRunners,
  };
}
