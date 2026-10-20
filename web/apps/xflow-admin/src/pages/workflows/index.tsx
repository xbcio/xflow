/**
 * 工作流中心（§6.6）。页面本身在 `WorkflowCenter.tsx`，路由文件只做导出：
 * 本页不重复判断权限——鉴权由服务端按调用者的 scope 独立执行；`config/routes.ts`
 * 目前也没有给这两条工作流路由声明 `access`（`access.ts` 的位见该文件说明）。
 */
export { WorkflowCenter as default } from './WorkflowCenter';
