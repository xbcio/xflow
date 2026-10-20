/**
 * 执行记录 / 运行列表（§6.6 / §6.9）。页面本身在 `ExecutionsCenter.tsx`，路由文件只做导出：
 * 路由级鉴权由 config/routes.ts 的 access: 'canViewExecutions' 负责，页面不重复判断。
 */
export { ExecutionsCenter as default } from './ExecutionsCenter';
