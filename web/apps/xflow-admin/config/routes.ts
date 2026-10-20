/**
 * 配置式路由。显式声明 routes 后 umi 关闭约定式路由（不再扫描 src/pages/）。
 *
 * 选配置式的原因：路由节点可直接挂 access 字段，与 access 插件联动做路由级鉴权，
 * 字段值对应 src/access.ts 派生出的权限位。
 *
 * 菜单不在此处描述——本项目不启用 layout 插件，导航由 src/layouts/index.tsx 自行实现。
 */
export const routes = [
  { path: '/', redirect: '/workflows' },
  { path: '/workflows', component: './workflows' },
  { path: '/workflows/:id', component: './workflows/detail' },
  { path: '/executions', component: './executions', access: 'canViewExecutions' },
  // 401 的落地页。它不是登录页：本应用没有凭证入口，鉴权由部署网关提供，
  // 页面内容与去向由 src/auth.ts 的构建期配置决定。
  { path: '/unauthorized', component: './unauthorized' },
  { path: '*', component: './404' },
];
