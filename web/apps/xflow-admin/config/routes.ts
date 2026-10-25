/**
 * 配置式路由。显式声明 routes 后 umi 关闭约定式路由（不再扫描 src/pages/）。
 *
 * 选配置式的原因：路由节点可直接挂 access 字段，与 access 插件联动做路由级鉴权，
 * 字段值对应 src/access.ts 派生出的权限位。
 *
 * 菜单不在此处描述——本项目不启用 layout 插件，导航由 src/layouts/index.tsx 自行实现。
 */
export const routes = [
  // `/` is a component, not a static redirect: the default landing depends on
  // whether the credential may read executions (see src/pages/index.tsx).
  // Without `canViewExecutions` it lands on /workflows instead of /overview 403.
  { path: '/', component: './index' },
  // 概览仪表盘（A-A）：GET /v1/overview 的只读投影。服务端把 overview.read
  // 映射到 execution scope（与其它执行读同权），因此复用 canViewExecutions；
  // 集群段有自己的第二道判定，缺 management.runner.list 时由页面降级说明。
  { path: '/overview', component: './overview', access: 'canViewExecutions' },
  { path: '/workflows', component: './workflows' },
  { path: '/workflows/:id', component: './workflows/detail' },
  { path: '/executions', component: './executions', access: 'canViewExecutions' },
  // 节点库（LS-01）：只读的 `GET /v1/node-types` 投影。该端点要求 workflow.read
  // （workflow 隐含），与工作流列表同源；这里不加 access——读不到时页面自己给出
  // 可理解的 403 说明，且 dev 令牌持有 workflow，页面不会被藏掉。
  { path: '/node-types', component: './node-types' },
  // Runner 与池（LS-02）：三个读取端点各要一个独立 scope
  // （management.runner.list / management.runner_pool.read /
  // management.leader.read）。canViewRunners 是「持有任一可用读 scope」的并集，
  // 让只有部分 scope 的凭据仍能进入页面，由各区块独立降级并说明缺哪一项。
  { path: '/runners', component: './runners', access: 'canViewRunners' },
  // 401 的落地页。它不是登录页：本应用没有凭证入口，鉴权由部署网关提供，
  // 页面内容与去向由 src/auth.ts 的构建期配置决定。
  { path: '/unauthorized', component: './unauthorized' },
  // 开发专用 UI 预览页（复刻 .claude/mockups 的静态稿，非产品功能）。
  // 只在显式开启时注册：默认构建不含此路由，scripts/check-production-fixtures.mjs
  // 才能在产物里确认 __XFLOW_DEV_ONLY__ 标记不存在。layout: false —— 预览页自带外壳，
  // 不继承 Admin 顶栏与内边距。
  ...(process.env.XFLOW_UI_PREVIEW === '1'
    ? [{ path: '/ui-preview', component: './ui-preview', layout: false }]
    : []),
  { path: '*', component: './404' },
];
