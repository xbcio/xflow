export default {
  'menu.workflows': '工作流',
  'menu.executions': '执行记录',

  'common.loading': '加载中',
  'common.empty': '暂无数据',

  'page.workflows.title': '工作流',
  'page.workflows.detail.title': '工作流详情',
  'page.executions.title': '执行记录',
  'page.404.title': '页面不存在',
  'page.404.back': '返回首页',

  'page.unauthorized.title': '需要登录',
  'page.unauthorized.subtitle':
    '鉴权由部署网关提供。请通过网关重新登录，或在部署时配置 XFLOW_AUTH_LOGIN_URL 指向网关登录入口。',
  'page.unauthorized.back': '返回工作流列表',
  'page.unauthorized.login': '前往登录',
  'page.unauthorized.devHint':
    '开发期可由开发代理注入令牌：设置 XFLOW_DEV_TOKEN 或 XFLOW_DEV_TOKEN_FILE 后重启 dev server。令牌只留在代理侧，不进入浏览器、URL 或日志。',
};
