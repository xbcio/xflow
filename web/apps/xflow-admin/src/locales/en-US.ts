export default {
  'menu.workflows': 'Workflows',
  'menu.executions': 'Executions',

  'common.loading': 'Loading',
  'common.empty': 'No data',

  'page.workflows.title': 'Workflows',
  'page.workflows.detail.title': 'Workflow detail',
  'page.executions.title': 'Executions',
  'page.404.title': 'Page not found',
  'page.404.back': 'Back to home',

  'page.unauthorized.title': 'Authentication required',
  'page.unauthorized.subtitle':
    'Authentication is provided by the deployment gateway. Sign in through the gateway again, or set XFLOW_AUTH_LOGIN_URL to the gateway login entry at deploy time.',
  'page.unauthorized.back': 'Back to workflows',
  'page.unauthorized.login': 'Go to sign-in',
  'page.unauthorized.devHint':
    'In development the proxy can inject a token: set XFLOW_DEV_TOKEN or XFLOW_DEV_TOKEN_FILE and restart the dev server. The token stays proxy-side and never enters the browser, URLs, or logs.',
};
