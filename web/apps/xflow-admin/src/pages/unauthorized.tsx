import { history, useIntl } from '@umijs/max';
import { Alert, Button, Result, Space } from 'antd';

import { AUTH_LOGIN_URL } from '@/auth';
import { getToken } from '@/services/token';

/**
 * 未授权页。
 *
 * 401 统一处理（见 src/app.ts）在未配置 XFLOW_AUTH_LOGIN_URL 时落到这里：本应用
 * 没有登录入口，开发期由代理注入 Bearer 令牌，生产由部署层网关鉴权。页面只陈述
 * 这个事实并给出可操作的出口——不展示、不收集令牌，也不把令牌写进 URL。
 */
export default function UnauthorizedPage() {
  const intl = useIntl();
  // 开发代理注入的令牌不会进入浏览器存储，因此「无令牌」在 dev 下是常态。
  // 只提示、不尝试读取任何构建期令牌，更不把它拼进链接。
  const showDevHint = process.env.NODE_ENV !== 'production' && getToken() === null;

  return (
    <Result
      status="warning"
      title={intl.formatMessage({ id: 'page.unauthorized.title' })}
      subTitle={intl.formatMessage({ id: 'page.unauthorized.subtitle' })}
      extra={
        <Space wrap>
          <Button type="primary" onClick={() => history.push('/workflows')}>
            {intl.formatMessage({ id: 'page.unauthorized.back' })}
          </Button>
          {AUTH_LOGIN_URL ? (
            <Button onClick={() => window.location.assign(AUTH_LOGIN_URL)}>
              {intl.formatMessage({ id: 'page.unauthorized.login' })}
            </Button>
          ) : null}
        </Space>
      }
    >
      {showDevHint ? (
        <Alert
          type="info"
          showIcon
          message={intl.formatMessage({ id: 'page.unauthorized.devHint' })}
        />
      ) : null}
    </Result>
  );
}
