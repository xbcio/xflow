import { history, useIntl } from '@umijs/max';
import { Button, Result } from 'antd';

export default function NotFoundPage() {
  const intl = useIntl();

  return (
    <Result
      status="404"
      title="404"
      subTitle={intl.formatMessage({ id: 'page.404.title' })}
      extra={
        <Button type="primary" onClick={() => history.push('/')}>
          {intl.formatMessage({ id: 'page.404.back' })}
        </Button>
      }
    />
  );
}
