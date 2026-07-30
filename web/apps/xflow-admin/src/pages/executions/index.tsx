import { useIntl } from '@umijs/max';
import { Card, Empty } from 'antd';

/** 执行记录（占位骨架）。路由级鉴权由 config/routes.ts 的 access: 'canViewExecutions' 负责。 */
export default function ExecutionsPage() {
  const intl = useIntl();

  return (
    <Card title={intl.formatMessage({ id: 'page.executions.title' })}>
      <Empty description={intl.formatMessage({ id: 'common.empty' })} />
    </Card>
  );
}
