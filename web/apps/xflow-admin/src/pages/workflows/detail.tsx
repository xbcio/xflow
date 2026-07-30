import { useIntl, useParams } from '@umijs/max';
import { Card, Descriptions } from 'antd';

/** 工作流详情（占位骨架）。 */
export default function WorkflowDetailPage() {
  const intl = useIntl();
  const { id } = useParams<{ id: string }>();

  return (
    <Card title={intl.formatMessage({ id: 'page.workflows.detail.title' })}>
      <Descriptions items={[{ key: 'id', label: 'ID', children: id }]} />
    </Card>
  );
}
