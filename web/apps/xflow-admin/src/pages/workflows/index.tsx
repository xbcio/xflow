import { useIntl } from '@umijs/max';
import { App, Button, Card, Typography } from 'antd';

/**
 * 工作流列表（占位骨架）。
 *
 * 同时充当两类验证的可视锚点：
 *   - 样式共存：h1 的 font-bold 必须解析为 700 而非 antd reset 的 500，绿底白字的
 *     tailwind 块必须真是绿底白字。这两处失效说明 tailwind.css 的 layer 方案被破坏。
 *   - antd 6 + React 19：静态方法必须真的弹出 DOM 节点（antd 5 在 React 19 下会静默失效）。
 *     这里用 App.useApp() 取上下文实例，而非直接 import message。
 */
export default function WorkflowsPage() {
  const intl = useIntl();
  const { message } = App.useApp();

  return (
    <Card>
      <Typography.Title level={2}>{intl.formatMessage({ id: 'page.workflows.title' })}</Typography.Title>
      <h1 className="font-bold text-2xl" data-testid="style-probe-heading">
        XFlow Admin
      </h1>
      <div className="mt-4 rounded bg-green-600 px-4 py-2 text-white" data-testid="style-probe-block">
        tailwind utilities active
      </div>
      <Button
        className="mt-4"
        type="primary"
        data-testid="message-probe"
        onClick={() => message.success('ok')}
      >
        {intl.formatMessage({ id: 'page.workflows.title' })}
      </Button>
    </Card>
  );
}
