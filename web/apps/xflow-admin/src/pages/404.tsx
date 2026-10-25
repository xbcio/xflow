import { history } from '@umijs/max';
import { Button } from 'antd';

import { PageHeader } from '../components/PageHeader';

/**
 * AD-12: the console is Chinese-only and the 404 renders inside AdminShell
 * (the catch-all route is a child of the global layout). It uses the same
 * PageHeader skeleton as every other route (提案 C): one visible h1 in the
 * same position and size, with the way back as the page action.
 */
export default function NotFoundPage() {
  return (
    <PageHeader
      title="页面不存在"
      description="地址已经变更，或这个页面不存在。"
      actions={
        <Button type="primary" onClick={() => history.push('/workflows')}>
          返回工作流
        </Button>
      }
    />
  );
}
