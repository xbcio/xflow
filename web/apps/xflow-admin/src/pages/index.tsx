import { history, useAccess } from '@umijs/max';
import { useEffect } from 'react';

/**
 * The console's default landing (R2). `/` is not a static redirect: a
 * credential without execution read must land on the workflow list, never on
 * `/overview`'s 403. The decision comes from the same access bit the overview
 * route is gated on (`canViewExecutions`), and `replace` keeps `/` out of the
 * history so Back does not bounce through this page again.
 */
export default function IndexPage() {
  const access = useAccess();
  const canViewExecutions = access?.canViewExecutions === true;

  useEffect(() => {
    history.replace(canViewExecutions ? '/overview' : '/workflows');
  }, [canViewExecutions]);

  return (
    <p className="xflow-admin-faint" role="status">
      正在进入控制台…
    </p>
  );
}
