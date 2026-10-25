import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const host = vi.hoisted(() => ({
  fetchRunners: vi.fn(),
  fetchRunnerPools: vi.fn(),
  fetchLeader: vi.fn(),
  drainRunner: vi.fn(),
  resumeRunner: vi.fn(),
  revokeRunnerIdentity: vi.fn(),
  scopes: [] as string[],
}));

vi.mock('../../services/management', () => ({
  fetchRunners: () => host.fetchRunners(),
  fetchRunnerPools: () => host.fetchRunnerPools(),
  fetchLeader: () => host.fetchLeader(),
  drainRunner: (id: string, options: unknown) => host.drainRunner(id, options),
  resumeRunner: (id: string, options: unknown) => host.resumeRunner(id, options),
  revokeRunnerIdentity: (id: string) => host.revokeRunnerIdentity(id),
}));

vi.mock('@umijs/max', () => ({
  useModel: () => ({
    initialState: { currentUser: { namespace: 'ns-a', scopes: host.scopes } },
  }),
}));

import RunnersPage from './index';

const ALL_SCOPES = [
  'management.runner.list',
  'management.runner_pool.read',
  'management.leader.read',
  'management.runner.drain',
  'management.runner.resume',
  'management.runner.control_global',
  'management.runner.revoke_identity',
];

const runners = [
  { runner_id: 'runner-online', enrolled: true, state: 'online', desired_state: 'active' },
  { runner_id: 'runner-draining', enrolled: true, state: 'online', desired_state: 'draining' },
  { runner_id: 'runner-never', enrolled: false, state: 'never_connected' },
];

const pools = [
  { id: 'pool-a', name: 'Alpha', paused: false },
  { id: 'pool-b', name: 'Beta', paused: true },
];

const runnerRow = (id: string) =>
  document.querySelector(`tr[data-runner="${id}"]`) as HTMLElement | null;
const poolRow = (id: string) =>
  document.querySelector(`tr[data-pool="${id}"]`) as HTMLElement | null;
const block = (id: string) => document.querySelector(`[data-block="${id}"]`) as HTMLElement | null;
/**
 * The modal root, not just the body: AntD renders the footer (the OK/cancel
 * buttons) beside the body, so button queries must start at the dialog.
 */
const controlDialog = () =>
  document.querySelector<HTMLElement>('[data-control-dialog]')?.closest<HTMLElement>('.ant-modal') ??
  null;
const revokeDialog = () =>
  document.querySelector<HTMLElement>('[data-revoke-dialog]')?.closest<HTMLElement>('.ant-modal') ??
  null;
const messageText = () => document.querySelector('.ant-message')?.textContent ?? '';
const renderPage = () =>
  render(
    <App>
      <RunnersPage />
    </App>,
  );

/** A row control found by its explicit aria-label (text alone is ambiguous). */
function actionButton(id: string, name: RegExp): HTMLElement | null {
  const row = runnerRow(id);
  if (!row) throw new Error(`row ${id} not rendered`);
  return within(row).queryByRole('button', { name });
}

const reasonField = (dialog: HTMLElement) =>
  within(dialog).getByLabelText('操作原因（必填）') as HTMLTextAreaElement;

beforeEach(() => {
  vi.resetAllMocks();
  host.scopes = [...ALL_SCOPES];
  host.fetchRunners.mockResolvedValue(runners);
  host.fetchRunnerPools.mockResolvedValue(pools);
  host.fetchLeader.mockResolvedValue(true);
  host.drainRunner.mockResolvedValue({ desired_state: 'draining', generation: 3 });
  host.resumeRunner.mockResolvedValue({ desired_state: 'active', generation: 4 });
  host.revokeRunnerIdentity.mockResolvedValue({ runner_id: 'runner-online', status: 'revoked' });
});

describe('runner and pool page', () => {
  it('renders leader, pools and the roster from their three independent reads', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    await waitFor(() => expect(poolRow('pool-a')).not.toBeNull());

    expect(host.fetchRunners).toHaveBeenCalledTimes(1);
    expect(host.fetchRunnerPools).toHaveBeenCalledTimes(1);
    expect(host.fetchLeader).toHaveBeenCalledTimes(1);

    // Liveness is the server's state, not a client-side heartbeat threshold.
    const online = runnerRow('runner-online');
    expect(online?.querySelector('[data-state]')?.getAttribute('data-state')).toBe('online');
    expect(online?.querySelector('[data-state]')?.textContent).toBe('在线');
    expect(online?.querySelector('[data-desired-state]')?.getAttribute('data-desired-state')).toBe(
      'active',
    );

    // draining stays online but stops accepting new work, and the page says so.
    const draining = runnerRow('runner-draining');
    expect(draining?.querySelector('[data-state]')?.getAttribute('data-state')).toBe('online');
    expect(
      draining?.querySelector('[data-desired-state]')?.getAttribute('data-desired-state'),
    ).toBe('draining');
    expect(draining?.querySelector('[data-desired-state]')?.textContent).toBe('排空中');

    const never = runnerRow('runner-never');
    expect(never?.querySelector('[data-state]')?.getAttribute('data-state')).toBe(
      'never_connected',
    );
    expect(never?.textContent).toContain('从未连接');

    expect(poolRow('pool-b')?.querySelector('[data-paused]')?.getAttribute('data-paused')).toBe(
      'true',
    );
    expect(poolRow('pool-b')?.textContent).toContain('已暂停');

    expect(block('leader')?.querySelector('[data-leader]')?.getAttribute('data-leader')).toBe(
      'true',
    );
    expect(block('leader')?.textContent).toContain('本节点');
    // The API never names the leader, so the page must not invent one.
    expect(block('leader')?.textContent).toContain('不返回 Leader 名称');
  });

  it('keeps the roster on screen under a progress bar while 刷新 re-reads (AL-28)', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    // Idle: no bar and no loading block.
    expect(screen.queryByRole('progressbar')).toBeNull();

    let resolve!: (value: typeof runners) => void;
    host.fetchRunners.mockReturnValueOnce(new Promise((done) => { resolve = done; }));
    fireEvent.click(screen.getByRole('button', { name: /刷\s*新/ }));

    const bar = await screen.findByRole('progressbar', { name: '正在刷新 Runner' });
    expect(bar.getAttribute('aria-busy')).toBe('true');
    expect(runnerRow('runner-online')).not.toBeNull();
    expect(block('runners')?.querySelector('[role="status"]')).toBeNull();
    expect(document.querySelector('.ant-spin-spinning')).toBeNull();

    resolve(runners);
    await waitFor(() => expect(screen.queryByRole('progressbar', { name: '正在刷新 Runner' })).toBeNull());
    expect(runnerRow('runner-online')).not.toBeNull();
  });

  it('disables the row controls and marks the block busy while 刷新 re-reads (AL-28)', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    expect(block('runners')?.getAttribute('aria-busy')).toBe('false');
    expect(actionButton('runner-online', /暂停/)?.hasAttribute('disabled')).toBe(false);

    let resolve!: (value: typeof runners) => void;
    host.fetchRunners.mockReturnValueOnce(new Promise((done) => { resolve = done; }));
    fireEvent.click(screen.getByRole('button', { name: /刷\s*新/ }));

    await waitFor(() => expect(block('runners')?.getAttribute('aria-busy')).toBe('true'));
    for (const name of [/暂停/, /吊销/]) {
      expect(actionButton('runner-online', name)?.hasAttribute('disabled')).toBe(true);
    }
    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    expect(controlDialog()).toBeNull();

    resolve(runners);
    await waitFor(() => expect(block('runners')?.getAttribute('aria-busy')).toBe('false'));
    expect(actionButton('runner-online', /暂停/)?.hasAttribute('disabled')).toBe(false);
  });

  it('draws the missing-scope banner on one line: scopes joined, no list (AL-30)', async () => {
    host.fetchRunners.mockRejectedValue({ response: { status: 403 } });
    host.fetchLeader.mockRejectedValue({ response: { status: 403 } });
    host.fetchRunnerPools.mockRejectedValue({ response: { status: 403 } });
    renderPage();

    const panel = () => document.querySelector('[data-panel="missing-scopes"]') as HTMLElement | null;
    await waitFor(() => expect(panel()).not.toBeNull());
    expect(panel()?.querySelector('ul, li')).toBeNull();
    const detail = panel()?.querySelector('.xflow-admin-mono') as HTMLElement;
    expect(detail.textContent).toBe(
      'management.leader.read、management.runner_pool.read、management.runner.list',
    );
    // A clipped line keeps the full text reachable.
    expect(detail.getAttribute('title')).toBe(detail.textContent);
  });

  it('gives the page a single h1 and no implementation-view copy', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Runner 与池');
    expect(screen.queryAllByRole('heading', { level: 2 })).toHaveLength(0);
    expect(document.body.textContent).not.toContain('前端只做展示裁剪');
  });

  it('offers drain on active rows, resume on draining rows and revoke on every row', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    expect(actionButton('runner-online', /暂停 Runner runner-online 的新任务准入/)).not.toBeNull();
    expect(actionButton('runner-online', /恢复 Runner runner-online 的新任务准入/)).toBeNull();
    expect(
      actionButton('runner-draining', /恢复 Runner runner-draining 的新任务准入/),
    ).not.toBeNull();
    expect(
      actionButton('runner-draining', /暂停 Runner runner-draining 的新任务准入/),
    ).toBeNull();
    // No control snapshot yet: the server treats it as active, so drain applies.
    expect(actionButton('runner-never', /暂停 Runner runner-never 的新任务准入/)).not.toBeNull();

    for (const id of ['runner-online', 'runner-draining', 'runner-never']) {
      expect(actionButton(id, new RegExp(`吊销 Runner ${id} 的身份`))).not.toBeNull();
    }
  });

  it('hides every write control when the credential has no write scope', async () => {
    host.scopes = ['management.runner.list'];

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    expect(within(runnerRow('runner-online') as HTMLElement).queryAllByRole('button')).toHaveLength(
      0,
    );
    expect(runnerRow('runner-online')?.textContent).toContain('—');
    expect(actionButton('runner-draining', /恢复/)).toBeNull();
    expect(actionButton('runner-never', /吊销/)).toBeNull();
  });

  it('requires control_global in addition to the drain/resume action scope', async () => {
    host.scopes = [
      'management.runner.list',
      'management.runner.drain',
      'management.runner.resume',
      'management.runner.revoke_identity',
    ];

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    expect(actionButton('runner-online', /暂停/)).toBeNull();
    expect(actionButton('runner-draining', /恢复/)).toBeNull();
    // Revoke rides its own scope and stays available.
    expect(actionButton('runner-online', /吊销/)).not.toBeNull();
  });

  it('merges every 403 into one page-level alert that names the missing scopes', async () => {
    host.fetchRunners.mockRejectedValue({ response: { status: 403 } });
    host.fetchLeader.mockRejectedValue({ response: { status: 403 } });
    host.fetchRunnerPools.mockRejectedValue({ response: { status: 403 } });

    renderPage();

    const panel = () => document.querySelector('[data-panel="missing-scopes"]') as HTMLElement | null;
    await waitFor(() => expect(panel()).not.toBeNull());
    expect(panel()?.textContent).toContain('当前凭据缺少以下权限');
    for (const scope of [
      'management.runner.list',
      'management.leader.read',
      'management.runner_pool.read',
    ]) {
      expect(panel()?.textContent).toContain(scope);
    }
    // One merged alert, not one copy per block.
    expect(document.querySelectorAll('[data-panel="missing-scopes"] .ant-alert')).toHaveLength(1);
    // AL-03: the top bar owns the only refresh; the alert names the scopes and
    // does not repeat a second, identical action.
    expect(panel()?.querySelector('button, .ant-btn')).toBeNull();
    expect(screen.getAllByRole('button', { name: /刷\s*新/ })).toHaveLength(1);
    expect(block('runners')?.querySelector('.ant-alert')).toBeNull();
    expect(block('leader')?.querySelector('.ant-alert')).toBeNull();
    // AD-14: implementation-view sentences are gone from the page copy.
    expect(document.body.textContent).not.toContain('前端只做展示裁剪');
  });

  it('does not call a read the credential has no scope for', async () => {
    host.scopes = [];
    renderPage();

    // The missing scopes still surface as the 403 the server would have sent…
    await waitFor(() =>
      expect(document.querySelector('[data-panel="missing-scopes"]')).not.toBeNull(),
    );
    expect(document.querySelector('[data-panel="missing-scopes"]')?.textContent).toContain(
      'management.runner.list',
    );
    // …but no forbidden request is sent for them.
    expect(host.fetchRunners).not.toHaveBeenCalled();
    expect(host.fetchRunnerPools).not.toHaveBeenCalled();
    expect(host.fetchLeader).not.toHaveBeenCalled();
  });

  it('degrades only the failed block with an alert for a non-403 failure', async () => {
    host.fetchRunnerPools.mockRejectedValue({ response: { status: 502 } });

    renderPage();

    await waitFor(() => expect(block('pools')?.querySelector('.ant-alert')).not.toBeNull());
    expect(block('pools')?.textContent).toContain('加载失败');
    expect(document.querySelector('[data-panel="missing-scopes"]')).toBeNull();

    // The other two blocks are untouched by the pool failure.
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    expect(block('leader')?.querySelector('[data-leader="true"]')).not.toBeNull();
    expect(block('runners')?.querySelector('.ant-alert')).toBeNull();
  });

  it('explains a 404 as a deployment without --management without blanking the page', async () => {
    host.fetchRunnerPools.mockRejectedValue({ response: { status: 404 } });

    renderPage();

    await waitFor(() => expect(block('pools')?.querySelector('.ant-alert')).not.toBeNull());
    expect(block('pools')?.textContent).toContain('管理模块未启用');
    expect(block('pools')?.textContent).toContain('--management');

    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    expect(block('leader')?.textContent).toContain('本节点');
  });

  it('keeps the 501 runner-listing failure distinct from an empty roster', async () => {
    host.fetchRunners.mockRejectedValue({ response: { status: 501 } });

    renderPage();

    expect(await screen.findByText('Runner 目录不支持枚举（501）')).toBeTruthy();
    expect(block('runners')?.textContent).toContain('runner_listing_unsupported');
  });

  it('refreshes all three reads on demand', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    // AntD's default autoInsertSpace splits a two-character label ("刷 新")
    // when the page renders outside the shell's ConfigProvider.
    fireEvent.click(screen.getByRole('button', { name: /刷\s*新/ }));

    await waitFor(() => {
      expect(host.fetchRunners).toHaveBeenCalledTimes(2);
      expect(host.fetchRunnerPools).toHaveBeenCalledTimes(2);
      expect(host.fetchLeader).toHaveBeenCalledTimes(2);
    });
  });

  it('reports a non-leader replica and an empty roster', async () => {
    host.fetchLeader.mockResolvedValue(false);
    host.fetchRunners.mockResolvedValue([]);

    renderPage();

    await waitFor(() =>
      expect(block('leader')?.querySelector('[data-leader="false"]')).not.toBeNull(),
    );
    expect(block('leader')?.textContent).toContain('非本节点');
    expect(
      await screen.findByText('暂无 Runner。Runner 注册后会出现在这里。'),
    ).toBeTruthy();
  });
});

describe('drain / resume confirmation', () => {
  it('requires a non-empty reason before draining', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog();
    expect(dialog).not.toBeNull();
    expect(dialog?.textContent).toContain('runner-online');
    expect(dialog?.textContent).toContain('接单中');
    expect(dialog?.textContent).toContain('不会终止 Runner 进程');

    fireEvent.click(within(dialog as HTMLElement).getByRole('button', { name: /确认暂停/ }));

    await waitFor(() =>
      expect(
        (dialog as HTMLElement).querySelector('#xflow-runner-control-reason-error')?.textContent,
      ).toContain('请填写操作原因'),
    );
    expect(reasonField(dialog as HTMLElement).getAttribute('aria-invalid')).toBe('true');
    expect(host.drainRunner).not.toHaveBeenCalled();
  });

  it('rejects a reason over 512 UTF-8 bytes in place', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;
    // 171 CJK characters are 513 bytes; the limit is bytes, not characters.
    fireEvent.change(reasonField(dialog), { target: { value: '维'.repeat(171) } });

    await waitFor(() => expect(dialog.textContent).toContain('超过 512 字节'));
    expect(dialog.textContent).toContain('513/512 字节');

    fireEvent.click(within(dialog).getByRole('button', { name: /确认暂停/ }));
    expect(host.drainRunner).not.toHaveBeenCalled();
  });

  it('reuses the same X-Request-Id on retry and rotates it after the body changes', async () => {
    host.drainRunner
      .mockRejectedValueOnce({
        response: { status: 500, data: { code: 'internal_error', message: 'temporarily unavailable' } },
      })
      .mockRejectedValueOnce({
        response: { status: 500, data: { code: 'internal_error', message: 'temporarily unavailable' } },
      });

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;

    fireEvent.change(reasonField(dialog), { target: { value: '  维护  ' } });
    const ok = within(dialog).getByRole('button', { name: /确认暂停/ });

    fireEvent.click(ok);
    await waitFor(() => expect(host.drainRunner).toHaveBeenCalledTimes(1));
    expect(host.drainRunner.mock.calls[0][1]).toMatchObject({ reason: '维护' });
    const firstId = host.drainRunner.mock.calls[0][1].requestId as string;
    expect(firstId).toMatch(/^[A-Za-z0-9._-]{1,128}$/);

    // Retrying the unchanged body reuses the id (server replays its receipt).
    await waitFor(() => expect(dialog.textContent).toContain('操作失败'));
    fireEvent.click(ok);
    await waitFor(() => expect(host.drainRunner).toHaveBeenCalledTimes(2));
    expect(host.drainRunner.mock.calls[1][1].requestId).toBe(firstId);

    // Editing the reason changes the body, so a new key is generated.
    await waitFor(() => expect(dialog.textContent).toContain('操作失败'));
    fireEvent.change(reasonField(dialog), { target: { value: '维护-2' } });
    fireEvent.click(ok);
    await waitFor(() => expect(host.drainRunner).toHaveBeenCalledTimes(3));
    expect(host.drainRunner.mock.calls[2][1].reason).toBe('维护-2');
    expect(host.drainRunner.mock.calls[2][1].requestId).not.toBe(firstId);
  });

  it('refreshes the roster and reports success after a drain', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;

    fireEvent.change(reasonField(dialog), { target: { value: '节点维护' } });
    fireEvent.click(within(dialog).getByRole('button', { name: /确认暂停/ }));

    await waitFor(() => expect(host.fetchRunners).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(messageText()).toContain('已暂停 runner-online 的新任务准入'));
    expect(controlDialog()).toBeNull();
  });

  it('resumes a draining runner through the resume endpoint', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-draining')).not.toBeNull());
    fireEvent.click(actionButton('runner-draining', /恢复/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;

    const alert = within(dialog).getByRole('alert');
    expect(alert.textContent).toContain('不会同步启动 Runner 进程');

    fireEvent.change(reasonField(dialog), { target: { value: '维护结束' } });
    fireEvent.click(within(dialog).getByRole('button', { name: /确认恢复/ }));

    await waitFor(() =>
      expect(host.resumeRunner).toHaveBeenCalledWith(
        'runner-draining',
        expect.objectContaining({ reason: '维护结束' }),
      ),
    );
    await waitFor(() => expect(messageText()).toContain('已恢复 runner-draining 的新任务准入'));
  });

  it('names the missing base and global scopes on a 403', async () => {
    host.drainRunner.mockRejectedValue({
      response: { status: 403, data: { code: 'forbidden', message: 'forbidden' } },
    });

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;
    fireEvent.change(reasonField(dialog), { target: { value: '维护' } });
    fireEvent.click(within(dialog).getByRole('button', { name: /确认暂停/ }));

    await waitFor(() => expect(dialog.textContent).toContain('management.runner.drain'));
    expect(dialog.textContent).toContain('management.runner.control_global');
    // The server message is a disclosure, not the title.
    expect(dialog.textContent).toContain('服务端详情');
  });

  it('explains a 409 as a reused X-Request-Id', async () => {
    host.drainRunner.mockRejectedValue({
      response: {
        status: 409,
        data: { code: 'idempotency_key_reused', message: 'X-Request-Id was reused with a different request' },
      },
    });

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
    const dialog = controlDialog() as HTMLElement;
    fireEvent.change(reasonField(dialog), { target: { value: '维护' } });
    fireEvent.click(within(dialog).getByRole('button', { name: /确认暂停/ }));

    await waitFor(() => expect(dialog.textContent).toContain('同一 X-Request-Id'));
    expect(dialog.textContent).toContain('重试会复用同一 id');
  });

  it('maps 404, 501 and 429 to distinct operator text', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());

    const failWith = async (error: unknown, expected: string) => {
      host.drainRunner.mockRejectedValueOnce(error);
      fireEvent.click(actionButton('runner-online', /暂停/) as HTMLElement);
      const dialog = controlDialog() as HTMLElement;
      fireEvent.change(reasonField(dialog), { target: { value: '维护' } });
      fireEvent.click(within(dialog).getByRole('button', { name: /确认暂停/ }));

      await waitFor(() => expect(controlDialog()?.textContent).toContain(expected));
      fireEvent.click(within(dialog).getByRole('button', { name: /取\s*消/ }));
      await waitFor(() => expect(controlDialog()).toBeNull());
    };

    await failWith(
      { response: { status: 404, data: { code: 'runner_not_found', message: 'runner not found' } } },
      '未知 Runner',
    );
    await failWith(
      {
        response: {
          status: 501,
          data: { code: 'runner_control_unsupported', message: 'runner control is not supported' },
        },
      },
      '不支持原子暂停/恢复',
    );
    await failWith(
      { response: { status: 429, data: { code: 'rate_limited', message: 'slow down' } } },
      '稍后重试',
    );
  });
});

describe('revoke identity confirmation', () => {
  it('requires the checkbox before revoking, then refreshes the roster', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /吊销/) as HTMLElement);

    const dialog = revokeDialog() as HTMLElement;
    expect(dialog.textContent).toContain('平台级破坏性操作');
    expect(dialog.textContent).toContain('下一次认证调用时生效');
    expect(dialog.textContent).toContain('与池 token 撤销相互独立');

    const ok = within(dialog).getByRole('button', { name: /确认吊销/ }) as HTMLButtonElement;
    expect(ok.disabled).toBe(true);

    fireEvent.click(within(dialog).getByRole('checkbox'));
    await waitFor(() => expect(ok.disabled).toBe(false));

    fireEvent.click(ok);
    await waitFor(() => expect(host.revokeRunnerIdentity).toHaveBeenCalledWith('runner-online'));
    await waitFor(() => expect(host.fetchRunners).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(messageText()).toContain('已吊销 runner-online 的身份'));
    expect(revokeDialog()).toBeNull();
  });

  it('offers revoke on a never-connected row too', async () => {
    renderPage();
    await waitFor(() => expect(runnerRow('runner-never')).not.toBeNull());

    fireEvent.click(actionButton('runner-never', /吊销/) as HTMLElement);
    expect(revokeDialog()?.textContent).toContain('runner-never');
  });

  it('names the revoke scope on a 403 and the identity-store gap on a 501', async () => {
    host.revokeRunnerIdentity.mockRejectedValueOnce({
      response: { status: 403, data: { code: 'forbidden', message: 'forbidden' } },
    });

    renderPage();
    await waitFor(() => expect(runnerRow('runner-online')).not.toBeNull());
    fireEvent.click(actionButton('runner-online', /吊销/) as HTMLElement);
    let dialog = revokeDialog() as HTMLElement;
    fireEvent.click(within(dialog).getByRole('checkbox'));
    fireEvent.click(within(dialog).getByRole('button', { name: /确认吊销/ }));

    await waitFor(() =>
      expect(revokeDialog()?.textContent).toContain('management.runner.revoke_identity'),
    );
    fireEvent.click(within(dialog).getByRole('button', { name: /取\s*消/ }));
    await waitFor(() => expect(revokeDialog()).toBeNull());

    host.revokeRunnerIdentity.mockRejectedValueOnce({
      response: {
        status: 501,
        data: { code: 'not_implemented', message: 'issued identity store is not configured' },
      },
    });

    fireEvent.click(actionButton('runner-online', /吊销/) as HTMLElement);
    dialog = revokeDialog() as HTMLElement;
    fireEvent.click(within(dialog).getByRole('checkbox'));
    fireEvent.click(within(dialog).getByRole('button', { name: /确认吊销/ }));

    await waitFor(() => expect(revokeDialog()?.textContent).toContain('身份存储'));
  });
});
