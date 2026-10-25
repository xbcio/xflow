import { GridIcon, ServerIcon } from '@xflow/icons';
import { useModel } from '@umijs/max';
import {
  Alert,
  App,
  Button,
  Checkbox,
  Input,
  Modal,
  Spin,
  Table,
  type TableColumnsType,
} from 'antd';
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';

import type { InitialState } from '../../app';
import { PageHeader } from '../../components/PageHeader';
import { TableProgressBar } from '../../components/TableProgressBar';
import {
  drainRunner,
  fetchLeader,
  fetchRunnerPools,
  fetchRunners,
  resumeRunner,
  revokeRunnerIdentity,
  type RunnerListItem,
  type RunnerPool,
} from '../../services/management';
import { PanelTitle } from '../workflows/parts';

interface BlockError {
  status?: number;
  title: string;
  detail: string;
  /** The read scope this block's endpoint requires (AD-14). */
  scope: string;
}

type Resource<T> =
  | { state: 'loading' }
  | { state: 'error'; error: BlockError }
  /** `refreshing` keeps the rows on screen while a later read is in flight (AL-28). */
  | { state: 'ready'; data: T; refreshing?: boolean };

/** umi request rejections carry `response.status`; XFlowApiError carries `status`. */
function statusOf(reason: unknown): number | undefined {
  const direct = (reason as { status?: unknown } | null)?.status;
  if (typeof direct === 'number') return direct;
  const response = (reason as { response?: { status?: unknown } } | null)?.response?.status;
  return typeof response === 'number' ? response : undefined;
}

/**
 * One block's failure, named by the scope its endpoint requires. Read failures
 * are never fatal to the page: the other two blocks keep their own state.
 */
function describeManagementError(reason: unknown, scope: string): BlockError {
  const status = statusOf(reason);
  if (status === 403) {
    return {
      status,
      scope,
      title: '缺少权限（403）',
      detail: `请为当前凭据授予 ${scope} scope。`,
    };
  }
  if (status === 404) {
    return {
      status,
      scope,
      title: '管理模块未启用（404）',
      detail:
        '服务端未启用 --management（或该端点未挂载）。本页其余区块独立加载，不受这一块影响。',
    };
  }
  if (status === 501) {
    return {
      status,
      scope,
      title: 'Runner 目录不支持枚举（501）',
      detail:
        '服务端返回 runner_listing_unsupported：当前 runner 目录实现无法枚举列表，空列表与「无法回答」保持区别。',
    };
  }
  return {
    status,
    scope,
    title: '加载失败',
    detail: reason instanceof Error && reason.message ? reason.message : '请稍后重试。',
  };
}

/** The two desired-state mutations; revoke is its own destructive flow. */
type RunnerControlAction = 'drain' | 'resume';

interface ControlTarget {
  action: RunnerControlAction;
  runner: RunnerListItem;
}

/** One failed write, rendered inside its dialog instead of as a bare toast. */
interface ActionFailure {
  status?: number;
  code?: string;
  title: string;
  detail?: string;
}

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === 'string' && value !== '') return value;
  }
  return undefined;
}

/**
 * umi's request rejects with an axios error: `response.status` is the HTTP
 * status and `response.data` is the parsed failure envelope. A XFlowApiError
 * (or a test double) can carry `status`/`code` directly. The server envelope
 * message is preferred for the disclosure; axios's own "Request failed with
 * status code N" is only used when no response arrived at all.
 */
function parseApiError(reason: unknown): {
  status?: number;
  code?: string;
  serverMessage?: string;
} {
  const record = reason as
    | {
        code?: unknown;
        message?: unknown;
        info?: { code?: unknown; message?: unknown };
        response?: { status?: unknown; data?: { code?: unknown; message?: unknown } };
      }
    | null;
  const status = statusOf(reason);
  return {
    status,
    code: firstString(record?.code, record?.response?.data?.code, record?.info?.code),
    serverMessage: firstString(
      record?.response?.data?.message,
      record?.info?.message,
      status === undefined && reason instanceof Error ? reason.message : undefined,
    ),
  };
}

/**
 * Operator-facing failure text. Codes come first because a 403/404/409 can
 * mean several things: a 403 on drain is a missing base OR global scope, a 409
 * is specifically an X-Request-Id reused with a different body. The server
 * message is kept as a disclosure, never merged into the title.
 */
function runnerActionFailure(reason: unknown, action: RunnerControlAction | 'revoke'): ActionFailure {
  const { status, code, serverMessage } = parseApiError(reason);
  const failure = (title: string): ActionFailure => ({ status, code, title, detail: serverMessage });

  if (status === 403) {
    return failure(
      action === 'revoke'
        ? '当前凭据缺少 management.runner.revoke_identity 权限（403）；吊销其他 namespace 或归属未知的历史身份还需要 management.runner.revoke_identity_global。'
        : `当前凭据缺少 ${
            action === 'drain' ? 'management.runner.drain' : 'management.runner.resume'
          } 或 management.runner.control_global 权限（403）。`,
    );
  }

  switch (code) {
    case 'request_id_required':
      return failure('请求缺少幂等 X-Request-Id（400），请关闭对话框后重新发起。');
    case 'idempotency_key_reused':
      return failure(
        '同一 X-Request-Id 已用于另一份请求内容（409）。未修改原因的重试会复用同一 id；修改原因后会自动换成新 id。',
      );
    case 'runner_not_found':
      return failure('未知 Runner，或该身份不在当前凭据可见范围内（404）。');
    case 'runner_control_unsupported':
      return failure('当前部署的 Runner 目录不支持原子暂停/恢复（501）。');
    case 'not_implemented':
      return failure('服务端未配置已签发身份存储，无法吊销身份（501）。');
    default:
      break;
  }
  if (status === 404) return failure('未知 Runner（404）。');
  if (status === 409) {
    return failure('请求 id 冲突（409）：重试会复用同一 X-Request-Id，修改原因后才会更换。');
  }
  if (status === 501) {
    return failure(action === 'revoke' ? '服务端不支持身份吊销（501）。' : '服务端不支持 Runner 控制（501）。');
  }
  if (status === 429) return failure('请求过于频繁，请稍后重试（429）。');
  return failure('操作失败，请稍后重试。');
}

/** Modal portal target: the shell root carries the `--xflow-*` tokens. */
function portalContainer(): HTMLElement {
  return document.querySelector<HTMLElement>('.xflow-admin') ?? document.body;
}

/** Server measures the reason after trimming, in UTF-8 bytes (OpenAPI 512). */
function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).length;
}

/** Server-side failure message behind a disclosure; the request body is never echoed. */
function failureDetail(failure: ActionFailure): ReactNode {
  if (!failure.detail) return undefined;
  return (
    <details>
      <summary>服务端详情</summary>
      <pre className="m-0 max-h-40 overflow-auto whitespace-pre-wrap break-all text-xs">
        {failure.detail}
      </pre>
    </details>
  );
}

/**
 * One management read. `reload` is the page-level refresh counter; a new value
 * refetches this block without touching the other two. The cancelled flag keeps
 * a slow response from a previous attempt from overwriting a newer one.
 *
 * `granted` is the scope check for this endpoint. Without the scope the block
 * settles as the 403 the server would return, without sending the request: the
 * page's permission model is unchanged, but a credential that cannot read the
 * block no longer paints forbidden requests into the browser console.
 */
function useResource<T>(
  load: () => Promise<T>,
  scope: string,
  granted: boolean,
  reload: number,
): Resource<T> {
  const [resource, setResource] = useState<Resource<T>>({ state: 'loading' });

  useEffect(() => {
    let cancelled = false;
    if (!granted) {
      setResource({ state: 'error', error: describeManagementError({ status: 403 }, scope) });
      return undefined;
    }
    // AL-28: a refresh keeps the rows already read; only the first read has
    // nothing to show and draws the loading block.
    setResource((current) =>
      current.state === 'ready' ? { ...current, refreshing: true } : { state: 'loading' },
    );
    load().then(
      (data) => {
        if (!cancelled) setResource({ state: 'ready', data });
      },
      (reason: unknown) => {
        if (!cancelled) {
          setResource({ state: 'error', error: describeManagementError(reason, scope) });
        }
      },
    );
    return () => {
      cancelled = true;
    };
  }, [load, scope, granted, reload]);

  return resource;
}

function BlockLoading() {
  return (
    <div className="flex items-center gap-2 p-3" role="status">
      <Spin size="small" />
      <span className="xflow-admin-note">加载中…</span>
    </div>
  );
}

function BlockAlert({ error }: { error: BlockError }) {
  // AD-14: every 403 is merged into one page-level alert naming the scopes;
  // a block must not repeat the same alert. It still says why it is empty.
  if (error.status === 403) {
    return (
      <p className="xflow-admin-banner xflow-admin-note">当前凭据无权读取该区块。</p>
    );
  }
  return (
    <div className="xflow-admin-banner">
      <Alert
        type={error.status === 404 ? 'warning' : 'error'}
        showIcon
        title={error.title}
        description={error.detail}
      />
    </div>
  );
}

const RUNNER_STATE: Record<
  RunnerListItem['state'],
  { label: string; tone?: 'success' | 'warning' }
> = {
  online: { label: '在线', tone: 'success' },
  offline: { label: '离线', tone: 'warning' },
  never_connected: { label: '从未连接' },
};

const DESIRED_STATE: Record<'active' | 'draining', { label: string; tone: string }> = {
  active: { label: '接单中', tone: 'info' },
  draining: { label: '排空中', tone: 'suspend' },
};

/** The dialog states the current snapshot in words; absent means no control snapshot yet. */
function desiredStateLabel(runner: RunnerListItem): string {
  return runner.desired_state ? DESIRED_STATE[runner.desired_state].label : '未设置（按接单中处理）';
}

const runnerColumns: TableColumnsType<RunnerListItem> = [
  {
    title: 'Runner',
    key: 'runner',
    width: 240,
    render: (_value: unknown, runner) => (
      <span className="xflow-admin-name">
        <strong title={runner.runner_id}>{runner.runner_id}</strong>
        <small title={runner.enrolled ? 'enrollment 签发的凭据' : '静态凭据'}>
          {runner.enrolled ? 'enrollment' : 'static'}
        </small>
      </span>
    ),
  },
  {
    title: '状态',
    key: 'state',
    width: 120,
    render: (_value: unknown, runner) => {
      const meta = RUNNER_STATE[runner.state];
      return (
        <span className="xflow-admin-chip" data-tone={meta.tone} data-state={runner.state}>
          {meta.label}
        </span>
      );
    },
  },
  {
    title: '期望状态',
    key: 'desired',
    width: 130,
    render: (_value: unknown, runner) => {
      const desired = runner.desired_state;
      if (!desired) return <span className="xflow-admin-faint">—</span>;
      const meta = DESIRED_STATE[desired];
      return (
        <span className="xflow-admin-chip" data-tone={meta.tone} data-desired-state={desired}>
          {meta.label}
        </span>
      );
    },
  },
];

const poolColumns: TableColumnsType<RunnerPool> = [
  {
    title: '池',
    key: 'pool',
    render: (_value: unknown, pool) => (
      <span className="xflow-admin-name">
        <strong title={pool.name}>{pool.name}</strong>
        <small title={pool.id}>{pool.id}</small>
      </span>
    ),
  },
  {
    title: '状态',
    key: 'paused',
    width: 110,
    render: (_value: unknown, pool) =>
      pool.paused === undefined ? (
        <span className="xflow-admin-faint">—</span>
      ) : (
        <span
          className="xflow-admin-chip"
          data-tone={pool.paused ? 'warning' : 'success'}
          data-paused={pool.paused ? 'true' : 'false'}
        >
          {pool.paused ? '已暂停' : '启用中'}
        </span>
      ),
  },
];

function LeaderCard({ resource }: { resource: Resource<boolean> }) {
  return (
    <div className="xflow-admin-card min-w-0" data-block="leader">
      <PanelTitle icon={<ServerIcon />} title="Leader" />
      {resource.state === 'loading' ? (
        <BlockLoading />
      ) : resource.state === 'error' ? (
        <BlockAlert error={resource.error} />
      ) : (
        <div className="flex flex-wrap items-center gap-3 p-3">
          <span
            className="xflow-admin-chip"
            data-tone={resource.data ? 'success' : undefined}
            data-leader={resource.data ? 'true' : 'false'}
          >
            {resource.data ? '本节点' : '非本节点'}
          </span>
          <span className="xflow-admin-note">
            接口只报告本副本是否持有 Leader，不返回 Leader 名称。
          </span>
        </div>
      )}
    </div>
  );
}

function PoolsCard({ resource }: { resource: Resource<RunnerPool[]> }) {
  return (
    <div
      className="xflow-admin-card xflow-admin-tablecard min-w-0"
      data-block="pools"
      aria-busy={resource.state === 'ready' && resource.refreshing === true}
    >
      <PanelTitle
        icon={<GridIcon />}
        title="Runner 池"
        count={resource.state === 'ready' ? resource.data.length : undefined}
      />
      {resource.state === 'loading' ? (
        <BlockLoading />
      ) : resource.state === 'error' ? (
        <BlockAlert error={resource.error} />
      ) : (
        <>
          <TableProgressBar loading={resource.refreshing === true} label="正在刷新 Runner 池" />
          <Table
            className="xflow-admin-table"
            size="small"
            rowKey="id"
            tableLayout="fixed"
            scroll={{ x: 360 }}
            pagination={false}
            locale={{ emptyText: '暂无可见的 Runner 池。' }}
            dataSource={resource.data}
            onRow={(pool) => ({ 'data-pool': pool.id }) as Record<string, string>}
            columns={poolColumns}
          />
        </>
      )}
    </div>
  );
}

interface RunnersCardProps {
  resource: Resource<RunnerListItem[]>;
  canDrain: boolean;
  canResume: boolean;
  canRevoke: boolean;
  onControl: (runner: RunnerListItem, action: RunnerControlAction) => void;
  onRevoke: (runner: RunnerListItem) => void;
}

/**
 * Action availability is a pure function of the row's desired state:
 * `draining` offers resume, everything else (active, or no control snapshot
 * yet — the server treats absent as active) offers drain. Revoke applies to
 * any row. Each button is additionally cropped by the caller's scopes; the
 * server independently authorizes every call.
 */
function RunnersCard({
  resource,
  canDrain,
  canResume,
  canRevoke,
  onControl,
  onRevoke,
}: RunnersCardProps) {
  // A refetch in flight replaces the roster, so no control starts from it.
  const refreshing = resource.state === 'ready' && resource.refreshing === true;
  const columns = useMemo<TableColumnsType<RunnerListItem>>(() => {
    const actions: TableColumnsType<RunnerListItem>[number] = {
      title: '操作',
      key: 'actions',
      width: 210,
      render: (_value: unknown, runner) => {
        const buttons: ReactNode[] = [];
        if (runner.desired_state === 'draining') {
          if (canResume) {
            buttons.push(
              <Button
                key="resume"
                size="small"
                className="xflow-admin-op"
                aria-label={`恢复 Runner ${runner.runner_id} 的新任务准入`}
                disabled={refreshing}
                onClick={() => onControl(runner, 'resume')}
              >
                恢复
              </Button>,
            );
          }
        } else if (canDrain) {
          buttons.push(
            <Button
              key="drain"
              size="small"
              className="xflow-admin-op"
              aria-label={`暂停 Runner ${runner.runner_id} 的新任务准入`}
              disabled={refreshing}
              onClick={() => onControl(runner, 'drain')}
            >
              暂停
            </Button>,
          );
        }
        if (canRevoke) {
          buttons.push(
            <Button
              key="revoke"
              danger
              size="small"
              className="xflow-admin-op"
              aria-label={`吊销 Runner ${runner.runner_id} 的身份`}
              disabled={refreshing}
              onClick={() => onRevoke(runner)}
            >
              吊销身份
            </Button>,
          );
        }
        if (buttons.length === 0) return <span className="xflow-admin-faint">—</span>;
        return <span className="xflow-admin-runner-actions">{buttons}</span>;
      },
    };
    return [...runnerColumns, actions];
  }, [canDrain, canResume, canRevoke, onControl, onRevoke, refreshing]);

  return (
    <div
      className="xflow-admin-card xflow-admin-tablecard min-w-0"
      data-block="runners"
      aria-busy={refreshing}
    >
      <PanelTitle
        icon={<ServerIcon />}
        title="Runner"
        count={resource.state === 'ready' ? resource.data.length : undefined}
      />
      {resource.state === 'loading' ? (
        <BlockLoading />
      ) : resource.state === 'error' ? (
        <BlockAlert error={resource.error} />
      ) : (
        <>
          <TableProgressBar loading={resource.refreshing === true} label="正在刷新 Runner" />
          <Table
            className="xflow-admin-table"
            size="small"
            rowKey="runner_id"
            tableLayout="fixed"
            scroll={{ x: 720 }}
            pagination={false}
            locale={{ emptyText: '暂无 Runner。Runner 注册后会出现在这里。' }}
            dataSource={resource.data}
            onRow={(runner) => ({ 'data-runner': runner.runner_id }) as Record<string, string>}
            columns={columns}
          />
        </>
      )}
    </div>
  );
}

const RUNNER_CONTROL_REASON_MAX_BYTES = 512;

interface RunnerControlDialogProps {
  target: ControlTarget;
  onClose: () => void;
  onDone: (text: string) => void;
}

/**
 * Drain/resume confirmation.
 *
 * `reason` is the entire mutation body, so the idempotency key lifecycle is
 * tied to it: one `X-Request-Id` is generated when the dialog opens, a retry
 * that leaves the body untouched reuses it (the server replays its receipt),
 * and editing the reason rotates it (a changed body must not hit 409 against
 * the earlier receipt). Submission is disabled while in flight.
 */
function RunnerControlDialog({ target, onClose, onDone }: RunnerControlDialogProps) {
  const { runner, action } = target;
  const [reason, setReason] = useState('');
  const [touched, setTouched] = useState(false);
  const [requestId, setRequestId] = useState(() => crypto.randomUUID());
  const [submitting, setSubmitting] = useState(false);
  const [failure, setFailure] = useState<ActionFailure>();

  const trimmed = reason.trim();
  const bytes = utf8Bytes(trimmed);
  const validation =
    trimmed === ''
      ? '请填写操作原因。'
      : bytes > RUNNER_CONTROL_REASON_MAX_BYTES
        ? `原因超过 ${RUNNER_CONTROL_REASON_MAX_BYTES} 字节（当前 ${bytes} 字节），请缩短。`
        : undefined;
  // Over-limit input is reported as it is typed; emptiness only after the
  // first submit attempt so an untouched field is not pre-flagged.
  const visibleError = bytes > RUNNER_CONTROL_REASON_MAX_BYTES || touched ? validation : undefined;
  const hintId = 'xflow-runner-control-reason-hint';
  const errorId = 'xflow-runner-control-reason-error';

  const submit = async () => {
    setTouched(true);
    if (validation || submitting) return;
    setSubmitting(true);
    setFailure(undefined);
    try {
      const call = action === 'drain' ? drainRunner : resumeRunner;
      await call(runner.runner_id, { reason: trimmed, requestId });
      onDone(
        action === 'drain'
          ? `已暂停 ${runner.runner_id} 的新任务准入`
          : `已恢复 ${runner.runner_id} 的新任务准入`,
      );
    } catch (reasonUnknown) {
      setFailure(runnerActionFailure(reasonUnknown, action));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open
      title={action === 'drain' ? '暂停 Runner 接单' : '恢复 Runner 接单'}
      okText={action === 'drain' ? '确认暂停' : '确认恢复'}
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: submitting }}
      cancelButtonProps={{ disabled: submitting }}
      closable={!submitting}
      keyboard={!submitting}
      mask={{ closable: !submitting }}
      getContainer={portalContainer}
      rootClassName="xflow-admin-modal-root"
      onOk={() => {
        void submit();
      }}
      onCancel={() => {
        if (!submitting) onClose();
      }}
    >
      <div className="flex flex-col gap-3" data-control-dialog={action}>
        <p className="xflow-admin-dialog-sub xflow-admin-runner-dialog-id">
          {runner.runner_id} · {RUNNER_STATE[runner.state].label} · 期望状态{' '}
          {desiredStateLabel(runner)}
        </p>
        <Alert
          type="info"
          showIcon
          title={action === 'drain' ? '暂停只关闭服务端的新任务准入' : '恢复只重新打开新任务准入'}
          description={
            action === 'drain'
              ? '不会终止 Runner 进程、不会取消正在执行的 handler，也不会回收 lease。返回成功仅表示服务端准入闸门已生效。'
              : '不会同步启动 Runner 进程；后续激活由正常调度收敛。已存在的 drain 回执保持不可变。'
          }
        />
        <div className="flex flex-col gap-1">
          <label className="xflow-admin-field-label" htmlFor="xflow-runner-control-reason">
            操作原因（必填）
          </label>
          <Input.TextArea
            id="xflow-runner-control-reason"
            rows={3}
            value={reason}
            disabled={submitting}
            placeholder="例如：节点维护，暂停接收新任务"
            aria-invalid={visibleError ? true : undefined}
            aria-describedby={visibleError ? `${hintId} ${errorId}` : hintId}
            onChange={(event) => {
              setReason(event.target.value);
              // The reason is the whole body; a changed body must carry a new
              // idempotency key instead of replaying the earlier receipt.
              setRequestId(crypto.randomUUID());
              setFailure(undefined);
            }}
          />
          <div className="xflow-admin-reason-meta">
            <span id={hintId} className="xflow-admin-hint">
              去除首尾空白后按 UTF-8 计，最多 {RUNNER_CONTROL_REASON_MAX_BYTES} 字节。
            </span>
            <span
              className="xflow-admin-reason-counter"
              data-over-limit={bytes > RUNNER_CONTROL_REASON_MAX_BYTES ? 'true' : 'false'}
            >
              {bytes}/{RUNNER_CONTROL_REASON_MAX_BYTES} 字节
            </span>
          </div>
          {visibleError ? (
            <p id={errorId} className="xflow-admin-reason-error" role="alert">
              {visibleError}
            </p>
          ) : null}
        </div>
        {failure ? (
          <Alert type="error" showIcon title={failure.title} description={failureDetail(failure)} />
        ) : null}
      </div>
    </Modal>
  );
}

interface RevokeIdentityDialogProps {
  target: RunnerListItem;
  onClose: () => void;
  onDone: (text: string) => void;
}

/** Destructive, platform-level revocation behind an explicit checkbox. */
function RevokeIdentityDialog({ target, onClose, onDone }: RevokeIdentityDialogProps) {
  const [confirmed, setConfirmed] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [failure, setFailure] = useState<ActionFailure>();

  const submit = async () => {
    if (!confirmed || submitting) return;
    setSubmitting(true);
    setFailure(undefined);
    try {
      await revokeRunnerIdentity(target.runner_id);
      onDone(`已吊销 ${target.runner_id} 的身份`);
    } catch (reason: unknown) {
      setFailure(runnerActionFailure(reason, 'revoke'));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open
      title="吊销 Runner 身份"
      okText="确认吊销"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ danger: true, disabled: !confirmed || submitting }}
      cancelButtonProps={{ disabled: submitting }}
      closable={!submitting}
      keyboard={!submitting}
      mask={{ closable: !submitting }}
      getContainer={portalContainer}
      rootClassName="xflow-admin-modal-root"
      onOk={() => {
        void submit();
      }}
      onCancel={() => {
        if (!submitting) onClose();
      }}
    >
      <div className="flex flex-col gap-3" data-revoke-dialog>
        <p className="xflow-admin-dialog-sub xflow-admin-runner-dialog-id">
          {target.runner_id} · {RUNNER_STATE[target.state].label} · 期望状态{' '}
          {desiredStateLabel(target)}
        </p>
        <Alert
          type="warning"
          showIcon
          title="平台级破坏性操作"
          description={
            <ul className="m-0 pl-4">
              <li>在 Runner 下一次认证调用时生效；不会立即中断已建立的连接或在途 handler。</li>
              <li>与池 token 撤销相互独立：吊销身份不会吊销池 token，反之亦然。</li>
              <li>这是平台级操作：吊销其他 namespace 或归属未知的已签发身份，需要平台运维授权。</li>
            </ul>
          }
        />
        <Checkbox
          checked={confirmed}
          disabled={submitting}
          onChange={(event) => {
            setConfirmed(event.target.checked);
            setFailure(undefined);
          }}
        >
          我确认吊销 <span className="xflow-admin-mono">{target.runner_id}</span> 的已签发身份
        </Checkbox>
        {failure ? (
          <Alert type="error" showIcon title={failure.title} description={failureDetail(failure)} />
        ) : null}
      </div>
    </Modal>
  );
}

/**
 * Runner 与池 (LS-02). Three independent management reads (`runner-pools`,
 * `leader`, `runners`) degrade separately, so a missing read scope never
 * blanks the page.
 *
 * The roster carries the runner-control writes: drain/resume (each requiring
 * its base scope plus `management.runner.control_global`) and identity revoke
 * (`management.runner.revoke_identity`). Scope gating here is display-only —
 * every call is independently authorized server-side — and a missing
 * `currentUser` projects the empty scope list rather than failing open.
 */
export default function RunnersPage() {
  const { message } = App.useApp();
  const { initialState } = useModel('@@initialState') as { initialState?: InitialState };
  const scopes = initialState?.currentUser?.scopes ?? [];
  const canDrain =
    scopes.includes('management.runner.drain') &&
    scopes.includes('management.runner.control_global');
  const canResume =
    scopes.includes('management.runner.resume') &&
    scopes.includes('management.runner.control_global');
  const canRevoke = scopes.includes('management.runner.revoke_identity');

  const [reload, setReload] = useState(0);
  const [controlTarget, setControlTarget] = useState<ControlTarget>();
  const [revokeTarget, setRevokeTarget] = useState<RunnerListItem>();

  const leader = useResource(
    fetchLeader,
    'management.leader.read',
    scopes.includes('management.leader.read'),
    reload,
  );
  const pools = useResource(
    fetchRunnerPools,
    'management.runner_pool.read',
    scopes.includes('management.runner_pool.read'),
    reload,
  );
  const runners = useResource(
    fetchRunners,
    'management.runner.list',
    scopes.includes('management.runner.list'),
    reload,
  );

  // AD-14: every 403 on the page becomes one alert that names the grants the
  // credential is missing. Scope names are not secrets; the repetition and the
  // implementation notes are what made the old alerts noise.
  const missingScopes = [
    ...new Set(
      [leader, pools, runners].flatMap((resource) =>
        resource.state === 'error' && resource.error.status === 403
          ? [resource.error.scope]
          : [],
      ),
    ),
  ];
  const refresh = (
    <Button onClick={() => setReload((value) => value + 1)}>
      刷新
    </Button>
  );

  const openControl = useCallback((runner: RunnerListItem, action: RunnerControlAction) => {
    setControlTarget({ runner, action });
  }, []);
  const openRevoke = useCallback((runner: RunnerListItem) => setRevokeTarget(runner), []);

  // A successful write refreshes the roster; the other two reads ride the same
  // counter so the page never shows a stale snapshot beside a fresh one.
  const completeWrite = useCallback(
    (text: string) => {
      message.success(text);
      setReload((value) => value + 1);
    },
    [message],
  );

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <PageHeader
        title="Runner 与池"
        description="Runner 状态、期望状态与可见池。暂停/恢复只控制服务端新任务准入，吊销身份是平台级操作；两者都需二次确认。"
        actions={refresh}
      />

      {missingScopes.length > 0 ? (
        <div data-panel="missing-scopes">
          <Alert
            type="warning"
            showIcon
            title="当前凭据缺少以下权限"
            // One line (banner ≤ 48px): the scopes are joined, and a list too
            // long for the row is clipped with an ellipsis while the full text
            // stays in the DOM and in the title.
            classNames={{ section: 'xflow-admin-alert-section--clip' }}
            description={
              <span className="xflow-admin-mono" title={missingScopes.join('、')}>
                {missingScopes.join('、')}
              </span>
            }
          />
        </div>
      ) : null}

      <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <LeaderCard resource={leader} />
        <PoolsCard resource={pools} />
      </div>
      <RunnersCard
        resource={runners}
        canDrain={canDrain}
        canResume={canResume}
        canRevoke={canRevoke}
        onControl={openControl}
        onRevoke={openRevoke}
      />

      {controlTarget ? (
        <RunnerControlDialog
          target={controlTarget}
          onClose={() => setControlTarget(undefined)}
          onDone={(text) => {
            setControlTarget(undefined);
            completeWrite(text);
          }}
        />
      ) : null}
      {revokeTarget ? (
        <RevokeIdentityDialog
          target={revokeTarget}
          onClose={() => setRevokeTarget(undefined)}
          onDone={(text) => {
            setRevokeTarget(undefined);
            completeWrite(text);
          }}
        />
      ) : null}
    </div>
  );
}
