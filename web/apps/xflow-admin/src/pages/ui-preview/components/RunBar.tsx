import {
  AimOutlined,
  CaretRightOutlined,
  CloseOutlined,
  PauseOutlined,
  ReloadOutlined,
  StopOutlined,
} from '@ant-design/icons';
import { Button, Popconfirm, Tooltip, Typography } from 'antd';
import type { ReactNode } from 'react';

import type { RunVariant } from '../data';
import { workflowStatusMeta, type ExecutionOp } from '../meta';
import { cx } from './cx';
import { Float, StatusChip, VerticalSep } from './primitives';
import { Todo } from './Todo';

const opMeta: Record<ExecutionOp, { label: string; icon: ReactNode; danger?: boolean }> = {
  pause: { label: '暂停', icon: <PauseOutlined /> },
  resume: { label: '继续', icon: <CaretRightOutlined /> },
  cancel: { label: '取消', icon: <StopOutlined />, danger: true },
  rerun: { label: '重跑…', icon: <ReloadOutlined /> },
};

/**
 * Canvas run bar (§6.3): the one place a running execution's state appears.
 *
 * The operations are generated from `workflowStatusMeta[status].ops` — the bar
 * never decides for itself which buttons exist. Pause / resume / cancel / rerun
 * are marked 待接入 because the client has no wrapper for those endpoints yet
 * (§6.7); the markers are absolute, so a marked bar keeps the 34px float height.
 */
export function RunBar({
  variant,
  onLocate,
  className,
}: {
  variant: RunVariant;
  onLocate?: (target: string) => void;
  className?: string;
}) {
  const status = workflowStatusMeta[variant.status];
  const label = variant.queuePosition
    ? `${status.label} · 第 ${variant.queuePosition} 位`
    : status.label;
  const hasOps = status.ops.length > 0 || status.terminal;

  const renderOp = (op: ExecutionOp) => {
    const meta = opMeta[op];
    const button = (
      <Button size="small" danger={meta.danger} icon={meta.icon}>
        {meta.label}
      </Button>
    );
    const control =
      op === 'cancel' ? (
        <Popconfirm
          title={`取消执行 ${variant.execId}？`}
          description="在途节点提交后停止，已完成的结果保留。"
          okText="取消执行"
          cancelText="返回"
          okButtonProps={{ danger: true }}
        >
          {button}
        </Popconfirm>
      ) : (
        button
      );
    return (
      <Todo key={op} label="待接入">
        {control}
      </Todo>
    );
  };

  return (
    <Float className={cx('xflow-pv-runbar', className)} role="status">
      <StatusChip tone={status.tone} icon={status.icon} label={label}>
        {variant.elapsed ? <span className="xflow-pv-runbar-timer">· {variant.elapsed}</span> : null}
      </StatusChip>

      {/* §6.3 item 8: the execution id, mono and copyable. */}
      <Typography.Text
        className="xflow-pv-runbar-id"
        copyable={{ text: variant.execId, tooltips: ['复制执行 id', '已复制'] }}
      >
        {variant.execId}
      </Typography.Text>

      <span className="flex items-center gap-1">
        {variant.counts?.suspend ? (
          <Tooltip title="定位到节点">
            <Button
              size="small"
              type="text"
              icon={<AimOutlined />}
              className="xflow-pv-count"
              data-tone="suspend"
              onClick={() => onLocate?.('manager_approval')}
            >
              {variant.counts.suspend} 等待审批
            </Button>
          </Tooltip>
        ) : null}
        {variant.counts?.danger ? (
          <Tooltip title="定位到节点">
            <Button
              size="small"
              type="text"
              icon={<AimOutlined />}
              className="xflow-pv-count"
              data-tone="danger"
              onClick={() => onLocate?.('enrich_score')}
            >
              {variant.counts.danger} 失败
            </Button>
          </Tooltip>
        ) : null}
      </span>

      {hasOps ? (
        <>
          <VerticalSep />
          <span className="xflow-pv-runbar-ops flex items-center gap-1">
            {status.terminal ? (
              <Button type="link" size="small" className="xflow-pv-runbar-link">
                查看运行记录
              </Button>
            ) : null}
            {status.ops.map(renderOp)}
            {status.terminal ? (
              <Tooltip title="关闭运行条">
                <Button
                  type="text"
                  size="small"
                  icon={<CloseOutlined />}
                  aria-label="关闭运行条"
                  className="xflow-pv-tool"
                />
              </Tooltip>
            ) : null}
          </span>
        </>
      ) : null}

      {variant.dismissHint ? (
        <span className="xflow-pv-runbar-hint">{variant.dismissHint}</span>
      ) : null}
    </Float>
  );
}
