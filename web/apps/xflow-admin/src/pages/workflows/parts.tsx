import {
  CheckOutlined,
  ClockCircleOutlined,
  CloseOutlined,
  FieldTimeOutlined,
  HourglassOutlined,
  InboxOutlined,
  LoadingOutlined,
  MinusCircleOutlined,
  PauseCircleOutlined,
  PushpinOutlined,
  StepForwardOutlined,
  StopOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { workflowStatusMeta, type ExecutionStatusKey, type StatusIconKey } from '@xflow/core';
import type { ReactNode } from 'react';

/**
 * StatusIconKey → AntD glyph. Core carries string keys (it stays framework
 * free); every admin chip, and later the execution list, maps them here so a
 * state can only be drawn one way.
 */
const statusIcon: Record<StatusIconKey, ReactNode> = {
  clock: <ClockCircleOutlined />,
  loading: <LoadingOutlined />,
  hourglass: <HourglassOutlined />,
  pause: <PauseCircleOutlined />,
  check: <CheckOutlined />,
  pin: <PushpinOutlined />,
  warning: <WarningOutlined />,
  close: <CloseOutlined />,
  skip: <StepForwardOutlined />,
  stop: <StopOutlined />,
  archive: <InboxOutlined />,
  disabled: <MinusCircleOutlined />,
  'clock-close': <FieldTimeOutlined />,
};

/** §4 status pill: tone colour + glyph + the label from `workflowStatusMeta`. */
export function StatusChip({ status }: { status: ExecutionStatusKey }) {
  const meta = workflowStatusMeta[status];
  return (
    <span className="xflow-admin-chip" data-tone={meta.tone} data-status={status}>
      <span className="xflow-admin-chip-icon" aria-hidden="true">
        {statusIcon[meta.icon]}
      </span>
      <span>{meta.label}</span>
    </span>
  );
}

/** 40px panel title bar shared by the workflow table and the recent-run card (§6.0). */
export function PanelTitle({
  icon,
  title,
  count,
}: {
  icon: ReactNode;
  title: ReactNode;
  count?: number;
}) {
  return (
    <div className="xflow-admin-panel-title">
      <span className="xflow-admin-panel-icon" aria-hidden="true">
        {icon}
      </span>
      <h3 className="xflow-admin-panel-heading">{title}</h3>
      {count === undefined ? null : <span className="xflow-admin-panel-count">{count}</span>}
    </div>
  );
}

/**
 * §6.7 marker for a column whose backend field does not exist yet. Placement
 * follows the live baseline (`Todo`'s default `hover`): the badge is out of
 * flow, right-aligned above the wrapped title, so a marked header keeps the
 * same row height as an unmarked one. The native `title` carries the detail.
 */
export function NewFieldMark({ title, children }: { title: string; children: ReactNode }) {
  return (
    <span className="xflow-admin-todo-wrap">
      {children}
      <span className="xflow-admin-todo" title={title}>
        需新字段
      </span>
    </span>
  );
}
