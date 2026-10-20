import {
  AppstoreOutlined,
  BellOutlined,
  BranchesOutlined,
  CheckOutlined,
  ClockCircleOutlined,
  CloseOutlined,
  CloudServerOutlined,
  CodeOutlined,
  DatabaseOutlined,
  FieldTimeOutlined,
  FilterOutlined,
  FlagOutlined,
  GlobalOutlined,
  HistoryOutlined,
  HourglassOutlined,
  InboxOutlined,
  InfoCircleOutlined,
  LinkOutlined,
  LoadingOutlined,
  MinusCircleOutlined,
  PauseCircleOutlined,
  PushpinOutlined,
  SafetyCertificateOutlined,
  StepForwardOutlined,
  StopOutlined,
  UploadOutlined,
  UserOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import {
  nodeStatusMeta as coreNodeStatusMeta,
  workflowStatusMeta as coreWorkflowStatusMeta,
  type EdgeVariant as CoreEdgeVariant,
  type ExecutionOp as CoreExecutionOp,
  type ExecutionStatusKey,
  type NodeCategory,
  type NodeStatusKey,
  type StatusIconKey,
  type StatusMeta as CoreStatusMeta,
  type Tone as CoreTone,
  type WorkflowStatusMeta as CoreWorkflowStatusMeta,
} from '@xflow/core';
import type { ReactNode } from 'react';

/**
 * The status and category tables live in @xflow/core (spec §4, §5); this file
 * keeps only what core cannot hold: the map from a table's string icon key to an
 * AntD glyph. Every chip, card border, run bar and table in the preview reads
 * the tables below, so a state can only look one way.
 */
export type Tone = CoreTone;
export type NodeStatus = NodeStatusKey;
export type ExecutionStatus = ExecutionStatusKey;
export type ExecutionOp = CoreExecutionOp;
export type EdgeVariant = CoreEdgeVariant;
export type Category = NodeCategory;

export { edgeVariant, executionOpLabel } from '@xflow/core';

/** Exhaustive over `StatusIconKey`: a new key in core fails the typecheck until it has a glyph. */
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

export interface StatusMeta extends Omit<CoreStatusMeta, 'icon'> {
  icon: ReactNode;
}

export interface WorkflowStatusMeta extends Omit<CoreWorkflowStatusMeta, 'icon'> {
  icon: ReactNode;
}

function withGlyph<K extends string, T extends { icon: StatusIconKey }>(
  table: Record<K, T>,
): Record<K, Omit<T, 'icon'> & { icon: ReactNode }> {
  return Object.fromEntries(
    Object.entries<T>(table).map(([key, meta]) => [key, { ...meta, icon: statusIcon[meta.icon] }]),
  ) as unknown as Record<K, Omit<T, 'icon'> & { icon: ReactNode }>;
}

export const nodeStatusMeta: Record<NodeStatus, StatusMeta> = withGlyph(coreNodeStatusMeta);

export const workflowStatusMeta: Record<ExecutionStatus, WorkflowStatusMeta> = withGlyph(coreWorkflowStatusMeta);

export const typeIcon: Record<string, ReactNode> = {
  link: <LinkOutlined />,
  clock: <ClockCircleOutlined />,
  branch: <BranchesOutlined />,
  hourglass: <HourglassOutlined />,
  globe: <GlobalOutlined />,
  db: <DatabaseOutlined />,
  bell: <BellOutlined />,
  code: <CodeOutlined />,
  filter: <FilterOutlined />,
  user: <UserOutlined />,
  flag: <FlagOutlined />,
  // Not a node type: the co-location frame label uses it (groups[], §6.2).
  server: <CloudServerOutlined />,
  // Not node types either: the exception-state banners and the empty canvas
  // (§6.5.1) name their icon the same way the mockup's `#i-*` sprites do.
  // `grid` is also core's icon key for the `unknown` category.
  grid: <AppstoreOutlined />,
  warn: <WarningOutlined />,
  x: <CloseOutlined />,
  shield: <SafetyCertificateOutlined />,
  info: <InfoCircleOutlined />,
  check: <CheckOutlined />,
  // 对话框与列表标题用的两个：回放 / 运行记录与「发布」。
  history: <HistoryOutlined />,
  upload: <UploadOutlined />,
};
