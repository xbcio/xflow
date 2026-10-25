import {
  ArchiveIcon,
  BanIcon,
  BellIcon,
  BranchIcon,
  CheckIcon,
  ClockIcon,
  CodeIcon,
  DbIcon,
  FilterIcon,
  FlagIcon,
  GlobeIcon,
  GridIcon,
  HistoryIcon,
  HourglassIcon,
  InfoIcon,
  LinkIcon,
  PauseIcon,
  PinIcon,
  ServerIcon,
  ShieldIcon,
  SkipIcon,
  SpinIcon,
  StopIcon,
  TimeoutIcon,
  UploadIcon,
  UserIcon,
  WarnIcon,
  XMarkIcon,
} from '@xflow/icons';
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
  clock: <ClockIcon />,
  loading: <SpinIcon />,
  hourglass: <HourglassIcon />,
  pause: <PauseIcon />,
  check: <CheckIcon />,
  pin: <PinIcon />,
  warning: <WarnIcon />,
  close: <XMarkIcon />,
  skip: <SkipIcon />,
  stop: <StopIcon />,
  archive: <ArchiveIcon />,
  disabled: <BanIcon />,
  'clock-close': <TimeoutIcon />,
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
  link: <LinkIcon />,
  clock: <ClockIcon />,
  branch: <BranchIcon />,
  hourglass: <HourglassIcon />,
  globe: <GlobeIcon />,
  db: <DbIcon />,
  bell: <BellIcon />,
  code: <CodeIcon />,
  filter: <FilterIcon />,
  user: <UserIcon />,
  flag: <FlagIcon />,
  // Not a node type: the co-location frame label uses it (groups[], §6.2).
  server: <ServerIcon />,
  // Not node types either: the exception-state banners and the empty canvas
  // (§6.5.1) name their icon the same way the mockup's `#i-*` sprites do.
  // `grid` is also core's icon key for the `unknown` category.
  grid: <GridIcon />,
  warn: <WarnIcon />,
  x: <XMarkIcon />,
  shield: <ShieldIcon />,
  info: <InfoIcon />,
  check: <CheckIcon />,
  // 对话框与列表标题用的两个：回放 / 运行记录与「发布」。
  history: <HistoryIcon />,
  upload: <UploadIcon />,
};
