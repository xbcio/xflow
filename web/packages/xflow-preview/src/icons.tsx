import * as React from "react";
import {
  AppstoreOutlined,
  BranchesOutlined,
  CheckOutlined,
  ClockCircleOutlined,
  CloseOutlined,
  CodeOutlined,
  FieldTimeOutlined,
  FilterOutlined,
  FlagOutlined,
  GlobalOutlined,
  HourglassOutlined,
  InboxOutlined,
  LoadingOutlined,
  MinusCircleOutlined,
  PauseCircleOutlined,
  PushpinOutlined,
  StepForwardOutlined,
  StopOutlined,
  UserOutlined,
  WarningOutlined
} from "@ant-design/icons";
import type { CategoryIconKey, StatusIconKey } from "@xflow/core";

/**
 * Core's status and category tables carry icon *keys* (core has no React). These
 * two maps are the preview's half of that contract: exhaustive over the key
 * unions, so a key added in core fails the typecheck until it has a glyph here.
 * Mirrors the glyphs of the admin `/ui-preview` page (`meta.tsx`).
 */
export const statusGlyph: Record<StatusIconKey, React.ReactNode> = {
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
  "clock-close": <FieldTimeOutlined />
};

export const categoryGlyph: Record<CategoryIconKey, React.ReactNode> = {
  flag: <FlagOutlined />,
  branch: <BranchesOutlined />,
  globe: <GlobalOutlined />,
  code: <CodeOutlined />,
  filter: <FilterOutlined />,
  user: <UserOutlined />,
  grid: <AppstoreOutlined />
};
