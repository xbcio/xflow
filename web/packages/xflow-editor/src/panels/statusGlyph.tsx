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
  WarningOutlined
} from "@ant-design/icons";
import type { StatusIconKey } from "@xflow/core";
import type { ReactNode } from "react";

/**
 * Core's status tables carry icon *keys*; each renderer owns the glyph. The
 * Inspector and the run bar both draw statuses, so the map lives here rather
 * than in either panel. Exhaustive over `StatusIconKey`: a new key in core
 * fails the typecheck until it has a glyph.
 */
export const statusGlyph: Record<StatusIconKey, ReactNode> = {
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
