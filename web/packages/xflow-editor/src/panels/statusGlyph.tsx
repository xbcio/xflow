import {
  ArchiveIcon,
  BanIcon,
  CheckIcon,
  ClockIcon,
  HourglassIcon,
  PauseIcon,
  PinIcon,
  SkipIcon,
  SpinIcon,
  StopIcon,
  TimeoutIcon,
  WarnIcon,
  XMarkIcon
} from "@xflow/icons";
import type { StatusIconKey } from "@xflow/core";
import type { ReactNode } from "react";

/**
 * Core's status tables carry icon *keys*; each renderer owns the glyph. The
 * Inspector and the run bar both draw statuses, so the map lives here rather
 * than in either panel. Exhaustive over `StatusIconKey`: a new key in core
 * fails the typecheck until it has a glyph.
 */
export const statusGlyph: Record<StatusIconKey, ReactNode> = {
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
  "clock-close": <TimeoutIcon />
};
