import * as React from "react";
import {
  ArchiveIcon,
  BanIcon,
  BranchIcon,
  CheckIcon,
  ClockIcon,
  CodeIcon,
  FilterIcon,
  FlagIcon,
  GlobeIcon,
  GridIcon,
  HourglassIcon,
  PauseIcon,
  PinIcon,
  SkipIcon,
  SpinIcon,
  StopIcon,
  TimeoutIcon,
  UserIcon,
  WarnIcon,
  XMarkIcon
} from "@xflow/icons";
import type { CategoryIconKey, StatusIconKey } from "@xflow/core";

/**
 * Core's status and category tables carry icon *keys* (core has no React). These
 * two maps are the preview's half of that contract: exhaustive over the key
 * unions, so a key added in core fails the typecheck until it has a glyph here.
 * Mirrors the glyphs of the admin `/ui-preview` page (`meta.tsx`).
 */
export const statusGlyph: Record<StatusIconKey, React.ReactNode> = {
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

export const categoryGlyph: Record<CategoryIconKey, React.ReactNode> = {
  flag: <FlagIcon />,
  branch: <BranchIcon />,
  globe: <GlobeIcon />,
  code: <CodeIcon />,
  filter: <FilterIcon />,
  user: <UserIcon />,
  grid: <GridIcon />
};
