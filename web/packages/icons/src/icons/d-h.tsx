import * as React from "react";
import { XIconSvg, type XIconProps } from "../base";

export function DbIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="db" {...props}>
      <ellipse cx="12" cy="6" rx="7" ry="2.5" />
      <path d="M5 6v12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5V6M5 12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5" />
    </XIconSvg>
  );
}

export function DockDownIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="dock-down" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <path d="M3.5 14h17M9.5 9 12 11.5 14.5 9" />
    </XIconSvg>
  );
}

export function DockMaxIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="dock-max" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <path d="M12 16v-6M9.5 12.5 12 10l2.5 2.5" />
    </XIconSvg>
  );
}

export function DockRestoreIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="dock-restore" {...props}>
      <rect height="14" rx="2" width="17" x="3.5" y="5" />
      <path d="M3.5 12h17M12 8V5.5M10 7l2 2 2-2" />
    </XIconSvg>
  );
}

export function DownIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="down" {...props}>
      <path d="M6 9l6 6 6-6" />
    </XIconSvg>
  );
}

/** Newly drawn for round 2 (no sprite symbol): a pencil for rename / edit. */
export function EditIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="edit" {...props}>
      <path d="M4 20l4.6-1.2L20 7.4a2.1 2.1 0 0 0-3-3L5.6 15.8 4 20z" />
      <path d="M14.5 5.5l4 4" />
    </XIconSvg>
  );
}

export function ExpandAllIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="expand-all" {...props}>
      <path d="M7 9l5-5 5 5M7 15l5 5 5-5" />
    </XIconSvg>
  );
}

/** Newly drawn for round 2 (no sprite symbol): an eye for preview / view. */
export function EyeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="eye" {...props}>
      <path d="M2.5 12s3.5-6.5 9.5-6.5S21.5 12 21.5 12s-3.5 6.5-9.5 6.5S2.5 12 2.5 12z" />
      <circle cx="12" cy="12" r="2.75" />
    </XIconSvg>
  );
}

export function FilterIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="filter" {...props}>
      <path d="M4 5h16l-6 7v6l-4 2v-8L4 5z" />
    </XIconSvg>
  );
}

export function FitIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="fit" {...props}>
      <path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5" />
    </XIconSvg>
  );
}

export function FlagIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="flag" {...props}>
      <path d="M6 21V4M6 4h11l-2 4 2 4H6" />
    </XIconSvg>
  );
}

/** Newly drawn for round 2 (no sprite symbol): a brush for copy-format. */
export function FormatPainterIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="format-painter" {...props}>
      <path d="M19.5 4.5L13 11" />
      <path d="M14 9l1.5 1.5-6 6H6v-3.5L14 9z" />
    </XIconSvg>
  );
}

export function GearIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="gear" {...props}>
      <circle cx="12" cy="12" r="3" />
      <path d="M12 3v2.5M12 18.5V21M3 12h2.5M18.5 12H21M5.6 5.6l1.8 1.8M16.6 16.6l1.8 1.8M5.6 18.4l1.8-1.8M16.6 7.4l1.8-1.8" />
    </XIconSvg>
  );
}

export function GlobeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="globe" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M3.5 12h17M12 3.5c2.5 2.5 3.5 5.5 3.5 8.5s-1 6-3.5 8.5c-2.5-2.5-3.5-5.5-3.5-8.5s1-6 3.5-8.5" />
    </XIconSvg>
  );
}

export function GridIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="grid" {...props}>
      <rect height="6.5" rx="1" width="6.5" x="4" y="4" />
      <rect height="6.5" rx="1" width="6.5" x="13.5" y="4" />
      <rect height="6.5" rx="1" width="6.5" x="4" y="13.5" />
      <rect height="6.5" rx="1" width="6.5" x="13.5" y="13.5" />
    </XIconSvg>
  );
}

export function HandIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="hand" {...props}>
      <path d="M8 12V6a1.5 1.5 0 0 1 3 0v5M11 11V4.5a1.5 1.5 0 0 1 3 0V11M14 11V6a1.5 1.5 0 0 1 3 0v7c0 4-2.5 7-6 7-2.5 0-4-1.5-5.5-4L4 13a1.5 1.5 0 0 1 2.5-1.5L8 13" />
    </XIconSvg>
  );
}

export function HelpIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="help" {...props}>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M9.5 9.5a2.5 2.5 0 1 1 3.5 2.3c-.6.3-1 .9-1 1.6v.6M12 17v.3" />
    </XIconSvg>
  );
}

export function HistoryIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="history" {...props}>
      <path d="M4 12a8 8 0 1 0 2.4-5.7M4 4v4h4M12 8v4l3 2" />
    </XIconSvg>
  );
}

export function HomeIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="home" {...props}>
      <path d="M4 11l8-7 8 7v9h-5v-6H9v6H4v-9z" />
    </XIconSvg>
  );
}

export function HourglassIcon(props: XIconProps): React.ReactElement {
  return (
    <XIconSvg name="hourglass" {...props}>
      <path d="M7 3.5h10M7 20.5h10M8 3.5c0 4 8 5 8 8.5s-8 4.5-8 8.5M16 3.5c0 4-8 5-8 8.5s8 4.5 8 8.5" />
    </XIconSvg>
  );
}
